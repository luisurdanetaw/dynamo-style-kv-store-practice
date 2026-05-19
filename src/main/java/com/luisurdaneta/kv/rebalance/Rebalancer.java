package com.luisurdaneta.kv.rebalance;

import com.luisurdaneta.kv.core.model.VersionedValue;
import com.luisurdaneta.kv.core.ports.KvStore;
import com.luisurdaneta.kv.core.ports.PeerClient;
import com.luisurdaneta.kv.core.ring.ConsistentHashRing;
import com.luisurdaneta.kv.core.service.ReplicaKvService;
import com.luisurdaneta.kv.http.Node;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Rebalancer — reacts to ring changes and moves data accordingly.
 *
 * Two phases per ring change:
 *
 *   Phase 1 — Pull gained arcs:
 *     For each arc where this node is now a replica but wasn't before, pull all
 *     key/value pairs from the old primary using paginated range requests.
 *     Each page is written via putReplica (LWW-safe, idempotent) before requesting
 *     the next page so memory stays bounded.
 *
 *   Phase 2 — Push and drop stale keys:
 *     Scan local store. For each key no longer covered by this node in the new ring,
 *     push the key to all new owners via the existing putReplica endpoint, then
 *     physically delete it locally ONLY after every push succeeds.
 *     Ordering guarantee: confirm before delete — data is never lost.
 *
 * Restart strategy (oldRing == null):
 *   RingManager sets previousRing = null until the first watch sync. When
 *   onRingChanged(null, watchRing) fires, gained arcs = ALL arcs where this node is
 *   a replica, triggering a full re-pull. LWW absorbs any data already present.
 *   No progress tracking is needed because re-applying an older value is harmless.
 */
public final class Rebalancer {

    private static final System.Logger LOG = System.getLogger(Rebalancer.class.getName());
    private static final int  CLEANUP_BATCH_SIZE = 100;
    private static final Duration TRANSFER_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration PUSH_TIMEOUT     = Duration.ofSeconds(5);

    private final String            localNodeId;
    private final int               rf;
    private final ReplicaKvService  replicaService;
    private final PeerClient        peerClient;
    private final KvStore           kvStore;
    private final ExecutorService   exec;

    public Rebalancer(String localNodeId, int rf,
                      ReplicaKvService replicaService,
                      PeerClient peerClient,
                      KvStore kvStore) {
        this.localNodeId   = localNodeId;
        this.rf            = rf;
        this.replicaService = replicaService;
        this.peerClient    = peerClient;
        this.kvStore       = kvStore;
        this.exec = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "rebalancer");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Enqueues a rebalance pass in the background. Non-blocking.
     * oldRing == null signals a node restart — re-pull everything.
     */
    public void onRingChanged(ConsistentHashRing oldRing,
                              ConsistentHashRing newRing,
                              long membershipVersion) {
        exec.submit(() -> {
            try {
                LOG.log(System.Logger.Level.INFO,
                        "Rebalance start: membershipVersion=" + membershipVersion
                        + (oldRing == null ? " (restart/first-sync)" : ""));
                pullGainedArcs(oldRing, newRing, membershipVersion);
                cleanupLostKeys(oldRing, newRing, membershipVersion);
                LOG.log(System.Logger.Level.INFO,
                        "Rebalance done: membershipVersion=" + membershipVersion);
            } catch (Exception e) {
                LOG.log(System.Logger.Level.ERROR, "Rebalance failed", e);
            }
        });
    }

    // ── Phase 1: pull gained arcs ────────────────────────────────────────────

    private void pullGainedArcs(ConsistentHashRing oldRing,
                                 ConsistentHashRing newRing,
                                 long membershipVersion) throws Exception {
        List<TokenRange> gained = computeGainedArcs(oldRing, newRing);
        for (TokenRange arc : gained) {
            Node source = findSource(oldRing, newRing, arc);
            if (source == null) {
                LOG.log(System.Logger.Level.WARNING,
                        "No pull source for arc (" + arc.start() + "," + arc.end() + "); skipping");
                continue;
            }
            pullArc(source, arc, membershipVersion);
        }
    }

    private List<TokenRange> computeGainedArcs(ConsistentHashRing oldRing,
                                                ConsistentHashRing newRing) {
        List<TokenRange> newArcs = newRing.replicaArcs(localNodeId, rf);

        if (oldRing == null) {
            // Restart: treat all current replica arcs as gained so we re-pull everything.
            return newArcs;
        }

        List<TokenRange> gained = new ArrayList<>();
        for (TokenRange arc : newArcs) {
            // Sample the arc at its right endpoint: if this node was already among the
            // RF replicas for that position in the old ring, the arc is not new.
            List<Node> oldReplicas = oldRing.replicasForHash(arc.end(), rf);
            boolean wasReplica = oldReplicas.stream()
                    .anyMatch(n -> n.id().equals(localNodeId));
            if (!wasReplica) gained.add(arc);
        }
        return gained;
    }

    /**
     * Finds the node to pull a gained arc from.
     * On restart (oldRing==null) pick any peer in the new ring for the arc.
     * On a real change, pick the old primary (first replica that isn't us).
     */
    private Node findSource(ConsistentHashRing oldRing,
                             ConsistentHashRing newRing,
                             TokenRange arc) {
        List<Node> candidates = (oldRing != null)
                ? oldRing.replicasForHash(arc.end(), rf)
                : newRing.replicasForHash(arc.end(), rf);
        return candidates.stream()
                .filter(n -> !n.id().equals(localNodeId))
                .findFirst()
                .orElse(null);
    }

    private void pullArc(Node source, TokenRange arc, long membershipVersion) throws Exception {
        String cursor = null;
        do {
            PeerClient.TransferPage page = peerClient.pullRangePage(
                    source, arc.start(), arc.end(),
                    cursor, 500, membershipVersion, TRANSFER_TIMEOUT);

            for (PeerClient.TransferEntry e : page.entries()) {
                replicaService.putReplica(e.key(), e.value()); // LWW-safe
            }
            cursor = page.nextCursor();
        } while (cursor != null);
    }

    // ── Phase 2: push and drop stale keys ────────────────────────────────────

    private void cleanupLostKeys(ConsistentHashRing oldRing,
                                  ConsistentHashRing newRing,
                                  long membershipVersion) throws Exception {
        // On restart there is nothing stale yet — we just re-pulled everything.
        if (oldRing == null) return;

        String cursor = null;
        boolean more  = true;

        while (more) {
            List<Map.Entry<String, VersionedValue>> batch = new ArrayList<>(CLEANUP_BATCH_SIZE);
            String[] lastVisited = {null};

            final String cur = cursor;
            kvStore.scan(cur, (key, vv) -> {
                lastVisited[0] = key;
                List<Node> newReplicas = newRing.replicasForKey(key, rf);
                boolean stillReplica = newReplicas.stream()
                        .anyMatch(n -> n.id().equals(localNodeId));
                if (!stillReplica) batch.add(Map.entry(key, vv));
                return batch.size() < CLEANUP_BATCH_SIZE;
            });

            // Batch is full → there may be more keys; advance cursor.
            // Batch is not full → we drained the remaining keyspace.
            more = (batch.size() >= CLEANUP_BATCH_SIZE);
            cursor = more ? lastVisited[0] : null;

            for (Map.Entry<String, VersionedValue> e : batch) {
                List<Node> newOwners = newRing.replicasForKey(e.getKey(), rf);
                pushAndDrop(e.getKey(), e.getValue(), newOwners);
            }
        }
    }

    /**
     * Push key to ALL new owners, then drop locally — only if every push succeeds.
     * Ordering guarantee: confirm before delete.
     */
    private void pushAndDrop(String key, VersionedValue vv, List<Node> newOwners) {
        boolean allOk = true;
        for (Node owner : newOwners) {
            if (owner.id().equals(localNodeId)) continue;
            try {
                PeerClient.ReplicaPutAck ack = peerClient.putReplica(owner, key, vv, PUSH_TIMEOUT);
                if (!ack.ok()) {
                    allOk = false;
                    LOG.log(System.Logger.Level.WARNING,
                            "Push to " + owner.id() + " not ok for key=" + key + "; retaining locally");
                }
            } catch (Exception e) {
                allOk = false;
                LOG.log(System.Logger.Level.WARNING,
                        "Push to " + owner.id() + " failed for key=" + key + "; retaining locally");
            }
        }

        if (allOk) {
            try {
                kvStore.drop(key);
            } catch (Exception e) {
                LOG.log(System.Logger.Level.WARNING, "drop(" + key + ") failed: " + e.getMessage());
            }
        }
    }

    public void shutdown() {
        exec.shutdown();
        try {
            exec.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
