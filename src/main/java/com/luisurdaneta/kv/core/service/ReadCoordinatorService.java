package com.luisurdaneta.kv.core.service;

import com.luisurdaneta.kv.core.model.VersionedValue;
import com.luisurdaneta.kv.core.ports.PeerClient;
import com.luisurdaneta.kv.core.ring.ConsistentHashRing;
import com.luisurdaneta.kv.http.Node;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public final class ReadCoordinatorService {
    private final String localNodeId;
    private final ConsistentHashRing ring;
    private final ReplicaKvService replicaLocal;
    private final PeerClient peers;

    private final int rf;    // replication factor
    private final int r;
    private final Duration perRequestTimeout;
    private final Duration overallTimeout;

    public ReadCoordinatorService(
            String localNodeId,
            ConsistentHashRing ring,
            ReplicaKvService replicaLocal,
            PeerClient peers,
            int rf,
            int r,
            Duration perRequestTimeout,
            Duration overallTimeout
    ) {
        this.localNodeId = localNodeId;
        this.ring = ring;
        this.replicaLocal = replicaLocal;
        this.peers = peers;
        this.rf = rf;
        this.r = r;
        this.perRequestTimeout = perRequestTimeout;
        this.overallTimeout = overallTimeout;
    }

    public ReadResult get(String key) throws Exception {
        List<Node> replicas = ring.replicasForKey(key, rf);

        try (ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor()) {
            CompletionService<ReadAck> cs = new ExecutorCompletionService<>(exec);
            List<Future<ReadAck>> futures = new ArrayList<>(replicas.size());

            for (Node peer : replicas) {
                futures.add(cs.submit(() -> readOne(peer, key)));
            }

            int acks = 0;
            List<ReadAck> responses = new ArrayList<>(r);
            long deadline = System.nanoTime() + overallTimeout.toNanos();

            for (int i = 0; i < replicas.size(); i++) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) break;

                Future<ReadAck> f = cs.poll(remaining, TimeUnit.NANOSECONDS);
                if (f == null) break;

                ReadAck ack;
                try {
                    ack = f.get();
                } catch (ExecutionException ee) {
                    continue;
                }

                if (!ack.ack) continue;

                acks++;
                responses.add(ack);

                if (acks >= r) {
                    for (Future<ReadAck> fut : futures) fut.cancel(true);
                    break;
                }
            }

            if (acks < r) {
                for (Future<ReadAck> fut : futures) fut.cancel(true);
                return ReadResult.fail(key, acks, r, replicas);
            }

            // lww
            VersionedValue winner = resolveWinner(responses);

            // read repair
            if (winner != null) {
                for (ReadAck ra : responses) {
                    if (isStale(ra.recordOrNull, winner)) {
                        Node peer = ra.peer;
                        exec.submit(() -> {
                            try { repairOne(peer, key, winner); } catch (Exception ignored) {}
                        });
                    }
                }
            }

            // hide tombstones to public
            if (winner == null || winner.tombstone) {
                return ReadResult.notFound(key, acks, r, replicas, winner);
            }
            return ReadResult.found(key, acks, r, replicas, winner);
        }
    }

    private ReadAck readOne(Node peer, String key) throws Exception {
        if (peer.id().equals(localNodeId)) {
            var r = replicaLocal.getReplica(key);
            if (r instanceof ReplicaKvService.GetReplicaResult.NotFound) {
                return new ReadAck(peer, true, null); // ack=true, record=null
            }
            var f = (ReplicaKvService.GetReplicaResult.Found) r;
            return new ReadAck(peer, true, f.record());
        }

        PeerClient.ReplicaGetAck resp = peers.getReplica(peer, key, perRequestTimeout);
        if (!resp.ack()) return new ReadAck(peer, false, null);
        return new ReadAck(peer, true, resp.recordOrNull());
    }

    private void repairOne(Node peer, String key, VersionedValue winner) throws Exception {
        if (peer.id().equals(localNodeId)) {
            replicaLocal.putReplica(key, winner);
        } else {
            peers.putReplica(peer, key, winner, perRequestTimeout);
        }
    }

    private static VersionedValue resolveWinner(List<ReadAck> acks) {
        VersionedValue best = null;
        for (ReadAck a : acks) {
            VersionedValue cur = a.recordOrNull;
            if (cur == null) continue;
            if (best == null || cur.isNewerThan(best)) best = cur;
        }
        return best;
    }

    private static boolean isStale(VersionedValue have, VersionedValue winner) {
        if (have == null) return true;
        // stale if winner strictly newer than what this node returned
        return winner.isNewerThan(have);
    }

    private record ReadAck(Node peer, boolean ack, VersionedValue recordOrNull) {}

    public record ReadResult(
            boolean ok,
            boolean found,
            String key,
            int acks,
            int required,
            long ts,
            String nodeId,
            byte[] payload,
            boolean tombstone
    ) {
        static ReadResult fail(String key, int acks, int required, List<Node> replicas) {
            return new ReadResult(false, false, key, acks, required, 0L, null, null, false);
        }
        static ReadResult notFound(String key, int acks, int required, List<Node> replicas, VersionedValue winner) {
            boolean tomb = winner != null && winner.tombstone;
            long ts = winner != null ? winner.ts : 0L;
            String nodeId = winner != null ? winner.nodeId : null;
            return new ReadResult(true, false, key, acks, required, ts, nodeId, null, tomb);
        }
        static ReadResult found(String key, int acks, int required, List<Node> replicas, VersionedValue winner) {
            return new ReadResult(true, true, key, acks, required, winner.ts, winner.nodeId, winner.payload, winner.tombstone);
        }
    }
}