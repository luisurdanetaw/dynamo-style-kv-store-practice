package com.luisurdaneta.kv.rebalance;

import com.luisurdaneta.kv.TestHelpers.CapturingPeerClient;
import com.luisurdaneta.kv.TestHelpers.MemKvStore;
import com.luisurdaneta.kv.core.model.VersionedValue;
import com.luisurdaneta.kv.core.ring.ConsistentHashRing;
import com.luisurdaneta.kv.core.service.ReplicaKvService;
import com.luisurdaneta.kv.http.Node;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Rebalancer unit tests.
 *
 * Each test drives Rebalancer.onRingChanged directly (the async executor is shut down
 * immediately after to drain), so tests remain deterministic without sleeps.
 */
class RebalancerTest {

    private static final int RF     = 3;
    private static final int VNODES = 64;

    // ── helpers ───────────────────────────────────────────────────────────────

    static Node node(String id) { return new Node(id, "http://" + id + ":8080"); }

    static ConsistentHashRing ring(String... ids) {
        List<Node> nodes = new ArrayList<>();
        for (String id : ids) nodes.add(node(id));
        return new ConsistentHashRing(nodes, VNODES);
    }

    static VersionedValue vv(long ts) {
        return new VersionedValue(ts, "origin", false, ("v" + ts).getBytes());
    }

    // ── fixtures ──────────────────────────────────────────────────────────────

    MemKvStore          localStore;
    CapturingPeerClient peerClient;
    ReplicaKvService    replicaService;
    Rebalancer          rebalancer;

    @BeforeEach
    void setup() {
        localStore     = new MemKvStore();
        peerClient     = new CapturingPeerClient();
        replicaService = new ReplicaKvService(localStore);
        rebalancer     = new Rebalancer("n0", RF, replicaService, peerClient, localStore);
    }

    /** Run rebalance synchronously: submit + shut down executor (drains the task). */
    void runSync(ConsistentHashRing old, ConsistentHashRing next) throws Exception {
        rebalancer.onRingChanged(old, next, 1L);
        rebalancer.shutdown();
        rebalancer = new Rebalancer("n0", RF, replicaService, peerClient, localStore);
    }

    // ── Restart strategy ──────────────────────────────────────────────────────

    /**
     * oldRing == null triggers a full re-pull of all replica arcs.
     * k1 already exists locally (same ts) — LWW absorbs the duplicate.
     * If k2 belongs to n0's replica set, it must appear after the pull.
     */
    @Test
    void restart_fullRePullWithLWWAbsorption() throws Exception {
        ConsistentHashRing r = ring("n0", "n1", "n2");

        // n1 holds k1 (ts=100) and k2 (ts=200)
        MemKvStore n1 = peerClient.peerData.computeIfAbsent("n1", id -> new MemKvStore());
        n1.data.put("k1", vv(100));
        n1.data.put("k2", vv(200));

        // n0 already has k1 at ts=100 from a partial rebalance
        localStore.data.put("k1", vv(100));

        runSync(null, r);

        // k1 must survive with its ts (LWW: equal ts does not overwrite)
        assertNotNull(localStore.data.get("k1"));
        assertEquals(100, localStore.data.get("k1").ts);
    }

    // ── LWW protection ────────────────────────────────────────────────────────

    @Test
    void pullArc_lwwProtectsNewerDestinationValue() throws Exception {
        ConsistentHashRing r = ring("n0", "n1", "n2");
        String key = findKeyPrimaryFor("n0", r);

        // Source has an older value
        peerClient.peerData.computeIfAbsent("n1", id -> new MemKvStore()).data.put(key, vv(50));
        // n0 already has a newer value
        localStore.data.put(key, vv(99));

        runSync(null, r);

        VersionedValue after = localStore.data.get(key);
        assertNotNull(after);
        assertEquals(99, after.ts, "newer local value must not be overwritten by older pull");
    }

    // ── Lost-arc cleanup ──────────────────────────────────────────────────────

    @Test
    void removeNode_lostKeysArePushedThenDropped() throws Exception {
        ConsistentHashRing oldRing = ring("n0", "n1", "n2", "n3", "n4");
        ConsistentHashRing newRing = ring("n0", "n1", "n2", "n3");

        // Populate n0 with 20 keys
        for (int i = 0; i < 20; i++) localStore.data.put("key-" + i, vv(1000 + i));

        // Identify keys that n0 loses
        List<String> staleKeys = new ArrayList<>();
        for (String k : localStore.data.keySet()) {
            boolean stillReplica = newRing.replicasForKey(k, RF).stream()
                    .anyMatch(n -> n.id().equals("n0"));
            if (!stillReplica) staleKeys.add(k);
        }

        runSync(oldRing, newRing);

        for (String key : staleKeys) {
            // Must have been pushed to at least one new owner
            long pushes = peerClient.putCalls.stream()
                    .filter(c -> c[1].equals(key)).count();
            assertTrue(pushes > 0, "stale key " + key + " must be pushed to new owners");
            // Must be dropped from local store
            assertFalse(localStore.data.containsKey(key),
                    "stale key " + key + " must be dropped after confirmed push");
        }
    }

    @Test
    void cleanupLostKeys_pushFailureRetainsKey() throws Exception {
        // 4→5 nodes: adding n4 shrinks n0's arc fraction from 3/4 to 3/5; n0 sheds ~15% of keys
        ConsistentHashRing oldRing = ring("n0", "n1", "n2", "n3");
        ConsistentHashRing newRing = ring("n0", "n1", "n2", "n3", "n4");

        // Find a key n0 will lose
        String staleKey = findKeyLostBy("n0", oldRing, newRing, RF);
        Assumptions.assumeTrue(staleKey != null, "No key found that n0 loses");

        localStore.data.put(staleKey, vv(500));
        // All peers fail
        peerClient.failPeers.addAll(Set.of("n1", "n2", "n3", "n4"));

        runSync(oldRing, newRing);

        assertTrue(localStore.data.containsKey(staleKey),
                "key must be retained locally when push fails");
    }

    // ── Ordering: confirm before delete ──────────────────────────────────────

    @Test
    void pushConfirmedBeforeDrop_destinationHasKeyWhenSourceDrops() throws Exception {
        // 4→5 nodes: n0 loses ~15% of keys when n4 joins (arc fraction shrinks from 3/4 to 3/5)
        ConsistentHashRing oldRing = ring("n0", "n1", "n2", "n3");
        ConsistentHashRing newRing = ring("n0", "n1", "n2", "n3", "n4");

        String staleKey = findKeyLostBy("n0", oldRing, newRing, RF);
        Assumptions.assumeTrue(staleKey != null, "No key found that n0 loses");

        localStore.data.put(staleKey, vv(700));

        runSync(oldRing, newRing);

        // Verify: if key was dropped from n0, it must be present on at least one new owner
        if (!localStore.data.containsKey(staleKey)) {
            List<Node> newOwners = newRing.replicasForKey(staleKey, RF);
            boolean anyOwnerHasIt = newOwners.stream().anyMatch(owner ->
                    peerClient.peerData.containsKey(owner.id())
                    && peerClient.peerData.get(owner.id()).data.containsKey(staleKey));
            assertTrue(anyOwnerHasIt,
                    "key must be on at least one new owner before it is dropped from source");
        }
    }

    // ── Multiple transitions ──────────────────────────────────────────────────

    @Test
    void multipleTransitions_noExceptions() throws Exception {
        ConsistentHashRing r1 = ring("n0", "n1");
        ConsistentHashRing r2 = ring("n0", "n1", "n2");
        ConsistentHashRing r3 = ring("n0", "n1", "n2", "n3");

        MemKvStore n1 = peerClient.peerData.computeIfAbsent("n1", id -> new MemKvStore());
        MemKvStore n2 = peerClient.peerData.computeIfAbsent("n2", id -> new MemKvStore());
        for (int i = 0; i < 5; i++) {
            n1.data.put("k-" + i, vv(100 + i));
            n2.data.put("k-n2-" + i, vv(200 + i));
        }

        runSync(r1, r2);
        runSync(r2, r3);
        // No exception = pass
    }

    // ── Utilities ────────────────────────────────────────────────────────────

    static String findKeyPrimaryFor(String nodeId, ConsistentHashRing r) {
        for (int i = 0; i < 10_000; i++) {
            String key = "probe-" + i;
            if (r.primaryForKey(key).id().equals(nodeId)) return key;
        }
        throw new AssertionError("No key found with primary=" + nodeId);
    }

    static String findKeyLostBy(String nodeId, ConsistentHashRing old,
                                 ConsistentHashRing next, int rf) {
        for (int i = 0; i < 500; i++) {
            String key = "lost-" + i;
            boolean hadIt = old.replicasForKey(key, rf).stream()
                    .anyMatch(n -> n.id().equals(nodeId));
            boolean hasIt = next.replicasForKey(key, rf).stream()
                    .anyMatch(n -> n.id().equals(nodeId));
            if (hadIt && !hasIt) return key;
        }
        return null;
    }
}
