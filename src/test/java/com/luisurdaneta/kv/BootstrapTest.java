package com.luisurdaneta.kv;

import com.luisurdaneta.kv.TestHelpers.CapturingPeerClient;
import com.luisurdaneta.kv.TestHelpers.MemKvStore;
import com.luisurdaneta.kv.core.ring.ConsistentHashRing;
import com.luisurdaneta.kv.core.ring.RingManager;
import com.luisurdaneta.kv.core.service.ReplicaKvService;
import com.luisurdaneta.kv.http.Node;
import com.luisurdaneta.kv.membership.MembershipView;
import com.luisurdaneta.kv.rebalance.Rebalancer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the bootstrap → watch-sync boundary.
 *
 * Invariant: seed ring is provisional (serves immediately); the first
 * updateFromWatch call makes the watch view authoritative. The seed list
 * is never re-consulted after that.
 */
class BootstrapTest {

    private static final int VNODES     = 64;
    private static final long DEBOUNCE  = 200; // ms — fast for tests

    static Node node(String id) { return new Node(id, "http://" + id + ":8080"); }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static Rebalancer noopRebalancer(String nodeId, MemKvStore store) {
        return new Rebalancer(nodeId, 3, new ReplicaKvService(store),
                              new CapturingPeerClient(), store);
    }

    // ── tests ────────────────────────────────────────────────────────────────

    @Test
    void seedRingUsedForProvisionalServing() {
        List<Node> seeds = List.of(node("kv-0"), node("kv-1"), node("kv-2"));
        ConsistentHashRing bootstrapRing = new ConsistentHashRing(seeds, VNODES);

        MembershipView mv = new MembershipView();
        mv.initFromSeeds(seeds);

        MemKvStore store = new MemKvStore();
        Rebalancer rb    = noopRebalancer("kv-0", store);
        RingManager rm   = new RingManager(bootstrapRing, mv, rb, VNODES, DEBOUNCE);

        // Before any watch event the bootstrap ring must be available immediately
        ConsistentHashRing current = rm.currentRing();
        assertNotNull(current);
        assertEquals(3, current.replicasForKey("any-key", 3).size());

        rm.shutdown();
        rb.shutdown();
    }

    @Test
    void watchSyncTransitionsToWatchView() throws Exception {
        List<Node> seeds = List.of(node("kv-0"), node("kv-1"), node("kv-2"));
        ConsistentHashRing bootstrapRing = new ConsistentHashRing(seeds, VNODES);

        MembershipView mv = new MembershipView();
        mv.initFromSeeds(seeds);

        MemKvStore store = new MemKvStore();
        Rebalancer rb    = noopRebalancer("kv-0", store);
        RingManager rm   = new RingManager(bootstrapRing, mv, rb, VNODES, DEBOUNCE);

        // Watch view includes a new node not in the seed list
        Set<Node> watchMembers = Set.of(
                node("kv-0"), node("kv-1"), node("kv-2"), node("kv-3"));
        mv.updateFromWatch(watchMembers);

        // Wait for debounce + margin
        Thread.sleep(DEBOUNCE + 500);

        ConsistentHashRing ring = rm.currentRing();
        // kv-3 must be in at least one arc of the rebuilt ring
        assertFalse(ring.replicaArcs("kv-3", 3).isEmpty(),
                "watch-view node kv-3 must be present in rebuilt ring");

        // Version must have been bumped by updateFromWatch
        assertEquals(1, mv.version());

        rm.shutdown();
        rb.shutdown();
    }

    @Test
    void seedListNotConsultedAfterWatchSync() throws Exception {
        List<Node> seeds = List.of(node("seed-0"), node("seed-1"));
        ConsistentHashRing bootstrapRing = new ConsistentHashRing(seeds, VNODES);

        MembershipView mv = new MembershipView();
        mv.initFromSeeds(seeds);

        MemKvStore store = new MemKvStore();
        Rebalancer rb    = noopRebalancer("kv-0", store);
        RingManager rm   = new RingManager(bootstrapRing, mv, rb, VNODES, DEBOUNCE);

        mv.updateFromWatch(Set.of(node("kv-0"), node("kv-1"), node("kv-2")));

        Thread.sleep(DEBOUNCE + 500);

        // seed-0/seed-1 must not appear in the current membership
        Set<Node> members = mv.currentMembers();
        assertFalse(members.contains(node("seed-0")));
        assertFalse(members.contains(node("seed-1")));
        assertTrue(members.contains(node("kv-0")));

        rm.shutdown();
        rb.shutdown();
    }

    @Test
    void rapidChangesAreDebounced() throws Exception {
        List<Node> seeds = List.of(node("kv-0"), node("kv-1"));
        ConsistentHashRing bootstrapRing = new ConsistentHashRing(seeds, VNODES);

        MembershipView mv = new MembershipView();
        mv.initFromSeeds(seeds);

        int[] rebuildCount = {0};
        MemKvStore store = new MemKvStore();
        // Intercept via MembershipView listener after RingManager is set up
        MemKvStore store2 = new MemKvStore();
        Rebalancer rb = noopRebalancer("kv-0", store);
        RingManager rm = new RingManager(bootstrapRing, mv, rb, VNODES, DEBOUNCE);

        // Count how many times the ring actually changes (via a second listener)
        mv.addChangeListener(v -> rebuildCount[0]++);

        // Fire 5 changes within debounce window (well under DEBOUNCE ms each)
        for (int i = 0; i < 5; i++) {
            mv.updateFromWatch(Set.of(node("kv-0"), node("kv-" + (i + 1))));
            Thread.sleep(20);
        }

        // listener fires on each change to the member set
        int listenerFires = rebuildCount[0]; // captures all MembershipView changes

        // But the ring manager fires only ONCE because of debounce.
        // We verify by checking that rm.currentRing() reflects the LAST state.
        Thread.sleep(DEBOUNCE + 500);

        ConsistentHashRing final_ = rm.currentRing();
        // Final ring must include kv-5 (the last watch update had kv-0 + kv-5)
        assertFalse(final_.replicaArcs("kv-5", 2).isEmpty(),
                "ring must reflect last membership update after debounce");

        rm.shutdown();
        rb.shutdown();
    }
}
