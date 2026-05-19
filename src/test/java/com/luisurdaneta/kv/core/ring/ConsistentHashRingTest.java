package com.luisurdaneta.kv.core.ring;

import com.luisurdaneta.kv.http.Node;
import com.luisurdaneta.kv.rebalance.TokenRange;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ConsistentHashRingTest {

    private static final int VNODES = 64;
    private static final int RF     = 3;

    private static Node node(String id) {
        return new Node(id, "http://" + id + ":8080");
    }

    private static ConsistentHashRing ring(String... ids) {
        List<Node> nodes = new ArrayList<>();
        for (String id : ids) nodes.add(node(id));
        return new ConsistentHashRing(nodes, VNODES);
    }

    // ── hash64 ────────────────────────────────────────────────────────────────

    @Test
    void hash64_isDeterministic() {
        assertEquals(ConsistentHashRing.hash64("foo"), ConsistentHashRing.hash64("foo"));
    }

    @Test
    void hash64_differentInputsDifferentHashes() {
        assertNotEquals(ConsistentHashRing.hash64("foo"), ConsistentHashRing.hash64("bar"));
    }

    // ── replicasForHash / replicasForKey parity ───────────────────────────────

    @Test
    void replicasForHash_matchesReplicasForKey() {
        ConsistentHashRing r = ring("a", "b", "c");
        String key = "some-key";
        long   h   = ConsistentHashRing.hash64(key);
        assertEquals(r.replicasForKey(key, RF), r.replicasForHash(h, RF));
    }

    // ── identical member set → identical ring ─────────────────────────────────

    @Test
    void identicalMemberSetProducesIdenticalRing() {
        List<Node> members = List.of(node("n0"), node("n1"), node("n2"));
        ConsistentHashRing r1 = new ConsistentHashRing(members, VNODES);
        ConsistentHashRing r2 = new ConsistentHashRing(members, VNODES);

        // Verify for 1000 sample keys
        Random rng = new Random(42);
        for (int i = 0; i < 1_000; i++) {
            String key = "key-" + rng.nextLong();
            assertEquals(r1.replicasForKey(key, RF), r2.replicasForKey(key, RF),
                    "rings must agree on replicas for key=" + key);
        }
    }

    // ── replicaArcs ───────────────────────────────────────────────────────────

    @Test
    void replicaArcs_withRF1_eachNodeCoversDisjointArcs() {
        ConsistentHashRing r = ring("a", "b", "c");

        Set<Long> coveredByA = new HashSet<>();
        Set<Long> coveredByB = new HashSet<>();
        Set<Long> coveredByC = new HashSet<>();

        for (TokenRange arc : r.replicaArcs("a", 1)) collectEnds(arc, coveredByA);
        for (TokenRange arc : r.replicaArcs("b", 1)) collectEnds(arc, coveredByB);
        for (TokenRange arc : r.replicaArcs("c", 1)) collectEnds(arc, coveredByC);

        // Arcs are disjoint for RF=1
        Set<Long> intersection = new HashSet<>(coveredByA);
        intersection.retainAll(coveredByB);
        assertTrue(intersection.isEmpty(), "RF=1 arcs must not overlap");
    }

    @Test
    void replicaArcs_withRF3_nodeCoversMoreArcsThanRF1() {
        ConsistentHashRing r = ring("a", "b", "c");
        assertTrue(r.replicaArcs("a", 3).size() >= r.replicaArcs("a", 1).size());
    }

    @Test
    void replicaArcs_allArcsCoverEveryKey() {
        // Every key must land in exactly one arc per node for RF=1
        ConsistentHashRing r = ring("a", "b", "c");
        Random rng = new Random(7);
        for (int i = 0; i < 1_000; i++) {
            String key = "k-" + rng.nextLong();
            long   h   = ConsistentHashRing.hash64(key);
            Node primary = r.primaryForKey(key);
            boolean inArc = r.replicaArcs(primary.id(), 1)
                              .stream()
                              .anyMatch(arc -> arc.contains(h));
            assertTrue(inArc, key + " (hash=" + h + ") not in any arc of its primary " + primary.id());
        }
    }

    @Test
    void tokenRange_containsHandlesWrapAround() {
        // Arc (MAX-10, MIN+10] wraps around
        TokenRange wrap = new TokenRange(Long.MAX_VALUE - 10, Long.MIN_VALUE + 10);
        assertTrue(wrap.contains(Long.MAX_VALUE));      // just inside right boundary
        assertTrue(wrap.contains(Long.MIN_VALUE));      // just inside left boundary
        assertTrue(wrap.contains(Long.MIN_VALUE + 10)); // inclusive end
        assertFalse(wrap.contains(0));                  // middle of the ring
    }

    // helper: collect arc end positions
    private static void collectEnds(TokenRange arc, Set<Long> set) {
        set.add(arc.end());
    }
}
