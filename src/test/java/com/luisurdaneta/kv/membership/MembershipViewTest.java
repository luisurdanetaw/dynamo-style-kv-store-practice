package com.luisurdaneta.kv.membership;

import com.luisurdaneta.kv.http.Node;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class MembershipViewTest {

    private static Node node(String id) {
        return new Node(id, "http://" + id + ":8080");
    }

    @Test
    void initFromSeeds_setsMembers() {
        MembershipView mv = new MembershipView();
        mv.initFromSeeds(List.of(node("a"), node("b")));
        assertEquals(Set.of(node("a"), node("b")), mv.currentMembers());
    }

    @Test
    void initFromSeeds_doesNotIncrementVersion() {
        MembershipView mv = new MembershipView();
        mv.initFromSeeds(List.of(node("a")));
        assertEquals(0, mv.version());
    }

    @Test
    void initFromSeeds_doesNotNotifyListeners() {
        MembershipView mv = new MembershipView();
        AtomicInteger calls = new AtomicInteger();
        mv.addChangeListener(v -> calls.incrementAndGet());
        mv.initFromSeeds(List.of(node("a")));
        assertEquals(0, calls.get());
    }

    @Test
    void updateFromWatch_changesNotifyListenerAndIncrementVersion() {
        MembershipView mv = new MembershipView();
        AtomicInteger calls = new AtomicInteger();
        mv.addChangeListener(v -> calls.incrementAndGet());

        boolean changed = mv.updateFromWatch(Set.of(node("a"), node("b")));

        assertTrue(changed);
        assertEquals(1, mv.version());
        assertEquals(1, calls.get());
        assertEquals(Set.of(node("a"), node("b")), mv.currentMembers());
    }

    @Test
    void updateFromWatch_noChangeDoesNotNotify() {
        MembershipView mv = new MembershipView();
        mv.updateFromWatch(Set.of(node("a")));

        AtomicInteger calls = new AtomicInteger();
        mv.addChangeListener(v -> calls.incrementAndGet());

        boolean changed = mv.updateFromWatch(Set.of(node("a")));

        assertFalse(changed);
        assertEquals(0, calls.get(), "listener must not fire when set is unchanged");
    }

    @Test
    void version_incrementsOnEachDistinctChange() {
        MembershipView mv = new MembershipView();
        mv.updateFromWatch(Set.of(node("a")));          // version 1
        mv.updateFromWatch(Set.of(node("a"), node("b"))); // version 2
        mv.updateFromWatch(Set.of(node("a"), node("b"))); // no change
        mv.updateFromWatch(Set.of(node("a")));           // version 3
        assertEquals(3, mv.version());
    }

    @Test
    void seedListNotConsultedAfterWatchSync() {
        // After updateFromWatch fires, the seed-derived set is gone.
        MembershipView mv = new MembershipView();
        mv.initFromSeeds(List.of(node("seed-0"), node("seed-1")));
        mv.updateFromWatch(Set.of(node("kv-0"), node("kv-1"), node("kv-2")));

        // Seed nodes must not appear in the current member set.
        Set<Node> members = mv.currentMembers();
        assertFalse(members.contains(node("seed-0")));
        assertTrue(members.contains(node("kv-0")));
        assertEquals(3, members.size());
    }
}
