package com.luisurdaneta.kv.membership;

import com.luisurdaneta.kv.http.Node;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * Thread-safe holder for the current cluster member set and a monotonic version counter.
 *
 * Bootstrap boundary:
 *   initFromSeeds()    — provisional bootstrap only; does NOT notify change listeners
 *                        and does NOT increment the version. Seed state is never authoritative.
 *   updateFromWatch()  — steady-state authority; increments version and notifies listeners
 *                        on every change. Once called once, the watch view supersedes seeds.
 */
public final class MembershipView {

    private final Object lock = new Object();
    private volatile Set<Node> members = Collections.emptySet();
    private volatile long version = 0;
    private final List<Consumer<MembershipView>> listeners = new CopyOnWriteArrayList<>();

    /**
     * Seed the provisional membership from the static StatefulSet peer list.
     * Does NOT notify listeners — this is bootstrap-only and never treated as authoritative.
     */
    public void initFromSeeds(List<Node> seeds) {
        synchronized (lock) {
            members = Collections.unmodifiableSet(new HashSet<>(seeds));
            // version intentionally stays at 0 — seeds are provisional
        }
    }

    /**
     * Called by PodWatcher whenever the Kubernetes API watch produces a new ready-pod set.
     * This is the authoritative update. Increments version and fires listeners if the set changed.
     *
     * @return true if the member set actually changed
     */
    public boolean updateFromWatch(Set<Node> readyPods) {
        boolean changed;
        synchronized (lock) {
            Set<Node> next = Collections.unmodifiableSet(new HashSet<>(readyPods));
            changed = !next.equals(members);
            if (changed) {
                members = next;
                version++;
            }
        }
        if (changed) {
            for (Consumer<MembershipView> l : listeners) l.accept(this);
        }
        return changed;
    }

    public Set<Node> currentMembers() {
        return members;
    }

    public long version() {
        return version;
    }

    public void addChangeListener(Consumer<MembershipView> listener) {
        listeners.add(listener);
    }
}
