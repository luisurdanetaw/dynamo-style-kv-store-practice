package com.luisurdaneta.kv.core.ring;

import com.luisurdaneta.kv.http.Node;
import com.luisurdaneta.kv.membership.MembershipView;
import com.luisurdaneta.kv.rebalance.Rebalancer;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages the live consistent-hash ring.
 *
 * Bootstrap boundary:
 *   - Starts with a bootstrapRing derived from seeds (provisional, for immediate serving).
 *   - previousRing is null until the first watch-sync fires. This means the first
 *     onRingChanged(null, watchRing) triggers a full re-pull (restart strategy).
 *   - All subsequent membership changes use the real delta.
 *
 * Debounce:
 *   A rolling update can flip many pods NotReady→Ready in quick succession.
 *   We wait DEBOUNCE_MS after the last membership change before rebuilding the ring
 *   and triggering the rebalancer, so a 6-pod rollout produces one rebalance, not six.
 */
public final class RingManager {

    private static final System.Logger LOG = System.getLogger(RingManager.class.getName());
    public  static final long DEFAULT_DEBOUNCE_MS = 3_000;

    private final AtomicReference<ConsistentHashRing> current;
    private final MembershipView membershipView;
    private final Rebalancer     rebalancer;
    private final int            vnodes;
    private final long           debounceMs;

    // previousRing == null until the first watch-sync completes (restart re-pull strategy).
    private volatile ConsistentHashRing previousRing = null;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "ring-rebuilder");
        t.setDaemon(true);
        return t;
    });
    private ScheduledFuture<?> pendingRebuild;

    public RingManager(ConsistentHashRing bootstrapRing,
                       MembershipView membershipView,
                       Rebalancer rebalancer,
                       int vnodes) {
        this(bootstrapRing, membershipView, rebalancer, vnodes, DEFAULT_DEBOUNCE_MS);
    }

    /** Constructor with configurable debounce — use the short form for tests. */
    public RingManager(ConsistentHashRing bootstrapRing,
                       MembershipView membershipView,
                       Rebalancer rebalancer,
                       int vnodes,
                       long debounceMs) {
        this.current        = new AtomicReference<>(bootstrapRing);
        this.membershipView = membershipView;
        this.rebalancer     = rebalancer;
        this.vnodes         = vnodes;
        this.debounceMs     = debounceMs;
        // previousRing intentionally left null — see class javadoc.
        membershipView.addChangeListener(view -> onMembershipChanged());
    }

    /** Hot path — just an atomic dereference. */
    public ConsistentHashRing currentRing() {
        return current.get();
    }

    // ── debounce ─────────────────────────────────────────────────────────────

    private void onMembershipChanged() {
        synchronized (this) {
            if (pendingRebuild != null) pendingRebuild.cancel(false);
            pendingRebuild = scheduler.schedule(this::rebuildRing, debounceMs, TimeUnit.MILLISECONDS);
        }
    }

    private void rebuildRing() {
        Set<Node> members = membershipView.currentMembers();
        if (members.isEmpty()) {
            LOG.log(System.Logger.Level.WARNING, "No ready pods; ring unchanged");
            return;
        }

        ConsistentHashRing newRing = new ConsistentHashRing(new ArrayList<>(members), vnodes);
        ConsistentHashRing oldRing = previousRing; // null on first watch-sync

        current.set(newRing);
        previousRing = newRing;

        LOG.log(System.Logger.Level.INFO,
                "Ring rebuilt: members=" + members.size()
                + " membershipVersion=" + membershipView.version()
                + (oldRing == null ? " [first-sync]" : ""));

        rebalancer.onRingChanged(oldRing, newRing, membershipView.version());
    }

    public void shutdown() {
        scheduler.shutdown();
    }
}
