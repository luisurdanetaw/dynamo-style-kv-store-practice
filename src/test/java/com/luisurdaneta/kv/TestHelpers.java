package com.luisurdaneta.kv;

import com.luisurdaneta.kv.core.model.VersionedValue;
import com.luisurdaneta.kv.core.ports.KvStore;
import com.luisurdaneta.kv.core.ports.PeerClient;
import com.luisurdaneta.kv.core.ring.ConsistentHashRing;
import com.luisurdaneta.kv.http.Node;
import com.luisurdaneta.kv.rebalance.TokenRange;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiPredicate;

/**
 * Shared test helpers: in-memory KvStore and capturing PeerClient stubs.
 */
public final class TestHelpers {

    private TestHelpers() {}

    // ── in-memory KvStore ─────────────────────────────────────────────────────

    public static class MemKvStore implements KvStore {
        public final Map<String, VersionedValue> data = new LinkedHashMap<>();

        @Override public VersionedValue get(String key) { return data.get(key); }

        @Override public boolean putIfNewer(String key, VersionedValue candidate) {
            VersionedValue ex = data.get(key);
            if (candidate.isNewerThan(ex)) { data.put(key, candidate); return true; }
            return false;
        }

        @Override public void scan(String startAfter, BiPredicate<String, VersionedValue> visitor) {
            for (Map.Entry<String, VersionedValue> e : data.entrySet()) {
                if (startAfter != null && e.getKey().compareTo(startAfter) <= 0) continue;
                if (!visitor.test(e.getKey(), e.getValue())) break;
            }
        }

        @Override public void drop(String key) { data.remove(key); }
        @Override public void close() {}
    }

    // ── capturing PeerClient ──────────────────────────────────────────────────

    public static class CapturingPeerClient implements PeerClient {
        /** Per-peer data used as source for pullRangePage responses. */
        public final Map<String, MemKvStore> peerData = new HashMap<>();
        /** Records all putReplica calls: each entry is [peerId, key]. */
        public final List<String[]> putCalls = new CopyOnWriteArrayList<>();
        /** Peer IDs for which putReplica returns failure. */
        public final Set<String> failPeers = new HashSet<>();

        @Override
        public ReplicaPutAck putReplica(Node peer, String key, VersionedValue vv, Duration t) {
            putCalls.add(new String[]{peer.id(), key});
            if (failPeers.contains(peer.id()))
                return new ReplicaPutAck(peer.id(), false, false, 503);
            peerData.computeIfAbsent(peer.id(), id -> new MemKvStore()).putIfNewer(key, vv);
            return new ReplicaPutAck(peer.id(), true, true, 200);
        }

        @Override
        public ReplicaGetAck getReplica(Node peer, String key, Duration t) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TransferPage pullRangePage(Node source, long start, long end,
                                          String cursor, int pageSize,
                                          long membershipVersion, Duration t) {
            MemKvStore src = peerData.getOrDefault(source.id(), new MemKvStore());
            TokenRange range = new TokenRange(start, end);
            List<TransferEntry> entries = new ArrayList<>();
            String[] lastKey = {null};

            src.scan(cursor, (key, vv) -> {
                lastKey[0] = key;
                if (range.contains(ConsistentHashRing.hash64(key))) {
                    entries.add(new TransferEntry(key, vv));
                }
                return entries.size() < pageSize;
            });

            boolean done = entries.size() < pageSize;
            return new TransferPage(entries, done ? null : lastKey[0]);
        }
    }
}
