package com.luisurdaneta.kv.core.ring;

import com.luisurdaneta.kv.http.Node;
import com.luisurdaneta.kv.rebalance.TokenRange;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;


public final class ConsistentHashRing {
    public static final int DEFAULT_VNODES = 64;

    private final NavigableMap<Long, Node> ring = new TreeMap<>();
    private final int vnodes;
    private final List<Node> nodes;

    public ConsistentHashRing(List<Node> nodes, int vnodes) {
        if (nodes == null || nodes.isEmpty()) throw new IllegalArgumentException("nodes empty");
        if (vnodes <= 0) throw new IllegalArgumentException("vnodes must be > 0");

        this.vnodes = vnodes;
        this.nodes = List.copyOf(nodes);

        for (Node n : this.nodes) {
            for (int i = 0; i < vnodes; i++) {
                long h = hash64(n.id() + "#" + i);
                // Very unlikely collision; if it happens, just linear probe by +1
                while (ring.containsKey(h)) h++;
                ring.put(h, n);
            }
        }
    }

    /** Walk clockwise from the given pre-computed hash, returning the first rf unique physical nodes. */
    public List<Node> replicasForHash(long hash, int rf) {
        if (rf <= 0) throw new IllegalArgumentException("rf must be > 0");
        int want = Math.min(rf, nodes.size());

        List<Node> out = new ArrayList<>(want);
        Set<String> seen = new HashSet<>(want);

        Iterator<Map.Entry<Long, Node>> it = tailThenWrap(hash);
        while (it.hasNext() && out.size() < want) {
            Node n = it.next().getValue();
            if (seen.add(n.id())) out.add(n);
        }
        return out;
    }

    public List<Node> replicasForKey(String key, int rf) {
        return replicasForHash(hash64(key), rf);
    }

    public Node primaryForKey(String key) {
        return replicasForKey(key, 1).get(0);
    }

    /**
     * Returns all token arcs (prev, pos] in this ring where nodeId appears among the
     * top-rf unique physical replicas — i.e. all arcs for which this node holds data.
     * Accounts for all replica positions, not just primary.
     */
    public List<TokenRange> replicaArcs(String nodeId, int rf) {
        List<TokenRange> result = new ArrayList<>();
        List<Map.Entry<Long, Node>> entries = new ArrayList<>(ring.entrySet());
        int n = entries.size();
        if (n == 0) return result;

        for (int i = 0; i < n; i++) {
            long pos  = entries.get(i).getKey();
            // Previous position is the exclusive start of this arc.
            // For the very first arc (i=0), wrap around to the last ring position.
            long prev = entries.get((i - 1 + n) % n).getKey();

            // Walk clockwise from pos, collecting up to rf unique physical nodes.
            Set<String> seen = new LinkedHashSet<>(rf * 2);
            for (int j = 0; j < n && seen.size() < rf; j++) {
                seen.add(entries.get((i + j) % n).getValue().id());
            }

            if (seen.contains(nodeId)) {
                result.add(new TokenRange(prev, pos));
            }
        }
        return result;
    }

    private Iterator<Map.Entry<Long, Node>> tailThenWrap(long keyHash) {
        NavigableMap<Long, Node> tail = ring.tailMap(keyHash, true);
        if (!tail.isEmpty()) {
            return new Iterator<>() {
                private Iterator<Map.Entry<Long, Node>> cur = tail.entrySet().iterator();
                private boolean wrapped = false;

                @Override public boolean hasNext() {
                    if (cur.hasNext()) return true;
                    if (!wrapped) return !ring.isEmpty();
                    return false;
                }

                @Override public Map.Entry<Long, Node> next() {
                    if (cur.hasNext()) return cur.next();
                    if (!wrapped) {
                        wrapped = true;
                        cur = ring.entrySet().iterator();
                        return cur.next();
                    }
                    throw new NoSuchElementException();
                }
            };
        } else {
            return ring.entrySet().iterator();
        }
    }

    public static long hash64(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] dig = md.digest(s.getBytes(StandardCharsets.UTF_8));
            return ByteBuffer.wrap(dig, 0, 8).getLong();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
