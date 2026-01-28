package com.luisurdaneta.kv;

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

    public List<Node> replicasForKey(String key, int rf) {
        if (rf <= 0) throw new IllegalArgumentException("rf must be > 0");
        int want = Math.min(rf, nodes.size());

        long keyHash = hash64(key);
        List<Node> out = new ArrayList<>(want);
        Set<String> seen = new HashSet<>(want);

        Iterator<Map.Entry<Long, Node>> it = tailThenWrap(keyHash);

        while (it.hasNext() && out.size() < want) {
            Node n = it.next().getValue();
            if (seen.add(n.id())) {
                out.add(n);
            }
        }
        return out;
    }

    public Node primaryForKey(String key) {
        return replicasForKey(key, 1).get(0);
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

    static long hash64(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] dig = md.digest(s.getBytes(StandardCharsets.UTF_8));

            return ByteBuffer.wrap(dig, 0, 8).getLong();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}