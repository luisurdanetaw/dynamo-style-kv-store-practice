package com.luisurdaneta.kv;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

public final class VersionedValue {
    public final long ts;
    public final String nodeId;
    public final boolean tombstone;
    public final byte[] payload; // null/empty allowed when tombstone

    public VersionedValue(long ts, String nodeId, boolean tombstone, byte[] payload) {
        this.ts = ts;
        this.nodeId = Objects.requireNonNull(nodeId);
        this.tombstone = tombstone;
        this.payload = payload == null ? new byte[0] : payload;
    }

    // LWW: higher (ts, nodeId) wins. nodeId tie-break must be stable.
    public boolean isNewerThan(VersionedValue other) {
        if (other == null) return true;
        if (this.ts != other.ts) return this.ts > other.ts;
        return this.nodeId.compareTo(other.nodeId) > 0;
    }

    // Simple binary encoding (fast, stable). Format:
    // [ts:8][tomb:1][nodeLen:4][nodeBytes][payloadLen:4][payloadBytes]
    public byte[] toBytes() {
        byte[] nodeBytes = nodeId.getBytes(StandardCharsets.UTF_8);
        int size = 8 + 1 + 4 + nodeBytes.length + 4 + payload.length;
        ByteBuffer bb = ByteBuffer.allocate(size);
        bb.putLong(ts);
        bb.put((byte) (tombstone ? 1 : 0));
        bb.putInt(nodeBytes.length);
        bb.put(nodeBytes);
        bb.putInt(payload.length);
        bb.put(payload);
        return bb.array();
    }

    public static VersionedValue fromBytes(byte[] bytes) {
        if (bytes == null) return null;
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long ts = bb.getLong();
        boolean tomb = bb.get() != 0;
        int nodeLen = bb.getInt();
        byte[] nodeBytes = new byte[nodeLen];
        bb.get(nodeBytes);
        String nodeId = new String(nodeBytes, StandardCharsets.UTF_8);
        int payloadLen = bb.getInt();
        byte[] payload = new byte[payloadLen];
        bb.get(payload);
        return new VersionedValue(ts, nodeId, tomb, payload);
    }

    @Override
    public String toString() {
        return "VersionedValue{ts=" + ts + ", nodeId=" + nodeId + ", tombstone=" + tombstone +
                ", payloadLen=" + payload.length + '}';
    }

    @Override public boolean equals(Object o) {
        if (!(o instanceof VersionedValue v)) return false;
        return ts == v.ts && tombstone == v.tombstone &&
                nodeId.equals(v.nodeId) && Arrays.equals(payload, v.payload);
    }
    @Override public int hashCode() { return Objects.hash(ts, nodeId, tombstone); }
}
