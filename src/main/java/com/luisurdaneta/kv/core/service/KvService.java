package com.luisurdaneta.kv.core.service;


import com.luisurdaneta.kv.core.model.VersionedValue;
import com.luisurdaneta.kv.core.ports.Clock;
import com.luisurdaneta.kv.core.ports.KvStore;

public final class KvService {
    private final KvStore store;
    private final Clock clock;

    public KvService(KvStore store, Clock clock) {
        this.store = store;
        this.clock = clock;
    }

    public PutResult put(String key, byte[] payload, String nodeId) throws Exception {
        VersionedValue vv = new VersionedValue(clock.nowMillis(), nodeId, false, payload);
        boolean applied = store.putIfNewer(key, vv);
        return new PutResult(applied, vv.ts, vv.nodeId);
    }

    public GetResult get(String key) throws Exception {
        VersionedValue vv = store.get(key);
        if (vv == null || vv.tombstone) return GetResult.notFound();
        return GetResult.found(vv.ts, vv.nodeId, vv.payload);
    }

    public DeleteResult delete(String key, String nodeId) throws Exception {
        VersionedValue vv = new VersionedValue(clock.nowMillis(), nodeId, true, new byte[0]);
        boolean applied = store.putIfNewer(key, vv);
        return new DeleteResult(applied, vv.ts);
    }

    public record PutResult(boolean applied, long ts, String nodeId) {}
    public record DeleteResult(boolean applied, long ts) {}

    public sealed interface GetResult permits GetResult.Found, GetResult.NotFound {
        static GetResult found(long ts, String nodeId, byte[] payload) {
            return new Found(ts, nodeId, payload);
        }
        static GetResult notFound() {
            return new NotFound();
        }

        record Found(long ts, String nodeId, byte[] payload) implements GetResult {}
        record NotFound() implements GetResult {}
    }
}
