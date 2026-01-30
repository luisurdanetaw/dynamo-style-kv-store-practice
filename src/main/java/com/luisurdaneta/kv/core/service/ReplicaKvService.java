package com.luisurdaneta.kv.core.service;

import com.luisurdaneta.kv.core.model.VersionedValue;
import com.luisurdaneta.kv.core.ports.KvStore;

public final class ReplicaKvService {
    private final KvStore store;

    public ReplicaKvService(KvStore store) {
        this.store = store;
    }

    public PutReplicaResult putReplica(String key, VersionedValue incoming) throws Exception {
        boolean applied = store.putIfNewer(key, incoming);
        return new PutReplicaResult(applied, incoming.ts, incoming.nodeId, incoming.tombstone);
    }

    public GetReplicaResult getReplica(String key) throws Exception {
        VersionedValue vv = store.get(key);
        if (vv == null) return GetReplicaResult.notFound();
        return GetReplicaResult.found(vv);
    }

    public record PutReplicaResult(boolean applied, long ts, String nodeId, boolean tombstone) {}

    public sealed interface GetReplicaResult permits GetReplicaResult.Found, GetReplicaResult.NotFound {
        static GetReplicaResult found(VersionedValue vv) { return new Found(vv); }
        static GetReplicaResult notFound() { return new NotFound(); }

        record Found(VersionedValue record) implements GetReplicaResult {}
        record NotFound() implements GetReplicaResult {}
    }
}
