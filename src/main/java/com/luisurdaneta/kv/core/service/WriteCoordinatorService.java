package com.luisurdaneta.kv.core.service;


import com.luisurdaneta.kv.core.model.VersionedValue;
import com.luisurdaneta.kv.core.ports.Clock;
import com.luisurdaneta.kv.core.ports.PeerClient;
import com.luisurdaneta.kv.core.ring.ConsistentHashRing;
import com.luisurdaneta.kv.http.Node;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public final class WriteCoordinatorService {
    private final String localNodeId;
    private final ConsistentHashRing ring;
    private final ReplicaKvService replicaLocal;
    private final PeerClient peers;
    private final Clock clock;

    private final int rf;       // replication factor
    private final int w;
    private final Duration perRequestTimeout;
    private final Duration overallTimeout;

    public WriteCoordinatorService(
            String localNodeId,
            ConsistentHashRing ring,
            ReplicaKvService replicaLocal,
            PeerClient peers,
            Clock clock,
            int rf,
            int w,
            Duration perRequestTimeout,
            Duration overallTimeout
    ) {
        this.localNodeId = localNodeId;
        this.ring = ring;
        this.replicaLocal = replicaLocal;
        this.peers = peers;
        this.clock = clock;
        this.rf = rf;
        this.w = w;
        this.perRequestTimeout = perRequestTimeout;
        this.overallTimeout = overallTimeout;
    }

    public WriteResult put(String key, byte[] payload) throws Exception {
        VersionedValue vv = new VersionedValue(clock.nowMillis(), localNodeId, false, payload);
        return writeQuorum(key, vv);
    }

    public WriteResult delete(String key) throws Exception {
        VersionedValue vv = new VersionedValue(clock.nowMillis(), localNodeId, true, new byte[0]);
        return writeQuorum(key, vv);
    }

    private WriteResult writeQuorum(String key, VersionedValue vv) throws Exception {
        List<Node> replicas = ring.replicasForKey(key, rf);

        try (ExecutorService exec = Executors.newVirtualThreadPerTaskExecutor()) {
            CompletionService<Ack> cs = new ExecutorCompletionService<>(exec);
            List<Future<Ack>> futures = new ArrayList<>(replicas.size());

            for (Node peer : replicas) {
                futures.add(cs.submit(() -> sendOne(peer, key, vv)));
            }

            int acks = 0;
            long deadline = System.nanoTime() + overallTimeout.toNanos();

            for (int i = 0; i < replicas.size(); i++) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) break;

                Future<Ack> f = cs.poll(remaining, TimeUnit.NANOSECONDS);
                if (f == null) break;

                Ack ack;
                try {
                    ack = f.get(); // task already completed
                } catch (ExecutionException ee) {
                    // Treat as failed ack
                    continue;
                }

                if (ack.ok) acks++;

                if (acks >= w) {
                    // quorum satisfied — cancel stragglers
                    for (Future<Ack> fut : futures) fut.cancel(true);
                    return WriteResult.success(key, vv.ts, vv.nodeId, vv.tombstone, acks, w, replicas);
                }
            }

            for (Future<Ack> fut : futures) fut.cancel(true);
            return WriteResult.fail(key, vv.ts, vv.nodeId, vv.tombstone, acks, w, replicas);
        }
    }

    private Ack sendOne(Node peer, String key, VersionedValue vv) throws Exception {
        if (peer.id().equals(localNodeId)) {
            replicaLocal.putReplica(key, vv);
            return new Ack(peer.id(), true);
        }
        PeerClient.ReplicaPutAck r = peers.putReplica(peer, key, vv, perRequestTimeout);
        return new Ack(peer.id(), r.ok());
    }

    private record Ack(String peerId, boolean ok) {}

    public record WriteResult(
            boolean ok,
            String key,
            long ts,
            String originNodeId,
            boolean tombstone,
            int acks,
            int required,
            List<Node> replicas
    ) {
        static WriteResult success(String key, long ts, String originNodeId, boolean tombstone,
                                   int acks, int required, List<Node> replicas) {
            return new WriteResult(true, key, ts, originNodeId, tombstone, acks, required, replicas);
        }
        static WriteResult fail(String key, long ts, String originNodeId, boolean tombstone,
                                int acks, int required, List<Node> replicas) {
            return new WriteResult(false, key, ts, originNodeId, tombstone, acks, required, replicas);
        }
    }
}