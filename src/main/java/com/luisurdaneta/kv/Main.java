package com.luisurdaneta.kv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.luisurdaneta.kv.adapters.peer.PeerClientImpl;
import com.luisurdaneta.kv.adapters.storage.RocksDbStore;
import com.luisurdaneta.kv.core.ports.Clock;
import com.luisurdaneta.kv.core.ports.KvStore;
import com.luisurdaneta.kv.core.ports.PeerClient;
import com.luisurdaneta.kv.core.ring.ConsistentHashRing;
import com.luisurdaneta.kv.core.ring.RingManager;
import com.luisurdaneta.kv.core.service.ReadCoordinatorService;
import com.luisurdaneta.kv.core.service.ReplicaKvService;
import com.luisurdaneta.kv.core.service.WriteCoordinatorService;
import com.luisurdaneta.kv.http.*;
import com.luisurdaneta.kv.membership.MembershipView;
import com.luisurdaneta.kv.membership.PodWatcher;
import com.luisurdaneta.kv.rebalance.Rebalancer;
import com.luisurdaneta.kv.util.HttpJson;
import com.sun.net.httpserver.HttpServer;

import java.time.Duration;
import java.util.List;

public final class Main {
    private static final ObjectMapper MAPPER = HttpJson.MAPPER;

    public static void main(String[] args) throws Exception {
        NodeConfig config = NodeConfig.fromEnv();

        // ── Bootstrap: provisional peer list from StatefulSet DNS ────────────
        // Seeds are used only to form the initial ring so the node can serve
        // immediately on startup, before the first Kubernetes API watch sync.
        List<Node> seedPeers = PeerDiscovery.computePeers(
                config.podName(),
                config.replicas(),
                config.headlessService(),
                config.namespace(),
                config.port()
        );
        ConsistentHashRing bootstrapRing =
                new ConsistentHashRing(seedPeers, ConsistentHashRing.DEFAULT_VNODES);

        // ── Storage & local replica service ──────────────────────────────────
        KvStore store = new RocksDbStore(config.dbPath());
        Clock clock   = Clock.system();
        ReplicaKvService replicaService = new ReplicaKvService(store);
        PeerClient peerClient = new PeerClientImpl(MAPPER);

        // ── Membership (seed = provisional; watch = authoritative) ───────────
        MembershipView membershipView = new MembershipView();
        membershipView.initFromSeeds(seedPeers); // provisional only, no listener fired

        // ── Rebalancer + RingManager ──────────────────────────────────────────
        Rebalancer rebalancer = new Rebalancer(
                config.nodeId(), 3, replicaService, peerClient, store);

        RingManager ringManager = new RingManager(
                bootstrapRing, membershipView, rebalancer,
                ConsistentHashRing.DEFAULT_VNODES);

        // ── Coordinators read current ring from RingManager on each request ───
        WriteCoordinatorService writeCoordinator = new WriteCoordinatorService(
                config.nodeId(),
                ringManager,
                replicaService,
                peerClient,
                clock,
                3,  // REPLICATION FACTOR
                2,  // W
                Duration.ofMillis(300),
                Duration.ofMillis(800)
        );

        ReadCoordinatorService readCoordinator = new ReadCoordinatorService(
                config.nodeId(),
                ringManager,
                replicaService,
                peerClient,
                3, // REPLICATION FACTOR
                2, // R
                Duration.ofMillis(300),
                Duration.ofMillis(800)
        );

        NodeContext ctx = new NodeContext(
                config, membershipView, ringManager,
                replicaService, writeCoordinator, readCoordinator,
                peerClient, store);

        HttpServer server = HttpServerBootstrap.start(ctx);

        // ── Pod watcher: Kubernetes API watch → supersedes seed view ──────────
        PodWatcher podWatcher = new PodWatcher(membershipView, config, MAPPER);
        podWatcher.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            podWatcher.stop();
            ringManager.shutdown();
            rebalancer.shutdown();
            server.stop(0);
            try { store.close(); } catch (Exception ignored) {}
        }));

        System.out.println("Listening on :" + config.port()
                + " nodeId=" + config.nodeId()
                + " provisionalPeers=" + seedPeers
                + " dbPath=" + config.dbPath()
                + " labelSelector=" + config.labelSelector());
    }
}
