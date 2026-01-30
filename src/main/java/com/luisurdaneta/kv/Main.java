package com.luisurdaneta.kv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.luisurdaneta.kv.adapters.storage.RocksDbStore;
import com.luisurdaneta.kv.core.ports.Clock;
import com.luisurdaneta.kv.core.ports.KvStore;
import com.luisurdaneta.kv.core.ring.ConsistentHashRing;
import com.luisurdaneta.kv.core.service.KvService;
import com.luisurdaneta.kv.core.service.ReplicaKvService;
import com.luisurdaneta.kv.http.*;
import com.sun.net.httpserver.HttpServer;

import java.util.*;

public final class Main {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        NodeConfig config = NodeConfig.fromEnv();

        List<Node> peers = PeerDiscovery.computePeers(
                config.podName(),
                config.replicas(),
                config.headlessService(),
                config.namespace(),
                config.port()
        );

        ConsistentHashRing ring = new ConsistentHashRing(peers, ConsistentHashRing.DEFAULT_VNODES);

        KvStore store = new RocksDbStore(config.dbPath());

        Clock clock = Clock.system();

        KvService kvService = new KvService(store, clock);
        ReplicaKvService replicaService = new ReplicaKvService(store);

        NodeContext ctx = new NodeContext(config, peers, ring, kvService, replicaService);

        HttpServer server = HttpServerBootstrap.start(ctx);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { store.close(); } catch (Exception ignored) {}
            try { server.stop(0); } catch (Exception ignored) {}
        }));

        System.out.println("Listening on :" + config.port()
                + " nodeId=" + config.nodeId()
                + " peers=" + peers
                + " dbPath=" + config.dbPath());
    }
}