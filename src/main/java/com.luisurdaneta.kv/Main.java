package com.luisurdaneta.kv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

public final class Main {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        int port = intEnv("PORT", 8080);

        String podName = env("POD_NAME", "local");
        String namespace = env("POD_NAMESPACE", "default");
        String headless = env("HEADLESS_SERVICE", "kv-headless");
        int replicas = intEnv("REPLICAS", 1);
        String nodeId = podName;

        List<Node> peers = computePeers(podName, replicas, headless, namespace, port);

        ConsistentHashRing ring = new ConsistentHashRing(peers, ConsistentHashRing.DEFAULT_VNODES);
        int RF = 3;

        String dbPath = env("DB_PATH", "./data/" + nodeId);

        Storage store = new Storage(dbPath);

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/healthz", ex ->
                json(ex, 200, Map.of("ok", true, "ts", Instant.now().toString()))
        );

        server.createContext("/whoami", ex -> json(ex, 200, Map.of(
                "nodeId", nodeId,
                "podName", podName,
                "namespace", namespace,
                "headlessService", headless,
                "replicas", replicas,
                "port", port,
                "dbPath", dbPath,
                "peers", peers
        )));

        server.createContext("/debug/replicas", ex -> {
            String key = ex.getRequestURI().getQuery(); // expecting ?key=foo
            if (key == null || !key.startsWith("key=")) {
                json(ex, 400, Map.of("error", "use ?key=..."));
                return;
            }
            String k = URLDecoder.decode(key.substring(4), StandardCharsets.UTF_8);
            List<Node> reps = ring.replicasForKey(k, 3);
            json(ex, 200, Map.of(
                    "key", k,
                    "replicas", reps.stream().map(n -> Map.of("id", n.id(), "url", n.baseUrl())).toList()
            ));
        });

        server.createContext("/kv", ex -> {
            try {
                String key = pathParamAfterPrefix(ex, "/kv/");
                if (key == null || key.isBlank()) {
                    json(ex, 400, Map.of("error", "missing key"));
                    return;
                }

                switch (ex.getRequestMethod()) {
                    case "PUT" -> {
                        byte[] body = ex.getRequestBody().readAllBytes();
                        VersionedValue vv = new VersionedValue(
                                System.currentTimeMillis(),
                                nodeId,
                                false,
                                body
                        );
                        store.putIfNewer(key, vv);
                        json(ex, 200, Map.of("ok", true, "key", key, "ts", vv.ts, "nodeId", vv.nodeId));
                    }
                    case "GET" -> {
                        VersionedValue vv = store.get(key);
                        if (vv == null || vv.tombstone) {
                            json(ex, 404, Map.of("found", false));
                            return;
                        }
                        ex.getResponseHeaders().set("Content-Type", "application/octet-stream");
                        ex.getResponseHeaders().set("X-KV-Ts", Long.toString(vv.ts));
                        ex.getResponseHeaders().set("X-KV-NodeId", vv.nodeId);
                        ex.sendResponseHeaders(200, vv.payload.length);
                        try (OutputStream os = ex.getResponseBody()) {
                            os.write(vv.payload);
                        }
                    }
                    case "DELETE" -> {
                        VersionedValue vv = new VersionedValue(
                                System.currentTimeMillis(),
                                nodeId,
                                true,
                                new byte[0]
                        );
                        store.putIfNewer(key, vv);
                        json(ex, 200, Map.of("ok", true, "deleted", true, "key", key, "ts", vv.ts));
                    }
                    default -> json(ex, 405, Map.of("error", "method not allowed"));
                }
            } catch (RocksDBException e) {
                json(ex, 500, Map.of("error", "rocksdb", "message", e.getMessage()));
            }
        });

        server.createContext("/internal/replica/kv", ex -> {
            try {
                String key = pathParamAfterPrefix(ex, "/internal/replica/kv/");
                if (key == null || key.isBlank()) {
                    json(ex, 400, Map.of("error", "missing key"));
                    return;
                }

                switch (ex.getRequestMethod()) {
                    case "PUT" -> {
                        byte[] body = ex.getRequestBody().readAllBytes();
                        ReplicaRecordDto dto = MAPPER.readValue(body, ReplicaRecordDto.class);
                        VersionedValue incoming = ReplicaCodec.fromDto(dto);

                        boolean applied = store.putIfNewerApplied(key, incoming);

                        json(ex, 200, Map.of(
                                "ok", true,
                                "key", key,
                                "applied", applied,
                                "ts", incoming.ts,
                                "nodeId", incoming.nodeId,
                                "tombstone", incoming.tombstone
                        ));
                    }
                    case "GET" -> {
                        VersionedValue vv = store.get(key);
                        if (vv == null) {
                            json(ex, 404, Map.of("found", false));
                            return;
                        }
                        // Return tombstones too (caller needs them)
                        json(ex, 200, Map.of(
                                "found", true,
                                "key", key,
                                "record", ReplicaCodec.toDto(vv)
                        ));
                    }
                    default -> json(ex, 405, Map.of("error", "method not allowed"));
                }
            } catch (RocksDBException e) {
                json(ex, 500, Map.of("error", "rocksdb", "message", e.getMessage()));
            } catch (Exception e) {
                json(ex, 400, Map.of("error", "bad_request", "message", e.getMessage()));
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { store.close(); } catch (Exception ignored) {}
        }));

        server.start();
        System.out.println("Listening on :" + port + " nodeId=" + nodeId + " peers=" + peers + " dbPath=" + dbPath);
    }

    private static String pathParamAfterPrefix(HttpExchange ex, String prefix) {
        String p = ex.getRequestURI().getPath();
        if (!p.startsWith(prefix)) return null;
        String raw = p.substring(prefix.length());
        if (raw.isEmpty()) return null;
        return URLDecoder.decode(raw, StandardCharsets.UTF_8);
    }

    private static List<Node> computePeers(String podName, int replicas, String headless, String namespace, int port) {
        String baseName = podName.contains("-") ? podName.substring(0, podName.lastIndexOf('-')) : podName;
        List<Node> peers = new ArrayList<>(replicas);
        for (int i = 0; i < replicas; i++) {
            String id = baseName + "-" + i;
            String host = id + "." + headless + "." + namespace + ".svc.cluster.local";
            String url = "http://" + host + ":" + port;
            peers.add(new Node(id, url));
        }
        return peers;
    }

    private static void json(HttpExchange ex, int status, Object obj) throws IOException {
        byte[] body = MAPPER.writeValueAsBytes(obj);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, body.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(body); }
    }

    private static String env(String k, String def) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? def : v.trim();
    }

    private static int intEnv(String k, int def) {
        String v = System.getenv(k);
        if (v == null || v.isBlank()) return def;
        return Integer.parseInt(v.trim());
    }
}