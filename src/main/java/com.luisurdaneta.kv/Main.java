package com.luisurdaneta.kv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

public final class Main {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        int port = intEnv("PORT", 8080);

        // Kubernetes downward API envs (we’ll set these in manifests)
        String podName = env("POD_NAME", "local");
        String namespace = env("POD_NAMESPACE", "default");
        String headless = env("HEADLESS_SERVICE", "kv-headless");
        int replicas = intEnv("REPLICAS", 1);

        String nodeId = podName; // stable ID; later you’ll use this for LWW tie-break
        List<String> peers = computePeers(podName, replicas, headless, namespace, port);

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

        // Health
        server.createContext("/healthz", ex ->
                json(ex, 200, Map.of("ok", true, "ts", Instant.now().toString()))
        );

        // Debug node identity + peers
        server.createContext("/whoami", ex ->
                json(ex, 200, Map.of(
                        "nodeId", nodeId,
                        "podName", podName,
                        "namespace", namespace,
                        "headlessService", headless,
                        "replicas", replicas,
                        "port", port,
                        "peers", peers
                ))
        );

        server.start();
        System.out.println("Listening on :" + port + " nodeId=" + nodeId + " peers=" + peers);
    }

    /**
     * Discovery strategy:
     * - StatefulSet pods have stable names: kv-0, kv-1, kv-2...
     * - Headless service gives stable DNS: kv-0.kv-headless.default.svc.cluster.local
     * - We build peer URLs deterministically without needing Kubernetes API permissions.
     */
    private static List<String> computePeers(String podName, int replicas, String headless, String namespace, int port) {
        String baseName = podName.contains("-") ? podName.substring(0, podName.lastIndexOf('-')) : podName;

        List<String> peers = new ArrayList<>(replicas);
        for (int i = 0; i < replicas; i++) {
            String host = baseName + "-" + i + "." + headless + "." + namespace + ".svc.cluster.local";
            peers.add("http://" + host + ":" + port);
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