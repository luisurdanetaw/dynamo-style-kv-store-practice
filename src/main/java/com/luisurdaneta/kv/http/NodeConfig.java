package com.luisurdaneta.kv.http;

import static com.luisurdaneta.kv.util.EnvUtils.env;
import static com.luisurdaneta.kv.util.EnvUtils.intEnv;

public record NodeConfig(
        int port,
        String podName,
        String namespace,
        String headlessService,
        int replicas,
        String nodeId,
        String dbPath
) {
    public static NodeConfig fromEnv() {
        int port = intEnv("PORT", 8080);

        String podName = env("POD_NAME", "local");
        String namespace = env("POD_NAMESPACE", "default");
        String headless = env("HEADLESS_SERVICE", "kv-headless");
        int replicas = intEnv("REPLICAS", 1);

        String nodeId = podName;
        String dbPath = env("DB_PATH", "./data/" + nodeId);

        return new NodeConfig(port, podName, namespace, headless, replicas, nodeId, dbPath);
    }
}