package com.luisurdaneta.kv.http;

import com.luisurdaneta.kv.http.handler.*;
import com.sun.net.httpserver.HttpServer;

public final class Routes {
    private Routes() {}

    public static void registerAll(HttpServer server, NodeContext ctx) {
        server.createContext("/healthz", new HealthHandler());
        server.createContext("/whoami", new WhoamiHandler(ctx));
        server.createContext("/debug/replicas", new DebugReplicasHandler(ctx));
        server.createContext("/kv", new KvHandler(ctx));
        server.createContext("/internal/replica/kv", new ReplicaKvHandler(ctx));
    }
}