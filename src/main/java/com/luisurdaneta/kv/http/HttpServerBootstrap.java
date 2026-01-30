package com.luisurdaneta.kv.http;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;

public final class HttpServerBootstrap {
    private HttpServerBootstrap() {}

    public static HttpServer start(NodeContext ctx) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(ctx.config().port()), 0);
        Routes.registerAll(server, ctx);
        server.start();
        return server;
    }
}
