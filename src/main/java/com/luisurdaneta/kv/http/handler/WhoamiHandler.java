package com.luisurdaneta.kv.http.handler;

import com.luisurdaneta.kv.http.NodeConfig;
import com.luisurdaneta.kv.http.NodeContext;
import com.luisurdaneta.kv.util.HttpJson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.util.Map;

public final class WhoamiHandler implements HttpHandler {
    private final NodeContext ctx;

    public WhoamiHandler(NodeContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void handle(HttpExchange ex) throws IOException {
        NodeConfig c = ctx.config();
        HttpJson.json(ex, 200, Map.of(
                "nodeId", c.nodeId(),
                "podName", c.podName(),
                "namespace", c.namespace(),
                "headlessService", c.headlessService(),
                "replicas", c.replicas(),
                "port", c.port(),
                "dbPath", c.dbPath(),
                "peers", ctx.peers()
        ));
    }
}
