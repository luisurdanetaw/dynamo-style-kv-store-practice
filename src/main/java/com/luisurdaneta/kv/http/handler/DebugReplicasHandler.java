package com.luisurdaneta.kv.http.handler;

import com.luisurdaneta.kv.http.Node;
import com.luisurdaneta.kv.http.NodeContext;
import com.luisurdaneta.kv.util.HttpJson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;


public final class DebugReplicasHandler implements HttpHandler {
    private final NodeContext ctx;

    public DebugReplicasHandler(NodeContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void handle(HttpExchange ex) throws IOException {
        String q = ex.getRequestURI().getQuery(); // expecting ?key=foo
        if (q == null || !q.startsWith("key=")) {
            HttpJson.json(ex, 400, Map.of("error", "use ?key=..."));
            return;
        }

        String k = URLDecoder.decode(q.substring(4), StandardCharsets.UTF_8);
        List<Node> reps = ctx.ring().replicasForKey(k, 3);

        HttpJson.json(ex, 200, Map.of(
                "key", k,
                "replicas", reps.stream().map(n -> Map.of("id", n.id(), "url", n.baseUrl())).toList()
        ));
    }
}
