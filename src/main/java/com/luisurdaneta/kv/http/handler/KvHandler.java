package com.luisurdaneta.kv.http.handler;

import com.luisurdaneta.kv.core.service.KvService;
import com.luisurdaneta.kv.http.NodeContext;
import com.luisurdaneta.kv.util.HttpJson;
import com.luisurdaneta.kv.util.PathParams;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public final class KvHandler implements HttpHandler {
    private final NodeContext ctx;

    public KvHandler(NodeContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void handle(HttpExchange ex) throws IOException {
        try {
            String key = PathParams.pathParamAfterPrefix(ex, "/kv/");
            if (key == null || key.isBlank()) {
                HttpJson.json(ex, 400, Map.of("error", "missing key"));
                return;
            }

            switch (ex.getRequestMethod()) {
                case "PUT" -> {
                    byte[] body = ex.getRequestBody().readAllBytes();

                    KvService.PutResult r =
                            ctx.kvService().put(key, body, ctx.config().nodeId());

                    HttpJson.json(ex, 200, Map.of(
                            "ok", true,
                            "key", key,
                            "applied", r.applied(),
                            "ts", r.ts(),
                            "nodeId", r.nodeId()
                    ));
                }

                case "GET" -> {
                    KvService.GetResult r = ctx.kvService().get(key);

                    if (r instanceof KvService.GetResult.NotFound) {
                        HttpJson.json(ex, 404, Map.of("found", false));
                        return;
                    }

                    KvService.GetResult.Found f = (KvService.GetResult.Found) r;

                    ex.getResponseHeaders().set("Content-Type", "application/octet-stream");
                    ex.getResponseHeaders().set("X-KV-Ts", Long.toString(f.ts()));
                    ex.getResponseHeaders().set("X-KV-NodeId", f.nodeId());
                    ex.sendResponseHeaders(200, f.payload().length);

                    try (OutputStream os = ex.getResponseBody()) {
                        os.write(f.payload());
                    }
                }

                case "DELETE" -> {
                    KvService.DeleteResult r =
                            ctx.kvService().delete(key, ctx.config().nodeId());

                    HttpJson.json(ex, 200, Map.of(
                            "ok", true,
                            "deleted", true,
                            "key", key,
                            "applied", r.applied(),
                            "ts", r.ts()
                    ));
                }

                default -> HttpJson.json(ex, 405, Map.of("error", "method not allowed"));
            }
        } catch (Exception e) {
            // Handler should not know about RocksDB; map to generic 500
            HttpJson.json(ex, 500, Map.of("error", "internal", "message", e.getMessage()));
        }
    }
}