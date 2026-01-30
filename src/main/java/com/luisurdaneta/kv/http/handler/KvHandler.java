package com.luisurdaneta.kv.http.handler;

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

                    var r = ctx.writeCoordinatorService().put(key, body);

                    if (!r.ok()) {
                        HttpJson.json(ex, 503, Map.of(
                                "ok", false,
                                "error", "write_quorum_failed",
                                "acks", r.acks(),
                                "required", r.required()
                        ));
                        return;
                    }

                    HttpJson.json(ex, 200, Map.of(
                            "ok", true,
                            "key", key,
                            "acks", r.acks(),
                            "required", r.required(),
                            "ts", r.ts(),
                            "nodeId", r.originNodeId()
                    ));
                }

                case "DELETE" -> {
                    var r = ctx.writeCoordinatorService().delete(key);

                    if (!r.ok()) {
                        HttpJson.json(ex, 503, Map.of(
                                "ok", false,
                                "error", "write_quorum_failed",
                                "acks", r.acks(),
                                "required", r.required()
                        ));
                        return;
                    }

                    HttpJson.json(ex, 200, Map.of(
                            "ok", true,
                            "deleted", true,
                            "key", key,
                            "acks", r.acks(),
                            "required", r.required(),
                            "ts", r.ts()
                    ));
                }

                case "GET" -> {
                    var rr = ctx.readCoordinatorService().get(key);

                    if (!rr.ok()) {
                        HttpJson.json(ex, 503, Map.of(
                                "ok", false,
                                "error", "read_quorum_failed",
                                "acks", rr.acks(),
                                "required", rr.required()
                        ));
                        return;
                    }

                    if (!rr.found()) {
                        HttpJson.json(ex, 404, Map.of("found", false));
                        return;
                    }

                    ex.getResponseHeaders().set("Content-Type", "application/octet-stream");
                    ex.getResponseHeaders().set("X-KV-Ts", Long.toString(rr.ts()));
                    ex.getResponseHeaders().set("X-KV-NodeId", rr.nodeId());
                    ex.sendResponseHeaders(200, rr.payload().length);
                    try (OutputStream os = ex.getResponseBody()) {
                        os.write(rr.payload());
                    }
                }

                default -> HttpJson.json(ex, 405, Map.of("error", "method not allowed"));
            }
        } catch (Exception e) {
            HttpJson.json(ex, 500, Map.of("error", "internal", "message", e.getMessage()));
        }
    }
}