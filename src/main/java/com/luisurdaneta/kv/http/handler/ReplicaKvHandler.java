package com.luisurdaneta.kv.http.handler;

import com.luisurdaneta.kv.adapters.peer.ReplicaCodec;
import com.luisurdaneta.kv.adapters.peer.ReplicaRecordDto;
import com.luisurdaneta.kv.core.model.VersionedValue;
import com.luisurdaneta.kv.core.service.ReplicaKvService;
import com.luisurdaneta.kv.http.NodeContext;
import com.luisurdaneta.kv.util.HttpJson;
import com.luisurdaneta.kv.util.PathParams;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.util.Map;


public final class ReplicaKvHandler implements HttpHandler {
    private final NodeContext ctx;

    public ReplicaKvHandler(NodeContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void handle(HttpExchange ex) throws IOException {
        try {
            String key = PathParams.pathParamAfterPrefix(ex, "/internal/replica/kv/");
            if (key == null || key.isBlank()) {
                HttpJson.json(ex, 400, Map.of("error", "missing key"));
                return;
            }

            switch (ex.getRequestMethod()) {
                case "PUT" -> {
                    byte[] body = ex.getRequestBody().readAllBytes();

                    ReplicaRecordDto dto = HttpJson.MAPPER.readValue(body, ReplicaRecordDto.class);
                    VersionedValue incoming = ReplicaCodec.fromDto(dto);

                    ReplicaKvService.PutReplicaResult r =
                            ctx.replicaKvService().putReplica(key, incoming);

                    HttpJson.json(ex, 200, Map.of(
                            "ok", true,
                            "key", key,
                            "applied", r.applied(),
                            "ts", r.ts(),
                            "nodeId", r.nodeId(),
                            "tombstone", r.tombstone()
                    ));
                }

                case "GET" -> {
                    ReplicaKvService.GetReplicaResult r =
                            ctx.replicaKvService().getReplica(key);

                    if (r instanceof ReplicaKvService.GetReplicaResult.NotFound) {
                        HttpJson.json(ex, 404, Map.of("found", false));
                        return;
                    }

                    ReplicaKvService.GetReplicaResult.Found f =
                            (ReplicaKvService.GetReplicaResult.Found) r;

                    HttpJson.json(ex, 200, Map.of(
                            "found", true,
                            "key", key,
                            "record", ReplicaCodec.toDto(f.record())
                    ));
                }

                default -> HttpJson.json(ex, 405, Map.of("error", "method not allowed"));
            }
        } catch (Exception e) {
            HttpJson.json(ex, 400, Map.of("error", "bad_request", "message", e.getMessage()));
        }
    }
}