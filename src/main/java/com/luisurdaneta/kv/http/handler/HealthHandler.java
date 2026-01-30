package com.luisurdaneta.kv.http.handler;

import com.luisurdaneta.kv.util.HttpJson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

public final class HealthHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange ex) throws IOException {
        HttpJson.json(ex, 200, Map.of("ok", true, "ts", Instant.now().toString()));
    }
}
