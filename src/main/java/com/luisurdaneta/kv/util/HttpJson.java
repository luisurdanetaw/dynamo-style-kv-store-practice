package com.luisurdaneta.kv.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;

public final class HttpJson {
    private HttpJson() {}

    public static final ObjectMapper MAPPER = new ObjectMapper();

    public static void json(HttpExchange ex, int status, Object obj) throws IOException {
        byte[] body = MAPPER.writeValueAsBytes(obj);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.sendResponseHeaders(status, body.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(body);
        }
    }
}