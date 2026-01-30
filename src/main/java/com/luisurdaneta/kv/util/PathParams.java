package com.luisurdaneta.kv.util;

import com.sun.net.httpserver.HttpExchange;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public final class PathParams {
    private PathParams() {}

    public static String pathParamAfterPrefix(HttpExchange ex, String prefix) {
        String p = ex.getRequestURI().getPath();
        if (!p.startsWith(prefix)) return null;
        String raw = p.substring(prefix.length());
        if (raw.isEmpty()) return null;
        return URLDecoder.decode(raw, StandardCharsets.UTF_8);
    }
}
