package com.luisurdaneta.kv.http.handler;

import com.luisurdaneta.kv.adapters.peer.ReplicaCodec;
import com.luisurdaneta.kv.core.ring.ConsistentHashRing;
import com.luisurdaneta.kv.http.NodeContext;
import com.luisurdaneta.kv.rebalance.TokenRange;
import com.luisurdaneta.kv.util.HttpJson;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * GET /internal/transfer/range
 *
 * Query params:
 *   start            long  — exclusive arc start (TokenRange.start)
 *   end              long  — inclusive arc end   (TokenRange.end)
 *   cursor           String — last key scanned by the previous page (absent on first call)
 *   pageSize         int   — max matching entries to return (default 500)
 *   membershipVersion long  — version stamp from the requesting node (informational only)
 *
 * Response:
 *   { "entries": [{key, record},...], "nextCursor": "<key>" | "" }
 *   nextCursor == "" (empty) means the keyspace was exhausted — no further pages needed.
 *
 * Membership-version mismatch: logged but always served.
 * Iterator lifecycle: opened and closed within this single call — never held across network.
 */
public final class TransferHandler implements HttpHandler {

    private static final System.Logger LOG = System.getLogger(TransferHandler.class.getName());
    private static final int DEFAULT_PAGE_SIZE = 500;

    private final NodeContext ctx;

    public TransferHandler(NodeContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public void handle(HttpExchange ex) throws IOException {
        if (!"GET".equals(ex.getRequestMethod())) {
            HttpJson.json(ex, 405, Map.of("error", "method not allowed"));
            return;
        }

        Map<String, String> params = parseQuery(ex.getRequestURI().getQuery());

        long rangeStart, rangeEnd;
        int  pageSize;
        long membershipVersion;
        String cursor; // may be null (first page)

        try {
            rangeStart        = Long.parseLong(params.get("start"));
            rangeEnd          = Long.parseLong(params.get("end"));
            pageSize          = params.containsKey("pageSize")
                                ? Integer.parseInt(params.get("pageSize"))
                                : DEFAULT_PAGE_SIZE;
            membershipVersion = Long.parseLong(params.getOrDefault("membershipVersion", "0"));
            cursor            = params.get("cursor"); // null if absent
            if (cursor != null && cursor.isEmpty()) cursor = null;
        } catch (Exception e) {
            HttpJson.json(ex, 400, Map.of("error", "bad params", "message", e.getMessage()));
            return;
        }

        // Version stamp: informational — log if mismatched but always serve.
        long localVersion = ctx.membershipView().version();
        if (membershipVersion != localVersion) {
            LOG.log(System.Logger.Level.WARNING,
                    "Transfer request membershipVersion=" + membershipVersion
                    + " but local=" + localVersion + "; serving anyway");
        }

        TokenRange range = new TokenRange(rangeStart, rangeEnd);
        List<Map<String, Object>> entries = new ArrayList<>(Math.min(pageSize, 256));
        String[] nextCursorHolder = {null};
        int[]    count            = {0};

        try {
            // Iterator is opened inside scan() and closed before scan() returns.
            ctx.kvStore().scan(cursor, (key, vv) -> {
                nextCursorHolder[0] = key; // track last visited key
                if (range.contains(ConsistentHashRing.hash64(key))) {
                    entries.add(Map.of("key", key, "record", ReplicaCodec.toDto(vv)));
                    count[0]++;
                }
                return count[0] < pageSize; // false = stop (page full)
            });
        } catch (Exception e) {
            HttpJson.json(ex, 500, Map.of("error", "scan failed", "message", e.getMessage()));
            return;
        }

        // If we filled the page the visitor returned false; there may be more keys.
        // If we didn't fill the page the keyspace was exhausted.
        boolean exhausted = count[0] < pageSize;
        String nextCursor = exhausted ? "" : (nextCursorHolder[0] != null ? nextCursorHolder[0] : "");

        HttpJson.json(ex, 200, Map.of(
                "entries",    entries,
                "nextCursor", nextCursor
        ));
    }

    private static Map<String, String> parseQuery(String query) {
        if (query == null || query.isEmpty()) return Collections.emptyMap();
        Map<String, String> map = new HashMap<>();
        for (String pair : query.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                String k = URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8);
                String v = URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
                map.put(k, v);
            }
        }
        return map;
    }
}
