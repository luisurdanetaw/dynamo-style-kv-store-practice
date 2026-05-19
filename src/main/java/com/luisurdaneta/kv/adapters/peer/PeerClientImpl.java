package com.luisurdaneta.kv.adapters.peer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.luisurdaneta.kv.core.model.VersionedValue;
import com.luisurdaneta.kv.core.ports.PeerClient;
import com.luisurdaneta.kv.http.Node;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class PeerClientImpl implements PeerClient {
    private final ObjectMapper mapper;

    public PeerClientImpl(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public ReplicaPutAck putReplica(Node peer, String key, VersionedValue vv, Duration timeout) throws Exception {
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        String url = peer.baseUrl() + "/internal/replica/kv/" + encodedKey;

        byte[] body = mapper.writeValueAsBytes(ReplicaCodec.toDto(vv));

        var resp = PeerHttpClient.putReplica(url, body, timeout);

        int code = resp.statusCode();
        if (code != 200) return new ReplicaPutAck(peer.id(), false, false, code);

        JsonNode json = mapper.readTree(resp.body());
        boolean ok      = json.path("ok").asBoolean(false);
        boolean applied = json.path("applied").asBoolean(false);

        return new ReplicaPutAck(peer.id(), ok, applied, code);
    }

    @Override
    public ReplicaGetAck getReplica(Node peer, String key, Duration timeout) throws Exception {
        String url = peer.baseUrl() + "/internal/replica/kv/"
                + URLEncoder.encode(key, StandardCharsets.UTF_8);

        var resp = PeerHttpClient.getBytes(url, timeout);
        int code = resp.statusCode();

        if (code == 404) {
            return new ReplicaGetAck(peer.id(), true, 404, null);
        }
        if (code != 200) {
            return new ReplicaGetAck(peer.id(), false, code, null);
        }

        JsonNode json = mapper.readTree(resp.body());
        JsonNode recordNode = json.get("record");
        if (recordNode == null || recordNode.isNull()) {
            return new ReplicaGetAck(peer.id(), true, 200, null);
        }

        ReplicaRecordDto dto = mapper.treeToValue(recordNode, ReplicaRecordDto.class);
        VersionedValue vv = ReplicaCodec.fromDto(dto);
        return new ReplicaGetAck(peer.id(), true, 200, vv);
    }

    @Override
    public TransferPage pullRangePage(Node source,
                                      long rangeStart, long rangeEnd,
                                      String cursor, int pageSize,
                                      long membershipVersion, Duration timeout) throws Exception {
        StringBuilder url = new StringBuilder(source.baseUrl())
                .append("/internal/transfer/range")
                .append("?start=").append(rangeStart)
                .append("&end=").append(rangeEnd)
                .append("&pageSize=").append(pageSize)
                .append("&membershipVersion=").append(membershipVersion);
        if (cursor != null) {
            url.append("&cursor=")
               .append(URLEncoder.encode(cursor, StandardCharsets.UTF_8));
        }

        var resp = PeerHttpClient.getBytes(url.toString(), timeout);
        if (resp.statusCode() != 200) {
            throw new RuntimeException("pullRangePage from " + source.id()
                    + " returned HTTP " + resp.statusCode());
        }

        JsonNode json  = mapper.readTree(resp.body());
        JsonNode items = json.path("entries");

        List<TransferEntry> entries = new ArrayList<>();
        for (JsonNode e : items) {
            String key = e.path("key").asText();
            ReplicaRecordDto dto = mapper.treeToValue(e.path("record"), ReplicaRecordDto.class);
            entries.add(new TransferEntry(key, ReplicaCodec.fromDto(dto)));
        }

        // Empty string sentinel means no next page (Jackson serialises null as "")
        String next = json.path("nextCursor").asText(null);
        if (next != null && next.isEmpty()) next = null;

        return new TransferPage(Collections.unmodifiableList(entries), next);
    }
}
