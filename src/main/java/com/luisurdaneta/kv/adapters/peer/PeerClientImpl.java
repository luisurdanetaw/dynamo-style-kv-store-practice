package com.luisurdaneta.kv.adapters.peer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.luisurdaneta.kv.core.model.VersionedValue;
import com.luisurdaneta.kv.core.ports.PeerClient;
import com.luisurdaneta.kv.http.Node;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public final class PeerClientImpl implements PeerClient {
    private final ObjectMapper mapper;

    public PeerClientImpl(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public ReplicaPutAck putReplica(Node peer, String key, VersionedValue vv, Duration timeout) throws Exception {
        String encodedKey = URLEncoder.encode(key, StandardCharsets.UTF_8);
        String url = peer.baseUrl() + "/internal/replica/kv/" + encodedKey;

        // You already have these:
        // ReplicaCodec.toDto(vv) -> ReplicaRecordDto -> JSON
        byte[] body = mapper.writeValueAsBytes(ReplicaCodec.toDto(vv));

        var resp = PeerHttpClient.putReplica(url, body, timeout);

        int code = resp.statusCode();
        if (code != 200) return new ReplicaPutAck(peer.id(), false, false, code);

        JsonNode json = mapper.readTree(resp.body());
        boolean ok = json.path("ok").asBoolean(false);
        boolean applied = json.path("applied").asBoolean(false);

        return new ReplicaPutAck(peer.id(), ok, applied, code);
    }

    @Override
    public ReplicaGetAck getReplica(Node peer, String key, Duration timeout) throws Exception {
        String url = peer.baseUrl() + "/internal/replica/kv/" + URLEncoder.encode(key, StandardCharsets.UTF_8);

        var resp = PeerHttpClient.getBytes(url, timeout);
        int code = resp.statusCode();

        if (code == 404) {
            // peer responded "not found" => counts toward quorum
            return new ReplicaGetAck(peer.id(), true, 404, null);
        }
        if (code != 200) {
            return new ReplicaGetAck(peer.id(), false, code, null);
        }

        JsonNode json = mapper.readTree(resp.body());
        // json: { found:true, key:..., record:{...dto...} }
        JsonNode recordNode = json.get("record");
        if (recordNode == null || recordNode.isNull()) {
            // defensive: treat as not found, but still ack
            return new ReplicaGetAck(peer.id(), true, 200, null);
        }

        ReplicaRecordDto dto = mapper.treeToValue(recordNode, ReplicaRecordDto.class);
        VersionedValue vv = ReplicaCodec.fromDto(dto);

        return new ReplicaGetAck(peer.id(), true, 200, vv);
    }
}