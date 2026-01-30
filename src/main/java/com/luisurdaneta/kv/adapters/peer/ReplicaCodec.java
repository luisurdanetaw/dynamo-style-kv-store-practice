package com.luisurdaneta.kv.adapters.peer;

import com.luisurdaneta.kv.core.model.VersionedValue;

import java.util.Base64;

public final class ReplicaCodec {
    private static final Base64.Decoder DEC = Base64.getDecoder();
    private static final Base64.Encoder ENC = Base64.getEncoder();

    public static ReplicaRecordDto toDto(VersionedValue vv) {
        if (vv == null) return null;
        String b64 = vv.payload == null ? "" : ENC.encodeToString(vv.payload);
        return new ReplicaRecordDto(vv.ts, vv.nodeId, vv.tombstone, b64);
    }

    public static VersionedValue fromDto(ReplicaRecordDto dto) {
        if (dto == null) return null;
        byte[] payload = (dto.payloadB64 == null || dto.payloadB64.isEmpty())
                ? new byte[0]
                : DEC.decode(dto.payloadB64);
        return new VersionedValue(dto.ts, dto.nodeId, dto.tombstone, payload);
    }
}