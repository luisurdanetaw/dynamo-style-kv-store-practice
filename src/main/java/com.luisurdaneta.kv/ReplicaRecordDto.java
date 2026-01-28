package com.luisurdaneta.kv;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class ReplicaRecordDto {
    public final long ts;
    public final String nodeId;
    public final boolean tombstone;
    public final String payloadB64; // empty when tombstone is true

    @JsonCreator
    public ReplicaRecordDto(
            @JsonProperty("ts") long ts,
            @JsonProperty("nodeId") String nodeId,
            @JsonProperty("tombstone") boolean tombstone,
            @JsonProperty("payloadB64") String payloadB64
    ) {
        this.ts = ts;
        this.nodeId = nodeId;
        this.tombstone = tombstone;
        this.payloadB64 = payloadB64;
    }
}
