package com.luisurdaneta.kv.core.ports;

import com.luisurdaneta.kv.core.model.VersionedValue;
import com.luisurdaneta.kv.http.Node;

import java.time.Duration;

public interface PeerClient {
    ReplicaPutAck putReplica(Node peer, String key, VersionedValue vv, Duration timeout) throws Exception;

    ReplicaGetAck getReplica(Node peer, String key, Duration timeout) throws Exception;

    record ReplicaPutAck(String peerId, boolean ok, boolean applied, int statusCode) {}

    record ReplicaGetAck(String peerId, boolean ack, int statusCode, VersionedValue recordOrNull) {}
}
