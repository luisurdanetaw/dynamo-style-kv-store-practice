package com.luisurdaneta.kv.core.ports;

import com.luisurdaneta.kv.core.model.VersionedValue;

public interface PeerClient {
    boolean putReplica(String peerBaseUrl, String key, VersionedValue record) throws Exception;
}
