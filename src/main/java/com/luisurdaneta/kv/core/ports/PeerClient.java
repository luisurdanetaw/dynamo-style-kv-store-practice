package com.luisurdaneta.kv.core.ports;

import com.luisurdaneta.kv.core.model.VersionedValue;
import com.luisurdaneta.kv.http.Node;

import java.time.Duration;
import java.util.List;

public interface PeerClient {
    ReplicaPutAck putReplica(Node peer, String key, VersionedValue vv, Duration timeout) throws Exception;

    ReplicaGetAck getReplica(Node peer, String key, Duration timeout) throws Exception;

    /**
     * Paginated range pull used by the rebalancer.
     *
     * cursor = null starts from the beginning of the keyspace; otherwise it is the
     * last key returned by the previous page (exclusive). A null nextCursor in the
     * returned page means the source keyspace was exhausted — no further pages needed.
     */
    TransferPage pullRangePage(Node source,
                               long rangeStart, long rangeEnd,
                               String cursor, int pageSize,
                               long membershipVersion, Duration timeout) throws Exception;

    record ReplicaPutAck(String peerId, boolean ok, boolean applied, int statusCode) {}

    record ReplicaGetAck(String peerId, boolean ack, int statusCode, VersionedValue recordOrNull) {}

    record TransferEntry(String key, VersionedValue value) {}

    record TransferPage(List<TransferEntry> entries, String nextCursor /* null = done */) {}
}
