package com.luisurdaneta.kv.core.ports;

import com.luisurdaneta.kv.core.model.VersionedValue;

import java.util.function.BiPredicate;

public interface KvStore extends AutoCloseable {
    VersionedValue get(String key) throws Exception;

    boolean putIfNewer(String key, VersionedValue candidate) throws Exception;

    /**
     * Iterates keys in lexicographic order starting strictly after startAfterExclusive
     * (null = from the beginning). Calls visitor for each key/value pair. The visitor
     * returns true to continue, false to stop early. The underlying iterator is closed
     * before this method returns — it is never held open across a network call.
     */
    void scan(String startAfterExclusive, BiPredicate<String, VersionedValue> visitor) throws Exception;

    /**
     * Physical delete — removes the key from local storage without creating a tombstone.
     * Used by the rebalancer after confirming the new owners have the data.
     */
    void drop(String key) throws Exception;

    @Override void close();
}
