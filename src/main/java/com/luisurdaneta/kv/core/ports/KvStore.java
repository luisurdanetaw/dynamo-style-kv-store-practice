package com.luisurdaneta.kv.core.ports;


import com.luisurdaneta.kv.core.model.VersionedValue;

public interface KvStore extends AutoCloseable {
    VersionedValue get(String key) throws Exception;

    boolean putIfNewer(String key, VersionedValue candidate) throws Exception;

    @Override void close();
}