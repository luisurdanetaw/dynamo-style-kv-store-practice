package com.luisurdaneta.kv;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.nio.charset.StandardCharsets;

import org.rocksdb.CompressionType;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class Storage implements AutoCloseable {
    static { RocksDB.loadLibrary(); }

    private final Options options;
    private final RocksDB db;

    public Storage(String path) throws RocksDBException {
        Path p = Paths.get(path);

        try {
            Files.createDirectories(p);
        } catch (FileAlreadyExistsException e) {
            throw new IllegalStateException("DB_PATH exists but is not a directory: " + p, e);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create DB_PATH directories: " + p, e);
        }

        this.options = new Options()
                .setCreateIfMissing(true)
                .setCompressionType(CompressionType.LZ4_COMPRESSION);

        try {
            this.db = RocksDB.open(options, p.toString());
        } catch (RocksDBException e) {
            options.close();
            throw e;
        }
    }

    public VersionedValue get(String key) throws RocksDBException {
        byte[] v = db.get(key.getBytes(StandardCharsets.UTF_8));
        return VersionedValue.fromBytes(v);
    }

    public void putIfNewer(String key, VersionedValue candidate) throws RocksDBException {
        VersionedValue existing = get(key);
        if (candidate.isNewerThan(existing)) {
            db.put(key.getBytes(StandardCharsets.UTF_8), candidate.toBytes());
        }
    }

    public boolean putIfNewerApplied(String key, VersionedValue candidate) throws RocksDBException {
        byte[] k = key.getBytes(StandardCharsets.UTF_8);

        // Single GET + conditional PUT
        VersionedValue existing = VersionedValue.fromBytes(db.get(k));
        if (candidate.isNewerThan(existing)) {
            db.put(k, candidate.toBytes());
            return true;
        }
        return false;
    }

    public RocksDB db() { return db; }

    @Override
    public void close() {
        db.close();
        options.close();
    }
}
