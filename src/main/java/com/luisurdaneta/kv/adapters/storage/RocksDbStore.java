package com.luisurdaneta.kv.adapters.storage;

import com.luisurdaneta.kv.core.model.VersionedValue;
import com.luisurdaneta.kv.core.ports.KvStore;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.BiPredicate;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class RocksDbStore implements KvStore {
    static { RocksDB.loadLibrary(); }

    private final Options options;
    private final RocksDB db;

    public RocksDbStore(String path) throws RocksDBException {
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

    @Override
    public VersionedValue get(String key) throws RocksDBException {
        byte[] v = db.get(key.getBytes(StandardCharsets.UTF_8));
        return VersionedValue.fromBytes(v);
    }

    @Override
    public boolean putIfNewer(String key, VersionedValue candidate) throws RocksDBException {
        byte[] k = key.getBytes(StandardCharsets.UTF_8);

        byte[] raw = db.get(k);
        VersionedValue existing = (raw == null) ? null : VersionedValue.fromBytes(raw);

        if (candidate.isNewerThan(existing)) {
            db.put(k, candidate.toBytes());
            return true;
        }
        return false;
    }

    /**
     * Iterates all keys in lexicographic order starting strictly after startAfterExclusive.
     * The RocksDB iterator is scoped to this call and closed before returning.
     */
    @Override
    public void scan(String startAfterExclusive, BiPredicate<String, VersionedValue> visitor)
            throws RocksDBException {
        try (RocksIterator it = db.newIterator()) {
            if (startAfterExclusive != null) {
                byte[] startBytes = startAfterExclusive.getBytes(StandardCharsets.UTF_8);
                it.seek(startBytes);
                // Skip the cursor key itself (exclusive start)
                if (it.isValid() && Arrays.equals(it.key(), startBytes)) {
                    it.next();
                }
            } else {
                it.seekToFirst();
            }

            while (it.isValid()) {
                String key = new String(it.key(), StandardCharsets.UTF_8);
                VersionedValue vv = VersionedValue.fromBytes(it.value());
                if (vv == null) { it.next(); continue; } // skip malformed entries
                if (!visitor.test(key, vv)) break;
                it.next();
            }
        }
    }

    /** Physical delete — no tombstone, does not propagate. */
    @Override
    public void drop(String key) throws RocksDBException {
        db.delete(key.getBytes(StandardCharsets.UTF_8));
    }

    public RocksDB db() { return db; }

    @Override
    public void close() {
        db.close();
        options.close();
    }
}
