package dev.ikm.ds.rocks.maps;

import dev.ikm.ds.rocks.KeyUtil;
import dev.ikm.ds.rocks.EntityKey;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

public class NidEntityKeyMap
        extends RocksDbMap {

    enum Mode {
        /**
         * UUIDs are not yet stored in the {@code uuidEntityKeyMap} and the db has not been checked for values.
         */
        NOT_STARTED,
        /**
         * All UUIDs are stored in the {@code uuidEntityKeyMap}, no need to check DB.
         */
        ALL_IN_MEMORY,
        /**
         * UUIDs are lazily retrieved from the DB and stored in the {@code uuidEntityKeyMap}.
         * UUIDs will remain in the {@code uuidEntityKeyMap} until removed, so new UUIDs will in memory, and
         * will need to be written to the DB before closing the db.
         */
        CACHING
    }

    final boolean loadOnStart = true;

    final AtomicReference<UuidEntityKeyNidMap.Mode> memoryMode = new AtomicReference<>(UuidEntityKeyNidMap.Mode.NOT_STARTED);

    final ConcurrentHashMap<Integer, EntityKey> nidEntityKeyMap = new ConcurrentHashMap<>();

    public NidEntityKeyMap(RocksDB db, ColumnFamilyHandle mapHandle) {
        super(db, mapHandle);
    }

    @Override
    protected void writeMemoryToDb() {
        try (org.rocksdb.WriteBatch batch = new org.rocksdb.WriteBatch()) {
            nidEntityKeyMap.forEach((nid, entityKey) -> {
                try {
                    batch.put(mapHandle, KeyUtil.intToByteArray(nid), entityKey.toBytes());
                    nidEntityKeyMap.remove(nid);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to add to batch: " + nid, e);
                }
            });
            db.write(new org.rocksdb.WriteOptions(), batch);
        } catch (Exception e) {
            throw new RuntimeException("Failed to write batch to RocksDB", e);
        }
    }

    public void put(EntityKey key) {
        nidEntityKeyMap.put(key.nid(), key);
    }

    public EntityKey get(int nid) {
        EntityKey entityKey = nidEntityKeyMap.get(nid);
        if (entityKey != null) {
            return entityKey;
        }
        entityKey = KeyUtil.bytesToEntityKey(get(KeyUtil.intToByteArray(nid)));
        nidEntityKeyMap.put(nid, entityKey);
        return entityKey;
    }

    public long getLongKey(int nid) {
        return get(nid).longKey();
    }


    @Override
    protected void closeMap() {
        // Nothing buffered, all direct writes to the DB. So no need to write before the flush.
    }

}