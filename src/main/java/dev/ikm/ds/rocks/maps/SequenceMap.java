package dev.ikm.ds.rocks.maps;

import dev.ikm.ds.rocks.KeyUtil;
import dev.ikm.ds.rocks.EntityKey;
import dev.ikm.ds.rocks.SpliteratorForEntityKeys;
import dev.ikm.ds.rocks.SpliteratorForLongKeyOfPattern;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static dev.ikm.tinkar.common.service.PrimitiveDataService.FIRST_NID;

public class SequenceMap extends RocksDbMap<RocksDB> {
    private static final Logger LOG = LoggerFactory.getLogger(SequenceMap.class);

    public static final int FIRST_ELEMENT_SEQUENCE_OF_PATTERN = 1;
    /**
     * Pattern sequence for all patterns.
     * - In two’s complement, -1 is all 1s. As a 16-bit value: 0xFFFF.
     * - The maximum 16-bit unsigned integer is also 0xFFFF, which equals 65535.
     */
    public static final int PATTERN_PATTERN_SEQUENCE = 0xFFFF;

    private static int nextPatternElementSequence = FIRST_ELEMENT_SEQUENCE_OF_PATTERN;
    private static int nextNid = FIRST_NID;
    /**
     * TODO: Temporary fixed UUID until patterns entities provide their own pattern PublicId field (currently only semantics do).
     */
    public static final UUID patternPatternUUID = UUID.fromString("8521a438-f84f-4f84-9f99-35aab5b10bd9");
    private static final int patternPatternElementSequence = nextPatternElementSequence++;
    private static final int patternPatternNid = nextNid++;

    public static EntityKey patternPatternEntityKey() {
        return EntityKey.of(PATTERN_PATTERN_SEQUENCE, patternPatternElementSequence, patternPatternNid);
    }
    /**
     * TODO: Temporary fixed UUID until concepts provide their own pattern PublicId field (currently only semantics do).
     */
    public static final UUID conceptPatternUUID = UUID.fromString("8e9a8888-9d06-45c8-af47-aacc78ed66ee");
    private static final int conceptPatternElementSequence = nextPatternElementSequence++;
    private static final int conceptPatternNid = nextNid++;

    public static EntityKey conceptPatternEntityKey() {
        return EntityKey.of(PATTERN_PATTERN_SEQUENCE, conceptPatternElementSequence, conceptPatternNid);
    }
    /**
     * TODO: Temporary fixed UUID until concepts provide their own pattern PublicId field (currently only semantics do).
     */
    public static final UUID stampPatternUUID = UUID.fromString("15687f5d-6028-4491-b005-7bb6f9f6ebad");
    private static final int stampPatternElementSequence = nextPatternElementSequence++;
    private static final int stampPatternNid = nextNid++;

    public static EntityKey stampPatternEntityKey() {
        return EntityKey.of(PATTERN_PATTERN_SEQUENCE, stampPatternElementSequence, stampPatternNid);
    }


    /**
     * NIDS are for backward compatibility. To be removed at some point in the future.
     */
    private static final Integer NID_SEQUENCE_KEY = -1;
    /**
     * Map of pattern sequences to atomic counters used to generate a unique, ordered, sequence for each new element.
     */
    final ConcurrentHashMap<Integer, AtomicLong> nextSequenceMap = new ConcurrentHashMap<>();

    public SequenceMap(RocksDB db, ColumnFamilyHandle mapHandle) {
        super(db, mapHandle);
        open();
    }

    public String sequenceReport() {
        return nextSequenceMap.toString();
    }

    /**
     * Open the nextSequenceMap from RocksDB.
     */
    public void open() {
        try (RocksIterator it = rocksIterator()) {
            it.seekToFirst();
            if (it.isValid()) {
                // At least one entry: populate from DB.
                for (; it.isValid(); it.next()) {
                    byte[] keyBytes = it.key();
                    byte[] valueBytes = it.value();
                    if (keyBytes.length == 4 && valueBytes.length == 8) {
                        int key = KeyUtil.byteArrayToInt(keyBytes);
                        long value = KeyUtil.byteArrayToLong(valueBytes);
                        nextSequenceMap.put(key, new AtomicLong(value));
                    } else {
                        throw new IllegalStateException("key/value lengths out of bounds: " + keyBytes.length + "/" + valueBytes.length);
                    }
                }
            } else {
                // Column family is empty: do identifier bootstrap initialization.
                nextSequenceMap.put(PATTERN_PATTERN_SEQUENCE, new AtomicLong(nextPatternElementSequence)); // Pattern pattern sequences
                nextSequenceMap.put(NID_SEQUENCE_KEY, new AtomicLong(nextNid)); // NIDs
            }
        }
    }

    /**
     * Save the nextSequenceMap to RocksDB.
     */
    @Override
    protected void writeMemoryToDb() {
        try (WriteBatch batch = new WriteBatch();
             WriteOptions writeOptions = new WriteOptions()) {
            for (Map.Entry<Integer, AtomicLong> entry : nextSequenceMap.entrySet()) {
                int key = entry.getKey();
                long value = entry.getValue().get();
                batch.put(mapHandle, KeyUtil.intToByteArray(key), KeyUtil.longToByteArray(value));
            }
            db.write(writeOptions, batch);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void closeMap() {
        save();
    }

    /**
     * Get the next sequence for a given pattern. Represented as a 48-bit unsigned integer inside a long value.
     * @param patternSequence
     * @return next sequence for the given pattern.
     */
    public long nextElementSequence(int patternSequence) {
        return nextSequenceMap.computeIfAbsent(patternSequence, k -> new AtomicLong(FIRST_ELEMENT_SEQUENCE_OF_PATTERN)).getAndIncrement();
    }

    /**
     * Generates the next pattern sequence as a long value. This method ensures that
     * sequences are incrementally generated starting at 1. Sequences are maintained per
     * pattern group using a map with atomic counters to provide thread-safe increments.
     *
     * @return the next pattern sequence value starting from 1.
     */
    public int nextPatternSequence() {
        return (int) nextSequenceMap.get(PATTERN_PATTERN_SEQUENCE).getAndIncrement();
    }

    /**
     * Get the next NID.
     * @return the next nid.
     */
    public int nextNid() {
        return (int) nextSequenceMap.get(NID_SEQUENCE_KEY).getAndIncrement();
    }

    public Spliterator.OfLong allEntityLongKeySpliterator() {
        Collection<SpliteratorForLongKeyOfPattern> spliterators = nextSequenceMap.entrySet().stream()
                .filter(entry -> entry.getKey() != NID_SEQUENCE_KEY)
                .map(entry -> new SpliteratorForLongKeyOfPattern(entry.getKey(), FIRST_ELEMENT_SEQUENCE_OF_PATTERN,
                        entry.getValue().get()))
                .toList();
        return new SpliteratorForEntityKeys(spliterators);
    }

}
