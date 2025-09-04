package dev.ikm.ds.rocks;

import dev.ikm.ds.rocks.internal.Get;
import dev.ikm.ds.rocks.internal.Put;
import dev.ikm.ds.rocks.maps.*;
import dev.ikm.tinkar.common.alert.AlertStreams;
import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.service.*;
import dev.ikm.tinkar.common.util.io.FileUtil;
import dev.ikm.tinkar.common.util.time.Stopwatch;
import dev.ikm.tinkar.entity.*;
import dev.ikm.tinkar.provider.search.Indexer;
import dev.ikm.tinkar.provider.search.RecreateIndex;
import dev.ikm.tinkar.provider.search.Searcher;
import org.eclipse.collections.api.block.procedure.primitive.IntProcedure;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.ImmutableIntList;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongConsumer;
import java.util.function.ObjIntConsumer;

public class RocksProvider implements PrimitiveDataService, NidGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(RocksProvider.class);
    public static final long defaultCacheSize = 256L * 1024 * 1024; // 256 MB cache
    public static final int defaultBloomFilterBitsPerKey = 10;

    protected static final File defaultDataDirectory = new File("target/rocksdb/");

    private LongAdder writeSequence = new LongAdder();

    final Indexer indexer;
    final Searcher searcher;
    final String name;

    final StableValue<ImmutableList<ChangeSetWriterService>> changeSetWriterServices = StableValue.of();

    private final RocksDB db;
    private final EntityMap entityMap;
    private final EntityReferencingSemanticMap entityReferencingSemanticMap;
    private final SequenceMap sequenceMap;
    private final UuidEntityKeyMap uuidEntityKeyMap;

    private final List<ColumnFamilyHandle> columnHandles = new ArrayList<>();

    public enum ColumnFamily {
        DEFAULT(RocksDB.DEFAULT_COLUMN_FAMILY, -1, 0, 1024 * 1024),
        ENTITY_MAP("EntityMap", 2, defaultBloomFilterBitsPerKey, 512L * 1024 * 1024),
        ENTITY_REFERENCING_SEMANTIC_MAP("EntityReferencingSemanticMap", 16, defaultBloomFilterBitsPerKey, 10L * 1024 * 1024),
        UUID_ENTITY_KEY_MAP("UuidEntityKeyMap", -1, defaultBloomFilterBitsPerKey, 10L * 1024 * 1024);

        private final byte[] value;
        private final int keyPrefixBytes; // For compound keys. -1 if no prefix;
        private final int bloomFilterBitsPerKey;
        private final long writeBufferSize;

        ColumnFamily(byte[] value, int keyPrefixBytes,
                     int bloomFilterBitsPerKey, long writeBufferSize ) {
            this.value = value;
            this.keyPrefixBytes = keyPrefixBytes;
            this.bloomFilterBitsPerKey = bloomFilterBitsPerKey;
            this.writeBufferSize = writeBufferSize;
        }

        ColumnFamily(String stringValue, int keyPrefixBytes,
                     int bloomFilterBitsPerKey, long writeBufferSize) {
            this(stringValue.getBytes(), keyPrefixBytes, bloomFilterBitsPerKey, writeBufferSize);
        }

        public byte[] getValue() {
            return value;
        }
    }

    private List<ColumnFamilyDescriptor> columnDescriptors;

    private ImmutableList changeSetWriterServicesList() {
        ServiceLoader<ChangeSetWriterService> changeSetServiceLoader = PluggableService.load(ChangeSetWriterService.class);
        MutableList<ChangeSetWriterService> changeSetWriters = Lists.mutable.empty();
        changeSetServiceLoader.stream().forEach(changeSetProvider -> {
            changeSetWriters.add(changeSetProvider.get());
        });
        return changeSetWriters.toImmutable();
    }

    public static RocksProvider singleton;

    private final Cache blockCache;

    public RocksProvider() throws IOException {
        Stopwatch stopwatch = new Stopwatch();
        if (singleton != null) {
            throw new IllegalStateException("Singleton already exists");
        }
        singleton = this;
        Get.singleton = this;
        Put.singleton = this;

        LOG.info("Opening " + this.getClass().getSimpleName());
        File configuredRoot = ServiceProperties.get(ServiceKeys.DATA_STORE_ROOT, defaultDataDirectory);
        this.name = configuredRoot.getName();
        configuredRoot.mkdirs();
        LOG.info("Datastore root: " + configuredRoot.getAbsolutePath());
        File rockFiles = new File(configuredRoot, "rocks");
        boolean kbExists = rockFiles.exists();
        rockFiles.mkdirs();
        new File(configuredRoot, "rocks-logs").mkdirs();


        RocksDB.loadLibrary();
        configuredRoot.mkdirs();

        this.blockCache = new LRUCache(defaultCacheSize);
        this.columnDescriptors = Arrays.stream(ColumnFamily.values())
                .map(cf -> {

                    BlockBasedTableConfig tableCfg = new BlockBasedTableConfig()
                            .setBlockCache(this.blockCache)
                            .setFilterPolicy(new BloomFilter(cf.bloomFilterBitsPerKey, /*useBlockBasedBuilder*/ false))
                            .setWholeKeyFiltering(false)                    // favor prefix Bloom (when prefix extractor is configured)
                            .setCacheIndexAndFilterBlocks(true)            // keep index/filter hot
                            .setPinL0FilterAndIndexBlocksInCache(true)     // optional
                            .setBlockSize(16 * 1024)                       // 16 KB data blocks (tune for your workload)
                            .setChecksumType(ChecksumType.kXXH3)           // modern checksum
                            .setFormatVersion(5)                           // stable table format
                            .setPartitionFilters(true);                    // scalable filters for large data

                    ColumnFamilyOptions cfo = new ColumnFamilyOptions();
                    cfo.setCompressionType(CompressionType.NO_COMPRESSION);
                    cfo.setTableFormatConfig(tableCfg);
                    cfo.setWriteBufferSize(cf.writeBufferSize);
                    if (cf.keyPrefixBytes >= 0) {
                        cfo.useFixedLengthPrefixExtractor(cf.keyPrefixBytes);
                    }

                    return new ColumnFamilyDescriptor(cf.getValue(), cfo);
                }).toList();

        try (DBOptions options = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setIncreaseParallelism(Runtime.getRuntime().availableProcessors())
                .setMaxBackgroundJobs(Math.max(8, Runtime.getRuntime().availableProcessors()))
                .setAllowConcurrentMemtableWrite(true)
                .setEnablePipelinedWrite(true)
                .setBytesPerSync(8 * 1024 * 1024)
                .setWalBytesPerSync(8 * 1024 * 1024)
                // Configure RocksDB log directory (creates LOG, LOG.old.* here)
                .setDbLogDir(Path.of(configuredRoot.getPath(), "rocks-logs").toString())
                // Optional: adjust verbosity of RocksDB's internal logging
                .setInfoLogLevel(InfoLogLevel.INFO_LEVEL)
        ) {
            db = RocksDB.open(options, rockFiles.getAbsolutePath(), columnDescriptors, columnHandles);
            entityReferencingSemanticMap = new EntityReferencingSemanticMap(db, getHandle(ColumnFamily.ENTITY_REFERENCING_SEMANTIC_MAP));
            sequenceMap = new SequenceMap(db, getHandle(ColumnFamily.DEFAULT));
            uuidEntityKeyMap = new UuidEntityKeyMap(db, getHandle(ColumnFamily.UUID_ENTITY_KEY_MAP), sequenceMap);
            entityMap = new EntityMap(db, getHandle(ColumnFamily.ENTITY_MAP), uuidEntityKeyMap);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

        Path indexPath = Path.of(configuredRoot.getPath(),"lucene");
        boolean indexExists = Files.exists(indexPath);
        Indexer indexer;
        try {
            indexer = new Indexer(indexPath);
        } catch (IllegalArgumentException ex) {
            // If Indexer Codec does not match, then delete and rebuild with new Codec
            FileUtil.recursiveDelete(indexPath.toFile());
            indexExists = Files.exists(indexPath);
            indexer = new Indexer(indexPath);
        }
        this.indexer = indexer;
        this.searcher = new Searcher();
        if (kbExists && !indexExists) {
            try {
                this.recreateLuceneIndex().get();
            } catch (Exception e) {
                LOG.error(e.getLocalizedMessage(), e);
            }
        }
        // Prime TypeAheadSearch
        //TypeAheadSearch.get();
        stopwatch.stop();
        LOG.info("Opened RocksProvider in: " + stopwatch.durationString());
    }

    public RocksDB getDb() {
        return db;
    }

    public ColumnFamilyHandle getHandle(ColumnFamily columnFamily) {
        return columnHandles.get(columnFamily.ordinal());
    }

    public void save() {
        entityMap.save();
        entityReferencingSemanticMap.save();
        sequenceMap.save();
        uuidEntityKeyMap.save();
        try (FlushOptions flushOptions = new FlushOptions()) {
            db.flush(flushOptions);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            if (this.entityMap != null) {
                this.entityMap.close();
            }
        } catch (Exception e) {
            LOG.warn("Error closing entityMap", e);
        }
        try {
            if (this.entityReferencingSemanticMap != null) {
                this.entityReferencingSemanticMap.close();
            }
        } catch (Exception e) {
            LOG.warn("Error closing entityReferencingSemanticMap", e);
        }
        try {
            if (this.uuidEntityKeyMap != null) {
                this.uuidEntityKeyMap.close();
            }
        } catch (Exception e) {
            LOG.warn("Error closing uuidEntityKeyNidMap", e);
        }
        try {
            if (this.sequenceMap != null) {
                this.sequenceMap.close();
            }
        } catch (Exception e) {
            LOG.warn("Error closing sequenceMap", e);
        }
        try {
            if (this.db != null) {
                this.db.close();
            }
        } catch (Exception e) {
            LOG.warn("Error closing RocksDB", e);
        }
        try {
            if (this.blockCache != null) {
                this.blockCache.close();
            }
        } catch (Exception e) {
            LOG.warn("Error closing blockCache", e);
        }
        try {
            if (this.indexer != null) {
                this.indexer.close();
            }
        } catch (Exception e) {
            LOG.warn("Error closing Indexer: {}", e.getMessage(), e);
        }
    }

    public EntityKey getEntityKey(PublicId patternId, PublicId entityId) {
        return uuidEntityKeyMap.getEntityKey(patternId, entityId);
    }

    public String sequenceReport() {
        return sequenceMap.sequenceReport();
    }

    @Override
    public int newNid() {
        throw new UnsupportedOperationException();
    }


    @Override
    public long writeSequence() {
        return writeSequence.sum();
    }

    public long sequenceForNid(int nid) {
        return NidCodec6.decodeElementSequence(nid);
    }
    public long longKeyForNid(int nid) {
        return NidCodec6.longKeyForNid(nid);
    }

    public int stampSequenceForStampNid(int nid) {
        return (int) NidCodec6.decodeElementSequence(nid);
    }

    @Override
    public int nidForUuids(UUID... uuids) {
        for (UUID uuid: uuids) {
            Optional<EntityKey> optionalKey = uuidEntityKeyMap.getEntityKey(uuid);
            if (optionalKey.isPresent()) {
                return optionalKey.get().nid();
            }
        }
        throw new IllegalStateException("No entity key found for UUIDs: " + Arrays.toString(uuids));
    }

    @Override
    public int nidForUuids(ImmutableList<UUID> uuidList) {
        return nidForUuids(uuidList.toArray(new UUID[uuidList.size()]));
    }

    @Override
    public boolean hasUuid(UUID uuid) {
        return this.uuidEntityKeyMap.keyExists(KeyUtil.uuidToByteArray(uuid));
    }

    @Override
    public boolean hasPublicId(PublicId publicId) {
        for (UUID uuid: publicId.asUuidArray()) {
            if (hasUuid(uuid)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void forEach(ObjIntConsumer<byte[]> action) {
        entityMap.forEach(action);
    }

    @Override
    public void forEachParallel(ObjIntConsumer<byte[]> action) {
        // 1) Build a list of SpliteratorForLongKeyOfPattern ranges from the all-entity spliterator
        Spliterator.OfLong allEntityKeys = this.sequenceMap.allEntityLongKeySpliterator();
        java.util.ArrayList<dev.ikm.ds.rocks.SpliteratorForLongKeyOfPattern> ranges = new java.util.ArrayList<>();

        // Keep splitting until we can’t split anymore; collect per-pattern ranges
        java.util.ArrayDeque<Spliterator.OfLong> queue = new java.util.ArrayDeque<>();
        queue.add(allEntityKeys);

        while (!queue.isEmpty()) {
            Spliterator.OfLong s = queue.pollFirst();
            Spliterator.OfLong split = s.trySplit();
            if (split != null) {
                // Recurse on both halves
                queue.addFirst(s);
                queue.addFirst(split);
                continue;
            }
            // No further split: if this is a per-pattern range, collect it
            if (s instanceof SpliteratorForLongKeyOfPattern sp) {
                ranges.add(sp);
            } else {
                // Fallback: if we encounter a non-per-pattern spliterator that can’t split,
                // try one more split attempt loop (defensive); otherwise, it should be tiny.
                // In practice, SpliteratorForEntityKeys.trySplit() hands out per-pattern spliterators,
                // so we should almost always end up here as SpliteratorForLongKeyOfPattern.
                // If this happens, we can drain it sequentially as a very small tail:
                s.forEachRemaining((LongConsumer) (longKey) -> {
                    // Create a 1-element range to reuse scanEntitiesInRange
                    int pattern = (int) ((longKey >>> 48) & 0xFFFF);
                    long elementSeq = (longKey & 0xFFFFFFFFFFFFL);
                    SpliteratorForLongKeyOfPattern singleton =
                            new SpliteratorForLongKeyOfPattern(pattern, elementSeq, elementSeq + 1);
                    this.entityMap.scanEntitiesInRange(singleton, action);
                });
            }
        }

        LOG.info("Found {} EntityKey ranges to process.", ranges.size());

        // 2) Run each range in parallel with structured concurrency
        try (StructuredTaskScope<Object, Void> scope = StructuredTaskScope.open()) {
            for (SpliteratorForLongKeyOfPattern range : ranges) {
                scope.fork(() -> {
                    this.entityMap.scanEntitiesInRange(range, action);
                    return null;
                });
            }
            scope.join();
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void forEachParallel(ImmutableIntList nids, ObjIntConsumer<byte[]> action) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBytes(int nid) {
        return this.entityMap.get(longKeyForNid(nid));
    }

    @Override
    public byte[] merge(int nid, int patternNid, int referencedComponentNid, byte[] value, final Object sourceObject, final DataActivity activity) {
        if (nid == Integer.MIN_VALUE) {
            LOG.error("NID should not be Integer.MIN_VALUE");
            throw new IllegalStateException("NID should not be Integer.MIN_VALUE");
        }
        switch (sourceObject) {
            case SemanticEntity semanticEntity -> entityReferencingSemanticMap.add(EntityKey.ofNid(semanticEntity.referencedComponentNid()), EntityKey.ofNid(semanticEntity.nid()));
            default -> {}
        }
        // The put operation does its own merge in a simpler way...
        this.entityMap.put(longKeyForNid(nid), value);
        byte[] mergedBytes = this.entityMap.get(longKeyForNid(nid));
        if (mergedBytes == null) {
            throw new IllegalStateException("Merged bytes should not be null");
        }

        this.writeSequence.increment();
        // TODO: Add back in change set writer service.
        //ImmutableList<ChangeSetWriterService> changeSetWriterServices = this.changeSetWriterServices.orElseSet(this::changeSetWriterServicesList);
        //changeSetWriterServices.forEach(writerService -> writerService.writeToChangeSet((Entity) sourceObject, activity));
        this.indexer.index(sourceObject);
        return mergedBytes;
    }

    @Override
    public PrimitiveDataSearchResult[] search(String query, int maxResultSize) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> recreateLuceneIndex() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return TinkExecutor.ioThreadPool().submit(new RecreateIndex(this.indexer)).get();
            } catch (InterruptedException | ExecutionException ex) {
                AlertStreams.dispatchToRoot(new CompletionException("Error encountered while creating Lucene indexes." +
                        "Search and Type Ahead Suggestions may not function as expected.", ex));
            }
            return null;
        }, TinkExecutor.ioThreadPool());
    }

    @Override
    public void forEachSemanticNidOfPattern(int patternNid, IntProcedure procedure) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEachPatternNid(IntProcedure procedure) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEachConceptNid(IntProcedure procedure) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEachStampNid(IntProcedure procedure) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEachSemanticNid(IntProcedure procedure) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEachSemanticNidForComponent(int componentNid, IntProcedure procedure) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEachSemanticNidForComponentOfPattern(int componentNid, int patternNid, IntProcedure procedure) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String name() {
        return this.name;
    }
}
