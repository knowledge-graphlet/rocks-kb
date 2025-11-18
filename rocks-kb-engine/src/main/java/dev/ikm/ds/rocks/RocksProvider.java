package dev.ikm.ds.rocks;

import dev.ikm.ds.rocks.internal.Get;
import dev.ikm.ds.rocks.maps.*;
import dev.ikm.ds.rocks.spliterator.LongSpliteratorOfPattern;
import dev.ikm.ds.rocks.spliterator.SortedLongArraySpliteratorOfPattern;
import dev.ikm.ds.rocks.spliterator.SpliteratorForLongKeyOfPattern;
import dev.ikm.tinkar.common.alert.AlertStreams;
import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.service.*;
import dev.ikm.tinkar.common.util.io.FileUtil;
import dev.ikm.tinkar.common.util.time.Stopwatch;
import dev.ikm.tinkar.entity.*;
import dev.ikm.tinkar.provider.search.Indexer;
import dev.ikm.tinkar.provider.search.RecreateIndex;
import dev.ikm.tinkar.provider.search.Searcher;
import dev.ikm.tinkar.terms.EntityBinding;
import org.eclipse.collections.api.block.procedure.primitive.IntProcedure;
import org.eclipse.collections.api.collection.primitive.MutableLongCollection;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.primitive.LongLists;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;
import java.util.function.ObjIntConsumer;

import static dev.ikm.tinkar.common.service.PrimitiveData.SCOPED_PATTERN_PUBLICID_FOR_NID;

public class RocksProvider implements PrimitiveDataService, NidGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(RocksProvider.class);
    public static final long defaultCacheSize = 256L * 1024 * 1024; // 256 MB cache
    public static final int defaultBloomFilterBitsPerKey = 10;

    protected static final File defaultDataDirectory = new File("target/rocksdb/");

    private LongAdder writeSequence = new LongAdder();

    final Semaphore startupShutdownSemaphore = new Semaphore(1);
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

    public void scanEntitiesInRange(LongSpliteratorOfPattern range, ObjIntConsumer<byte[]> entityHandler) {
        entityMap.scanEntitiesInRange(range, entityHandler);
    }

    public RocksDB getDb() {
        return db;
    }

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


    public static RocksProvider get() {
        try {
            return stableProvider.orElseSet(RocksProvider::new);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    private static StableValue<RocksProvider> stableProvider = StableValue.of();

    private final Cache blockCache;
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    // Add these fields to track native resources that need cleanup
    private DBOptions dbOptions;
    private final List<Filter> bloomFilters = new ArrayList<>();
    private final List<BlockBasedTableConfig> tableConfigs = new ArrayList<>();

    private RocksProvider() {
        startupShutdownSemaphore.acquireUninterruptibly();
        try {
            Stopwatch stopwatch = new Stopwatch();
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

                        BloomFilter bloomFilter = new BloomFilter(cf.bloomFilterBitsPerKey, /*useBlockBasedBuilder*/ false);
                        bloomFilters.add(bloomFilter); // Track for cleanup
                        
                        BlockBasedTableConfig tableCfg = new BlockBasedTableConfig()
                                .setBlockCache(this.blockCache)
                                .setFilterPolicy(bloomFilter)
                                .setWholeKeyFiltering(false)                    // favor prefix Bloom (when prefix extractor is configured)
                                .setCacheIndexAndFilterBlocks(true)            // keep index/filter hot
                                .setPinL0FilterAndIndexBlocksInCache(true)     // optional
                                .setBlockSize(16 * 1024)                       // 16 KB data blocks (tune for your workload)
                                .setChecksumType(ChecksumType.kXXH3)           // modern checksum
                                .setFormatVersion(5)                           // stable table format
                                .setPartitionFilters(true);                    // scalable filters for large data
                        
                        tableConfigs.add(tableCfg); // Track for cleanup

                        ColumnFamilyOptions cfo = new ColumnFamilyOptions();
                        cfo.setCompressionType(CompressionType.NO_COMPRESSION);
                        cfo.setTableFormatConfig(tableCfg);
                        cfo.setWriteBufferSize(cf.writeBufferSize);
                        if (cf.keyPrefixBytes >= 0) {
                            cfo.useFixedLengthPrefixExtractor(cf.keyPrefixBytes);
                        }

                        return new ColumnFamilyDescriptor(cf.getValue(), cfo);
                    }).toList();

            // Don't use try-with-resources - we need to keep DBOptions alive
            this.dbOptions = new DBOptions()
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
                    .setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
                    
            try {
                db = RocksDB.open(dbOptions, rockFiles.getAbsolutePath(), columnDescriptors, columnHandles);
                entityReferencingSemanticMap = new EntityReferencingSemanticMap(db, getHandle(ColumnFamily.ENTITY_REFERENCING_SEMANTIC_MAP));
                sequenceMap = new SequenceMap(db, getHandle(ColumnFamily.DEFAULT));
                uuidEntityKeyMap = new UuidEntityKeyMap(db, getHandle(ColumnFamily.UUID_ENTITY_KEY_MAP), sequenceMap);
                entityMap = new EntityMap(db, getHandle(ColumnFamily.ENTITY_MAP), uuidEntityKeyMap);
            } catch (RocksDBException e) {
                // Close ColumnFamilyOptions/Table configs if open failed
                safeCloseColumnFamilyDescriptors();
                safeCloseNativeResources();
                throw new RuntimeException(e);
            }

            Path indexPath = Path.of(configuredRoot.getPath(), "lucene");
            boolean indexExists = Files.exists(indexPath);
            Indexer indexer;
            try {
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
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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
        } finally {
            startupShutdownSemaphore.release();
        }
    }

    private void safeCloseColumnFamilyDescriptors() {
        if (this.columnDescriptors != null) {
            for (ColumnFamilyDescriptor d : this.columnDescriptors) {
                try {
                    ColumnFamilyOptions opts = d.getOptions();
                    if (opts != null) {
                        // Don't close filters here - we'll close them separately in safeCloseNativeResources
                        // to avoid double-close
                        opts.close();
                    }
                } catch (Throwable t) {
                    LOG.debug("Error closing ColumnFamilyOptions", t);
                }
            }
        }
    }

    private void checkOpen() {
        if (closed.get() || closing.get()) {
            throw new IllegalStateException("RocksProvider is closed");
        }
    }

    public ImmutableList<SpliteratorForLongKeyOfPattern> allPatternSpliterators() {
        return this.sequenceMap.allPatternSpliterators();
    }

    public ColumnFamilyHandle getHandle(ColumnFamily columnFamily) {
        return columnHandles.get(columnFamily.ordinal());
    }

    public void save() {
        checkOpen();
        entityMap.save();
        entityReferencingSemanticMap.save();
        sequenceMap.save();
        uuidEntityKeyMap.save();
        try (FlushOptions flushOptions = new FlushOptions()) {
            db.flush(flushOptions);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        try {
            this.indexer.commit();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean running() {
        return !closed.get() && !closing.get();
    }

        @Override
        public void close() {
            startupShutdownSemaphore.acquireUninterruptibly();
            try {
                if (closed.get()) {
                    LOG.info("Called close() but RocksProvider already closed.");
                    return;
                }
                
                if (!closing.compareAndSet(false, true)) {
                    LOG.info("Close already in progress");
                    return;
                }
                
                LOG.info("Closing RocksProvider...");
                
                // 1. Flush and sync DB and index first
                try {
                    save();
                } catch (Exception e) {
                    LOG.warn("Error while saving during close", e);
                }
                
                // 2. Close Lucene indexer
                try {
                    if (this.indexer != null) {
                        this.indexer.close();
                    }
                } catch (Exception e) {
                    LOG.warn("Error closing Indexer: {}", e.getMessage(), e);
                }

                // 3. Close maps (they will close their column family handles)
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

                // 4. Clear the column handles list (already closed by maps above)
                columnHandles.clear();

                // 5. Close DB after all column family handles are closed
                try {
                    if (this.db != null) {
                        this.db.close();
                    }
                } catch (Exception e) {
                    LOG.warn("Error closing RocksDB", e);
                }

                // 6. Close ColumnFamilyOptions (without closing filters inside them)
                try {
                    safeCloseColumnFamilyDescriptors();
                } catch (Exception e) {
                    LOG.warn("Error closing ColumnFamilyOptions", e);
                }

                // 7. Close cache
                try {
                    if (this.blockCache != null) {
                        this.blockCache.close();
                    }
                } catch (Exception e) {
                    LOG.warn("Error closing blockCache", e);
                }
                
                // 8. Close native resources (BloomFilters, DBOptions) LAST
                try {
                    safeCloseNativeResources();
                } catch (Exception e) {
                    LOG.warn("Error closing native resources", e);
                }

                closed.set(true);
                LOG.info("RocksProvider closed.");
            } finally {
                startupShutdownSemaphore.release();
            }
        }

    private void safeCloseNativeResources() {
        // Close bloom filters - these DO have native resources
        /*
Claude: The crash is almost certainly in the native RocksDB layer during shutdown,
specifically around BloomFilter or cache cleanup. The NULL pointer dereference
suggests RocksDB's C++ code tried to access a freed resource.

Fix: Do NOT manually close BloomFilters that are owned by ColumnFamilyOptions.
Remove the explicit BloomFilter closing from safeCloseNativeResources() or
ensure they're not already freed when ColumnFamilyOptions closes.
         */
//        for (Filter f : this.bloomFilters) {
//            try {
//                if (f != null) {
//                    f.close();
//                }
//            } catch (Throwable t) {
//                LOG.debug("Error closing BloomFilter", t);
//            }
//        }
        this.bloomFilters.clear();

        // BlockBasedTableConfig doesn't need explicit closing - it's just a config holder
        // The native resources it represents are owned by ColumnFamilyOptions
        this.tableConfigs.clear();

        // Close DB options
        if (this.dbOptions != null) {
            try {
                this.dbOptions.close();
            } catch (Throwable t) {
                LOG.debug("Error closing DBOptions", t);
            }
            this.dbOptions = null;
        }
    }
    
    public EntityKey getEntityKey(PublicId patternId, PublicId entityId) {
        return uuidEntityKeyMap.getEntityKey(patternId, entityId);
    }

    public Optional<EntityKey> getEntityKey(UUID uuid) {
        return uuidEntityKeyMap.getEntityKey(uuid);
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

    public long elementSequenceForNid(int nid) {
        return NidCodec6.decodeElementSequence(nid);
    }
    public int patternSequenceForNid(int nid) {
        return NidCodec6.decodePatternSequence(nid);
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
        if (SCOPED_PATTERN_PUBLICID_FOR_NID.isBound()) {
            PublicId patternPublicId = SCOPED_PATTERN_PUBLICID_FOR_NID.get();
            EntityKey stampEntityKey = uuidEntityKeyMap.getEntityKey(patternPublicId, PublicIds.of(uuids));
            return stampEntityKey.nid();
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
        forEachParallel(action, allEntityKeys);
    }

    private void forEachParallel(ObjIntConsumer<byte[]> action, Spliterator.OfLong entityKeys) {
        MutableList<SpliteratorForLongKeyOfPattern> ranges = Lists.mutable.empty();

        // Keep splitting until we can’t split anymore; collect per-pattern ranges
        ArrayDeque<Spliterator.OfLong> queue = new ArrayDeque<>();
        queue.add(entityKeys);

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
                    this.entityMap.scanEntitiesInRange((LongSpliteratorOfPattern) range, action);
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
        MutableLongCollection longCollection = LongLists.mutable.withInitialCapacity(nids.size());
        nids.collectLong(nid -> NidCodec6.longKeyForNid(nid), longCollection);
        longCollection = longCollection.toSortedList();
        long[] keys = longCollection.toArray();
        ImmutableList<LongSpliteratorOfPattern> parts = SortedLongArraySpliteratorOfPattern.of(keys);
        for (LongSpliteratorOfPattern part : parts) {
            this.entityMap.scanEntitiesInRange(part, action);
        }
    }

    @Override
    public byte[] getBytes(int nid) {
        checkOpen();
        return this.entityMap.get(longKeyForNid(nid));
    }

    @Override
    public byte[] merge(int nid, int patternNid, int referencedComponentNid, byte[] value, final Object sourceObject, final DataActivity activity) {
        checkOpen();
        if (nid == Integer.MIN_VALUE) {
            LOG.error("NID should not be Integer.MIN_VALUE");
            throw new IllegalStateException("NID should not be Integer.MIN_VALUE");
        }
        if (nid == 0) {
            LOG.error("NID should not be 0");
            throw new IllegalStateException("NID should not be 0");
        }
        if (nid == Integer.MAX_VALUE) {
            LOG.error("NID should not be Integer.MAX_VALUE");
            throw new IllegalStateException("NID should not be Integer.MAX_VALUE");
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

        ImmutableList<ChangeSetWriterService> changeSetWriterServices = this.changeSetWriterServices.orElseSet(this::changeSetWriterServicesList);
        changeSetWriterServices.forEach(writerService -> writerService.writeToChangeSet((Entity) sourceObject, activity));
        this.indexer.index(sourceObject);
        return mergedBytes;
    }

    @Override
    public PrimitiveDataSearchResult[] search(String query, int maxResultSize) throws Exception {
        return this.searcher.search(query, maxResultSize);
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
        int patternSequence = (int) NidCodec6.decodeElementSequence(patternNid);
        LongSpliteratorOfPattern spliteratorOfPattern = this.sequenceMap.spliteratorOfPattern(patternSequence);
        spliteratorOfPattern.forEachRemaining((LongConsumer) longKey -> procedure.accept(NidCodec6.nidForLongKey(longKey)));
    }

    @Override
    public void forEachPatternNid(IntProcedure procedure) {
        sequenceMap.spliteratorOfPatterns().forEachRemaining((LongConsumer) longKey -> procedure.accept(NidCodec6.nidForLongKey(longKey)));
    }

    @Override
    public void forEachConceptNid(IntProcedure procedure) {
        forEachSemanticNidOfPattern(
                (int) NidCodec6.decodeElementSequence(EntityBinding.Concept.pattern().nid()), procedure);
    }

    @Override
    public void forEachStampNid(IntProcedure procedure) {
        forEachSemanticNidOfPattern((int) NidCodec6.decodeElementSequence(EntityBinding.Stamp.pattern().nid()), procedure);
    }

    @Override
    public void forEachSemanticNid(IntProcedure procedure) {
        BitSet excludedPatternSequences = new BitSet();
        excludedPatternSequences.set((int) NidCodec6.decodeElementSequence(EntityBinding.Concept.pattern().nid()));
        excludedPatternSequences.set((int) NidCodec6.decodeElementSequence(EntityBinding.Stamp.pattern().nid()));
        excludedPatternSequences.set((int) NidCodec6.decodeElementSequence(EntityBinding.Pattern.pattern().nid()));

        ImmutableList<SpliteratorForLongKeyOfPattern> semanticSpliterators = this.sequenceMap.allPatternSpliterators().select(spliterator -> !excludedPatternSequences.get(spliterator.patternSequence()));

        // Create a list to collect all the sub-spliterators
        MutableList<Spliterator.OfLong> subSpliterators = Lists.mutable.empty();

        // Process each semantic spliterator
        for (SpliteratorForLongKeyOfPattern semanticSpliterator : semanticSpliterators) {
            // Keep splitting until we get chunks of the appropriate size
            Deque<Spliterator.OfLong> queue = new ArrayDeque<>();
            queue.add(semanticSpliterator);

            while (!queue.isEmpty()) {
                Spliterator.OfLong current = queue.poll();
                long estimatedSize = current.estimateSize();

                // Target chunk size - adjust this value based on your needs
                int targetChunkSize = 10000;

                if (estimatedSize > targetChunkSize) {
                    Spliterator.OfLong split = current.trySplit();
                    if (split != null) {
                        queue.add(split);
                        queue.add(current);
                        continue;
                    }
                }

                subSpliterators.add(current);
            }
        }

        LOG.info("Split into {} sub-tasks for parallel processing", subSpliterators.size());

        // Process chunks in parallel using structured concurrency
        try (StructuredTaskScope<Object, Void> scope = StructuredTaskScope.open()) {
            for (Spliterator.OfLong subSpliterator : subSpliterators) {
                scope.fork(() -> {
                    subSpliterator.forEachRemaining((LongConsumer) longKey ->
                            procedure.accept(NidCodec6.nidForLongKey(longKey)));
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
    public void forEachSemanticNidForComponent(int componentNid, IntProcedure procedure) {
        ImmutableList<EntityKey> referencingEntityKeys = this.entityReferencingSemanticMap.getReferencingEntityKeys(NidCodec6.longKeyForNid(componentNid));
        referencingEntityKeys.forEach(entityKey -> procedure.accept(entityKey.nid()));
    }

    @Override
    public void forEachSemanticNidForComponentOfPattern(int componentNid, int patternNid, IntProcedure procedure) {
        ImmutableList<EntityKey> referencingEntityKeys = this.entityReferencingSemanticMap.getReferencingEntityKeysOfPattern(NidCodec6.longKeyForNid(componentNid), (int) NidCodec6.decodeElementSequence(patternNid));
        referencingEntityKeys.forEach(entityKey -> procedure.accept(entityKey.nid()));
    }

    @Override
    public String name() {
        return this.name;
    }
}
