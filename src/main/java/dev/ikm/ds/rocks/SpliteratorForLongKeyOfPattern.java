package dev.ikm.ds.rocks;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.LongConsumer;

public class SpliteratorForLongKeyOfPattern implements Spliterator.OfLong, Comparator<SpliteratorForLongKeyOfPattern> {
    final int patternSequence;
    long currentElementSequence;
    long lastElementExclusive;

    // Heuristics for CPU-responsiveness (overridable via system properties)
    private static final int RANGES_PER_CPU =
            Integer.getInteger("spliterator.rangesPerCpu", 8);
    private static final int MIN_SPLIT_FLOOR =
            Integer.getInteger("spliterator.minSplitFloor", 4096);
    private static final int MIN_SPLIT_CEILING =
            Integer.getInteger("spliterator.minSplitCeiling", 32768);

    public SpliteratorForLongKeyOfPattern(int patternSequence, long currentElementSequence, long lastElementExclusive) {
        this.patternSequence = patternSequence;
        this.currentElementSequence = currentElementSequence;
        this.lastElementExclusive = lastElementExclusive;
    }

    public long getCurrentLongKey() {
        return KeyUtil.patternSequenceElementSequenceToLongKey(patternSequence, currentElementSequence);
    }

    public long getLastLongKeyExclusive() {
        return KeyUtil.patternSequenceElementSequenceToLongKey(patternSequence, lastElementExclusive);
    }

    public int getPatternSequence() {
        return patternSequence;
    }


    // Compute a dynamic min-split threshold based on CPUs and current sub-range size.
    // Goal: stop splitting when the leaf size is about size / (cpus * rangesPerCpu),
    // clamped to [MIN_SPLIT_FLOOR, MIN_SPLIT_CEILING].
    private static long dynamicMinSplitThreshold(long currentRangeSize) {
        int cpus = Math.max(1, Runtime.getRuntime().availableProcessors());
        long targetLeaves = Math.max(1L, (long) cpus * Math.max(1, RANGES_PER_CPU));
        long suggested = Math.max(1L, currentRangeSize / targetLeaves);
        long clamped = Math.max(MIN_SPLIT_FLOOR, Math.min(MIN_SPLIT_CEILING, suggested));
        return clamped;
    }

    @Override
    public OfLong trySplit() {
        long lo = this.currentElementSequence;
        long hi = this.lastElementExclusive;
        long size = hi - lo; // half-open: [lo, hi)
        if (size <= dynamicMinSplitThreshold(size)) {
            return null;
        }
        long mid = lo + (size / 2);
        // Split gets the lower half [lo, mid); this keeps [mid, hi)
        SpliteratorForLongKeyOfPattern split = new SpliteratorForLongKeyOfPattern(this.patternSequence, lo, mid);
        this.currentElementSequence = mid;
        return split;
    }

    @Override
    public long estimateSize() {
        long size = lastElementExclusive - currentElementSequence; // remaining in [next, last)
        return size >= 0 ? size : 0;
    }

    @Override
    public int characteristics() {
        return ORDERED | SIZED | SUBSIZED | SORTED | DISTINCT | IMMUTABLE | NONNULL;
    }

    @Override
    public Comparator<? super Long> getComparator() {
        // SORTED by natural order of the produced long keys
        return null;
    }

    @Override
    public boolean tryAdvance(LongConsumer action) {
        if (action == null) {
            throw new NullPointerException("action");
        }
        if (currentElementSequence < lastElementExclusive) {
            long elementSeq = currentElementSequence++; // consume current, then advance
            long key = KeyUtil.patternSequenceElementSequenceToLongKey(patternSequence, elementSeq);
            action.accept(key);
            return true;
        }
        return false;
    }

    @Override
    public int compare(SpliteratorForLongKeyOfPattern o1, SpliteratorForLongKeyOfPattern o2) {
        return Integer.compare(o1.patternSequence, o2.patternSequence);
    }

    @Override
    public String toString() {
        java.text.NumberFormat nf = java.text.NumberFormat.getIntegerInstance(java.util.Locale.getDefault());
        String patternStr = "0x" + String.format("%04X", patternSequence);
        String startStr = nf.format(currentElementSequence);
        String endStr = nf.format(lastElementExclusive);
        return patternStr + ":[" + startStr + "–" + endStr + ")";
    }

}
