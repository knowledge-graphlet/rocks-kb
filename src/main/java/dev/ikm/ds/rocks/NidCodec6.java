package dev.ikm.ds.rocks;

/**
 * Packs and unpacks a 6-bit {@code patternSequence} and a 26-bit
 * {@code elementSequence} into a 32-bit {@code nid}.
 *
 * <p>Bit layout (MSB → LSB):</p>
 * <pre>{@code
 * [6-bit patternSequence][26-bit elementIndex]
 * }</pre>
 *
 * <dl>
 *   <dt>patternSequence (P)</dt>
 *   <dd>Unsigned, 1..63 (6 bits; 0 is disallowed)</dd>
 *   <dt>elementSequence (E)</dt>
 *   <dd>Unsigned, 1..67,108,864 (inclusive), represented as
 *       {@code elementIndex = E - 1} in 26 bits</dd>
 *   <dt>elementIndex (I)</dt>
 *   <dd>{@code I = E - 1}, range 0..(2^26 - 1) = 0..67,108,863</dd>
 * </dl>
 *
 * <p>Encoding (compose):</p>
 * <pre>{@code
 * I   = E - 1
 * nid = (P << 26) | I
 * }</pre>
 *
 * <p>Decoding (extract):</p>
 * <pre>{@code
 * P = (nid >>> 26) & 0x3F
 * I =  nid         & 0x03FF_FFFF
 * E = I + 1
 * }</pre>
 *
 * <p>Ranges and capacities:</p>
 * <ul>
 *   <li>P ∈ [1, 63] → 63 usable patterns</li>
 *   <li>I ∈ [0, 2^26 - 1] → E ∈ [1, 2^26] = [1, 67,108,864]</li>
 * </ul>
 *
 * <p>Forbidden nid values (must not be produced):</p>
 * <ul>
 *   <li>{@code 0}: unreachable because P ≠ 0</li>
 *   <li>{@code Integer.MIN_VALUE (0x8000_0000)}:
 *       maps to {@code (P=32, E=1)} → reject this pair</li>
 *   <li>{@code Integer.MAX_VALUE (0x7FFF_FFFF)}:
 *       maps to {@code (P=31, E=67,108,864)} → reject this pair</li>
 * </ul>
 *
 * @apiNote
 * The mapping is injective except for the two excluded pairs above. Per-pattern
 * issuance stays sequential because {@code E} increments linearly within the
 * lower 26 bits.
 *
 * @implNote
 * Masks and shifts are fixed-width and safe:
 * <ul>
 *   <li>{@code PATTERN_MASK = 0x3F}</li>
 *   <li>{@code ELEMENT_MASK = 0x03FF_FFFF}</li>
 * </ul>
 *
 * Examples:
 * {@snippet lang="java":
 * // Encode example
 * int nid = NidCodec6.encode(5, 123_456L);
 *
 * // Decode example
 * int p = NidCodec6.decodePatternSequence(nid);   // 5
 * long e = NidCodec6.decodeElementSequence(nid); // 123_456
 * }
 */
public final class NidCodec6 {
    // Layout constants
    private static final int PATTERN_BITS = 6;                 // P ∈ [1, 63]
    private static final int ELEMENT_BITS = 26;                // I ∈ [0, 2^26 - 1]
    private static final int PATTERN_MASK = (1 << PATTERN_BITS) - 1;   // 0x3F
    private static final int ELEMENT_MASK = (1 << ELEMENT_BITS) - 1;   // 0x03FF_FFFF
    public static final int MAX_PATTERN_SEQUENCE = (int) ((1L << PATTERN_BITS) - 1);
    public static final long MAX_ELEMENT_SEQUENCE = ((1L << ELEMENT_BITS) - 1);

    private NidCodec6() { }

    /**
     * Encodes {@code (patternSequence, elementSequence)} into a 32-bit {@code nid}.
     *
     * <p>Preconditions:</p>
     * <ul>
     *   <li>{@code 1 <= patternSequence <= 63}</li>
     *   <li>{@code 1 <= elementSequence <= 67_108_864}</li>
     *   <li>Rejects pairs mapping to {@code Integer.MIN_VALUE} and
     *       {@code Integer.MAX_VALUE}:
     *       {@code (32,1)} and {@code (31,67_108_864)}</li>
     * </ul>
     *
     * <p>Computation:</p>
     * <pre>{@code
     * I   = elementSequence - 1
     * nid = (patternSequence << 26) | I
     * }</pre>
     *
     * @param patternSequence unsigned pattern in [1, 63]
     * @param elementSequence unsigned element in [1, 67_108_864]
     * @return packed {@code nid}
     * @throws IllegalArgumentException if arguments are out of range or map to a
     *                                  forbidden {@code nid}
     */
    public static int encode(int patternSequence, long elementSequence) {
        if (patternSequence <= 0 || patternSequence > PATTERN_MASK) {
            throw new IllegalArgumentException(
                    "patternSequence out of range (1..63): " + patternSequence
            );
        }
        if (elementSequence <= 0 || elementSequence > (1L << ELEMENT_BITS)) {
            throw new IllegalArgumentException(
                    "elementSequence out of range (1..67,108,864): " + elementSequence
            );
        }

        int elementIndex = (int) (elementSequence - 1); // I ∈ [0, 2^26 - 1]
        int nid = (patternSequence << ELEMENT_BITS) | elementIndex;

        // Reject the two forbidden mappings
        if (nid == Integer.MIN_VALUE || nid == Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Forbidden nid mapping for pair (pattern=" + patternSequence
                            + ", element=" + elementSequence + ")"
            );
        }
        return nid;
    }

    /**
     * Extracts {@code patternSequence} from {@code nid}.
     *
     * <p>Computation:</p>
     * <pre>{@code
     * P = (nid >>> 26) & 0x3F
     * }</pre>
     *
     * @param nid packed {@code nid}
     * @return {@code patternSequence} in [0, 63]; caller should validate
     *         {@code != 0} if required by domain
     */
    public static int decodePatternSequence(int nid) {
        return (nid >>> ELEMENT_BITS) & PATTERN_MASK;
    }

    /**
     * Extracts {@code elementSequence} from {@code nid}.
     *
     * <p>Computation:</p>
     * <pre>{@code
     * I =  nid & 0x03FF_FFFF
     * E = I + 1
     * }</pre>
     *
     * @param nid packed {@code nid}
     * @return {@code elementSequence} in [1, 67_108_864]
     */
    public static long decodeElementSequence(int nid) {
        int elementIndex = nid & ELEMENT_MASK;
        return 1L + (elementIndex & ELEMENT_MASK);
    }

    /**
     * Returns the 64-bit entity long key for the given nid using only bit operations.
     * Layout: upper 16 bits = patternSequence, lower 48 bits = elementSequence.
     * No intermediate allocations or method calls.
     *
     * @param nid packed nid
     * @return 64-bit key: [patternSequence:16][elementSequence:48]
     */
    public static long longKeyForNid(int nid) {
        int pattern = (nid >>> ELEMENT_BITS) & PATTERN_MASK;          // 6-bit value in [1..63]
        long element = (nid & ELEMENT_MASK) + 1L;                      // 48-bit lane (here ≤ 2^26)
        return (((long) pattern) << 48) | (element & 0xFFFF_FFFF_FFFFL);
    }

    public static int nidForLongKey(long longKey) {
       int patternSequence = KeyUtil.longKeyToPatternSequence( longKey);
       long elementSequence = KeyUtil.longKeyToElementSequence(longKey);
       return encode(patternSequence, elementSequence);

    }
    /**
     * Validates that {@code nid} adheres to this codec.
     *
     * <p>Checks:</p>
     * <ul>
     *   <li>{@code nid} is not {@code 0}, {@code Integer.MIN_VALUE},
     *       {@code Integer.MAX_VALUE}</li>
     *   <li>Decoded {@code patternSequence} in [1, 63]</li>
     *   <li>Decoded {@code elementSequence} in [1, 67_108_864]</li>
     * </ul>
     *
     * @param nid packed {@code nid}
     * @throws IllegalArgumentException if validation fails
     */
    public static void validateNid(int nid) {
        if (nid == 0 || nid == Integer.MIN_VALUE || nid == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Forbidden nid value: " + nid);
        }
        int pattern = decodePatternSequence(nid);
        if (pattern <= 0 || pattern > PATTERN_MASK) {
            throw new IllegalArgumentException(
                    "Decoded patternSequence out of range: " + pattern
            );
        }
        long elementSeq = decodeElementSequence(nid);
        if (elementSeq <= 0 || elementSeq > (1L << ELEMENT_BITS)) {
            throw new IllegalArgumentException(
                    "Decoded elementSequence out of range: " + elementSeq
            );
        }
    }
}