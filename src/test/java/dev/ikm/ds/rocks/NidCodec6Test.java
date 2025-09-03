package dev.ikm.ds.rocks;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("NidCodec6 encode/decode tests")
class NidCodec6Test {

    @Nested
    @DisplayName("Round-trip encode/decode")
    class RoundTrip {
        @ParameterizedTest(name = "pattern={0}, element={1}")
        @CsvSource({
                // Typical values
                "1,1",
                "1,2",
                "2,1",
                "5,123456",
                "63,67108864",
                // Boundaries around element sequence
                "10,1",
                "10,2",
                "10,67108863",
                "10,67108864",
                // Boundaries around pattern sequence
                "1,100",
                "63,100"
        })
        void roundTrip(int pattern, long element) {
            int nid = NidCodec6.encode(pattern, element);
            assertEquals(pattern, NidCodec6.decodePatternSequence(nid));
            assertEquals(element, NidCodec6.decodeElementSequence(nid));
            // validate should not throw
            assertDoesNotThrow(() -> NidCodec6.validateNid(nid));
        }

        @Test
        @DisplayName("Randomized sampling within ranges")
        void randomizedRoundTrip() {
            Random r = new Random(42);
            for (int i = 0; i < 10_000; i++) {
                int pattern = 1 + r.nextInt(63); // [1,63]
                long element = 1L + (r.nextInt(1 << 20) | ((long) r.nextInt(1 << 6) << 20));
                // Ensure element within [1, 2^26]
                element = 1L + (element - 1) % (1L << 26);
                // Avoid the two forbidden mappings explicitly
                if (pattern == 32 && element == 1) { element = 2; }
                if (pattern == 31 && element == (1L << 26)) { element--; }
                int nid = NidCodec6.encode(pattern, element);
                assertEquals(pattern, NidCodec6.decodePatternSequence(nid));
                assertEquals(element, NidCodec6.decodeElementSequence(nid));
                NidCodec6.validateNid(nid);
            }
        }
    }

    @Nested
    @DisplayName("Invalid ranges rejected")
    class InvalidRanges {
        @ParameterizedTest
        @ValueSource(ints = {0, -1, 64, 100})
        void invalidPatternRejected(int pattern) {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> NidCodec6.encode(pattern, 1));
            assertTrue(ex.getMessage().contains("patternSequence"));
        }

        @ParameterizedTest
        @ValueSource(longs = {0L, -1L, 67108865L, Long.MAX_VALUE})
        void invalidElementRejected(long element) {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> NidCodec6.encode(1, element));
            assertTrue(ex.getMessage().contains("elementSequence"));
        }

        @Test
        @DisplayName("Forbidden pair (32,1) → Integer.MIN_VALUE is rejected")
        void forbiddenMinValue() {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> NidCodec6.encode(32, 1));
            assertTrue(ex.getMessage().contains("Forbidden nid mapping"));
        }

        @Test
        @DisplayName("Forbidden pair (31, 67,108,864) → Integer.MAX_VALUE is rejected")
        void forbiddenMaxValue() {
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> NidCodec6.encode(31, 1L << 26));
            assertTrue(ex.getMessage().contains("Forbidden nid mapping"));
        }
    }

    @Nested
    @DisplayName("validateNid checks")
    class ValidateChecks {
        @Test
        void validateRejectsForbiddenNids() {
            for (int bad : new int[]{0, Integer.MIN_VALUE, Integer.MAX_VALUE}) {
                IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                        () -> NidCodec6.validateNid(bad));
                assertTrue(ex.getMessage().contains("Forbidden nid value")
                        || ex.getMessage().contains("out of range"));
            }
        }

        @Test
        void validateRejectsZeroPattern() {
            // Construct a nid with pattern=0, elementIndex arbitrary (e.g., 5)
            int nid = 5; // lower 26 bits only, pattern bits are zero
            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> NidCodec6.validateNid(nid));
            assertTrue(ex.getMessage().contains("patternSequence"));
        }

        @Test
        void validateAcceptsValidNid() {
            int nid = NidCodec6.encode(7, 12345);
            assertDoesNotThrow(() -> NidCodec6.validateNid(nid));
        }
    }
}
