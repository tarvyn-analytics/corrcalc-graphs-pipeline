package ch.tarvynanalytics.corrcalc.graphs.pipeline.detect;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PairUniverseTest {

    @Test
    void all_ActivePairs_IsZeroForAnyInstant() {
        assertEquals(0L, PairUniverse.ALL.activePairs(Instant.EPOCH));
        assertEquals(0L, PairUniverse.ALL.activePairs(Instant.parse("2024-01-01T00:00:00Z")));
    }

    @Test
    void activePairs_LambdaImplementation_ReturnsWhatItIsGivenPerInstant() {
        PairUniverse fixed = asOf -> asOf.equals(Instant.EPOCH) ? 3L : 6L;

        assertEquals(3L, fixed.activePairs(Instant.EPOCH));
        assertEquals(6L, fixed.activePairs(Instant.parse("2024-01-01T00:00:00Z")));
    }
}
