package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SymbolVolatilityObservationTest {

    private static SymbolVolatilityObservation observation() {
        return new SymbolVolatilityObservation(Instant.EPOCH, "crypto", "intraday",
                List.of("A", "B"), new double[]{1.5, Double.NaN});
    }

    @Test
    void constructor_DefensivelyCopiesZScores() {
        double[] z = {1.5, 2.5};
        SymbolVolatilityObservation o = new SymbolVolatilityObservation(Instant.EPOCH, "crypto",
                "intraday", List.of("A", "B"), z);

        z[0] = 99.0;   // mutate the source array after construction
        assertEquals(1.5, o.zScores()[0], "observation must not see the post-construction mutation");
    }

    @Test
    void equalsHashCodeToString_CompareZScoresByContent() {
        SymbolVolatilityObservation a = observation();
        SymbolVolatilityObservation b = observation();

        assertEquals(a, b);                        // same content (NaN-aware) -> equal
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, "not an observation");
        assertTrue(a.toString().contains("1.5"), a.toString());
        assertTrue(a.toString().contains("NaN"), a.toString());
    }

    @Test
    void equals_EachDifferingComponent_NotEqual() {
        SymbolVolatilityObservation a = observation();

        assertNotEquals(a, new SymbolVolatilityObservation(Instant.EPOCH.plusSeconds(1), "crypto",
                "intraday", List.of("A", "B"), new double[]{1.5, Double.NaN}));
        assertNotEquals(a, new SymbolVolatilityObservation(Instant.EPOCH, "fx",
                "intraday", List.of("A", "B"), new double[]{1.5, Double.NaN}));
        assertNotEquals(a, new SymbolVolatilityObservation(Instant.EPOCH, "crypto",
                "daily", List.of("A", "B"), new double[]{1.5, Double.NaN}));
        assertNotEquals(a, new SymbolVolatilityObservation(Instant.EPOCH, "crypto",
                "intraday", List.of("A", "C"), new double[]{1.5, Double.NaN}));
        assertNotEquals(a, new SymbolVolatilityObservation(Instant.EPOCH, "crypto",
                "intraday", List.of("A", "B"), new double[]{1.5, 2.5}));
    }

    @Test
    void constructor_InvalidComponents_RejectedWithBracketedValue() {
        assertThrows(IllegalArgumentException.class, () -> new SymbolVolatilityObservation(
                null, "crypto", "intraday", List.of("A"), new double[]{1.0}));
        assertThrows(IllegalArgumentException.class, () -> new SymbolVolatilityObservation(
                Instant.EPOCH, "crypto", "intraday", null, new double[]{1.0}));
        assertThrows(IllegalArgumentException.class, () -> new SymbolVolatilityObservation(
                Instant.EPOCH, "crypto", "intraday", List.of(), new double[]{}));
        assertThrows(IllegalArgumentException.class, () -> new SymbolVolatilityObservation(
                Instant.EPOCH, "crypto", "intraday", List.of("A"), null));
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new SymbolVolatilityObservation(
                        Instant.EPOCH, "crypto", "intraday", List.of("A"), new double[]{1.0, 2.0}));
        assertTrue(ex.getMessage().contains("[2]"), ex.getMessage());
    }
}
