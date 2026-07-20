package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SymbolVolatilityBaselineTest {

    private static final Instant FROM = Instant.parse("2024-01-01T00:00:00Z");
    private static final Instant TO = Instant.parse("2024-02-01T00:00:00Z");

    private static SymbolVolatilityBaseline baseline() {
        return new SymbolVolatilityBaseline("crypto", "intraday", 0L, FROM, TO,
                List.of("A", "B"), new double[]{0.01, 0.02}, new double[]{0.001, 0.002});
    }

    @Test
    void constructor_DefensivelyCopiesArrays() {
        double[] mu = {0.01, 0.02};
        double[] sigma = {0.001, 0.002};
        SymbolVolatilityBaseline b = new SymbolVolatilityBaseline("crypto", "intraday", 0L, FROM, TO,
                List.of("A", "B"), mu, sigma);

        mu[0] = 99.0;      // mutate the source arrays after construction
        sigma[0] = 99.0;
        assertEquals(0.01, b.mu()[0], "baseline must not see the post-construction mutation");
        assertEquals(0.001, b.sigma()[0], "baseline must not see the post-construction mutation");
    }

    @Test
    void equalsHashCodeToString_CompareArraysByContent() {
        SymbolVolatilityBaseline a = baseline();
        SymbolVolatilityBaseline b = baseline();

        assertEquals(a, b);                        // same content -> equal
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, "not a baseline");
        assertTrue(a.toString().contains("0.02"), a.toString());
        assertTrue(a.toString().contains("crypto"), a.toString());
    }

    @Test
    void equals_EachDifferingComponent_NotEqual() {
        SymbolVolatilityBaseline a = baseline();

        assertNotEquals(a, new SymbolVolatilityBaseline("fx", "intraday", 0L, FROM, TO,
                List.of("A", "B"), new double[]{0.01, 0.02}, new double[]{0.001, 0.002}));
        assertNotEquals(a, new SymbolVolatilityBaseline("crypto", "daily", 0L, FROM, TO,
                List.of("A", "B"), new double[]{0.01, 0.02}, new double[]{0.001, 0.002}));
        assertNotEquals(a, new SymbolVolatilityBaseline("crypto", "intraday", 1L, FROM, TO,
                List.of("A", "B"), new double[]{0.01, 0.02}, new double[]{0.001, 0.002}));
        assertNotEquals(a, new SymbolVolatilityBaseline("crypto", "intraday", 0L, FROM.plusSeconds(1), TO,
                List.of("A", "B"), new double[]{0.01, 0.02}, new double[]{0.001, 0.002}));
        assertNotEquals(a, new SymbolVolatilityBaseline("crypto", "intraday", 0L, FROM, TO.plusSeconds(1),
                List.of("A", "B"), new double[]{0.01, 0.02}, new double[]{0.001, 0.002}));
        assertNotEquals(a, new SymbolVolatilityBaseline("crypto", "intraday", 0L, FROM, TO,
                List.of("A", "C"), new double[]{0.01, 0.02}, new double[]{0.001, 0.002}));
        assertNotEquals(a, new SymbolVolatilityBaseline("crypto", "intraday", 0L, FROM, TO,
                List.of("A", "B"), new double[]{0.01, 0.03}, new double[]{0.001, 0.002}));
        assertNotEquals(a, new SymbolVolatilityBaseline("crypto", "intraday", 0L, FROM, TO,
                List.of("A", "B"), new double[]{0.01, 0.02}, new double[]{0.001, 0.003}));
    }

    @Test
    void constructor_InvalidProvenance_RejectedWithBracketedValue() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new SymbolVolatilityBaseline(" ", "intraday", 0L, FROM, TO,
                        List.of("A"), new double[]{0.01}, new double[]{0.001}));
        assertTrue(ex.getMessage().contains("["), ex.getMessage());
        assertThrows(IllegalArgumentException.class,
                () -> new SymbolVolatilityBaseline("crypto", "weekly", 0L, FROM, TO,
                        List.of("A"), new double[]{0.01}, new double[]{0.001}));
        assertThrows(IllegalArgumentException.class,
                () -> new SymbolVolatilityBaseline("crypto", "intraday", -1L, FROM, TO,
                        List.of("A"), new double[]{0.01}, new double[]{0.001}));
        assertThrows(IllegalArgumentException.class,
                () -> new SymbolVolatilityBaseline("crypto", "intraday", 0L, null, TO,
                        List.of("A"), new double[]{0.01}, new double[]{0.001}));
        assertThrows(IllegalArgumentException.class,
                () -> new SymbolVolatilityBaseline("crypto", "intraday", 0L, FROM, null,
                        List.of("A"), new double[]{0.01}, new double[]{0.001}));
        assertThrows(IllegalArgumentException.class,
                () -> new SymbolVolatilityBaseline("crypto", "intraday", 0L, TO, FROM,
                        List.of("A"), new double[]{0.01}, new double[]{0.001}));
    }

    @Test
    void constructor_InvalidColumns_RejectedWithBracketedValue() {
        assertThrows(IllegalArgumentException.class,
                () -> new SymbolVolatilityBaseline("crypto", "intraday", 0L, FROM, TO,
                        null, new double[]{0.01}, new double[]{0.001}));
        assertThrows(IllegalArgumentException.class,
                () -> new SymbolVolatilityBaseline("crypto", "intraday", 0L, FROM, TO,
                        List.of(), new double[]{}, new double[]{}));
        assertThrows(IllegalArgumentException.class,
                () -> new SymbolVolatilityBaseline("crypto", "intraday", 0L, FROM, TO,
                        List.of("A"), null, new double[]{0.001}));
        assertThrows(IllegalArgumentException.class,
                () -> new SymbolVolatilityBaseline("crypto", "intraday", 0L, FROM, TO,
                        List.of("A"), new double[]{0.01}, null));
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new SymbolVolatilityBaseline("crypto", "intraday", 0L, FROM, TO,
                        List.of("A"), new double[]{0.01, 0.02}, new double[]{0.001}));
        assertTrue(ex.getMessage().contains("[2]"), ex.getMessage());
        assertThrows(IllegalArgumentException.class,
                () -> new SymbolVolatilityBaseline("crypto", "intraday", 0L, FROM, TO,
                        List.of("A"), new double[]{0.01}, new double[]{0.001, 0.002}));
    }
}
