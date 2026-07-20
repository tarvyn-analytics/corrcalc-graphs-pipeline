package ch.tarvynanalytics.corrcalc.graphs.pipeline.detect;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SymbolVolatilityConfigTest {

    @Test
    void constructor_ValidValues_Accepted() {
        SymbolVolatilityConfig cfg = new SymbolVolatilityConfig(4, 3, Duration.ofSeconds(90));

        assertEquals(4, cfg.volWindow());
        assertEquals(3, cfg.smoothWindow());
        assertEquals(Duration.ofSeconds(90), cfg.gapMask());
    }

    @Test
    void constructor_VolWindowBelowTwo_RejectedWithBracketedValue() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new SymbolVolatilityConfig(1, 3, Duration.ofSeconds(90)));
        assertTrue(ex.getMessage().contains("[1]"), ex.getMessage());
    }

    @Test
    void constructor_SmoothWindowBelowOne_RejectedWithBracketedValue() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new SymbolVolatilityConfig(4, 0, Duration.ofSeconds(90)));
        assertTrue(ex.getMessage().contains("[0]"), ex.getMessage());
    }

    @Test
    void constructor_NonPositiveGapMask_Rejected() {
        assertThrows(IllegalArgumentException.class, () -> new SymbolVolatilityConfig(4, 3, null));
        assertThrows(IllegalArgumentException.class,
                () -> new SymbolVolatilityConfig(4, 3, Duration.ZERO));
        assertThrows(IllegalArgumentException.class,
                () -> new SymbolVolatilityConfig(4, 3, Duration.ofSeconds(-1)));
    }
}
