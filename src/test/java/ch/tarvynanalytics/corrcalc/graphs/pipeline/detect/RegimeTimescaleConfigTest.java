package ch.tarvynanalytics.corrcalc.graphs.pipeline.detect;

import ch.tarvynanalytics.graphs.algos.RegimeConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RegimeTimescaleConfigTest {

    @Test
    void crypto_CarriesTheSettledSpikeDefaults() {
        RegimeTimescaleConfig cfg = RegimeTimescaleConfig.crypto();

        assertEquals(0.85, cfg.regime().hi(), 1e-12);
        assertEquals(0.45, cfg.regime().lo(), 1e-12);
        assertEquals(3, cfg.regime().confirmBars());
        assertEquals(3, cfg.smoothWindow());
    }

    @Test
    void nullRegime_Throws() {
        assertThrows(IllegalArgumentException.class, () -> new RegimeTimescaleConfig(null, 3));
    }

    @Test
    void smoothWindowBelowOne_Throws() {
        assertThrows(IllegalArgumentException.class,
                () -> new RegimeTimescaleConfig(RegimeConfig.crypto(), 0));
    }

    @Test
    void evenSmoothWindow_IsAllowed() {
        // The trailing median requires no symmetric (odd) window; even windows are valid.
        RegimeTimescaleConfig cfg = new RegimeTimescaleConfig(RegimeConfig.crypto(), 2);
        assertEquals(2, cfg.smoothWindow());
    }
}
