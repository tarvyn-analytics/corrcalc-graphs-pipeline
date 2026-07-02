package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

import ch.tarvynanalytics.graphs.algos.Calibration;
import ch.tarvynanalytics.graphs.algos.ChangeDetectors;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeadingWarmupCalibrationTest {

    private static final DetectorConfig CONFIG = DetectorConfig.crypto();
    private static final Instant T0 = Instant.parse("2021-01-01T00:00:00Z");

    @Test
    void observe_FewerThanCalmBars_IsNotReady() {
        CalibrationSource source = CalibrationSources.leadingWarmup(3, CONFIG);
        source.observe(T0, Double.NaN, 0.30);
        source.observe(T0.plusSeconds(60), 0.02, 0.35);
        assertFalse(source.isReady());
        assertThrows(IllegalStateException.class, source::calibration);
    }

    @Test
    void calibration_AfterCalmBars_MatchesIndependentCalibrateOnTheSameSlices() {
        // The extracted code path must equal calling ChangeDetectors.calibrate directly on the
        // accumulated (change, density) slices — the engine's original inline behaviour.
        double[] change = {Double.NaN, 0.02, 0.05, 0.03};
        double[] density = {0.30, 0.35, 0.40, 0.20};
        CalibrationSource source = CalibrationSources.leadingWarmup(4, CONFIG);
        for (int i = 0; i < change.length; i++) {
            source.observe(T0.plusSeconds(60L * i), change[i], density[i]);
        }
        assertTrue(source.isReady());
        Calibration expected = ChangeDetectors.calibrate(change, density, CONFIG);
        Calibration actual = source.calibration();
        assertEquals(expected.mu(), actual.mu());
        assertEquals(expected.sigma(), actual.sigma());
        assertEquals(expected.level(), actual.level());
        assertEquals(expected.muDensity(), actual.muDensity());
        assertEquals(expected.sigmaDensity(), actual.sigmaDensity());
    }

    @Test
    void calibration_CalledTwice_ReturnsTheSameFrozenInstance() {
        CalibrationSource source = CalibrationSources.leadingWarmup(2, CONFIG);
        source.observe(T0, Double.NaN, 0.30);
        source.observe(T0.plusSeconds(60), 0.02, 0.35);
        assertEquals(source.calibration(), source.calibration());
    }

    @Test
    void leadingWarmup_CalmBarsBelowTwo_Throws() {
        assertThrows(IllegalArgumentException.class, () -> CalibrationSources.leadingWarmup(1, CONFIG));
    }

    @Test
    void leadingWarmup_NullConfig_Throws() {
        assertThrows(IllegalArgumentException.class, () -> CalibrationSources.leadingWarmup(2, null));
    }
}
