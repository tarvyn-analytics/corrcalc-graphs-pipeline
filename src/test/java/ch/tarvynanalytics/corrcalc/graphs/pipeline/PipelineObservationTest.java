package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import ch.tarvynanalytics.graphs.algos.model.ChangeMetrics;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineObservationTest {

    @Test
    void activation_IsMaxArmOverThreshold() {
        PipelineObservation obs = observation(0.05, 4.0, 1.0, false);   // h = 8 below
        assertEquals(0.5, obs.activation(), 1e-12);   // max(4,1)/8
    }

    @Test
    void magnitude_IsWeightedChange() {
        assertEquals(0.05, observation(0.05, 4.0, 1.0, false).magnitude(), 1e-12);
    }

    @Test
    void magnitude_NaNGap_IsNaN() {
        assertTrue(Double.isNaN(observation(Double.NaN, 0.0, 0.0, false).magnitude()));
    }

    @Test
    void constructor_RejectsNullMetricsAndNonPositiveThreshold() {
        assertThrows(IllegalArgumentException.class, () -> new PipelineObservation(
                Instant.EPOCH, "crypto", "intraday", null, 0, 0, false, null, 8.0, 0.05, 0.02, 0.5));
        assertThrows(IllegalArgumentException.class, () -> new PipelineObservation(
                Instant.EPOCH, "crypto", "intraday", metrics(0.05), 0, 0, false, null, 0.0, 0.05, 0.02, 0.5));
    }

    @Test
    void zScore_IsSigmasAboveCalmMean() {
        // (0.13 - 0.05) / 0.02 = 4
        assertEquals(4.0, obs(0.7, 1.0, 0.13, 0.05, 0.02, 0.5, 0, 0, false).zScore(), 1e-12);
    }

    @Test
    void zScore_DegenerateSigma_IsNaN() {
        assertTrue(Double.isNaN(obs(0.7, 1.0, 0.13, 0.05, 0.0, 0.5, 0, 0, false).zScore()));
    }

    @Test
    void levelGateOpen_TracksDensityAgainstGate() {
        assertTrue(obs(0.50, 1.0, 0.05, 0.05, 0.02, 0.50, 0, 0, false).levelGateOpen());   // 0.50 >= 0.50
        assertFalse(obs(0.49, 1.0, 0.05, 0.05, 0.02, 0.50, 0, 0, false).levelGateOpen());  // 0.49 <  0.50
    }

    @Test
    void severity_TiersTrackActivation() {
        assertEquals(Severity.CALM, observation(0.05, 1.6, 0.0, false).severity());    // 1.6/8 = 0.20
        assertEquals(Severity.WATCH, observation(0.05, 4.0, 0.0, false).severity());   // 4.0/8 = 0.50
        assertEquals(Severity.WARN, observation(0.05, 6.4, 0.0, false).severity());    // 6.4/8 = 0.80
        assertEquals(Severity.FIRE, observation(0.05, 4.0, 0.0, true).severity());     // fired wins
    }

    @Test
    void reasonCodes_Fire_IncludesFusionLevelMagnitudeAndBreach() {
        List<ReasonCode> codes = obs(1.0, 1.0, 0.81, 0.05, 0.02, 0.167, 34.0, 0.0, true).reasonCodes();
        assertTrue(codes.contains(ReasonCode.FIRE_FUSION));
        assertTrue(codes.contains(ReasonCode.MAG_GE_3SIGMA));
        assertTrue(codes.contains(ReasonCode.LEVEL_GATE_OPEN));
        assertTrue(codes.contains(ReasonCode.DENSITY_SATURATED));
        assertTrue(codes.contains(ReasonCode.COMPONENTS_COLLAPSED));
        assertTrue(codes.contains(ReasonCode.CUSUM_BREACH));
        assertFalse(codes.contains(ReasonCode.DEBOUNCED));
    }

    @Test
    void reasonCodes_HotButDensityBelowGate_IsBlockedByLevelGate() {
        // the intraday May-2021 case: S+ enormous, but density 0.936 < L 1.0 -> held back
        List<ReasonCode> codes = obs(0.936, 0.9, 0.0006, 0.0013, 0.0014, 1.0, 206.0, 0.0, false).reasonCodes();
        assertTrue(codes.contains(ReasonCode.CUSUM_BREACH));
        assertTrue(codes.contains(ReasonCode.BLOCKED_BY_LEVEL_GATE));
        assertFalse(codes.contains(ReasonCode.LEVEL_GATE_OPEN));
        assertFalse(codes.contains(ReasonCode.DEBOUNCED));
    }

    @Test
    void reasonCodes_HotBothGatesOpenButNotFired_IsDebounced() {
        // already fired this regime: both gates open, meter past threshold, yet no new fire
        List<ReasonCode> codes = obs(1.0, 1.0, 0.001, 0.0013, 0.0014, 1.0, 98.0, 0.0, false).reasonCodes();
        assertTrue(codes.contains(ReasonCode.DEBOUNCED));
        assertFalse(codes.contains(ReasonCode.BLOCKED_BY_LEVEL_GATE));
    }

    @Test
    void reasonCodes_MagnitudeBucketsAndBuildingTrackTheMeter() {
        assertTrue(obs(0.7, 0.5, 0.10, 0.05, 0.02, 0.5, 4.0, 0.0, false)   // z=2.5, act=0.5, not breached
                .reasonCodes().contains(ReasonCode.MAG_GE_2SIGMA));
        assertTrue(obs(0.7, 0.5, 0.10, 0.05, 0.02, 0.5, 4.0, 0.0, false)
                .reasonCodes().contains(ReasonCode.BUILDING));
    }

    @Test
    void lifecycle_TracksFiredDebouncedAndArmed() {
        assertEquals(DetectorState.FIRED,
                obs(1.0, 1.0, 0.81, 0.05, 0.02, 0.167, 34.0, 0.0, true).lifecycle());          // fired
        assertEquals(DetectorState.DEBOUNCED,
                obs(1.0, 1.0, 0.001, 0.0013, 0.0014, 1.0, 98.0, 0.0, false).lifecycle());       // breached, gate open, refractory
        assertEquals(DetectorState.ARMED,
                obs(0.7, 0.5, 0.05, 0.05, 0.02, 0.5, 1.0, 0.0, false).lifecycle());             // watching, not breached
        assertEquals(DetectorState.ARMED,
                obs(0.936, 0.9, 0.0006, 0.0013, 0.0014, 1.0, 206.0, 0.0, false).lifecycle());   // breached but gate shut -> still armed
    }

    @Test
    void reasonCodes_NaNGap_FlagsDataGap() {
        assertTrue(obs(0.7, 0.5, Double.NaN, 0.05, 0.02, 0.5, 1.0, 0.0, false)
                .reasonCodes().contains(ReasonCode.DATA_GAP));
    }

    @Test
    void reasonCodes_DefusionFire_IsFlaggedDistinctly() {
        PipelineObservation defusion = new PipelineObservation(Instant.EPOCH, "crypto", "daily",
                metrics(0.0001), 0.0, 30.0, true, SignalKind.DEFUSION, 8.0, 0.05, 0.02, 0.5);
        assertTrue(defusion.reasonCodes().contains(ReasonCode.FIRE_DEFUSION));
        assertFalse(defusion.reasonCodes().contains(ReasonCode.FIRE_FUSION));
    }

    static PipelineObservation observation(double weightedChange, double sPlus, double sMinus, boolean fired) {
        return obs(0.7, 1.0, weightedChange, 0.05, 0.02, 0.5, sPlus, sMinus, fired);
    }

    static PipelineObservation obs(double density, double largestFraction, double weightedChange,
                                   double mu, double sigma, double level,
                                   double sPlus, double sMinus, boolean fired) {
        ChangeMetrics m = new ChangeMetrics(weightedChange, density, 0.1, 1, largestFraction, List.of(2));
        return new PipelineObservation(Instant.parse("2021-05-18T01:06:00Z"), "crypto", "intraday",
                m, sPlus, sMinus, fired, fired ? SignalKind.FUSION : null, 8.0, mu, sigma, level);
    }

    static ChangeMetrics metrics(double weightedChange) {
        return new ChangeMetrics(weightedChange, 0.7, 0.1, 1, 1.0, List.of(2));
    }
}
