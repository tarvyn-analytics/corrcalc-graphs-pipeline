package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import ch.tarvynanalytics.graphs.algos.model.ChangeMetrics;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
                Instant.EPOCH, "crypto", "intraday", null, 0, 0, false, null, 8.0));
        assertThrows(IllegalArgumentException.class, () -> new PipelineObservation(
                Instant.EPOCH, "crypto", "intraday", metrics(0.05), 0, 0, false, null, 0.0));
    }

    static PipelineObservation observation(double weightedChange, double sPlus, double sMinus, boolean fired) {
        return new PipelineObservation(Instant.parse("2021-05-18T01:06:00Z"), "crypto", "intraday",
                metrics(weightedChange), sPlus, sMinus, fired, fired ? SignalKind.FUSION : null, 8.0);
    }

    static ChangeMetrics metrics(double weightedChange) {
        return new ChangeMetrics(weightedChange, 0.7, 0.1, 1, 1.0, List.of(2));
    }
}
