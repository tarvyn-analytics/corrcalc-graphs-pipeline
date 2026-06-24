package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import org.junit.jupiter.api.Test;

import static ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObservationTest.observation;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ObservationPolicyTest {

    private static final PipelineObservation FIRE = observation(0.20, 8.5, 0.0, true);   // activation > 1
    private static final PipelineObservation CALM = observation(0.02, 1.0, 0.0, false);  // activation 0.125
    private static final PipelineObservation GAP = observation(Double.NaN, 0.0, 0.0, false);

    @Test
    void all_ForwardsEverything() {
        assertTrue(ObservationPolicy.all().emit(CALM));
        assertTrue(ObservationPolicy.all().emit(FIRE));
        assertTrue(ObservationPolicy.all().emit(GAP));
    }

    @Test
    void firesOnly_ForwardsFiresOnly() {
        assertTrue(ObservationPolicy.firesOnly().emit(FIRE));
        assertFalse(ObservationPolicy.firesOnly().emit(CALM));
    }

    @Test
    void minWeightedChange_GatesOnMagnitude_AndDropsNaNGap() {
        ObservationPolicy p = ObservationPolicy.minWeightedChange(0.1);
        assertTrue(p.emit(FIRE));    // 0.20 >= 0.1
        assertFalse(p.emit(CALM));   // 0.02 < 0.1
        assertFalse(p.emit(GAP));    // NaN never passes
    }

    @Test
    void minActivation_GatesOnCusumFraction() {
        ObservationPolicy p = ObservationPolicy.minActivation(1.0);
        assertTrue(p.emit(FIRE));    // 8.5/8 > 1
        assertFalse(p.emit(CALM));   // 1/8 < 1
    }

    @Test
    void compose_AndOr() {
        ObservationPolicy fireOrBig = ObservationPolicy.firesOnly().or(ObservationPolicy.minWeightedChange(0.1));
        assertTrue(fireOrBig.emit(FIRE));
        assertFalse(fireOrBig.emit(CALM));

        ObservationPolicy fireAndBig = ObservationPolicy.firesOnly().and(ObservationPolicy.minActivation(0.5));
        assertTrue(fireAndBig.emit(FIRE));
        assertFalse(fireAndBig.emit(CALM));
    }

    @Test
    void factories_RejectNegativeThresholdsAndNullCompose() {
        assertThrows(IllegalArgumentException.class, () -> ObservationPolicy.minWeightedChange(-0.1));
        assertThrows(IllegalArgumentException.class, () -> ObservationPolicy.minActivation(-1.0));
        assertThrows(IllegalArgumentException.class, () -> ObservationPolicy.all().and(null));
        assertThrows(IllegalArgumentException.class, () -> ObservationPolicy.all().or(null));
    }
}
