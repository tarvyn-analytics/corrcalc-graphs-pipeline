package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import org.junit.jupiter.api.Test;

import static ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObservationTest.observation;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SeverityHysteresisTest {

    @Test
    void step_EscalatesImmediately() {
        SeverityHysteresis h = new SeverityHysteresis();
        assertEquals(Severity.CALM, h.step(act(0.20)));
        assertEquals(Severity.WATCH, h.step(act(0.55)));   // crosses the watch fraction at once
        assertEquals(Severity.WARN, h.step(act(0.85)));    // crosses the warn fraction at once
    }

    @Test
    void step_DeEscalatesOnlyPastTheLowerBand() {
        SeverityHysteresis h = new SeverityHysteresis();
        h.step(act(0.85));                                  // -> WARN
        assertEquals(Severity.WARN, h.step(act(0.72)));     // 0.72 > 0.8-0.1 : stays WARN (anti-flap)
        assertEquals(Severity.WATCH, h.step(act(0.68)));    // 0.68 < 0.7 : drops to WATCH
        assertEquals(Severity.WATCH, h.step(act(0.42)));    // 0.42 > 0.5-0.1 : stays WATCH
        assertEquals(Severity.CALM, h.step(act(0.38)));     // 0.38 < 0.4 : drops to CALM
    }

    @Test
    void step_FireIsAPerBarOverrideThatDoesNotStick() {
        SeverityHysteresis h = new SeverityHysteresis();
        h.step(act(0.55));                                  // WATCH sticky
        assertEquals(Severity.FIRE, h.step(fire(1.20)));    // fired -> FIRE for this bar only
        assertEquals(Severity.WARN, h.step(act(0.85)));     // sticky state tracked the meter, not FIRE
        assertEquals(Severity.CALM, h.step(act(0.05)));     // de-escalates normally afterwards
    }

    private static PipelineObservation act(double activation) {
        return observation(0.0, activation * 8.0, 0.0, false);   // h = 8 in the fixture -> activation = sPlus/8
    }

    private static PipelineObservation fire(double activation) {
        return observation(0.0, activation * 8.0, 0.0, true);
    }
}
