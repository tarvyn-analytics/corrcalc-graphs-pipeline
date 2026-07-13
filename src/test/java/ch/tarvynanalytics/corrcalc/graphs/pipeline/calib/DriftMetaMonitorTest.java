package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Pins the drift meta-monitor to an independent hand trace: the two-sided Page-Hinkley
 * arms with tolerance {@code δ = driftDelta·σ_epoch} and threshold {@code λ = driftLambda·σ_epoch},
 * plus the σ-ratio guard. Every expectation below is a hand-computed literal from the spec's
 * worked example (μ_epoch = 2.0, σ_epoch = 1.0, δ = 0.25) — never the code's own output.
 */
class DriftMetaMonitorTest {

    private static final double EPS = 1e-9;

    /** The spec's calm phase: every increment {@code c − μ − δ} is negative, so PH pins at 0. */
    private static final double[] CALM = {2.0, 2.05, 1.95, 2.0, 2.1, 1.9};

    @Test
    void observeAdmitted_SustainedUpShift_OpensEpochOnTheHandTracedBar() {
        // The hand trace with λ = 2σ: six calm bars (PH ≡ 0), then a genuine +1.0 shift;
        // PH climbs 0.75 → 1.55 → 2.25 and crosses λ on the THIRD post-shift bar (index 8).
        DriftMetaMonitor monitor = monitor(0.25, 2.0);
        monitor.open(2.0, 1.0, 1.0);

        double[] stream = {2.0, 2.05, 1.95, 2.0, 2.1, 1.9, 3.0, 3.05, 2.95};
        for (int i = 0; i < 6; i++) {
            assertEquals(DriftMetaMonitor.Trigger.NONE, monitor.observeAdmitted(stream[i], 1.0), "calm bar " + i);
            assertEquals(0.0, monitor.phUp(), EPS, "PH pinned at 0 during calm, bar " + i);
        }
        assertEquals(DriftMetaMonitor.Trigger.NONE, monitor.observeAdmitted(stream[6], 1.0));
        assertEquals(0.75, monitor.phUp(), EPS, "first post-shift bar");
        assertEquals(DriftMetaMonitor.Trigger.NONE, monitor.observeAdmitted(stream[7], 1.0));
        assertEquals(1.55, monitor.phUp(), EPS, "second post-shift bar");
        assertEquals(DriftMetaMonitor.Trigger.DRIFT, monitor.observeAdmitted(stream[8], 1.0),
                "PH = 2.25 > λ = 2.0 opens the epoch on the third post-shift bar");
    }

    @Test
    void observeAdmitted_SustainedDownShift_OpensEpochSymmetrically() {
        // The lower arm mirrors the upper: a −1.0 shift (baseline vol falling) must open too.
        DriftMetaMonitor monitor = monitor(0.25, 2.0);
        monitor.open(2.0, 1.0, 1.0);

        double[] stream = {2.0, 2.05, 1.95, 2.0, 2.1, 1.9, 1.0, 0.95, 1.05};
        DriftMetaMonitor.Trigger last = DriftMetaMonitor.Trigger.NONE;
        for (double c : stream) {
            last = monitor.observeAdmitted(c, 1.0);
        }
        assertEquals(DriftMetaMonitor.Trigger.DRIFT, last,
                "the symmetric −1.0 shift opens on its third post-shift bar");
    }

    @Test
    void observeAdmitted_CalmNoise_NeverOpensEpoch() {
        // Specificity (spec: "pure calm noise never fires"): the running-min reference tracks m
        // down, so PH stays ~0 over arbitrarily long calm stretches.
        DriftMetaMonitor monitor = monitor(0.25, 5.0);
        monitor.open(2.0, 1.0, 1.0);

        for (int rep = 0; rep < 50; rep++) {
            for (double c : CALM) {
                assertEquals(DriftMetaMonitor.Trigger.NONE, monitor.observeAdmitted(c, 1.0));
            }
        }
        assertEquals(0.0, monitor.phUp(), EPS, "calm noise leaves no accumulated excess");
    }

    @Test
    void observeAdmitted_TransientBlip_AbsorbedBelowLambda() {
        // Spec case 3: a 2-bar spike to 4.0 under the λ = 5σ default peaks PH at exactly
        // 2·(4.0 − 2.0 − 0.25) = 3.5 < 5 and decays — no epoch opens.
        DriftMetaMonitor monitor = monitor(0.25, 5.0);
        monitor.open(2.0, 1.0, 1.0);

        for (double c : new double[]{2.0, 2.0, 2.0, 2.0, 2.0, 2.0}) {
            assertEquals(DriftMetaMonitor.Trigger.NONE, monitor.observeAdmitted(c, 1.0));
        }
        assertEquals(DriftMetaMonitor.Trigger.NONE, monitor.observeAdmitted(4.0, 1.0));
        assertEquals(1.75, monitor.phUp(), EPS);
        assertEquals(DriftMetaMonitor.Trigger.NONE, monitor.observeAdmitted(4.0, 1.0));
        assertEquals(3.5, monitor.phUp(), EPS, "the blip's peak stays below λ = 5");
        for (double c : new double[]{2.0, 2.0, 2.0, 2.0}) {
            assertEquals(DriftMetaMonitor.Trigger.NONE, monitor.observeAdmitted(c, 1.0));
        }
    }

    @Test
    void observeAdmitted_SigmaLeavesTheBand_SustainedOpensEpoch() {
        // The σ-guard: a flat mean but σ̂ past 2.0·σ_epoch for sigmaSustainBars (3 here) opens an
        // epoch a mean-only PH would miss; an in-band bar resets the streak.
        DriftMetaMonitor monitor = new DriftMetaMonitor(config(0.25, 5.0, 0.5, 2.0), 3);
        monitor.open(2.0, 1.0, 1.0);

        assertEquals(DriftMetaMonitor.Trigger.NONE, monitor.observeAdmitted(2.0, 2.5));
        assertEquals(DriftMetaMonitor.Trigger.NONE, monitor.observeAdmitted(2.0, 2.5));
        assertEquals(DriftMetaMonitor.Trigger.NONE, monitor.observeAdmitted(2.0, 1.0), "in-band resets the streak");
        assertEquals(DriftMetaMonitor.Trigger.NONE, monitor.observeAdmitted(2.0, 2.5));
        assertEquals(DriftMetaMonitor.Trigger.NONE, monitor.observeAdmitted(2.0, 2.5));
        assertEquals(DriftMetaMonitor.Trigger.SIGMA, monitor.observeAdmitted(2.0, 2.5),
                "the third consecutive outside-band bar trips the guard");
    }

    @Test
    void open_AfterAnEpoch_ResetsThePhStateAndDetectsAfresh() {
        // Q1's rule applied at the meta level: an epoch open invalidates the old accumulation, and
        // an identical later shift against the NEW baseline is detected with the same delay.
        DriftMetaMonitor monitor = monitor(0.25, 2.0);
        monitor.open(2.0, 1.0, 1.0);
        for (double c : new double[]{2.0, 2.0, 2.0, 3.0, 3.0}) {
            monitor.observeAdmitted(c, 1.0);
        }
        assertEquals(DriftMetaMonitor.Trigger.DRIFT, monitor.observeAdmitted(3.0, 1.0), "first epoch opens");

        monitor.open(3.0, 1.0, 1.0);
        assertEquals(0.0, monitor.phUp(), EPS, "the arms re-baseline at the epoch open");
        assertEquals(DriftMetaMonitor.Trigger.NONE, monitor.observeAdmitted(4.0, 1.0));
        assertEquals(0.75, monitor.phUp(), EPS, "the same +1 shift accumulates identically vs the new μ_epoch");
        assertEquals(DriftMetaMonitor.Trigger.NONE, monitor.observeAdmitted(4.05, 1.0));
        assertEquals(DriftMetaMonitor.Trigger.DRIFT, monitor.observeAdmitted(3.95, 1.0),
                "detected afresh on its own third post-shift bar");
    }

    /** A monitor whose σ-guard can never interfere (wide band, long sustain). */
    private static DriftMetaMonitor monitor(double delta, double lambda) {
        return new DriftMetaMonitor(config(delta, lambda, 1e-6, 1e6), Integer.MAX_VALUE);
    }

    private static AdaptiveCalibrationConfig config(double delta, double lambda, double lo, double hi) {
        return new AdaptiveCalibrationConfig(2, 2.5, 0, delta, lambda, lo, hi, 1000, 8);
    }
}
