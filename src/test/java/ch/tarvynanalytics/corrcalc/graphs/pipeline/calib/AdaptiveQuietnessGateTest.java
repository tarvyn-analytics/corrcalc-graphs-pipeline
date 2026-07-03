package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.CalibrationEvent;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.CalibrationEventKind;
import ch.tarvynanalytics.graphs.algos.Calibration;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The numerics-spec Q2 oracle for the adaptive quietness gate: the learn-gate dead zone, the
 * freeze-during-alert rule (never learn the run-up — the covid rule), the cold-start protocol, the
 * MAD floor, and the starvation timeout. The independent reference is a naive batch filter+sort
 * re-implementation inside this test ({@link #naiveAdmitted}) plus hand-computed literals — never
 * the streaming ring-buffer code's own output.
 */
class AdaptiveQuietnessGateTest {

    private static final DetectorConfig DETECTOR = DetectorConfig.crypto();
    private static final Instant T0 = Instant.parse("2021-05-01T00:00:00Z");
    private static final double CALM_DENSITY = 0.3;
    private static final double EPS = 1e-12;

    @Test
    void learnGate_DeadZone_ExcludesTremorBar() {
        // The covid rule in miniature: 10 calm bars around 2.0, one 6.0 tremor, 10 more calm bars.
        // The tremor sits far above the |z| <= 2.5 learn ceiling, so it must never enter the calm
        // set and the estimate must be exactly the median of the 20 calm bars.
        AdaptiveCalibration source = source(config(4, 2.5, 2, 1000, 64));
        double[] calm = {2.0, 2.1, 1.9, 2.0, 2.1, 1.9, 2.0, 2.1, 1.9, 2.0};
        int bar = 0;
        for (double c : calm) {
            source.observe(at(bar++), c, CALM_DENSITY);
        }
        double muBeforeTremor = source.muHat();
        assertEquals(10, source.admittedCount(), "all calm bars admitted");

        source.observe(at(bar++), 6.0, CALM_DENSITY);
        assertEquals(10, source.admittedCount(), "the tremor is in the dead zone: not learned");
        assertEquals(muBeforeTremor, source.muHat(), EPS, "the estimate is untouched by the tremor");

        for (double c : calm) {
            source.observe(at(bar++), c, CALM_DENSITY);
        }
        assertEquals(20, source.admittedCount());
        assertEquals(2.0, source.muHat(), EPS, "median of the 20 calm bars only (hand value)");

        // Cross-check the whole stream against the independent batch reference.
        double[] stream = new double[21];
        System.arraycopy(calm, 0, stream, 0, 10);
        stream[10] = 6.0;
        System.arraycopy(calm, 0, stream, 11, 10);
        List<Double> reference = naiveAdmitted(stream, 2.5, 4);
        assertEquals(reference.size(), source.admittedCount(), "same admitted set as the naive filter");
        assertEquals(naiveMedian(reference), source.muHat(), EPS);
    }

    @Test
    void densityAboveTheLevelGate_IsNotCalm() {
        // Density-stable rule (spec 2.1): once an epoch is live, a bar whose density sits above the
        // epoch's level gate must not seed the baseline even if its change is quiet.
        AdaptiveCalibration source = source(config(4, 2.5, 2, 1000, 64));
        int bar = feedCalm(source, 0, 6);
        assertTrue(source.isReady());
        int admitted = source.admittedCount();

        source.observe(at(bar), 2.0, 0.9);   // quiet change, saturated density
        assertEquals(admitted, source.admittedCount(), "a dense bar is not calm");
    }

    @Test
    void freeze_DuringAlert_NoAdmission() {
        // Spec 2.3: no admission while the alarm is active, nor for coolDownBars after it clears;
        // admission resumes exactly at fireBar + coolDownBars + 1.
        AdaptiveCalibration source = source(config(2, 2.5, 3, 1000, 64));
        source.observe(at(0), 2.0, CALM_DENSITY);
        source.observe(at(1), 2.1, CALM_DENSITY);
        assertTrue(source.isReady(), "K = 2 admitted bars promote");
        double muFrozen = source.muHat();

        source.observeDetection(at(2), 6.0, CALM_DENSITY, true);   // the in-fire bar
        for (int i = 0; i < 3; i++) {
            source.observeDetection(at(3 + i), 2.0, CALM_DENSITY, false);   // the refractory tail
            assertEquals(2, source.admittedCount(), "cool-down bar " + i + " is not admitted");
            assertEquals(muFrozen, source.muHat(), EPS, "the estimate is frozen through the cool-down");
        }

        source.observeDetection(at(6), 2.0, CALM_DENSITY, false);   // fireBar + coolDown + 1
        assertEquals(3, source.admittedCount(), "admission resumes on the first post-cool-down bar");
        assertEquals(2.0, source.muHat(), EPS, "median of [2.0, 2.1, 2.0]");
    }

    @Test
    void coldStart_NotReadyBeforeK_PromotesOnTheKthAdmittedBar() {
        AdaptiveCalibration source = source(config(4, 2.5, 2, 1000, 64));
        source.observe(at(0), Double.NaN, Double.NaN);   // a data gap is neither calm nor an event
        assertEquals(0, source.admittedCount(), "a gap never counts toward K");

        int bar = feedCalm(source, 1, 3);
        assertFalse(source.isReady(), "K − 1 admitted bars: still CALIBRATING");
        assertFalse(source.live());
        assertTrue(source.pollEvent().isEmpty());
        assertThrows(IllegalStateException.class, source::calibration);

        source.observe(at(bar), 2.0, CALM_DENSITY);   // the K-th admitted bar
        assertTrue(source.isReady());
        assertTrue(source.live());
        CalibrationEvent promoted = source.pollEvent().orElseThrow();
        assertEquals(CalibrationEventKind.PROMOTED_TO_LIVE, promoted.kind());
        assertEquals(0L, promoted.epochId());
        assertTrue(Double.isNaN(promoted.muBefore()), "a cold start has no baseline before");
        assertEquals(source.muHat(), promoted.muAfter(), EPS);
        assertTrue(source.pollEvent().isEmpty(), "one bounded event per promotion");
    }

    @Test
    void madFloor_MostlyConstantWindow_FloorsToEpsilonSigma() {
        // Spec case 4 (hand literals): window [1,1,1,1,1,1,2,3,4,5] — median 1, MAD 0 (6 of 10
        // identical: MAD floors where population stdev would not) → σ̂ = ε = 1.0.
        assertEquals(1.0, DETECTOR.epsilonSigma(), "the settled crypto sigma floor");
        AdaptiveCalibration source = source(config(10, 10.0, 2, 1000, 10));
        double[] window = {1, 1, 1, 1, 1, 1, 2, 3, 4, 5};
        for (int i = 0; i < window.length; i++) {
            source.observe(at(i), window[i], CALM_DENSITY);
        }
        assertEquals(10, source.admittedCount());
        assertEquals(1.0, source.muHat(), EPS);
        assertEquals(DETECTOR.epsilonSigma(), source.sigmaHat(), EPS, "MAD = 0 floors to ε");
        assertEquals(DETECTOR.epsilonSigma(), source.calibration().sigma(), EPS);
    }

    @Test
    void estimate_SeededCalmStream_MatchesTheNaiveReferenceAndConvergesWithinSE() {
        // Spec case 5: on a seeded calm stream the streaming estimate must equal the independent
        // batch filter+sort reference exactly, and sit within 2·SE of the true calm mean
        // (SE(median) ≈ 1.253·σ/√n → tolerance ≈ 0.09 for σ = 0.5, n ≈ 200). The warm-up (K = 50)
        // is admitted whole — the offline calm-block analogue — then the gate goes live.
        AdaptiveCalibration source = source(config(50, 2.5, 0, 100000, 256));
        Random rng = new Random(7);
        double[] stream = new double[200];
        for (int i = 0; i < stream.length; i++) {
            stream[i] = 2.0 + 0.5 * rng.nextGaussian();
            source.observe(at(i), stream[i], CALM_DENSITY);
        }

        List<Double> reference = naiveAdmitted(stream, 2.5, 50);
        assertEquals(reference.size(), source.admittedCount(), "identical admitted set");
        assertEquals(naiveMedian(reference), source.muHat(), EPS, "identical median");
        assertEquals(2.0, source.muHat(), 0.09, "within 2·SE of the true calm mean");
        assertEquals(0.5, source.sigmaHat(), 0.15, "1.4826·MAD recovers the stdev scale");
    }

    @Test
    void starvation_ForcesEpochDemotesAndRewarms() {
        // Spec 2.5: nothing admitted for regimeShiftTimeoutBars → re-baseline on the raw trailing
        // window (REGIME_TIMEOUT), demote to CALIBRATING, then a fresh K-bar warm-up re-promotes.
        AdaptiveCalibration source = source(config(2, 2.5, 0, 5, 8));
        source.observe(at(0), 2.0, CALM_DENSITY);
        source.observe(at(1), 2.1, CALM_DENSITY);
        assertEquals(CalibrationEventKind.PROMOTED_TO_LIVE, source.pollEvent().orElseThrow().kind());

        for (int i = 0; i < 5; i++) {
            assertTrue(source.live(), "still live before the timeout trips");
            source.observe(at(2 + i), 10.0, CALM_DENSITY);   // a sustained new regime, all dead-zone
        }
        CalibrationEvent timeout = source.pollEvent().orElseThrow();
        assertEquals(CalibrationEventKind.REGIME_TIMEOUT, timeout.kind());
        assertEquals(1L, timeout.epochId());
        assertEquals(10.0, timeout.muAfter(), EPS, "re-baselined on the raw window's median");
        assertEquals(CalibrationEventKind.DEMOTED_TO_CALIBRATING, source.pollEvent().orElseThrow().kind());
        assertFalse(source.live(), "demoted: alerts are not vouched for");
        assertTrue(source.isReady(), "the run keeps a calibration to score with");
        assertEquals(10.0, source.calibration().mu(), EPS);

        source.observe(at(7), 10.0, CALM_DENSITY);
        source.observe(at(8), 10.1, CALM_DENSITY);
        CalibrationEvent repromoted = source.pollEvent().orElseThrow();
        assertEquals(CalibrationEventKind.PROMOTED_TO_LIVE, repromoted.kind());
        assertEquals(2L, repromoted.epochId(), "a re-promotion opens a fresh epoch");
        assertTrue(source.live());
    }

    @Test
    void driftReference_SkewedCalmStream_NeverOpensAFalseEpoch() {
        // The PH reference is the admitted window's arithmetic MEAN (not the calibration median)
        // and its δ/λ scale is the window's SAMPLE std (not the MAD-σ̂). Hand literals: the
        // {1, 1, 4} rotation has median 1.0 but mean 2.0; MAD is 0 (σ̂ floors to ε = 1.0) while
        // the sample std is s = √2.4 ≈ 1.549 — so δ = 0.5s ≈ 0.775, λ = 3s ≈ 4.648. A median
        // reference leaves a persistent +1.0 > δ mean excess that ratchets PH up per cycle into
        // a false epoch within a few cycles (a stale 0.0 reference fires even sooner). Against
        // the mean the per-cycle sum is negative and the excess never exceeds
        // 4 − 2 − δ ≈ 1.225 < λ: the epoch must stand forever.
        AdaptiveCalibration source = new AdaptiveCalibration(
                new AdaptiveCalibrationConfig(6, 10.0, 0, 0.5, 3.0, 1e-6, 1e6, 1000, 64),
                DETECTOR, null);
        double[] rotation = {1.0, 1.0, 4.0};
        int bar = 0;
        for (int i = 0; i < 6; i++) {
            source.observe(at(bar++), rotation[i % 3], CALM_DENSITY);
        }
        assertEquals(CalibrationEventKind.PROMOTED_TO_LIVE, source.pollEvent().orElseThrow().kind());

        for (int i = 0; i < 30; i++) {
            source.observe(at(bar++), rotation[i % 3], CALM_DENSITY);
            assertTrue(source.pollEvent().isEmpty(),
                    "no drift epoch on the steady skewed calm, bar " + bar);
        }
    }

    @Test
    void driftReference_GenuineMeanShift_OpensARecalibratedEpochOnTheHandTracedBar() {
        // The companion guard: the mean reference must not deaden the monitor. Ending the calm
        // feed on a 4.0 bar leaves the PH upper excess at 4 − 2 − δ ≈ 1.225 (δ ≈ 0.775 and
        // λ ≈ 4.648 from the √2.4 sample-std scale at the epoch open); each bar of the shifted
        // regime adds 5 − 2.0 − δ ≈ +2.225 → the excess passes λ on the SECOND shifted bar
        // (1.225 + 2·2.225 ≈ 5.68 > 4.648) → RECALIBRATED.
        AdaptiveCalibration source = new AdaptiveCalibration(
                new AdaptiveCalibrationConfig(6, 10.0, 0, 0.5, 3.0, 1e-6, 1e6, 1000, 64),
                DETECTOR, null);
        double[] rotation = {1.0, 1.0, 4.0};
        int bar = 0;
        for (int i = 0; i < 12; i++) {
            source.observe(at(bar++), rotation[i % 3], CALM_DENSITY);
        }
        assertEquals(CalibrationEventKind.PROMOTED_TO_LIVE, source.pollEvent().orElseThrow().kind());
        assertTrue(source.pollEvent().isEmpty(), "the calm rotation opens nothing by itself");

        source.observe(at(bar++), 5.0, CALM_DENSITY);
        assertTrue(source.pollEvent().isEmpty(), "one shifted bar stays below λ");
        source.observe(at(bar), 5.0, CALM_DENSITY);
        CalibrationEvent recalibrated = source.pollEvent().orElseThrow();
        assertEquals(CalibrationEventKind.RECALIBRATED, recalibrated.kind());
        assertEquals(1L, recalibrated.epochId(), "the drift epoch follows the promotion epoch");
    }

    @Test
    void onRegimeExpired_LiveSource_RebaselinesOnTheRawWindowAndRewarms() {
        // The backstop-expiry escape (spec Q4.1 amendment): same move as the starvation timeout —
        // re-baseline on the raw trailing window, demote, fresh warm-up — but triggered by the
        // cadence expiring an unresolved regime question instead of the rejection counter.
        AdaptiveCalibration source = source(config(2, 2.5, 0, 1000, 8));
        source.observe(at(0), 2.0, CALM_DENSITY);
        source.observe(at(1), 2.1, CALM_DENSITY);
        assertEquals(CalibrationEventKind.PROMOTED_TO_LIVE, source.pollEvent().orElseThrow().kind());
        source.observeDetection(at(2), 10.0, CALM_DENSITY, true);   // the fused span, frozen
        source.observeDetection(at(3), 10.2, CALM_DENSITY, true);

        source.onRegimeExpired(at(4));
        CalibrationEvent timeout = source.pollEvent().orElseThrow();
        assertEquals(CalibrationEventKind.REGIME_TIMEOUT, timeout.kind());
        assertEquals(1L, timeout.epochId());
        assertEquals(6.05, timeout.muAfter(), EPS,
                "re-baselined on the raw window's median: median{2.0, 2.1, 10.0, 10.2}");
        assertEquals(CalibrationEventKind.DEMOTED_TO_CALIBRATING, source.pollEvent().orElseThrow().kind());
        assertFalse(source.live(), "demoted: alerts are not vouched until the re-warm completes");

        source.observe(at(5), 10.0, CALM_DENSITY);
        source.observe(at(6), 10.1, CALM_DENSITY);
        assertEquals(CalibrationEventKind.PROMOTED_TO_LIVE, source.pollEvent().orElseThrow().kind());
        assertTrue(source.live(), "a fresh warm-up re-promotes on the new regime's level");
    }

    @Test
    void onRegimeExpired_WhileReWarming_IsMoot() {
        // A second expiry while already demoted must not stack epochs: the question already expired.
        AdaptiveCalibration source = source(config(2, 2.5, 0, 1000, 8));
        source.observe(at(0), 2.0, CALM_DENSITY);
        source.observe(at(1), 2.1, CALM_DENSITY);
        source.pollEvent();   // PROMOTED_TO_LIVE
        source.onRegimeExpired(at(2));
        source.pollEvent();   // REGIME_TIMEOUT
        source.pollEvent();   // DEMOTED_TO_CALIBRATING

        source.onRegimeExpired(at(3));
        assertTrue(source.pollEvent().isEmpty(), "no new epoch from an expiry while re-warming");
    }

    @Test
    void priorArtifact_StartsLiveAndPersistsThePriorUntilItLearns() {
        // The pragmatic v1 seeding path (spec 2.4): an operator-vouched artifact starts the source
        // LIVE immediately; before two admissions the persisted form is the prior itself.
        CalibrationArtifact prior = new CalibrationArtifact(CalibrationArtifact.SCHEMA_VERSION,
                "crypto", "intraday", 3L, T0, T0.plusSeconds(3600), 50,
                new Calibration(0.02, 0.01, 0.8, 0.3, 0.05));
        AdaptiveCalibration source = new AdaptiveCalibration(config(4, 2.5, 2, 1000, 64), DETECTOR, prior);

        assertTrue(source.isReady());
        assertTrue(source.live());
        assertEquals(prior.calibration(), source.calibration());
        assertEquals(3L, source.provenance().epochId());
        assertEquals("adaptive", source.provenance().calibrationMode());
        assertEquals(prior, source.artifact("crypto", "intraday"),
                "nothing learned yet: the prior is what this run runs on");
    }

    @Test
    void artifact_ColdStartBeforeReady_Throws() {
        AdaptiveCalibration source = source(config(4, 2.5, 2, 1000, 64));
        source.observe(at(0), 2.0, CALM_DENSITY);
        assertThrows(IllegalStateException.class, () -> source.artifact("crypto", "intraday"));
    }

    // ---- the independent naive reference: a batch filter over a growing list (spec 2.7) ----

    /**
     * The learn-gate mask replayed batch-style: the warm-up window is admitted whole (the offline
     * calm-block analogue — the gate needs a live yardstick), then |z| ≤ zLearn against the
     * median/MAD of the admitted-so-far set.
     */
    private static List<Double> naiveAdmitted(double[] stream, double zLearn, int warmup) {
        List<Double> admitted = new ArrayList<>();
        for (double c : stream) {
            if (admitted.size() >= warmup) {
                double mu = naiveMedian(admitted);
                double sigma = 1.4826 * naiveMad(admitted, mu);
                if (sigma == 0.0) {
                    sigma = DETECTOR.epsilonSigma();
                }
                if (Math.abs((c - mu) / sigma) > zLearn) {
                    continue;
                }
            }
            admitted.add(c);
        }
        return admitted;
    }

    private static double naiveMedian(List<Double> values) {
        List<Double> sorted = new ArrayList<>(values);
        sorted.sort(Double::compareTo);
        int n = sorted.size();
        return n % 2 == 1 ? sorted.get(n / 2) : (sorted.get(n / 2 - 1) + sorted.get(n / 2)) / 2.0;
    }

    private static double naiveMad(List<Double> values, double center) {
        List<Double> deviations = new ArrayList<>(values.size());
        for (double v : values) {
            deviations.add(Math.abs(v - center));
        }
        return naiveMedian(deviations);
    }

    @Test
    void sigmaFloor_LiftsACollapsedTrailingSigmaTowardTheLongWindow() {
        // Q3 σ-floor (H2R-1 amendment, design §5.2/§7): a brief flat calm patch collapses the 4-bar
        // trailing MAD to the ε floor, but the 12-bar reference window still carries the 0↔6 swing
        // (sample σ = sqrt(72/11) ≈ 2.558), so the floor lifts σ̂ to 0.5·σ_ref ≈ 1.279 — the RUN-1
        // calm-metronome guard. Off (frac=0) the same tape leaves σ̂ at ε, byte-for-byte pre-Q3.
        // ref: sum=36, sumSq=180, var=(180−36²/12)/11 = 72/11 over the 12 admitted bars.
        AdaptiveCalibrationConfig floored =
                new AdaptiveCalibrationConfig(2, 1e9, 0, 0.25, 1e6, 1e-6, 1e6, 1_000_000, 4, 0.5, 12);
        AdaptiveCalibrationConfig off =
                new AdaptiveCalibrationConfig(2, 1e9, 0, 0.25, 1e6, 1e-6, 1e6, 1_000_000, 4);
        double[] cs = {0, 6, 0, 6, 0, 6, 0, 6, 3, 3, 3, 3};

        AdaptiveCalibration on = feedSeries(floored, cs);
        AdaptiveCalibration bare = feedSeries(off, cs);

        assertEquals(DETECTOR.epsilonSigma(), bare.sigmaHat(), EPS,
                "floor off: the flat trailing window collapses to the ε floor");
        assertEquals(0.5 * Math.sqrt(72.0 / 11.0), on.sigmaHat(), 1e-9,
                "floor on: σ̂ lifted to 0.5·σ_ref over the 12-bar reference window");
        assertTrue(on.sigmaHat() > bare.sigmaHat(), "the relative floor strictly lifts the collapsed σ̂");
    }

    @Test
    void config_RejectsNegativeFloorFracAndTinyRefWindow() {
        assertThrows(IllegalArgumentException.class,
                () -> new AdaptiveCalibrationConfig(2, 2.5, 0, 0.25, 5.0, 0.5, 2.0, 90, 45, -0.1, 30));
        assertThrows(IllegalArgumentException.class,
                () -> new AdaptiveCalibrationConfig(2, 2.5, 0, 0.25, 5.0, 0.5, 2.0, 90, 45, 0.5, 1));
    }

    // ---- fixtures ----

    private static AdaptiveCalibration source(AdaptiveCalibrationConfig config) {
        return new AdaptiveCalibration(config, DETECTOR, null);
    }

    /** Feeds a raw weighted-change series (constant calm density) and returns the source. */
    private static AdaptiveCalibration feedSeries(AdaptiveCalibrationConfig config, double[] cs) {
        AdaptiveCalibration source = new AdaptiveCalibration(config, DETECTOR, null);
        for (int i = 0; i < cs.length; i++) {
            source.observe(at(i), cs[i], CALM_DENSITY);
        }
        return source;
    }

    /** Benign drift constants: the meta-monitor and σ-guard can never interfere with a gate test. */
    private static AdaptiveCalibrationConfig config(int warmupBars, double learnThreshold,
                                                    int coolDownBars, int timeoutBars, int windowBars) {
        return new AdaptiveCalibrationConfig(warmupBars, learnThreshold, coolDownBars,
                0.25, 1e6, 1e-6, 1e6, timeoutBars, windowBars);
    }

    /** Feeds {@code n} admitted calm bars ({@code 2.0/2.1/1.9} rotation) starting at {@code bar}. */
    private static int feedCalm(AdaptiveCalibration source, int bar, int n) {
        double[] rotation = {2.0, 2.1, 1.9};
        for (int i = 0; i < n; i++) {
            source.observe(at(bar++), rotation[i % 3], CALM_DENSITY);
        }
        return bar;
    }

    private static Instant at(int bar) {
        return T0.plusSeconds(60L * bar);
    }
}
