package ch.tarvynanalytics.corrcalc.graphs.pipeline.engine;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.CalibrationEvent;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.CollectingSink;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.ObservationPolicy;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObservation;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObserver;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.RegimeEvent;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationArtifact;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationProvenance;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationSource;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationSources;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.PairUniverse;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.RearmConfig;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.RegimeTimescaleConfig;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.TimescaleConfig;
import ch.tarvynanalytics.graphs.algos.Calibration;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;
import ch.tarvynanalytics.graphs.algos.RegimeConfig;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@link PairUniverse} seam through the real engine (design/23 3.5): absent/{@link PairUniverse#ALL}
 * must be byte-identical to the seam not existing, and a known active/all ratio must rescale density
 * exactly at the four named consumer points and nowhere else. Order-4 symbols ({@code allPairs = 6})
 * with a fixed 3 active pairs gives an exact ×2.0 multiplier -- exact not because ×2.0 is a power of
 * two (in general {@code (raw*6.0)/3.0} is not bit-identical to {@code raw*2.0}: they differ by 1 ulp
 * for roughly 1 in 6 arbitrary doubles), but because an order-4 universe's {@code densityLevel} always
 * lands on the {@code k/6} lattice ({@code k} the integer edge count, {@code allPairs} fixed at 6) --
 * verified empirically across these tapes -- so the rescale of a {@code k/6} value by an exact 2.0
 * stays bit-exact, letting these tests assert exact equality rather than a tolerance.
 */
class PipelineEnginePairUniverseTest {

    private static final int WINDOW = 12;
    private static final TimescaleConfig CFG = new TimescaleConfig(WINDOW, DetectorConfig.crypto());
    private static final PairUniverse HALF = asOf -> 3L;   // order 4 -> allPairs 6 -> exact x2.0

    @Test
    void pairUniverse_AbsentExplicitAllAndNull_ProduceByteIdenticalStreams() {
        // The regression proof (design/23 3.5): with no seam configured, explicitly set to ALL, or
        // set to null (the Builder's null-guard), every existing code path is untouched.
        Returns tape = calmThenFused(48, 24, 4, 42L);

        Recorded absent = drive(builder().sink(new CollectingSink()), tape);
        Recorded explicitAll = drive(builder().sink(new CollectingSink()).pairUniverse(PairUniverse.ALL), tape);
        Recorded nullSetter = drive(builder().sink(new CollectingSink()).pairUniverse(null), tape);

        assertEquals(absent.observations(), explicitAll.observations());
        assertEquals(absent.events(), explicitAll.events());
        assertEquals(absent.regimeEvents(), explicitAll.regimeEvents());
        assertEquals(absent.observations(), nullSetter.observations());
        assertEquals(absent.events(), nullSetter.events());
        assertEquals(absent.regimeEvents(), nullSetter.regimeEvents());
    }

    @Test
    void normalizedDensity_KnownRatio_RescalesPipelineObservationDensityExactlyAndOnlyDensity() {
        Returns tape = calmThenFused(48, 24, 4, 42L);

        List<PipelineObservation> baseline = drive(builder().sink(new CollectingSink()), tape).observations();
        List<PipelineObservation> scaled =
                drive(builder().sink(new CollectingSink()).pairUniverse(HALF), tape).observations();

        assertEquals(baseline.size(), scaled.size());
        assertTrue(!baseline.isEmpty());
        for (int i = 0; i < baseline.size(); i++) {
            PipelineObservation b = baseline.get(i);
            PipelineObservation s = scaled.get(i);
            String at = "observation " + i + " (" + b.asOf() + ")";
            assertEquals(b.metrics().densityLevel() * 2.0, s.metrics().densityLevel(), 0.0,
                    "density is rescaled by allPairs/activePairs exactly: " + at);
            // Every other ChangeMetrics field derives from the raw matrix, untouched by the seam.
            assertEquals(b.metrics().weightedChange(), s.metrics().weightedChange(), 0.0, at);
            assertEquals(b.metrics().edgeXor(), s.metrics().edgeXor(), 0.0, at);
            assertEquals(b.metrics().nComponents(), s.metrics().nComponents(), at);
            assertEquals(b.metrics().largestComponentFraction(), s.metrics().largestComponentFraction(), 0.0, at);
            assertEquals(b.metrics().componentSizes(), s.metrics().componentSizes(), at);
            assertEquals(b.asOf(), s.asOf(), at);
            assertEquals(b.fired(), s.fired(), at);
            // FMCSM regression: withNormalizedDensity must rebuild ChangeMetrics through the 7-arg
            // constructor, carrying definedPairCount through -- the legacy 6-arg form silently drops
            // it to -1 on every bar. Order-4, no gaps: every bar sees all 6 pairs finite in both windows.
            assertTrue(b.metrics().definedPairCount() >= 0, "never the legacy-ctor sentinel: " + at);
            assertEquals(b.metrics().definedPairCount(), s.metrics().definedPairCount(),
                    "density normalization must not touch definedPairCount: " + at);
        }
    }

    @Test
    void normalizedDensity_KnownRatio_RescalesCalibrationSourceDensityArgsExactly() {
        Returns tape = calmThenFused(48, 24, 4, 42L);
        DensityRecordingSource baselineSource = new DensityRecordingSource(CFG.detector());
        DensityRecordingSource scaledSource = new DensityRecordingSource(CFG.detector());

        drive(builder().sink(new CollectingSink()).calibrationSource(baselineSource), tape);
        drive(builder().sink(new CollectingSink()).pairUniverse(HALF).calibrationSource(scaledSource), tape);

        assertEquals(baselineSource.densities.size(), scaledSource.densities.size());
        assertTrue(!baselineSource.densities.isEmpty());
        for (int i = 0; i < baselineSource.densities.size(); i++) {
            assertEquals(baselineSource.densities.get(i) * 2.0, scaledSource.densities.get(i), 0.0,
                    "calibration-source density arg #" + i);
        }
    }

    @Test
    void normalizedDensity_KnownRatio_RelaxationGateComparesNormalizedDensityNotRaw() {
        // The 4th consumer point (design/23 3.5): RearmCadence's S+-relaxation-with-gate-closed rule
        // compares density against levelGate, and levelGate is learned in normalized units the moment
        // a PairUniverse is active (calibrate() feeds it normalizedDensity too) -- so the density
        // handed to that compare must be normalized as well, or the gate closes far too easily and
        // the cadence re-arms early. De-fusion off (crypto's default) + a 5-bar relax sustain, no
        // calendar interference within this tape (calendarRearmBars 100). The second shock lands at
        // bar 40 of the post-fire calm span: chosen (by scanning bar 35..65 against a deliberately
        // reintroduced raw-density regression) to sit AFTER the bug's early, too-permissive re-arm but
        // BEFORE the correct one -- exactly the window where a raw-vs-normalized mismatch is visible
        // and a unit-consistent compare is not. Drive the identical tape baseline (PairUniverse absent,
        // ratio 1.0) and scaled (PairUniverse HALF, ratio 2.0): a unit-consistent compare is scale
        // invariant (2x < 2y iff x < y), so a correct cadence re-arms on the identical bar either way
        // and the second shock is debounced in both runs alike; the bug re-arms the scaled run early
        // and lets its second shock fire where the baseline's is still suppressed.
        TimescaleConfig relaxCfg = new TimescaleConfig(WINDOW, DetectorConfig.crypto(),
                new RearmConfig(true, 2, 0.25, 5, 100));
        Returns tape = relaxationTape(48, 24, 40, 4, 11L);

        Recorded baseline = drive(PipelineEngine.builder(syms(4), relaxCfg).calmBars(24)
                .market("crypto").timescale("intraday").observationPolicy(ObservationPolicy.all())
                .sink(new CollectingSink()), tape);
        Recorded scaled = drive(PipelineEngine.builder(syms(4), relaxCfg).calmBars(24)
                .market("crypto").timescale("intraday").observationPolicy(ObservationPolicy.all())
                .pairUniverse(HALF).sink(new CollectingSink()), tape);

        assertEquals(baseline.observations().size(), scaled.observations().size());
        List<Instant> baselineFired = baseline.observations().stream().filter(PipelineObservation::fired)
                .map(PipelineObservation::asOf).toList();
        List<Instant> scaledFired = scaled.observations().stream().filter(PipelineObservation::fired)
                .map(PipelineObservation::asOf).toList();
        assertEquals(1, baselineFired.size(),
                "the second shock must still be debounced in the untouched baseline: " + baselineFired);
        assertEquals(baselineFired, scaledFired,
                "the relaxation gate must re-arm at the identical bar in both runs -- a "
                        + "raw-vs-normalized mismatch would re-arm the scaled run early");
        for (int i = 0; i < baseline.observations().size(); i++) {
            PipelineObservation b = baseline.observations().get(i);
            PipelineObservation s = scaled.observations().get(i);
            assertEquals(b.metrics().densityLevel() * 2.0, s.metrics().densityLevel(), 0.0, "observation " + i);
            assertEquals(b.levelGate() * 2.0, s.levelGate(), 0.0, "observation " + i);
        }
    }

    /** Calm warm-up, a fusing shock, a second calm span long enough for S+ to drain and density to
     * relax through the gate (giving the relaxation rule room to resolve), then a second fusing
     * shock to prove whether the cadence has re-armed. */
    private static Returns relaxationTape(int calmRows, int fusedRows, int secondCalmRows, int symbols, long seed) {
        Random rng = new Random(seed);
        int rows = calmRows + fusedRows + secondCalmRows + fusedRows;
        double[][] returns = new double[rows][symbols];
        List<Instant> timestamps = new ArrayList<>();
        Instant t0 = Instant.parse("2021-05-01T00:00:00Z");
        int t = 0;
        for (int i = 0; i < calmRows; i++, t++) {
            timestamps.add(t0.plusSeconds(60L * t));
            for (int s = 0; s < symbols; s++) {
                returns[t][s] = 0.2 * rng.nextGaussian();
            }
        }
        for (int i = 0; i < fusedRows; i++, t++) {
            timestamps.add(t0.plusSeconds(60L * t));
            double shared = 5.0 * rng.nextGaussian();
            for (int s = 0; s < symbols; s++) {
                returns[t][s] = shared;
            }
        }
        for (int i = 0; i < secondCalmRows; i++, t++) {
            timestamps.add(t0.plusSeconds(60L * t));
            for (int s = 0; s < symbols; s++) {
                returns[t][s] = 0.2 * rng.nextGaussian();
            }
        }
        for (int i = 0; i < fusedRows; i++, t++) {
            timestamps.add(t0.plusSeconds(60L * t));
            double shared = 5.0 * rng.nextGaussian();
            for (int s = 0; s < symbols; s++) {
                returns[t][s] = shared;
            }
        }
        return new Returns(timestamps, returns);
    }

    @Test
    void normalizedDensity_KnownRatio_RescalesRegimeDensityReadExactly() {
        // hi 0.85 / lo 0.45 / confirm 3, no smoothing (smoothWindow 1) so a day's level is its mean
        // density -- the same clean daily-mean pattern PipelineEngineRegimeTest pins.
        RegimeTimescaleConfig regime = new RegimeTimescaleConfig(new RegimeConfig(0.85, 0.45, 3), 1);
        boolean[] fusedPerDay = {false, false, false, true, true, true, true, false, false, false, false};

        List<Double> baseline = driveDailyDensity(regime, fusedPerDay, null);
        List<Double> scaled = driveDailyDensity(regime, fusedPerDay, HALF);

        assertEquals(baseline.size(), scaled.size());
        assertTrue(!baseline.isEmpty());
        for (int i = 0; i < baseline.size(); i++) {
            assertEquals(baseline.get(i) * 2.0, scaled.get(i), 0.0, "daily density level #" + i);
        }
    }

    private static List<Double> driveDailyDensity(RegimeTimescaleConfig regime, boolean[] fusedPerDay,
                                                   PairUniverse universe) {
        List<Double> levels = new ArrayList<>();
        PipelineEngine.Builder b = PipelineEngine.builder(syms(4), CFG).calmBars(6)
                .market("crypto").timescale("intraday").sink(new CollectingSink())
                .observer(new PipelineObserver() {
                    @Override
                    public void onObservation(PipelineObservation observation) {
                        // the daily density series is what this test asserts on, not the annotation stream
                    }

                    @Override
                    public void onDensityLevel(Instant asOf, double smoothedLevel) {
                        levels.add(smoothedLevel);
                    }
                })
                .observeDensity(true).regime(regime);
        if (universe != null) {
            b.pairUniverse(universe);
        }
        PipelineEngine engine = b.build();
        Random rng = new Random(7L);
        for (int d = 0; d < fusedPerDay.length; d++) {
            if (d > 0) {
                engine.onSessionBoundary();
            }
            Instant base = day(d);
            for (int i = 0; i < 16; i++) {
                engine.onReturns(base.plus(i, ChronoUnit.MINUTES), bar(fusedPerDay[d], i, rng));
            }
        }
        engine.finish();
        return levels;
    }

    private static double[] bar(boolean fused, int i, Random rng) {
        double[] row = new double[4];
        if (fused) {
            double shared = rng.nextGaussian();
            for (int s = 0; s < 4; s++) {
                row[s] = shared;
            }
        } else {
            row[i % 4] = 1.0;
        }
        return row;
    }

    private static Instant day(int d) {
        return Instant.parse("2021-05-01T00:00:00Z").plus(d, ChronoUnit.DAYS);
    }

    /** Records every density argument handed to {@link #observe} / {@link #observeDetection}, delegating
     * behaviour to a real leading-warmup source underneath so the run drives identically either way. */
    private static final class DensityRecordingSource implements CalibrationSource {
        private final CalibrationSource delegate;
        final List<Double> densities = new ArrayList<>();

        DensityRecordingSource(DetectorConfig config) {
            this.delegate = CalibrationSources.leadingWarmup(24, config);
        }

        @Override
        public void observe(Instant asOf, double weightedChange, double density) {
            densities.add(density);
            delegate.observe(asOf, weightedChange, density);
        }

        @Override
        public void observeDetection(Instant asOf, double weightedChange, double density, boolean alarmActive) {
            densities.add(density);
            delegate.observeDetection(asOf, weightedChange, density, alarmActive);
        }

        @Override
        public boolean isReady() {
            return delegate.isReady();
        }

        @Override
        public Calibration calibration() {
            return delegate.calibration();
        }

        @Override
        public CalibrationProvenance provenance() {
            return delegate.provenance();
        }

        @Override
        public CalibrationArtifact artifact(String market, String timescale) {
            return delegate.artifact(market, timescale);
        }
    }

    private static PipelineEngine.Builder builder() {
        return PipelineEngine.builder(syms(4), CFG).calmBars(24).market("crypto").timescale("intraday")
                .observationPolicy(ObservationPolicy.all());
    }

    private record Recorded(List<PipelineObservation> observations, List<CalibrationEvent> events,
                            List<RegimeEvent> regimeEvents) {
    }

    private static Recorded drive(PipelineEngine.Builder b, Returns tape) {
        List<PipelineObservation> observations = new ArrayList<>();
        List<CalibrationEvent> events = new ArrayList<>();
        List<RegimeEvent> regimeEvents = new ArrayList<>();
        PipelineEngine engine = b.observer(new PipelineObserver() {
            @Override
            public void onObservation(PipelineObservation observation) {
                observations.add(observation);
            }

            @Override
            public void onCalibrationEvent(CalibrationEvent event) {
                events.add(event);
            }

            @Override
            public void onRegimeEvent(RegimeEvent event) {
                regimeEvents.add(event);
            }
        }).build();
        for (int t = 0; t < tape.rows().length; t++) {
            engine.onReturns(tape.timestamps().get(t), tape.rows()[t]);
            if (engine.stopRequested()) {
                break;
            }
        }
        return new Recorded(observations, events, regimeEvents);
    }

    private static String[] syms(int n) {
        String[] out = new String[n];
        for (int s = 0; s < n; s++) {
            out[s] = "S" + s;
        }
        return out;
    }

    /** Calm independent returns, then (optionally) a block of large identical coordinated shocks. */
    private static Returns calmThenFused(int calmRows, int fusedRows, int symbols, long seed) {
        Random rng = new Random(seed);
        int rows = calmRows + fusedRows;
        double[][] returns = new double[rows][symbols];
        List<Instant> timestamps = new ArrayList<>();
        Instant t0 = Instant.parse("2021-05-01T00:00:00Z");
        for (int t = 0; t < rows; t++) {
            timestamps.add(t0.plusSeconds(60L * t));
            if (t < calmRows) {
                for (int s = 0; s < symbols; s++) {
                    returns[t][s] = 0.2 * rng.nextGaussian();
                }
            } else {
                double shared = 5.0 * rng.nextGaussian();
                for (int s = 0; s < symbols; s++) {
                    returns[t][s] = shared;
                }
            }
        }
        return new Returns(timestamps, returns);
    }

    private record Returns(List<Instant> timestamps, double[][] rows) {
    }
}
