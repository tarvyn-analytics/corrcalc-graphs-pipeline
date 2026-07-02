package ch.tarvynanalytics.corrcalc.graphs.pipeline.engine;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.CollectingSink;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.ObservationPolicy;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PairContribution;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObservation;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObserver;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SignalKind;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.TimescaleConfig;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineEngineTest {

    private static final TimescaleConfig CFG = new TimescaleConfig(12, DetectorConfig.crypto());
    private static final int WINDOW = 12;

    @Test
    void onReturns_CalmThenFused_FiresPublishesAndObservesEverything() {
        CollectingSink sink = new CollectingSink();
        List<PipelineObservation> observed = new ArrayList<>();
        PipelineEngine engine = builder(24).sink(sink).observer(observed::add)
                .observationPolicy(ObservationPolicy.all()).build();

        Returns r = calmThenFused(48, 24, 4, 42L);
        drive(engine, r);

        RunSummary s = engine.summary();
        assertTrue(s.fires() >= 1, "expected at least one fusion fire");
        assertEquals(s.fires(), s.published(), "acceptAll publishes every fire");
        assertEquals(s.published(), sink.count(), "every published fire reaches the sink");
        assertEquals(SignalKind.FUSION, sink.signals().get(0).kind());
        assertEquals("crypto", sink.signals().get(0).market());
        // the "all" policy forwards one observation per scored transition
        assertEquals(s.detectionPoints(), s.observationsEmitted());
        assertEquals(s.detectionPoints(), observed.size());
        assertTrue(observed.stream().anyMatch(PipelineObservation::fired), "a fire is present in the series");
    }

    @Test
    void onReturns_FiresOnlyPolicy_ForwardsOnlyFiresButStillPublishesAll() {
        CollectingSink sink = new CollectingSink();
        List<PipelineObservation> observed = new ArrayList<>();
        PipelineEngine engine = builder(24).sink(sink).observer(observed::add)
                .observationPolicy(ObservationPolicy.firesOnly()).build();

        drive(engine, calmThenFused(48, 24, 4, 42L));

        RunSummary s = engine.summary();
        assertTrue(s.detectionPoints() > s.fires(), "most transitions do not fire");
        assertEquals(s.fires(), s.observationsEmitted(), "firesOnly forwards exactly the fires");
        assertEquals((int) s.fires(), observed.size());
        assertTrue(observed.stream().allMatch(PipelineObservation::fired));
        assertEquals(s.fires(), sink.count(), "the product fire-stream is unaffected by the observation policy");
    }

    @Test
    void onReturns_PurelyCalm_NoFiresButObservesTransitions() {
        CollectingSink sink = new CollectingSink();
        PipelineEngine engine = builder(60).sink(sink).build();

        drive(engine, calmThenFused(160, 0, 4, 7L));

        RunSummary s = engine.summary();
        assertEquals(0L, s.fires());
        assertEquals(0, sink.count());
        assertTrue(s.detectionPoints() > 0);
    }

    @Test
    void onSessionBoundary_PostCalibration_ClearsWindowAndDetectorPredecessor() {
        PipelineEngine engine = builder(10).sink(new CollectingSink()).build();

        // 30 calm bars: 19 snapshots, first 10 calibrate, then 9 detection points.
        Returns calm = calmThenFused(30, 0, 4, 3L);
        drive(engine, calm);
        long before = engine.summary().detectionPoints();
        assertTrue(engine.isCalibrated());
        assertTrue(before > 0);

        engine.onSessionBoundary();

        // Re-warm: exactly WINDOW post-boundary bars produce one snapshot, which is the re-primed
        // predecessor (no transition) — so detection points must NOT advance yet.
        Returns post = calmThenFused(WINDOW + 1, 0, 4, 9L);
        Instant t = Instant.parse("2021-06-01T00:00:00Z");
        for (int i = 0; i < WINDOW; i++) {
            engine.onReturns(t.plusSeconds(60L * i), post.rows[i]);
        }
        assertEquals(before, engine.summary().detectionPoints(), "window cleared: no detection on the re-prime");

        engine.onReturns(t.plusSeconds(60L * WINDOW), post.rows[WINDOW]);
        assertEquals(before + 1, engine.summary().detectionPoints(), "next bar yields the first post-gap transition");
    }

    @Test
    void onReturns_PopulatesTopKContributors_LabelledFromTheRunSymbols() {
        List<PipelineObservation> observed = new ArrayList<>();
        PipelineEngine engine = builder(24).sink(new CollectingSink()).observer(observed::add)
                .observationPolicy(ObservationPolicy.all()).build();

        drive(engine, calmThenFused(48, 24, 4, 42L));

        PipelineObservation withContrib = observed.stream()
                .filter(o -> !o.contributors().isEmpty()).findFirst().orElseThrow();
        assertTrue(withContrib.contributors().size() <= 3, "default top-k caps at 3");
        Set<String> syms = Set.of("S0", "S1", "S2", "S3");
        for (PairContribution c : withContrib.contributors()) {
            assertTrue(syms.contains(c.a()) && syms.contains(c.b()), c.toString());
            assertNotEquals(c.a(), c.b());
        }
    }

    @Test
    void contributorsTopK_Zero_DisablesAttribution() {
        List<PipelineObservation> observed = new ArrayList<>();
        PipelineEngine engine = builder(24).sink(new CollectingSink()).observer(observed::add)
                .observationPolicy(ObservationPolicy.all()).contributorsTopK(0).build();

        drive(engine, calmThenFused(48, 24, 4, 42L));

        assertFalse(observed.isEmpty());
        assertTrue(observed.stream().allMatch(o -> o.contributors().isEmpty()));
        assertThrows(IllegalArgumentException.class, () -> builder(24).contributorsTopK(-1));
    }

    @Test
    void onReturns_Limit_StopsRequestingAfterNDetectionPoints() {
        PipelineEngine engine = builder(20).sink(new CollectingSink()).limit(3).build();

        drive(engine, calmThenFused(120, 0, 4, 5L));

        assertTrue(engine.stopRequested());
        assertEquals(3L, engine.summary().detectionPoints());
    }

    @Test
    void calibrationSource_ReadyImmediately_CalibratesOnTheFirstSnapshotAndScoresTheRest() {
        // A walk-forward-style source (ready before the stream, e.g. from a calm-block artifact)
        // must prime the detector on the FIRST window-fill snapshot, so every later snapshot is a
        // scored transition — the seam PR-3's calm-block mode plugs into.
        ch.tarvynanalytics.graphs.algos.Calibration external =
                new ch.tarvynanalytics.graphs.algos.Calibration(0.01, 0.02, 0.9, 0.3, 0.05);
        ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationSource ready =
                new ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationSource() {
                    @Override
                    public void observe(Instant asOf, double weightedChange, double density) {
                        // an externally-calibrated source ignores the stream
                    }

                    @Override
                    public boolean isReady() {
                        return true;
                    }

                    @Override
                    public ch.tarvynanalytics.graphs.algos.Calibration calibration() {
                        return external;
                    }
                };
        PipelineEngine engine = builder(24).sink(new CollectingSink()).calibrationSource(ready).build();

        Returns r = calmThenFused(WINDOW + 5, 0, 4, 11L);
        for (int t = 0; t < WINDOW; t++) {
            engine.onReturns(r.timestamps.get(t), r.rows[t]);
        }
        assertTrue(engine.isCalibrated(), "ready source calibrates on the first window-fill snapshot");
        assertEquals(external, engine.calibration().orElseThrow());
        assertEquals(0L, engine.summary().detectionPoints(), "the first snapshot primes, not scores");

        for (int t = WINDOW; t < r.rows.length; t++) {
            engine.onReturns(r.timestamps.get(t), r.rows[t]);
        }
        assertEquals(5L, engine.summary().detectionPoints(), "every post-prime snapshot is scored");
    }

    @Test
    void build_RejectsBadCalmBarsAndMissingSink() {
        assertThrows(IllegalArgumentException.class, () -> builder(1).sink(new CollectingSink()).build());
        assertThrows(IllegalArgumentException.class, () -> PipelineEngine.builder(syms(4), CFG).calmBars(10).build());
        assertThrows(IllegalArgumentException.class, () -> PipelineEngine.builder(new String[0], CFG));
    }

    private static PipelineEngine.Builder builder(int calmBars) {
        return PipelineEngine.builder(syms(4), CFG).calmBars(calmBars).market("crypto").timescale("intraday")
                .observer(PipelineObserver.noOp());
    }

    private static void drive(PipelineEngine engine, Returns r) {
        for (int t = 0; t < r.rows.length; t++) {
            engine.onReturns(r.timestamps.get(t), r.rows[t]);
            if (engine.stopRequested()) {
                break;
            }
        }
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
