package ch.tarvynanalytics.corrcalc.graphs.pipeline.engine;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.CollectingSink;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObservation;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObserver;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.RegimeEvent;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.RegimeEventKind;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SignalKind;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.RegimeTimescaleConfig;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.TimescaleConfig;
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
 * The H2R-2 regime-backbone fire mode through the real engine. Each "day" of the synthetic tape is one
 * UTC date of either fused bars (all symbols identical → correlation +1 → density 1.0) or calm bars
 * (a rotating one-hot → every pairwise |r| = 1/3 &lt; τ=0.5 → density 0.0), so the daily-aggregated
 * density is a clean 1.0/0.0 and the Schmitt trigger's onsets are hand-predictable. A UTC-day window
 * reset between days keeps each day's density within-day (the continuous straddle numerics are RUN-2's
 * job, not this wiring test).
 */
class PipelineEngineRegimeTest {

    private static final int WINDOW = 12;
    private static final int BARS_PER_DAY = 16;   // 12-fill + 5 clean snapshots, a multiple of 4 for the one-hot
    private static final TimescaleConfig CFG = new TimescaleConfig(WINDOW, DetectorConfig.crypto());
    // hi 0.85 / lo 0.45 / confirm 3, no smoothing (smoothWindow 1) so a day's level is its mean density.
    private static final RegimeTimescaleConfig REGIME =
            new RegimeTimescaleConfig(new RegimeConfig(0.85, 0.45, 3), 1);

    @Test
    void regimeMode_ThreeFusedDaysThenThreeCalm_FiresOneOnsetAndOneAllClear() {
        CollectingSink sink = new CollectingSink();
        List<RegimeEvent> regime = new ArrayList<>();
        PipelineEngine engine = engine(sink, collect(regime));

        // 3 calm (density 0, warm-up), 4 fused (onset on the 3rd fused day), 4 calm (all-clear on the 3rd).
        boolean[] fused = {false, false, false, true, true, true, true, false, false, false, false};
        driveDays(engine, fused, 7L);
        engine.finish();

        List<RegimeEventKind> kinds = regime.stream().map(RegimeEvent::kind).toList();
        assertEquals(List.of(RegimeEventKind.FUSION_ONSET, RegimeEventKind.CALM_ONSET), kinds,
                "one onset + one all-clear per fused regime: " + kinds);

        List<SignalKind> published = sink.signals().stream().map(s -> s.kind()).toList();
        assertEquals(List.of(SignalKind.FUSION, SignalKind.DEFUSION), published,
                "the regime edge is the product fire; the demoted CUSUM never reaches the sink: " + published);

        // onset confirms on the 3rd consecutive fused day = day index 5 (0-based); all-clear on the 3rd
        // consecutive calm day after = day index 9. Days are UTC-midnight timestamps.
        assertEquals(day(5), regime.get(0).asOf(), "fusion onset on the 3rd fused day");
        assertEquals(day(9), regime.get(1).asOf(), "all-clear on the 3rd calm day");
        assertEquals(day(5), regime.get(1).regimeOnset(), "the all-clear pairs back to its onset");

        RunSummary s = engine.summary();
        assertEquals(2L, s.fires(), "two regime edges fired");
        assertEquals(2L, s.published(), "both reached the product stream");
    }

    @Test
    void regimeMode_StillFusedAtEnd_ReportsOpenAtEofNotForceClosed() {
        CollectingSink sink = new CollectingSink();
        List<RegimeEvent> regime = new ArrayList<>();
        PipelineEngine engine = engine(sink, collect(regime));

        boolean[] fused = {false, false, false, true, true, true, true, true};   // ends fused
        driveDays(engine, fused, 11L);
        engine.finish();

        List<RegimeEventKind> kinds = regime.stream().map(RegimeEvent::kind).toList();
        assertEquals(List.of(RegimeEventKind.FUSION_ONSET, RegimeEventKind.OPEN_AT_EOF), kinds,
                "an unrecovered regime is reported open at EOF, not force-closed: " + kinds);
        // OPEN_AT_EOF is an observability marker, not a product fire.
        assertEquals(1, sink.count(), "only the fusion onset is published; open-at-EOF is not a fire");
        assertEquals(SignalKind.FUSION, sink.signals().get(0).kind());
        RegimeEvent open = regime.get(1);
        assertEquals(day(5), open.regimeOnset(), "the open regime keeps its onset");
        assertTrue(open.fusedDwell().toDays() >= 2, "the open regime carries its fused dwell");
    }

    @Test
    void regimeMode_FinishIsIdempotent_NoDoubleOpenAtEof() {
        CollectingSink sink = new CollectingSink();
        List<RegimeEvent> regime = new ArrayList<>();
        PipelineEngine engine = engine(sink, collect(regime));

        driveDays(engine, new boolean[]{false, false, false, true, true, true}, 3L);
        engine.finish();
        engine.finish();   // a second finish must not re-emit

        long openAtEof = regime.stream().filter(e -> e.kind() == RegimeEventKind.OPEN_AT_EOF).count();
        assertEquals(1, openAtEof, "finish is idempotent");
    }

    @Test
    void regimeMode_CusumFiresWithinTheDay_ButIsDemotedAndNeverReachesTheSink() {
        // A calm→fused jump inside ONE UTC day (no session reset): the CUSUM breaches and fires, but
        // the day is a single daily sample (< confirmBars) so no regime edge opens. The demotion must
        // hold — the CUSUM fire stays on the observation stream (annotation) and the product sink is
        // empty. This exercises the sig.fired() && regimeMode() branch the clean-day tests never take.
        CollectingSink sink = new CollectingSink();
        List<PipelineObservation> obs = new ArrayList<>();
        List<RegimeEvent> regime = new ArrayList<>();
        PipelineObserver observer = new PipelineObserver() {
            @Override
            public void onObservation(PipelineObservation o) {
                obs.add(o);
            }

            @Override
            public void onRegimeEvent(RegimeEvent event) {
                regime.add(event);
            }
        };
        PipelineEngine engine = PipelineEngine.builder(syms(4), CFG).calmBars(24)
                .market("crypto").timescale("intraday").sink(sink).observer(observer)
                .regime(REGIME).build();

        // 48 calm then 24 coordinated shocks, all on 2021-05-01 (72 one-minute bars, one UTC day).
        Instant t0 = day(0);
        Random rng = new Random(29L);
        for (int t = 0; t < 72; t++) {
            engine.onReturns(t0.plus(t, ChronoUnit.MINUTES), t < 48 ? calm(rng) : coordinated(rng));
        }
        engine.finish();

        assertTrue(obs.stream().anyMatch(PipelineObservation::fired),
                "the CUSUM still fires internally — the raw fact stays on the annotation stream");
        assertTrue(regime.isEmpty(), "one UTC day is a single daily sample: no regime edge opens");
        assertEquals(0, sink.count(), "the demoted CUSUM fire never reaches the product fire-stream");
        assertEquals(0L, engine.summary().published());
    }

    @Test
    void cusumMode_EmitsNoRegimeEvents_AndFinishIsANoOp() {
        CollectingSink sink = new CollectingSink();
        List<RegimeEvent> regime = new ArrayList<>();
        PipelineEngine engine = PipelineEngine.builder(syms(4), CFG).calmBars(6)
                .market("crypto").timescale("intraday").sink(sink).observer(collect(regime)).build();

        driveDays(engine, new boolean[]{false, false, true, true, true, false, false}, 5L);
        engine.finish();

        assertTrue(regime.isEmpty(), "the default (CUSUM) fire mode emits no regime events");
    }

    private static PipelineEngine engine(CollectingSink sink, PipelineObserver observer) {
        return PipelineEngine.builder(syms(4), CFG).calmBars(6)
                .market("crypto").timescale("intraday").sink(sink).observer(observer)
                .regime(REGIME).build();
    }

    private static PipelineObserver collect(List<RegimeEvent> into) {
        return new PipelineObserver() {
            @Override
            public void onObservation(ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObservation o) {
                // annotation stream; not asserted here
            }

            @Override
            public void onRegimeEvent(RegimeEvent event) {
                into.add(event);
            }
        };
    }

    /** Feeds one UTC day per {@code fusedPerDay} entry, resetting the S1 window at each day boundary. */
    private static void driveDays(PipelineEngine engine, boolean[] fusedPerDay, long seed) {
        Random rng = new Random(seed);
        for (int d = 0; d < fusedPerDay.length; d++) {
            if (d > 0) {
                engine.onSessionBoundary();   // the real intraday tape drops the cross-midnight return
            }
            Instant base = day(d);
            for (int i = 0; i < BARS_PER_DAY; i++) {
                engine.onReturns(base.plus(i, ChronoUnit.MINUTES), bar(fusedPerDay[d], i, rng));
            }
        }
    }

    /** A fused bar is 4 identical shocks (corr +1); a calm bar is a one-hot (near-zero pairwise corr). */
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

    /** Small independent returns — a calm structure (few edges). */
    private static double[] calm(Random rng) {
        double[] row = new double[4];
        for (int s = 0; s < 4; s++) {
            row[s] = 0.2 * rng.nextGaussian();
        }
        return row;
    }

    /** A large shared shock across all symbols — a coordinated jump that breaches the CUSUM. */
    private static double[] coordinated(Random rng) {
        double[] row = new double[4];
        double shared = 5.0 * rng.nextGaussian();
        for (int s = 0; s < 4; s++) {
            row[s] = shared;
        }
        return row;
    }

    private static Instant day(int d) {
        return Instant.parse("2021-05-01T00:00:00Z").plus(d, ChronoUnit.DAYS);
    }

    private static String[] syms(int n) {
        String[] out = new String[n];
        for (int s = 0; s < n; s++) {
            out[s] = "S" + s;
        }
        return out;
    }
}
