package ch.tarvynanalytics.corrcalc.graphs.pipeline.engine;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.CollectingSink;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.ReturnBuilder;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.SessionPolicy;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.TimescaleConfig;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.IterableMarketDataSource;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.MarketDataSource;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.MarketSnapshot;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineDriverTest {

    private static final TimescaleConfig CFG = new TimescaleConfig(12, DetectorConfig.crypto());

    @Test
    void run_DrivesSourceThroughBuilderIntoEngine_AndScoresTransitions() {
        CollectingSink sink = new CollectingSink();
        PipelineEngine engine = engine(sink, 20, null);

        RunSummary summary = PipelineDriver.run(source(120, 4, 11L), builder(4), engine, Pace.none());

        assertTrue(summary.detectionPoints() > 0, "the driver fed bars and the engine scored them");
        assertEquals(0L, summary.fires(), "calm random-walk prices do not fire");
        assertEquals(0, sink.count());
    }

    @Test
    void run_PacesExactlyOncePerDetectionBar() {
        int[] paceCount = {0};
        Pace counting = (prev, cur) -> paceCount[0]++;
        PipelineEngine engine = engine(new CollectingSink(), 20, null);

        RunSummary summary = PipelineDriver.run(source(120, 4, 11L), builder(4), engine, counting);

        // pacing is gated on isCalibrated(), so it fires once per post-calibration (detection) bar.
        assertEquals((int) summary.detectionPoints(), paceCount[0]);
        assertTrue(paceCount[0] > 0);
    }

    @Test
    void run_StopsWhenEngineRequestsStop() {
        PipelineEngine engine = engine(new CollectingSink(), 20, 3);

        RunSummary summary = PipelineDriver.run(source(120, 4, 11L), builder(4), engine, Pace.none());

        assertTrue(engine.stopRequested());
        assertEquals(3L, summary.detectionPoints());
    }

    @Test
    void run_RejectsNullArguments() {
        PipelineEngine engine = engine(new CollectingSink(), 20, null);
        assertThrows(IllegalArgumentException.class,
                () -> PipelineDriver.run(null, builder(4), engine, Pace.none()));
        assertThrows(IllegalArgumentException.class,
                () -> PipelineDriver.run(source(120, 4, 11L), null, engine, Pace.none()));
        assertThrows(IllegalArgumentException.class,
                () -> PipelineDriver.run(source(120, 4, 11L), builder(4), null, Pace.none()));
        assertThrows(IllegalArgumentException.class,
                () -> PipelineDriver.run(source(120, 4, 11L), builder(4), engine, null));
    }

    private static PipelineEngine engine(CollectingSink sink, int calmBars, Integer limit) {
        return PipelineEngine.builder(syms(4), CFG).calmBars(calmBars).market("crypto").timescale("daily")
                .sink(sink).limit(limit).build();
    }

    private static ReturnBuilder builder(int symbols) {
        return new ReturnBuilder(syms(symbols), SessionPolicy.DAILY_SINGLE);
    }

    /** Calm random-walk closes, daily-spaced (a single DAILY_SINGLE session, so only the first drops). */
    private static MarketDataSource source(int rows, int symbols, long seed) {
        Random rng = new Random(seed);
        Instant t0 = Instant.parse("2024-01-01T00:00:00Z");
        double[] price = new double[symbols];
        java.util.Arrays.fill(price, 100.0);
        List<MarketSnapshot> snapshots = new ArrayList<>();
        for (int t = 0; t < rows; t++) {
            double[] closes = new double[symbols];
            for (int s = 0; s < symbols; s++) {
                price[s] *= Math.exp(0.01 * rng.nextGaussian());
                closes[s] = price[s];
            }
            snapshots.add(new MarketSnapshot(t0.plus(t, ChronoUnit.DAYS), closes));
        }
        return new IterableMarketDataSource(syms(symbols), snapshots);
    }

    private static String[] syms(int n) {
        String[] out = new String[n];
        for (int s = 0; s < n; s++) {
            out[s] = "S" + s;
        }
        return out;
    }
}
