package ch.tarvynanalytics.corrcalc.graphs.pipeline.replay;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.CollectingSink;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SignalKind;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.ReturnPanel;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.TimescaleConfig;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PacedReplayTest {

    private static final TimescaleConfig CFG = new TimescaleConfig(12, DetectorConfig.crypto());

    @Test
    void stream_CalmThenPerfectlyFusedPanel_FiresFusion() {
        // First block: small independent returns per symbol (low correlation → calm). Second block:
        // large *identical* coordinated shocks across symbols — one big shared bar dominates the
        // window covariance, so correlation jumps to ~1 within a bar (a sharp structural fusion). The
        // fusion arm must fire.
        ReturnPanel panel = panel(48, 24, 4, 42L);
        CollectingSink sink = new CollectingSink();

        PacedReplay.Summary summary = PacedReplay.stream(panel, CFG, options(null), sink, ReplayClock.noSleep());

        assertTrue(summary.fires() >= 1, "expected at least one fusion fire");
        assertEquals(summary.fires(), summary.published(), "acceptAll filter publishes every fire");
        assertEquals(summary.published(), sink.count(), "every published fire reaches the sink");
        assertEquals(SignalKind.FUSION, sink.signals().get(0).kind());
        assertEquals("crypto", sink.signals().get(0).market());
    }

    @Test
    void stream_PurelyCalmPanel_PublishesNothing() {
        ReturnPanel panel = panel(160, 0, 4, 7L);
        CollectingSink sink = new CollectingSink();

        PacedReplay.Summary summary = PacedReplay.stream(panel, CFG, options(null), sink, ReplayClock.noSleep());

        assertEquals(0L, summary.fires());
        assertEquals(0, sink.count());
        assertTrue(summary.detectionPoints() > 0);
    }

    @Test
    void stream_SeriesTooShort_Throws() {
        ReturnPanel panel = panel(13, 0, 3, 1L);   // 13 bars, window 12 → only 2 window-points
        CollectingSink sink = new CollectingSink();

        assertThrows(IllegalArgumentException.class,
                () -> PacedReplay.stream(panel, CFG, options(null), sink, ReplayClock.noSleep()));
    }

    @Test
    void stream_CalmBarsLeavesNoDetectionPoints_Throws() {
        ReturnPanel panel = panel(30, 0, 3, 2L);    // 30 bars, window 12 → 19 window-points
        CollectingSink sink = new CollectingSink();

        // ask for all 19 calm bars when only 19 points exist → no detection transition left.
        assertThrows(IllegalArgumentException.class,
                () -> PacedReplay.stream(panel, CFG, options(19), sink, ReplayClock.noSleep()));
    }

    private static ReplayOptions options(Integer calmBars) {
        return new ReplayOptions("synthetic", "crypto", "intraday",
                1000.0, 0L, calmBars, null, 1000, null, null, null);
    }

    private static ReturnPanel panel(int calmRows, int fusedRows, int symbols, long seed) {
        Random rng = new Random(seed);
        int rows = calmRows + fusedRows;
        double[][] returns = new double[rows][symbols];
        List<Instant> timestamps = new ArrayList<>();
        int[] sessionId = new int[rows];
        Instant t0 = Instant.parse("2021-05-01T00:00:00Z");
        for (int t = 0; t < rows; t++) {
            timestamps.add(t0.plusSeconds(60L * t));
            if (t < calmRows) {
                for (int s = 0; s < symbols; s++) {
                    returns[t][s] = 0.2 * rng.nextGaussian();   // small, independent → low correlation
                }
            } else {
                double shared = 5.0 * rng.nextGaussian();       // large, identical → correlation ≈ 1
                for (int s = 0; s < symbols; s++) {
                    returns[t][s] = shared;
                }
            }
        }
        String[] syms = new String[symbols];
        for (int s = 0; s < symbols; s++) {
            syms[s] = "S" + s;
        }
        return new ReturnPanel(List.copyOf(timestamps), syms, returns, sessionId);
    }
}
