package ch.tarvynanalytics.corrcalc.graphs.pipeline.detect;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.ReturnPanel;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SeriesBuilderTest {

    /**
     * Live S1→S3 wiring: drive the corrcalc-lib rolling engine over a tiny two-symbol panel and
     * reduce its snapshots to the density/change series via the graphs-algos-lib metric. The two
     * symbols move together (B = 2·A), so every window's correlation is +1 (one edge over one pair
     * ⇒ density 1.0) and consecutive correlations are identical (weighted change 0).
     */
    @Test
    void build_PerfectlyCorrelatedPair_GivesFullDensityAndZeroChange() {
        String[] symbols = {"A", "B"};
        double[][] returns = {
                {0.01, 0.02}, {-0.02, -0.04}, {0.03, 0.06}, {0.005, 0.010}
        };
        Instant t0 = Instant.parse("2024-01-01T00:00:00Z");
        List<Instant> timestamps = new ArrayList<>();
        for (int i = 0; i < returns.length; i++) {
            timestamps.add(t0.plus(i, ChronoUnit.MINUTES));
        }
        ReturnPanel panel = new ReturnPanel(timestamps, symbols, returns, new int[]{0, 0, 0, 0});

        DensityChangeSeries series = SeriesBuilder.build(panel, 3, 0.5);

        // 4 bars, window 3, cadence 1 -> 2 window-ends.
        assertEquals(2, series.size());
        assertEquals(t0.plus(2, ChronoUnit.MINUTES), series.timestamps().get(0));
        assertEquals(1.0, series.density()[0], 1e-12);
        assertEquals(1.0, series.density()[1], 1e-12);
        // first window has no predecessor -> NaN change; second is corr(+1) -> corr(+1) -> 0.
        assertTrue(Double.isNaN(series.weightedChange()[0]));
        assertEquals(0.0, series.weightedChange()[1], 1e-12);
    }

    @Test
    void build_PanelShorterThanWindow_YieldsEmptySeries() {
        String[] symbols = {"A", "B"};
        double[][] returns = {{0.01, 0.02}, {-0.02, -0.04}};
        List<Instant> timestamps = List.of(
                Instant.parse("2024-01-01T00:00:00Z"), Instant.parse("2024-01-01T00:01:00Z"));
        ReturnPanel panel = new ReturnPanel(timestamps, symbols, returns, new int[]{0, 0});

        DensityChangeSeries series = SeriesBuilder.build(panel, 5, 0.5);

        assertEquals(0, series.size());
    }
}
