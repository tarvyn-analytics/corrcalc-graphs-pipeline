package ch.tarvynanalytics.corrcalc.graphs.pipeline.detect;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.RegimeSeries.DailyAggregator;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.RegimeSeries.DailyLevel;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Density prep oracle. The expected daily means and centered-median-smoothed levels are hand-computed
 * from a literal density/timestamp fixture (never the code's own output), and both the batch
 * {@link RegimeSeries#smoothedDailyLevels} and the streaming {@link DailyAggregator} are asserted
 * against them — and against each other on a larger series.
 */
class RegimeSeriesTest {

    private static final double EPS = 1e-12;

    // Five UTC days, uneven samples/day, so aggregation + smoothing + the streaming delay all bite.
    //   2020-01-01: 0.10, 0.20        -> mean 0.15
    //   2020-01-02: 0.90              -> mean 0.90
    //   2020-01-03: 0.80, 0.60, 0.40  -> mean 0.60
    //   2020-01-04: 0.30              -> mean 0.30
    //   2020-01-05: 0.50, 0.70        -> mean 0.60
    // daily means: [0.15, 0.90, 0.60, 0.30, 0.60]
    // 3-day centered median (clipped): [median(.15,.90), median(.15,.90,.60), median(.90,.60,.30),
    //                                   median(.60,.30,.60), median(.30,.60)]
    //                                = [0.525, 0.60, 0.60, 0.60, 0.45]
    private static final List<Instant> TS = List.of(
            Instant.parse("2020-01-01T00:00:00Z"), Instant.parse("2020-01-01T12:00:00Z"),
            Instant.parse("2020-01-02T06:00:00Z"),
            Instant.parse("2020-01-03T01:00:00Z"), Instant.parse("2020-01-03T02:00:00Z"),
            Instant.parse("2020-01-03T03:00:00Z"),
            Instant.parse("2020-01-04T09:00:00Z"),
            Instant.parse("2020-01-05T10:00:00Z"), Instant.parse("2020-01-05T20:00:00Z"));
    private static final double[] D = {0.10, 0.20, 0.90, 0.80, 0.60, 0.40, 0.30, 0.50, 0.70};

    private static final Instant[] EXPECT_DAYS = {
            Instant.parse("2020-01-01T00:00:00Z"), Instant.parse("2020-01-02T00:00:00Z"),
            Instant.parse("2020-01-03T00:00:00Z"), Instant.parse("2020-01-04T00:00:00Z"),
            Instant.parse("2020-01-05T00:00:00Z")};
    private static final double[] EXPECT_LEVELS = {0.525, 0.60, 0.60, 0.60, 0.45};

    @Test
    void smoothedDailyLevels_UnevenDaysThreeDayMedian_MatchesHandComputed() {
        List<DailyLevel> got = RegimeSeries.smoothedDailyLevels(TS, D, 3);

        assertEquals(EXPECT_LEVELS.length, got.size());
        for (int i = 0; i < got.size(); i++) {
            assertEquals(EXPECT_DAYS[i], got.get(i).day());
            assertEquals(EXPECT_LEVELS[i], got.get(i).level(), EPS);
        }
    }

    @Test
    void dailyAggregator_SameInput_ReproducesBatchExactly() {
        List<DailyLevel> streamed = drain(TS, D, 3);

        assertEquals(EXPECT_LEVELS.length, streamed.size());
        for (int i = 0; i < streamed.size(); i++) {
            assertEquals(EXPECT_DAYS[i], streamed.get(i).day());
            assertEquals(EXPECT_LEVELS[i], streamed.get(i).level(), EPS);
        }
    }

    @Test
    void smoothWindowOne_IsNoSmoothing_DailyMeansPassThrough() {
        // half=0 exercises the immediate-emit path (no centered delay).
        List<DailyLevel> batch = RegimeSeries.smoothedDailyLevels(TS, D, 1);
        List<DailyLevel> streamed = drain(TS, D, 1);

        double[] expectDailyMeans = {0.15, 0.90, 0.60, 0.30, 0.60};
        assertEquals(expectDailyMeans.length, batch.size());
        for (int i = 0; i < batch.size(); i++) {
            assertEquals(expectDailyMeans[i], batch.get(i).level(), EPS);
            assertEquals(batch.get(i).day(), streamed.get(i).day());
            assertEquals(batch.get(i).level(), streamed.get(i).level(), EPS);
        }
    }

    @Test
    void singleDay_SmoothIsThatDaysMean_AndFlushEmitsIt() {
        List<Instant> ts = List.of(
                Instant.parse("2021-05-18T00:00:00Z"), Instant.parse("2021-05-18T12:00:00Z"));
        double[] d = {0.4, 0.8};   // mean 0.6

        List<DailyLevel> batch = RegimeSeries.smoothedDailyLevels(ts, d, 3);
        List<DailyLevel> streamed = drain(ts, d, 3);

        assertEquals(1, batch.size());
        assertEquals(0.6, batch.get(0).level(), EPS);
        assertEquals(Instant.parse("2021-05-18T00:00:00Z"), batch.get(0).day());
        assertEquals(1, streamed.size());
        assertEquals(0.6, streamed.get(0).level(), EPS);
    }

    @Test
    void nanDensity_IsAGapAndDoesNotEnterTheMean() {
        List<Instant> ts = List.of(
                Instant.parse("2020-02-01T00:00:00Z"), Instant.parse("2020-02-01T06:00:00Z"),
                Instant.parse("2020-02-01T12:00:00Z"));
        double[] d = {0.2, Double.NaN, 0.8};   // mean over the two finite = 0.5

        List<DailyLevel> batch = RegimeSeries.smoothedDailyLevels(ts, d, 3);

        assertEquals(1, batch.size());
        assertEquals(0.5, batch.get(0).level(), EPS);
    }

    @Test
    void aggregatorRejectsUseAfterFlush() {
        DailyAggregator agg = new DailyAggregator(3);
        agg.onDensity(Instant.parse("2020-01-01T00:00:00Z"), 0.5);
        agg.flush();

        assertThrows(IllegalStateException.class,
                () -> agg.onDensity(Instant.parse("2020-01-02T00:00:00Z"), 0.5));
        assertThrows(IllegalStateException.class, agg::flush);
    }

    @Test
    void lengthMismatch_Throws() {
        assertThrows(IllegalArgumentException.class,
                () -> RegimeSeries.smoothedDailyLevels(TS, new double[]{0.1}, 3));
    }

    @Test
    void streamingMatchesBatch_OnALongerFiveDayMedianSeries() {
        // A longer, irregular series read through a 5-day window — proves the half=2 delay + tail flush
        // agree with the batch transform (the aggregator is the batch numerics, streamed).
        List<Instant> ts = new ArrayList<>();
        List<Double> d = new ArrayList<>();
        double[] perDay = {0.9, 0.1, 0.5, 0.95, 0.2, 0.6, 0.3, 0.85, 0.15, 0.7};
        for (int day = 0; day < perDay.length; day++) {
            // two intraday samples per day, straddling the day mean
            Instant base = Instant.parse("2022-03-01T00:00:00Z").plusSeconds(day * 86400L);
            ts.add(base.plusSeconds(3600));
            d.add(perDay[day] - 0.05);
            ts.add(base.plusSeconds(7200));
            d.add(perDay[day] + 0.05);
        }
        double[] dArr = d.stream().mapToDouble(Double::doubleValue).toArray();

        List<DailyLevel> batch = RegimeSeries.smoothedDailyLevels(ts, dArr, 5);
        List<DailyLevel> streamed = drain(ts, dArr, 5);

        assertEquals(batch.size(), streamed.size());
        assertTrue(batch.size() == perDay.length);
        for (int i = 0; i < batch.size(); i++) {
            assertEquals(batch.get(i).day(), streamed.get(i).day());
            assertEquals(batch.get(i).level(), streamed.get(i).level(), EPS);
        }
    }

    private static List<DailyLevel> drain(List<Instant> ts, double[] d, int smoothWindow) {
        DailyAggregator agg = new DailyAggregator(smoothWindow);
        List<DailyLevel> out = new ArrayList<>();
        for (int i = 0; i < d.length; i++) {
            out.addAll(agg.onDensity(ts.get(i), d[i]));
        }
        out.addAll(agg.flush());
        return out;
    }
}
