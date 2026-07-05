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
 * Density prep oracle. The expected daily means and trailing-median-smoothed levels are hand-computed
 * from a literal density/timestamp fixture (never the code's own output), and both the batch
 * {@link RegimeSeries#smoothedDailyLevels} and the streaming {@link DailyAggregator} are asserted
 * against them — and against each other on a larger series.
 *
 * <p><strong>Oracle derivation (3-day trailing median over window [i-2..i], left-clipped):</strong>
 * daily means [0.15, 0.90, 0.60, 0.30, 0.60]:
 * <ul>
 *   <li>i=0: window [0..0]=[0.15] → median 0.15</li>
 *   <li>i=1: window [0..1]=[0.15,0.90] → median (0.15+0.90)/2 = 0.525</li>
 *   <li>i=2: window [0..2]=[0.15,0.90,0.60] → sorted [0.15,0.60,0.90] → median 0.60</li>
 *   <li>i=3: window [1..3]=[0.90,0.60,0.30] → sorted [0.30,0.60,0.90] → median 0.60</li>
 *   <li>i=4: window [2..4]=[0.60,0.30,0.60] → sorted [0.30,0.60,0.60] → median 0.60</li>
 * </ul>
 * Each smoothed level for day D is knowable at the end of D — zero look-ahead.</p>
 */
class RegimeSeriesTest {

    private static final double EPS = 1e-12;

    // Five UTC days, uneven samples/day, so aggregation + smoothing + the streaming all bite.
    //   2020-01-01: 0.10, 0.20        -> mean 0.15
    //   2020-01-02: 0.90              -> mean 0.90
    //   2020-01-03: 0.80, 0.60, 0.40  -> mean 0.60
    //   2020-01-04: 0.30              -> mean 0.30
    //   2020-01-05: 0.50, 0.70        -> mean 0.60
    // daily means: [0.15, 0.90, 0.60, 0.30, 0.60]
    // 3-day trailing median (oracle, see class-level javadoc):
    //   [0.15, 0.525, 0.60, 0.60, 0.60]
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
    // Trailing-median oracle (hand-computed — see class-level javadoc; never the code's own output).
    private static final double[] EXPECT_LEVELS = {0.15, 0.525, 0.60, 0.60, 0.60};

    @Test
    void smoothedDailyLevels_UnevenDaysThreeDayTrailingMedian_MatchesHandComputed() {
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
        // smoothWindow=1: trailing window is [i..i] = just the day's mean, so the smooth is identity.
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
    void streamingMatchesBatch_OnALongerFiveDayWindowSeries() {
        // A longer, irregular series read through a 5-day trailing window — proves the buffer
        // management and immediate-emit agree with the batch transform (the aggregator IS the
        // batch numerics, streamed).
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

    /**
     * An even smoothWindow (e.g. 2) is now allowed — the trailing median requires no symmetric
     * window; an even-length clip averages the two central order statistics.
     *
     * <p>Oracle (2-day trailing, daily means [0.15, 0.90, 0.60, 0.30, 0.60]):
     * i=0:[0.15]→0.15, i=1:[0.15,0.90]→0.525, i=2:[0.90,0.60]→0.75,
     * i=3:[0.60,0.30]→0.45, i=4:[0.30,0.60]→0.45.</p>
     */
    @Test
    void evenSmoothWindow_IsAllowed_AndProducesCorrectTrailingMedian() {
        // Oracle for 2-day trailing median on daily means [0.15, 0.90, 0.60, 0.30, 0.60]:
        // i=0: [0.15]        -> 0.15
        // i=1: [0.15, 0.90]  -> (0.15+0.90)/2 = 0.525
        // i=2: [0.90, 0.60]  -> (0.60+0.90)/2 = 0.75
        // i=3: [0.60, 0.30]  -> (0.30+0.60)/2 = 0.45
        // i=4: [0.30, 0.60]  -> (0.30+0.60)/2 = 0.45
        double[] expected = {0.15, 0.525, 0.75, 0.45, 0.45};

        List<DailyLevel> batch = RegimeSeries.smoothedDailyLevels(TS, D, 2);
        List<DailyLevel> streamed = drain(TS, D, 2);

        assertEquals(expected.length, batch.size());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], batch.get(i).level(), EPS);
            assertEquals(batch.get(i).level(), streamed.get(i).level(), EPS);
        }
    }

    /**
     * Coverage gap #4 from H2R-4: the {@link ch.tarvynanalytics.corrcalc.graphs.pipeline.RegimeEventKind#OPEN_AT_EOF}
     * path — a tape that ends mid-regime so the flush emits the open day and the engine reports the
     * regime as open rather than force-closing it. (RUN-2 never exercised this: its last regime
     * cleared 2024-05-18, ~4.5 months before tape end.)
     *
     * <p>Oracle: 1-day smooth (no smoothing), 3 days of density 1.0 (fused above hi=0.85 for 3
     * consecutive days, confirming a fusion onset) and then the tape ends. The aggregator's
     * {@link DailyAggregator#flush()} must emit the open day immediately (no tail delay under
     * trailing smooth), and the emitted levels must match the batch.</p>
     */
    @Test
    void openAtEof_FlushEmitsOpenDay_BatchAndStreamAgree() {
        // Three full fused days (daily mean 1.0 each), no calm tail.
        //   2020-03-10: two samples of 1.0 -> mean 1.0
        //   2020-03-11: two samples of 1.0 -> mean 1.0
        //   2020-03-12: one sample of 1.0 (the tape ends here, day still open on last bar)
        // smoothWindow=1: trailing median over [i..i] = the day's mean (identity smooth).
        List<Instant> ts = List.of(
                Instant.parse("2020-03-10T06:00:00Z"), Instant.parse("2020-03-10T18:00:00Z"),
                Instant.parse("2020-03-11T06:00:00Z"), Instant.parse("2020-03-11T18:00:00Z"),
                Instant.parse("2020-03-12T06:00:00Z"));
        double[] d = {1.0, 1.0, 1.0, 1.0, 1.0};

        List<DailyLevel> batch = RegimeSeries.smoothedDailyLevels(ts, d, 1);
        List<DailyLevel> streamed = drain(ts, d, 1);

        // Batch produces all three days: flush on the last bar via the day-rollover logic is not
        // needed (batch processes the full tape at once).
        assertEquals(3, batch.size(), "batch must emit all three fused days");
        assertEquals(Instant.parse("2020-03-12T00:00:00Z"), batch.get(2).day());
        assertEquals(1.0, batch.get(2).level(), EPS);

        // Streaming must also produce all three, with the third day from flush().
        assertEquals(3, streamed.size(), "streaming must emit the open day via flush()");
        for (int i = 0; i < 3; i++) {
            assertEquals(batch.get(i).day(), streamed.get(i).day());
            assertEquals(batch.get(i).level(), streamed.get(i).level(), EPS);
        }
    }

    /**
     * OPEN_AT_EOF variant with a 3-day trailing window: the open day's window may include previous
     * days; flush must produce exactly one emission and it must match the batch.
     */
    @Test
    void openAtEof_ThreeDayWindow_FlushEmitsOneEntryMatchingBatch() {
        // Two full days then one partial open day:
        //   2021-06-01: mean 0.30
        //   2021-06-02: mean 0.70
        //   2021-06-03: one sample 0.90, tape ends (still open — no day 04 to trigger rollover)
        // 3-day trailing oracle:
        //   i=0: [0.30] -> 0.30
        //   i=1: [0.30, 0.70] -> (0.30+0.70)/2 = 0.50
        //   i=2: [0.30, 0.70, 0.90] -> sorted [0.30,0.70,0.90] -> median 0.70
        List<Instant> ts = List.of(
                Instant.parse("2021-06-01T06:00:00Z"), Instant.parse("2021-06-01T18:00:00Z"),
                Instant.parse("2021-06-02T06:00:00Z"), Instant.parse("2021-06-02T18:00:00Z"),
                Instant.parse("2021-06-03T06:00:00Z"));
        double[] d = {0.20, 0.40, 0.60, 0.80, 0.90};  // means: 0.30, 0.70, 0.90

        double[] expectedLevels = {0.30, 0.50, 0.70};
        Instant[] expectedDays = {
                Instant.parse("2021-06-01T00:00:00Z"),
                Instant.parse("2021-06-02T00:00:00Z"),
                Instant.parse("2021-06-03T00:00:00Z")};

        List<DailyLevel> batch = RegimeSeries.smoothedDailyLevels(ts, d, 3);
        List<DailyLevel> streamed = drain(ts, d, 3);

        assertEquals(3, batch.size());
        assertEquals(3, streamed.size(), "streaming must emit the open day via flush()");
        for (int i = 0; i < 3; i++) {
            assertEquals(expectedDays[i], batch.get(i).day());
            assertEquals(expectedLevels[i], batch.get(i).level(), EPS);
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
