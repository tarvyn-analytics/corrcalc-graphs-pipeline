package ch.tarvynanalytics.corrcalc.graphs.pipeline.detect;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.SymbolVolatilityBaseline;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Numerics of the per-symbol volatility prep stage, proven against an <strong>independent naive
 * oracle</strong>: straightforward loops over explicit arrays recompute returns (with the gap
 * mask), fresh window sums for the RMS volatility, the robust z, and the trailing NaN-aware median
 * — never the stage's own rolling state. The seeded 4-column tape exercises every specified
 * discontinuity: a &gt;gapMask hole (5 minutes at snapshot 8), a UTC-midnight boundary (snapshot 5,
 * whose return must be KEPT — the channel has no session handling), a {@code sigma == 0} column
 * (B), a {@code sigma == NaN} column (D), and the stacked warm-up (vol window over 4 returns, then
 * 3 positional z-rows for the smoother — z-rows exist only from the first full vol window, so the
 * first emittable bar is return row {@code volWindow + smoothWindow − 1 = 6}).
 */
class SymbolVolatilitySeriesTest {

    private static final int VOL_WINDOW = 4;
    private static final int SMOOTH_WINDOW = 3;
    private static final Duration GAP_MASK = Duration.ofSeconds(90);
    private static final SymbolVolatilityConfig CONFIG =
            new SymbolVolatilityConfig(VOL_WINDOW, SMOOTH_WINDOW, GAP_MASK);
    private static final List<String> SYMBOLS = List.of("A", "B", "C", "D");
    private static final double[] MU = {0.008, 0.008, 0.012, 0.010};
    /** B has a zero scale, D a NaN scale — both must stay permanently unscored. */
    private static final double[] SIGMA = {0.004, 0.0, 0.006, Double.NaN};

    /**
     * A seeded random-walk close tape (Python {@code random.Random(42)}, 1.2% lognormal steps,
     * rounded to 4 decimals), 15 snapshots over 4 columns. One-minute spacing except a 5-minute
     * hole between snapshots 7 and 8; the tape crosses UTC midnight between snapshots 4 and 5.
     */
    private static final double[][] CLOSES = {
            {100.0, 250.0, 3.5, 42.0},
            {99.8272, 249.4818, 3.4953, 42.3553},
            {99.6745, 245.0391, 3.5093, 42.2196},
            {99.4154, 245.3801, 3.5191, 42.8133},
            {100.2018, 245.7057, 3.488, 42.2951},
            {100.4984, 249.602, 3.4898, 42.2412},
            {101.1418, 245.286, 3.4767, 42.4905},
            {102.2074, 244.5788, 3.4925, 42.6173},
            {103.1715, 241.3332, 3.5164, 41.8497},
            {99.9783, 239.5821, 3.478, 42.292},
            {100.7784, 236.1028, 3.5135, 41.7864},
            {100.6742, 235.2715, 3.5183, 42.1989},
            {101.4484, 236.2614, 3.5459, 42.4419},
            {100.688, 234.2363, 3.5259, 42.697},
            {100.3862, 240.8946, 3.4914, 42.1376},
    };

    @Test
    void onCloses_SeededTapeWithGapAndMidnight_MatchesNaiveOracle() {
        List<Instant> times = times();
        Map<Integer, double[]> expected = naiveSmoothed(times, CLOSES, GAP_MASK);
        SymbolVolatilitySeries stage = new SymbolVolatilitySeries(CONFIG, baseline(SIGMA));

        for (int i = 0; i < CLOSES.length; i++) {
            double[] emitted = stage.onCloses(times.get(i), CLOSES[i]);
            double[] want = expected.get(i);
            if (want == null) {
                assertNull(emitted, "no emission expected at snapshot [" + i + "]");
            } else {
                assertNotNull(emitted, "emission expected at snapshot [" + i + "]");
                for (int c = 0; c < SYMBOLS.size(); c++) {
                    if (Double.isNaN(want[c])) {
                        assertTrue(Double.isNaN(emitted[c]),
                                "snapshot [" + i + "] column [" + c + "] must be unscored");
                    } else {
                        assertEquals(want[c], emitted[c], 1e-12,
                                "snapshot [" + i + "] column [" + c + "]");
                    }
                }
            }
        }
    }

    /**
     * Pinned values from an independent reference recomputation of the oracle above (fresh window
     * sums, no shared code with the stage). Derivations: the vol window {r1..r4} first fills at
     * return row 4, giving the first z-row; the smoother needs 3 z-rows, so the first emittable
     * bar is return row 6, where the smoother window {z4, z5, z6} is fully scored and column A's
     * value is the median of the three raw z {@code (sqrt(Σr²/4) − 0.008) / 0.004}. The window
     * behind snapshot 6 includes the 23:59→00:00 return (midnight KEPT); the snapshot-8 value
     * includes the zeroed hole row.
     */
    @Test
    void onCloses_PinnedValues_MatchIndependentReference() {
        Map<Integer, double[]> emitted = sweep(new SymbolVolatilitySeries(CONFIG, baseline(SIGMA)));

        assertEquals(-0.8824227927894641, emitted.get(6)[0], 1e-12);   // first emission: median{z4,z5,z6}
        assertEquals(-1.1550023911095837, emitted.get(6)[2], 1e-12);   // midnight return in-window
        assertEquals(-0.422301108983017, emitted.get(8)[0], 1e-12);    // hole row zeroed
        assertEquals(-0.3234261295002221, emitted.get(14)[0], 1e-12);  // fully warm, odd median
        assertEquals(-0.8272569375673241, emitted.get(14)[2], 1e-12);
    }

    @Test
    void onCloses_WarmupSnapshots_EmitNothingUntilFirstScoredColumn() {
        Map<Integer, double[]> emitted = sweep(new SymbolVolatilitySeries(CONFIG, baseline(SIGMA)));

        // Return row r first exists at snapshot r; the vol window fills at r4 (the first z-row),
        // and the smoother needs 3 z-rows — so the first emission is exactly snapshot 6, then
        // every bar. Nothing may leak earlier: an under-smoothed median is not an emission.
        for (int i = 0; i <= 5; i++) {
            assertNull(emitted.get(i), "snapshot [" + i + "] is pre-warm-up");
        }
        for (int i = 6; i < CLOSES.length; i++) {
            assertNotNull(emitted.get(i), "snapshot [" + i + "] is post-warm-up");
        }
    }

    @Test
    void onCloses_GapWiderThanMask_ZeroesTheWholeReturnRow() {
        // Two stages over the identical tape, differing only in the gap mask: with the 90 s mask
        // the 5-minute hole row is zeroed; with a mask wider than the hole it is a plain log
        // return. The emissions must diverge exactly where the zeroed row is in scope.
        Map<Integer, double[]> masked = sweep(new SymbolVolatilitySeries(CONFIG, baseline(SIGMA)));
        Map<Integer, double[]> unmasked = sweep(new SymbolVolatilitySeries(
                new SymbolVolatilityConfig(VOL_WINDOW, SMOOTH_WINDOW, Duration.ofDays(1)),
                baseline(SIGMA)));

        assertEquals(masked.get(7)[0], unmasked.get(7)[0], 0.0, "identical before the hole");
        assertNotEquals(masked.get(8)[0], unmasked.get(8)[0], "the hole row must be zeroed");
        for (int c = 0; c < SYMBOLS.size(); c++) {   // panel-level: every column is masked alike
            if (SIGMA[c] > 0.0) {
                assertNotEquals(masked.get(10)[c], unmasked.get(10)[c],
                        "column [" + c + "] must carry the zeroed row while it is in the window");
            }
        }
    }

    @Test
    void onCloses_DegenerateScaleColumns_StayPermanentlyUnscored() {
        Map<Integer, double[]> emitted = sweep(new SymbolVolatilitySeries(CONFIG, baseline(SIGMA)));

        for (Map.Entry<Integer, double[]> bar : emitted.entrySet()) {
            assertTrue(Double.isNaN(bar.getValue()[1]),
                    "sigma == 0 column must stay unscored at snapshot [" + bar.getKey() + "]");
            assertTrue(Double.isNaN(bar.getValue()[3]),
                    "sigma == NaN column must stay unscored at snapshot [" + bar.getKey() + "]");
        }
    }

    @Test
    void onCloses_AllColumnsDegenerate_NeverEmits() {
        double[] degenerate = {0.0, -1.0, 0.0, Double.NaN};
        SymbolVolatilitySeries stage = new SymbolVolatilitySeries(CONFIG, baseline(degenerate));
        List<Instant> times = times();

        for (int i = 0; i < CLOSES.length; i++) {
            assertNull(stage.onCloses(times.get(i), CLOSES[i]),
                    "an all-NaN vector is never emitted (snapshot [" + i + "])");
        }
    }

    @Test
    void onCloses_SmoothWindowOne_EmitsRawZFromFirstFullVolWindow() {
        SymbolVolatilityConfig cfg = new SymbolVolatilityConfig(2, 1, GAP_MASK);
        SymbolVolatilityBaseline base = new SymbolVolatilityBaseline("crypto", "intraday", 0L,
                Instant.parse("2024-01-01T00:00:00Z"), Instant.parse("2024-02-01T00:00:00Z"),
                List.of("A"), new double[]{0.0}, new double[]{1.0});
        SymbolVolatilitySeries stage = new SymbolVolatilitySeries(cfg, base);
        Instant t0 = Instant.parse("2024-03-01T00:00:00Z");

        assertNull(stage.onCloses(t0, new double[]{100.0}));                            // seed
        assertNull(stage.onCloses(t0.plusSeconds(60), new double[]{101.0}));            // r1: vol not warm
        double[] out = stage.onCloses(t0.plusSeconds(120), new double[]{102.0});        // r2: warm

        assertNotNull(out);
        // A 1-bar median is the raw z: sqrt((r1² + r2²)/2) with mu=0, sigma=1.
        double r1 = Math.log(101.0 / 100.0);
        double r2 = Math.log(102.0 / 101.0);
        assertEquals(Math.sqrt((r1 * r1 + r2 * r2) / 2.0), out[0], 1e-12);
    }

    @Test
    void onCloses_EvenSmoothWindow_AveragesTheTwoMiddleZ() {
        SymbolVolatilityConfig cfg = new SymbolVolatilityConfig(2, 2, GAP_MASK);
        SymbolVolatilityBaseline base = new SymbolVolatilityBaseline("crypto", "intraday", 0L,
                Instant.parse("2024-01-01T00:00:00Z"), Instant.parse("2024-02-01T00:00:00Z"),
                List.of("A"), new double[]{0.0}, new double[]{1.0});
        SymbolVolatilitySeries stage = new SymbolVolatilitySeries(cfg, base);
        Instant t0 = Instant.parse("2024-03-01T00:00:00Z");

        assertNull(stage.onCloses(t0, new double[]{100.0}));                            // seed
        assertNull(stage.onCloses(t0.plusSeconds(60), new double[]{101.0}));            // r1: vol not warm
        assertNull(stage.onCloses(t0.plusSeconds(120), new double[]{102.0}));           // r2: first z-row only
        double[] out = stage.onCloses(t0.plusSeconds(180), new double[]{103.0});        // r3: 2 z-rows

        assertNotNull(out);
        // An even window averages the two mids: (z2 + z3) / 2 with mu=0, sigma=1.
        double r1 = Math.log(101.0 / 100.0);
        double r2 = Math.log(102.0 / 101.0);
        double r3 = Math.log(103.0 / 102.0);
        double z2 = Math.sqrt((r1 * r1 + r2 * r2) / 2.0);
        double z3 = Math.sqrt((r2 * r2 + r3 * r3) / 2.0);
        assertEquals((z2 + z3) / 2.0, out[0], 1e-12);
    }

    @Test
    void constructor_NullArguments_Rejected() {
        assertThrows(IllegalArgumentException.class, () -> new SymbolVolatilitySeries(null, baseline(SIGMA)));
        assertThrows(IllegalArgumentException.class, () -> new SymbolVolatilitySeries(CONFIG, null));
    }

    @Test
    void onCloses_InvalidArguments_RejectedWithBracketedValue() {
        SymbolVolatilitySeries stage = new SymbolVolatilitySeries(CONFIG, baseline(SIGMA));

        assertThrows(IllegalArgumentException.class, () -> stage.onCloses(null, CLOSES[0]));
        assertThrows(IllegalArgumentException.class, () -> stage.onCloses(Instant.EPOCH, null));
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> stage.onCloses(Instant.EPOCH, new double[]{1.0}));
        assertTrue(ex.getMessage().contains("[1]"), ex.getMessage());
    }

    // ---------------------------------------------------------------- fixture + naive oracle

    /** 15 snapshot instants: 1-minute spacing with a 5-minute hole after snapshot 7. */
    private static List<Instant> times() {
        List<Instant> out = new ArrayList<>();
        Instant t = Instant.parse("2024-03-01T23:55:00Z");
        for (int i = 0; i < CLOSES.length; i++) {
            out.add(t);
            t = t.plus(i == 7 ? 5 : 1, ChronoUnit.MINUTES);
        }
        return out;
    }

    private static SymbolVolatilityBaseline baseline(double[] sigma) {
        return new SymbolVolatilityBaseline("crypto", "intraday", 0L,
                Instant.parse("2024-01-01T00:00:00Z"), Instant.parse("2024-02-01T00:00:00Z"),
                SYMBOLS, MU, sigma);
    }

    /** Runs the stage over the whole tape, collecting emissions by snapshot index. */
    private static Map<Integer, double[]> sweep(SymbolVolatilitySeries stage) {
        List<Instant> times = times();
        Map<Integer, double[]> out = new HashMap<>();
        for (int i = 0; i < CLOSES.length; i++) {
            double[] emitted = stage.onCloses(times.get(i), CLOSES[i]);
            if (emitted != null) {
                out.put(i, emitted);
            }
        }
        return out;
    }

    /**
     * The naive oracle: recomputes the whole chain with straightforward loops — per-row log
     * returns with the gap mask, a <em>fresh</em> window sum per RMS volatility (no incremental
     * state), the robust z, and the trailing NaN-aware median — and returns only the emittable
     * rows (at least one scored column), keyed by snapshot index.
     */
    private static Map<Integer, double[]> naiveSmoothed(List<Instant> times, double[][] closes,
                                                        Duration gapMask) {
        int rows = closes.length - 1;
        int cols = closes[0].length;
        double[][] returns = new double[rows][cols];
        for (int r = 1; r <= rows; r++) {
            boolean gap = Duration.between(times.get(r - 1), times.get(r)).compareTo(gapMask) > 0;
            for (int c = 0; c < cols; c++) {
                returns[r - 1][c] = gap ? 0.0 : Math.log(closes[r][c] / closes[r - 1][c]);
            }
        }
        double[][] z = new double[rows][cols];
        for (double[] row : z) {
            java.util.Arrays.fill(row, Double.NaN);
        }
        for (int r = VOL_WINDOW; r <= rows; r++) {
            for (int c = 0; c < cols; c++) {
                double sum = 0.0;
                for (int i = r - VOL_WINDOW; i < r; i++) {
                    sum += returns[i][c] * returns[i][c];
                }
                double vol = Math.sqrt(sum / VOL_WINDOW);
                if (SIGMA[c] > 0.0) {
                    z[r - 1][c] = (vol - MU[c]) / SIGMA[c];
                }
            }
        }
        Map<Integer, double[]> out = new HashMap<>();
        // z-rows exist only from return row VOL_WINDOW, and the trailing smoother needs
        // SMOOTH_WINDOW of them — the first emittable row is VOL_WINDOW + SMOOTH_WINDOW − 1.
        for (int r = VOL_WINDOW + SMOOTH_WINDOW - 1; r <= rows; r++) {
            double[] smoothed = new double[cols];
            boolean any = false;
            for (int c = 0; c < cols; c++) {
                List<Double> window = new ArrayList<>();
                for (int i = r - SMOOTH_WINDOW; i < r; i++) {
                    if (!Double.isNaN(z[i][c])) {
                        window.add(z[i][c]);
                    }
                }
                smoothed[c] = naiveMedian(window);
                any |= !Double.isNaN(smoothed[c]);
            }
            if (any) {
                out.put(r, smoothed);   // return row r is produced by snapshot index r
            }
        }
        return out;
    }

    /** Plain median of an already-NaN-filtered list: sort, mid element or mean of the two mids. */
    private static double naiveMedian(List<Double> values) {
        if (values.isEmpty()) {
            return Double.NaN;
        }
        List<Double> sorted = new ArrayList<>(values);
        sorted.sort(Double::compareTo);
        int mid = sorted.size() / 2;
        return sorted.size() % 2 == 1
                ? sorted.get(mid)
                : (sorted.get(mid - 1) + sorted.get(mid)) / 2.0;
    }
}
