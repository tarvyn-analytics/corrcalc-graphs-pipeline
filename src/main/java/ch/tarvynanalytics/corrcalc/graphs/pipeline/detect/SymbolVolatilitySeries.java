package ch.tarvynanalytics.corrcalc.graphs.pipeline.detect;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.SymbolVolatilityBaseline;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.ReturnPanels;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

/**
 * The per-symbol realized-volatility prep stage: turns a stream of aligned close cross-sections
 * into one smoothed robust volatility z-score per universe column — the numerics behind the
 * engine's per-symbol diagnostic channel. Like {@link RegimeSeries}, this is a CGP prep stage the
 * engine drives; the pipeline owns the cadence policy.
 *
 * <p>Per snapshot, in order:</p>
 * <ol>
 *   <li><strong>log return per column</strong> ({@link ReturnPanels#logReturn}); the first snapshot
 *       only seeds the predecessor state and produces no return row;</li>
 *   <li><strong>gap mask (panel-level)</strong> — a snapshot arriving more than
 *       {@link SymbolVolatilityConfig#gapMask()} after its predecessor zeroes the <em>entire</em>
 *       return row (every column). The UTC-midnight return is kept: this channel deliberately
 *       treats the tape as continuous and applies no session handling at all;</li>
 *   <li><strong>rolling RMS volatility per column</strong> — the sum of squares over the last
 *       {@code volWindow} returns, recomputed freshly at each scored bar in temporal order (oldest
 *       first), so the volatility is a pure function of the trailing window — a replay of the last
 *       {@code volWindow} bars reproduces it bit-for-bit; {@code vol = sqrt(sum / volWindow)}; no
 *       partial-window volatility — a column stays unscored until {@code volWindow} returns have
 *       accumulated;</li>
 *   <li><strong>robust z per column</strong> — {@code z = (vol − mu[k]) / sigma[k]} against the
 *       frozen {@link SymbolVolatilityBaseline}; a {@code NaN} or non-positive {@code sigma[k]}
 *       leaves the column permanently unscored ({@link Double#NaN});</li>
 *   <li><strong>trailing-median smooth</strong> over the last {@code smoothWindow} z-values,
 *       NaN-aware (the median of the non-NaN values inside the positional window, an all-NaN
 *       window staying {@link Double#NaN}). Positional z-rows are counted from the first full
 *       volatility window — nothing is emitted for a bar before {@code smoothWindow} z-rows exist,
 *       i.e. before {@code volWindow + smoothWindow − 1} returns have accumulated — so warm-up
 *       never leaks an under-smoothed median.</li>
 * </ol>
 *
 * <p><strong>Session boundaries never reset this stage</strong> — its windows and predecessor state
 * slide straight through day boundaries; the gap mask in step 2 is the only discontinuity
 * treatment. {@link Double#NaN} always means <em>unscored</em>, never zero.</p>
 *
 * <p><strong>Single-writer</strong>, like the engine that drives it.</p>
 */
public final class SymbolVolatilitySeries {

    private final int volWindow;
    private final int smoothWindow;
    private final Duration gapMask;
    private final double[] mu;
    private final double[] sigma;
    private final int columns;

    private final double[][] squares;    // per column: circular buffer of the last volWindow squared returns
    private final double[][] zWindow;    // per column: circular buffer of the last smoothWindow z-values
    private double[] prevCloses;         // per-run predecessor cross-section (null until seeded)
    private Instant prevAsOf;
    private long returnRows;             // return rows accumulated so far (row 1 = second snapshot)

    /**
     * @param config   the channel tuning (windows + gap mask)
     * @param baseline the frozen per-column baseline; its symbol order defines the column count
     * @throws IllegalArgumentException if either argument is null
     */
    public SymbolVolatilitySeries(SymbolVolatilityConfig config, SymbolVolatilityBaseline baseline) {
        if (config == null || baseline == null) {
            throw new IllegalArgumentException("config and baseline must not be null");
        }
        this.volWindow = config.volWindow();
        this.smoothWindow = config.smoothWindow();
        this.gapMask = config.gapMask();
        this.mu = baseline.mu();         // zero-copy: the record cloned defensively on construction
        this.sigma = baseline.sigma();
        this.columns = baseline.symbols().size();
        this.squares = new double[columns][volWindow];
        this.zWindow = new double[columns][smoothWindow];
    }

    /**
     * Ingests the next aligned close cross-section and, once the channel is warm, returns the
     * smoothed z vector for this bar (full column length, {@link Double#NaN} where unscored).
     * Returns {@code null} while nothing is emittable — before the first return row, before the
     * smoother is positionally warm, or while every column is still NaN.
     *
     * @param asOf   the cross-section's instant (UTC)
     * @param closes one close per universe column, in the baseline's column order
     * @return the smoothed z vector for this bar, or {@code null} when nothing is emitted
     * @throws IllegalArgumentException if {@code asOf} is null or {@code closes} is null or the wrong length
     */
    public double[] onCloses(Instant asOf, double[] closes) {
        if (asOf == null) {
            throw new IllegalArgumentException("asOf must not be null");
        }
        if (closes == null || closes.length != columns) {
            throw new IllegalArgumentException("closes must carry one value per symbol ["
                    + (closes == null ? null : closes.length) + "], expected [" + columns + "]");
        }
        if (prevCloses == null) {
            prevCloses = closes.clone();
            prevAsOf = asOf;
            return null;   // first snapshot: seed the predecessor, no return row yet
        }
        // Gap mask is panel-level: one decision zeroes the whole row, every column alike.
        boolean masked = Duration.between(prevAsOf, asOf).compareTo(gapMask) > 0;
        int slot = (int) (returnRows % volWindow);
        int zSlot = (int) (returnRows % smoothWindow);
        returnRows++;
        boolean volWarm = returnRows >= volWindow;
        // Positional z-rows exist only from the first full vol window, so the trailing smoother
        // window is complete once smoothWindow z-rows have accumulated on top of the vol warm-up.
        boolean smootherWarm = returnRows >= (long) volWindow + smoothWindow - 1;
        double[] out = smootherWarm ? new double[columns] : null;
        boolean anyScored = false;
        for (int c = 0; c < columns; c++) {
            double r = masked ? 0.0 : ReturnPanels.logReturn(prevCloses[c], closes[c]);
            squares[c][slot] = r * r;
            double z = Double.NaN;
            if (volWarm && sigma[c] > 0.0) {   // a NaN or non-positive sigma leaves the column unscored
                // Fresh window sum in temporal order (oldest slot first): the volatility is a pure
                // function of the trailing volWindow returns — no rounding path from older history —
                // so a replay of the trailing bars reproduces it bit-for-bit.
                double sum = 0.0;
                for (int k = 1; k <= volWindow; k++) {
                    sum += squares[c][(slot + k) % volWindow];
                }
                double vol = Math.sqrt(sum / volWindow);
                z = (vol - mu[c]) / sigma[c];
            }
            zWindow[c][zSlot] = z;
            if (smootherWarm) {
                double smoothed = nanMedian(zWindow[c]);
                out[c] = smoothed;
                anyScored |= !Double.isNaN(smoothed);
            }
        }
        prevCloses = closes.clone();
        prevAsOf = asOf;
        return anyScored ? out : null;   // no callback until at least one column is scored
    }

    /**
     * The NaN-aware median (like {@code numpy.nanmedian}): the median of the non-NaN values in
     * {@code window}, or {@link Double#NaN} when every value is NaN. Even counts average the two
     * mid elements.
     */
    private static double nanMedian(double[] window) {
        double[] scored = new double[window.length];
        int m = 0;
        for (double v : window) {
            if (!Double.isNaN(v)) {
                scored[m++] = v;
            }
        }
        if (m == 0) {
            return Double.NaN;   // an all-NaN window stays unscored
        }
        double[] w = Arrays.copyOf(scored, m);
        Arrays.sort(w);
        int mid = m / 2;
        return (m % 2 == 1) ? w[mid] : (w[mid - 1] + w[mid]) / 2.0;
    }
}
