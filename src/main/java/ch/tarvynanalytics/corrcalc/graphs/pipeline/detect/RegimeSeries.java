package ch.tarvynanalytics.corrcalc.graphs.pipeline.detect;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * Density prep for the H2R-2 regime-state backbone (design §5.1): reduces the per-window correlation
 * {@code density} series to the <strong>daily-aggregated, centered-median-smoothed</strong> level
 * series the GAL {@code RegimeStateDetector} reads. This is the market/cadence policy the pipeline
 * owns; the detector stays cadence-agnostic (it only counts samples).
 *
 * <p>The transform mirrors the H2R-1 spike ({@code h2r1_regime_model.py}) bar-for-bar:</p>
 * <ol>
 *   <li><strong>daily aggregation</strong> — the density values are grouped by UTC calendar day and
 *       each day becomes its mean (finite densities only; a NaN density is a data gap and does not
 *       enter the mean). Only days that carry data produce a sample (gaps collapse, exactly as the
 *       spike's {@code np.unique(day)} does); the sample is timestamped at the day's UTC midnight;</li>
 *   <li><strong>centered-median smooth</strong> — each daily level is replaced by the median of the
 *       {@code smoothWindow}-day window centered on it, clipped at both ends of the series (the spike's
 *       {@code median(dd[max(0,i-1):i+2])} for a 3-day window). An even clipped window (at the two
 *       ends) takes the mean of the two central order statistics, matching {@code numpy.median}.</li>
 * </ol>
 *
 * <p>Provided in two shapes over the identical numerics: a batch {@link #smoothedDailyLevels} for
 * offline/tested use, and a streaming {@link DailyAggregator} for the constant-memory replay engine
 * (it retains only the last {@code smoothWindow} finalized days and emits each smoothed sample with a
 * {@code smoothWindow/2}-day delay — the unavoidable latency of a <em>centered</em> smooth).</p>
 */
public final class RegimeSeries {

    private RegimeSeries() {
    }

    /**
     * One daily-aggregated, smoothed level sample: the UTC-midnight timestamp of the day and its
     * smoothed density level (the value fed to the regime detector).
     *
     * @param day   the day's UTC-midnight instant
     * @param level the centered-median-smoothed daily mean density
     */
    public record DailyLevel(Instant day, double level) {
    }

    /**
     * Batch density prep: aggregate {@code density} to UTC-daily means aligned with {@code timestamps},
     * then centered-median smooth over {@code smoothWindow} days.
     *
     * @param timestamps   the window-end timestamps of the density series (ascending)
     * @param density      the per-window density, parallel to {@code timestamps}
     * @param smoothWindow the centered-median window in days ({@code >= 1}, odd)
     * @return the smoothed daily level series (empty when the inputs are empty)
     * @throws IllegalArgumentException if the arrays differ in length or {@code smoothWindow < 1}
     */
    public static List<DailyLevel> smoothedDailyLevels(List<Instant> timestamps, double[] density,
                                                       int smoothWindow) {
        if (timestamps.size() != density.length) {
            throw new IllegalArgumentException("timestamps [" + timestamps.size() + "] and density ["
                    + density.length + "] must be the same length");
        }
        if (smoothWindow < 1) {
            throw new IllegalArgumentException("smoothWindow must be >= 1 [" + smoothWindow + "]");
        }
        List<Instant> days = new ArrayList<>();
        List<Double> daily = new ArrayList<>();
        LocalDate current = null;
        double sum = 0.0;
        int count = 0;
        for (int i = 0; i < density.length; i++) {
            LocalDate day = LocalDate.ofInstant(timestamps.get(i), ZoneOffset.UTC);
            if (current == null) {
                current = day;
            } else if (!day.equals(current)) {
                days.add(midnight(current));
                daily.add(count > 0 ? sum / count : Double.NaN);
                current = day;
                sum = 0.0;
                count = 0;
            }
            if (Double.isFinite(density[i])) {
                sum += density[i];
                count++;
            }
        }
        if (current != null) {
            days.add(midnight(current));
            daily.add(count > 0 ? sum / count : Double.NaN);
        }
        return smooth(days, daily, smoothWindow);
    }

    /** Centered-median smooth of a daily level series (clipped at both ends). */
    private static List<DailyLevel> smooth(List<Instant> days, List<Double> daily, int smoothWindow) {
        int half = smoothWindow / 2;
        int n = daily.size();
        List<DailyLevel> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            int from = Math.max(0, i - half);
            int to = Math.min(n - 1, i + half);
            out.add(new DailyLevel(days.get(i), median(daily, from, to)));
        }
        return out;
    }

    /** {@code numpy.median} over {@code values[from..to]} (inclusive): mid element, or mean of the two mids. */
    private static double median(List<Double> values, int from, int to) {
        int len = to - from + 1;
        double[] w = new double[len];
        for (int i = 0; i < len; i++) {
            w[i] = values.get(from + i);
        }
        return medianOf(w);
    }

    /** {@code numpy.median} of {@code w} (mutated by sorting): NaN propagates; even length averages the mids. */
    private static double medianOf(double[] w) {
        for (double v : w) {
            if (Double.isNaN(v)) {
                return Double.NaN;   // a NaN in the window propagates, matching numpy.median
            }
        }
        java.util.Arrays.sort(w);
        int mid = w.length / 2;
        return (w.length % 2 == 1) ? w[mid] : (w[mid - 1] + w[mid]) / 2.0;
    }

    private static Instant midnight(LocalDate day) {
        return day.atStartOfDay(ZoneOffset.UTC).toInstant();
    }

    /**
     * The streaming counterpart of {@link #smoothedDailyLevels}: fed the per-window density stream one
     * sample at a time, it aggregates to UTC-daily means and emits centered-median-smoothed daily levels
     * as they become computable (each with a {@code smoothWindow/2}-day delay). It retains only the last
     * {@code smoothWindow} finalized daily means — constant memory in the tape length — and reproduces
     * {@link #smoothedDailyLevels} exactly over the same input (a rollover finalizes the previous day;
     * {@link #flush()} finalizes the open day and drains the right-clipped tail).
     *
     * <p>Single-writer, like the engine that drives it.</p>
     */
    public static final class DailyAggregator {

        private final int smoothWindow;
        private final int half;
        private final List<Instant> dayBuf = new ArrayList<>();   // last <= smoothWindow finalized day instants
        private final List<Double> levelBuf = new ArrayList<>();  // last <= smoothWindow finalized daily means
        private LocalDate currentDay;
        private double sum;
        private int count;
        private int finalizedCount;   // total days finalized so far (absolute index of the next finalize)
        private int emittedCount;     // total smoothed samples emitted so far (absolute index of the next emit)
        private boolean flushed;

        /**
         * @param smoothWindow the centered-median window in days ({@code >= 1}, odd)
         */
        public DailyAggregator(int smoothWindow) {
            if (smoothWindow < 1) {
                throw new IllegalArgumentException("smoothWindow must be >= 1 [" + smoothWindow + "]");
            }
            this.smoothWindow = smoothWindow;
            this.half = smoothWindow / 2;
        }

        /**
         * Folds one per-window density sample into the current UTC day; when it crosses into a new UTC
         * day it finalizes the previous day and returns any smoothed daily levels that became computable.
         *
         * @param asOf    the window-end timestamp
         * @param density the window density (a non-finite value is a gap and does not enter the mean)
         * @return the smoothed daily levels newly emitted by this sample (usually empty; one per day roll)
         */
        public List<DailyLevel> onDensity(Instant asOf, double density) {
            if (flushed) {
                throw new IllegalStateException("aggregator already flushed");
            }
            LocalDate day = LocalDate.ofInstant(asOf, ZoneOffset.UTC);
            List<DailyLevel> emitted = List.of();
            if (currentDay == null) {
                currentDay = day;
            } else if (!day.equals(currentDay)) {
                emitted = finalizeDay(false);
                currentDay = day;
            }
            if (Double.isFinite(density)) {
                sum += density;
                count++;
            }
            return emitted;
        }

        /**
         * Finalizes the open day and drains every remaining smoothed daily level (the last
         * {@code smoothWindow/2} samples, whose centered window is right-clipped at the series end).
         *
         * @return the smoothed daily levels for the tail of the series
         */
        public List<DailyLevel> flush() {
            if (flushed) {
                throw new IllegalStateException("aggregator already flushed");
            }
            List<DailyLevel> out = new ArrayList<>();
            if (currentDay != null) {
                out.addAll(finalizeDay(true));
            }
            flushed = true;
            return out;
        }

        private List<DailyLevel> finalizeDay(boolean atEof) {
            pushDay(midnight(currentDay), count > 0 ? sum / count : Double.NaN);
            finalizedCount++;
            sum = 0.0;
            count = 0;
            return emitReady(atEof);
        }

        private void pushDay(Instant day, double level) {
            dayBuf.add(day);
            levelBuf.add(level);
            if (dayBuf.size() > smoothWindow) {
                dayBuf.remove(0);
                levelBuf.remove(0);
            }
        }

        /**
         * Emits every centered-median sample whose window is fully known: during the stream, indices up
         * to {@code (finalizedCount-1) - half} (the right edge is available); at EOF, the remaining
         * indices up to {@code finalizedCount-1} with the window right-clipped at the series end.
         */
        private List<DailyLevel> emitReady(boolean atEof) {
            int lastIdx = finalizedCount - 1;
            int target = atEof ? lastIdx : lastIdx - half;
            List<DailyLevel> out = new ArrayList<>();
            int frontIdx = finalizedCount - levelBuf.size();
            while (emittedCount <= target) {
                int i = emittedCount;
                int from = Math.max(0, i - half);
                int to = Math.min(lastIdx, i + half);
                out.add(new DailyLevel(dayBuf.get(i - frontIdx), medianAbs(from, to, frontIdx)));
                emittedCount++;
            }
            return out;
        }

        /** Median over the buffer for absolute index range {@code [from, to]}. */
        private double medianAbs(int from, int to, int frontIdx) {
            int len = to - from + 1;
            double[] w = new double[len];
            for (int k = 0; k < len; k++) {
                w[k] = levelBuf.get(from - frontIdx + k);
            }
            return medianOf(w);
        }
    }
}
