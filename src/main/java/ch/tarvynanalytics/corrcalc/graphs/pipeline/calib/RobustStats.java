package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

import java.util.Arrays;

/**
 * The robust location/scale selections the adaptive estimator uses (numerics spec Q2.2, Q5:
 * pipeline-side — the calm-selection policy is asset-specific, so this never moves into the lib).
 * All are order statistics (sort + pick): exact in {@code double}, no accumulation.
 */
final class RobustStats {

    /** MAD → stdev consistency factor under normality: {@code 1 / Φ⁻¹(0.75)} (spec 2.2). */
    static final double MAD_TO_SIGMA = 1.4826;

    private RobustStats() {
    }

    /** The sample median (mean of the two middle order statistics for an even {@code n}). */
    static double median(double[] values) {
        double[] sorted = values.clone();
        Arrays.sort(sorted);
        int n = sorted.length;
        return n % 2 == 1 ? sorted[n / 2] : (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0;
    }

    /** The median absolute deviation around {@code center}. */
    static double mad(double[] values, double center) {
        double[] dev = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            dev[i] = Math.abs(values[i] - center);
        }
        return median(dev);
    }

    /**
     * Nearest-rank percentile (no interpolation) — byte-for-byte the lib's
     * {@code ChangeDetectors.nearestRankPercentile} / the spike's {@code _percentile}: sort
     * ascending, {@code rank = clamp(ceil(pct/100 · n), 1, n)}, return {@code sorted[rank-1]}.
     */
    static double nearestRankPercentile(double[] values, double pct) {
        int n = values.length;
        if (n == 0) {
            return Double.NaN;
        }
        double[] sorted = values.clone();
        Arrays.sort(sorted);
        int rank = (int) Math.ceil(pct / 100.0 * n);
        if (rank < 1) {
            rank = 1;
        }
        if (rank > n) {
            rank = n;
        }
        return sorted[rank - 1];
    }
}
