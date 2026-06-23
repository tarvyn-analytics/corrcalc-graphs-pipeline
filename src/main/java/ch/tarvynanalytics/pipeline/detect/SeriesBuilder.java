package ch.tarvynanalytics.pipeline.detect;

import ch.tarvynanalytics.corrcalc.lib.matrix.DoubleMatrix;
import ch.tarvynanalytics.corrcalc.lib.stream.RollingCorrelations;
import ch.tarvynanalytics.graphs.algos.ChangeMetricsAnalyzer;
import ch.tarvynanalytics.graphs.algos.model.ChangeMetrics;
import ch.tarvynanalytics.pipeline.data.ReturnPanel;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Drives the S1 online correlation engine (corrcalc-lib) over a {@link ReturnPanel} and reduces each
 * emitted matrix snapshot to the two scalar series the alert layer needs (the {@code density} and
 * {@code weightedChange} of {@link DensityChangeSeries}), using the S3 metric engine
 * (graphs-algos-lib {@link ChangeMetricsAnalyzer}) so the density denominator matches S3 exactly.
 *
 * <p>This is the live S1→S3 wiring. It reproduces the spike's {@code rolling_fusedness_vec} +
 * change-metric path: the window slides over the contiguous return rows (the panel already dropped
 * the per-session boundary return), with no window reset between sessions, emitting one point per
 * window-end once the window has filled. Only the previous matrix is retained, so memory is
 * {@code O(N²)}, not {@code O(windows · N²)}.</p>
 */
public final class SeriesBuilder {

    private SeriesBuilder() {
    }

    /**
     * Builds the density + weighted-change series for one timescale.
     *
     * @param panel         the aligned return panel (one frequency, column order = variable set)
     * @param window        the rolling-window width in bars
     * @param edgeThreshold the edge magnitude threshold {@code τ}
     * @return the derived scalar series (empty when the panel is shorter than the window)
     */
    public static DensityChangeSeries build(ReturnPanel panel, int window, double edgeThreshold) {
        List<Instant> timestamps = new ArrayList<>();
        List<Double> density = new ArrayList<>();
        List<Double> change = new ArrayList<>();
        double[][][] previous = new double[1][][];   // single-element holder mutated by the listener

        var engine = RollingCorrelations.pearson(panel.symbols(), window,
                (seq, asOf, pearson, labels) -> {
                    double[][] current = toArray(pearson, labels.length);
                    double[][] prev = previous[0];
                    ChangeMetrics metrics = ChangeMetricsAnalyzer.analyze(
                            prev == null ? current : prev, current, edgeThreshold);
                    timestamps.add(asOf);
                    density.add(metrics.densityLevel());
                    change.add(prev == null ? Double.NaN : metrics.weightedChange());
                    previous[0] = current;
                });

        double[][] returns = panel.returns();
        List<Instant> rowTimestamps = panel.timestamps();
        for (int t = 0; t < returns.length; t++) {
            engine.onBar(rowTimestamps.get(t), returns[t]);
        }

        return new DensityChangeSeries(List.copyOf(timestamps), toDoubleArray(density), toDoubleArray(change));
    }

    private static double[][] toArray(DoubleMatrix matrix, int n) {
        double[][] out = new double[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                out[i][j] = matrix.get(i, j);
            }
        }
        return out;
    }

    private static double[] toDoubleArray(List<Double> values) {
        double[] out = new double[values.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = values.get(i);
        }
        return out;
    }
}
