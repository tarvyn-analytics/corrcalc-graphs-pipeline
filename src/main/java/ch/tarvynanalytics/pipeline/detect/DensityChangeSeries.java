package ch.tarvynanalytics.pipeline.detect;

import java.time.Instant;
import java.util.List;

/**
 * The two scalar series the alert layer consumes, derived from one timescale's S1 matrix stream:
 * per window-end, the absolute edge {@code density} (the secondary level-gate feature) and the
 * primary {@code weightedChange} ({@code mean |Δr|} vs the previous matrix). Parallel arrays, all
 * the same length; {@code weightedChange[0]} is {@link Double#NaN} (the first window has no
 * predecessor transition).
 *
 * @param timestamps     the window-end timestamp of each point
 * @param density        the all-pairs edge density {@code n_edges/|P|} at each window
 * @param weightedChange the primary change metric {@code mean |Δr|} at each transition (NaN at index 0)
 */
public record DensityChangeSeries(List<Instant> timestamps, double[] density, double[] weightedChange) {

    /** The number of window-end points in the series. */
    public int size() {
        return timestamps.size();
    }
}
