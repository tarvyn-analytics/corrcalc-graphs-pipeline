package ch.tarvynanalytics.pipeline.replay;

import ch.tarvynanalytics.graphs.algos.Calibration;

import java.time.Instant;
import java.util.List;

/**
 * The scored input for one timescale of one event: the calm-window {@link Calibration} (computed
 * walk-forward on the calm block) plus the event-window slice of the derived series (timestamps,
 * density, weighted change). This is exactly what the committed regression fixture carries — because
 * the CUSUM resets to zero at the event-window start, the calibration plus the event-window slice
 * fully determine the first alert, so the multi-month calm series never needs to be committed.
 *
 * @param calibration the calm-window calibration {@code (μ, σ, L)} for this timescale
 * @param timestamps  the event-window window-end timestamps
 * @param density     the event-window density series (parallel to {@code timestamps})
 * @param change      the event-window weighted-change series (parallel to {@code timestamps})
 */
public record TimescaleScoring(Calibration calibration, List<Instant> timestamps,
                               double[] density, double[] change) {
}
