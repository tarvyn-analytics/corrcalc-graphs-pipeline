package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import java.time.Duration;
import java.time.Instant;

/**
 * One regime-state edge on the continuous-tape backbone, emitted by the pipeline engine
 * when the smoothed-density Schmitt trigger crosses a mark. It is the regime-layer
 * counterpart of {@link CalibrationEvent}: a bounded {@link RegimeEventKind} plus the raw facts a
 * consumer needs (the daily-smoothed density at the edge, and the onset the edge pairs with, so the
 * fused dwell / recovery lag is derivable). Interleaved with {@link PipelineObservation} in stream
 * order and delivered to {@link PipelineObserver#onRegimeEvent}; never gated by an
 * {@link ObservationPolicy} (a regime edge is always reported, like a calibration event).
 *
 * <p>The heavier "confidence/label" reporting is layered on this by the observability
 * stage, not baked into the detector; this record carries only detector-level facts.</p>
 *
 * @param asOf            the UTC-midnight timestamp of the day the edge was confirmed on — under
 *                        the causal (trailing) smooth, a {@link RegimeEventKind#FUSION_ONSET} or
 *                        {@link RegimeEventKind#CALM_ONSET} stamped {@code asOf=D} is knowable at
 *                        the <em>end</em> of day D (the trailing window {@code [D-(W-1)..D]} is
 *                        fully determined when D finalizes; no look-ahead required)
 * @param kind           which edge this is (fusion onset / calm onset / still-open-at-EOF)
 * @param smoothedDensity the daily-smoothed density level at the edge (the value the trigger read)
 * @param confidence      how decisive the crossing was, in {@code [0, 1]} — the margin past the mark
 *                        normalised by the room beyond it (0 at the mark, 1 at a saturated/empty graph)
 *                        — computed by the pipeline (which owns the marks), never the GAL trigger;
 *                        {@link Double#NaN} for {@link RegimeEventKind#OPEN_AT_EOF}
 *                        (no crossing occurred)
 * @param regimeOnset     the fusion-onset day of the regime this edge belongs to — equal to
 *                        {@code asOf} for a {@link RegimeEventKind#FUSION_ONSET}, and the paired onset
 *                        for a {@link RegimeEventKind#CALM_ONSET} / {@link RegimeEventKind#OPEN_AT_EOF}
 */
public record RegimeEvent(Instant asOf, RegimeEventKind kind, double smoothedDensity, double confidence,
                          Instant regimeOnset) {

    /** Validates the required components. */
    public RegimeEvent {
        if (asOf == null) {
            throw new IllegalArgumentException("asOf must not be null");
        }
        if (kind == null) {
            throw new IllegalArgumentException("kind must not be null");
        }
        if (regimeOnset == null) {
            throw new IllegalArgumentException("regimeOnset must not be null");
        }
    }

    /**
     * The fused dwell of this regime up to this edge — {@code asOf − regimeOnset}. Zero for a fusion
     * onset (the regime just opened); the full fused span for a calm-onset all-clear or an open-at-EOF
     * regime.
     *
     * @return the fused dwell duration ({@link Duration#ZERO} at onset, never negative)
     */
    public Duration fusedDwell() {
        return Duration.between(regimeOnset, asOf);
    }
}
