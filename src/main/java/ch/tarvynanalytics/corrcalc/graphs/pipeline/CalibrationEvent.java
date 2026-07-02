package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import java.time.Instant;

/**
 * A bounded calibration-lifecycle event (H2 design §3.2): a finite {@link CalibrationEventKind}
 * plus the raw before/after facts — never free-form narration (the human phrase is a fixed lookup,
 * exactly like {@link ReasonCode}). "recalibrated, μ X→Y" is {@code RECALIBRATED} +
 * {@code (muBefore, muAfter)}. Emitted by the adaptive calibration source and forwarded once per
 * event through {@link PipelineObserver#onCalibrationEvent}.
 *
 * @param kind        what happened (the finite vocabulary)
 * @param epochId     the calibration epoch live <em>after</em> this event
 * @param muBefore    the calm mean before ({@link Double#NaN} when there was none, e.g. promotion)
 * @param muAfter     the calm mean after
 * @param sigmaBefore the calm sigma before ({@link Double#NaN} when there was none)
 * @param sigmaAfter  the calm sigma after
 * @param asOf        the stream timestamp the event occurred at
 */
public record CalibrationEvent(CalibrationEventKind kind, long epochId,
                               double muBefore, double muAfter,
                               double sigmaBefore, double sigmaAfter, Instant asOf) {

    /** Validates the event, throwing {@link IllegalArgumentException} on a missing kind/timestamp. */
    public CalibrationEvent {
        if (kind == null) {
            throw new IllegalArgumentException("kind must not be null");
        }
        if (asOf == null) {
            throw new IllegalArgumentException("asOf must not be null");
        }
        if (epochId < 0) {
            throw new IllegalArgumentException("epochId must be >= 0 [" + epochId + "]");
        }
    }
}
