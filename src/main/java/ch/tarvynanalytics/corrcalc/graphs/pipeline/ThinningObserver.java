package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.engine.RunSummary;

/**
 * A {@link PipelineObserver} decorator that forwards only every {@code n}-th observation to a delegate —
 * a consumer-side knob for thinning a noisy full-series stream (the CLI's {@code --heartbeat-every}).
 * This is deliberately <em>not</em> an {@link ObservationPolicy}: "every n-th" is stateful (it counts),
 * whereas a policy is a stateless per-observation predicate. Stack it over any observer, including after
 * a policy gate. Single-writer, like the engine that drives it.
 */
public final class ThinningObserver implements PipelineObserver {

    private final PipelineObserver delegate;
    private final int everyN;
    private long seen;

    /**
     * @param delegate the observer to forward the kept observations to
     * @param everyN   forward one observation in every {@code everyN} ({@code >= 1}; {@code 1} forwards all)
     * @throws IllegalArgumentException if {@code delegate} is {@code null} or {@code everyN < 1}
     */
    public ThinningObserver(PipelineObserver delegate, int everyN) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate observer must not be null");
        }
        if (everyN < 1) {
            throw new IllegalArgumentException("everyN must be >= 1 [" + everyN + "]");
        }
        this.delegate = delegate;
        this.everyN = everyN;
    }

    @Override
    public void onObservation(PipelineObservation observation) {
        seen++;
        if (seen % everyN == 0) {
            delegate.onObservation(observation);
        }
    }

    @Override
    public void onStart(RunContext context) {
        delegate.onStart(context);
    }

    @Override
    public void onComplete(RunSummary summary) {
        delegate.onComplete(summary);
    }

    @Override
    public void onCalibrationEvent(CalibrationEvent event) {
        // never thinned: lifecycle events are rare and each one is load-bearing
        delegate.onCalibrationEvent(event);
    }
}
