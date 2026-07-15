package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.engine.RunSummary;

/**
 * The consumer end of the <em>observation seam</em>: receives every {@link PipelineObservation} the
 * configured {@link ObservationPolicy} lets through. This is the counterpart to {@link SignalSink} for
 * the <em>full transition series</em> rather than the censored product fire-stream — a visualization,
 * a metrics exporter, a dashboard feed, or (the CLI default) a {@link LoggingObserver}.
 *
 * <p>The engine never logs the per-transition heartbeat itself; it hands each observation here, so the
 * consumer composing the pipeline owns the verbosity. Implementations should be cheap and non-blocking
 * (an observation arrives per scored transition); a slow observer paces the ingest thread.</p>
 */
@FunctionalInterface
public interface PipelineObserver {

    /**
     * Receives one policy-passed observation.
     *
     * @param observation the per-transition observation
     */
    void onObservation(PipelineObservation observation);

    /**
     * Signals the start of the observation stream — called once before the first observation, carrying
     * the static run context (config + provenance). The default is a no-op; observers that echo a config
     * block override it. The single abstract method stays {@link #onObservation}, so this remains a
     * functional interface.
     *
     * @param context the static run context
     */
    default void onStart(RunContext context) {
        // default: observers that do not echo a config block ignore the run context
    }

    /**
     * Signals the end of the observation stream — called once after the final observation, carrying the
     * run outcome. The default is a no-op; stateful observers (e.g. an end-of-run digest) override it to
     * flush a summary. The single abstract method stays {@link #onObservation}, so this remains a
     * functional interface.
     *
     * @param summary the run outcome
     */
    default void onComplete(RunSummary summary) {
        // default: observers that do not summarize ignore the run outcome
    }

    /**
     * Receives one calibration-lifecycle event — an adaptive run's promotion, recalibration or
     * timeout ({@code CalibrationEvent}, a bounded kind + raw before/after facts). Interleaved with
     * {@link #onObservation} in stream order; never called by the frozen calibration modes. The
     * default is a no-op.
     *
     * @param event the lifecycle event
     */
    default void onCalibrationEvent(CalibrationEvent event) {
        // default: observers that do not track the calibration lifecycle ignore it
    }

    /**
     * Receives one regime-state edge from the continuous-tape backbone ({@link RegimeEvent}: a bounded
     * kind + the smoothed density and paired onset). Interleaved with {@link #onObservation} in stream
     * order and never gated by an {@link ObservationPolicy} (a regime edge is always reported); emitted
     * only when the engine runs the regime-backbone fire mode. The default is a no-op.
     *
     * @param event the regime edge
     */
    default void onRegimeEvent(RegimeEvent event) {
        // default: observers that do not track the regime backbone ignore it
    }

    /**
     * Receives one smoothed daily density level from the regime-backbone density prep
     * ({@code RegimeSeries.DailyAggregator}): the UTC-midnight day and the trailing-median-smoothed
     * density level fed to the Schmitt trigger. Emitted only when the engine runs the
     * regime-backbone fire mode ({@code --fire-mode regime}) <em>and</em> the run is configured to
     * observe density records ({@code --observe density}). The default is a no-op; stateful observers
     * that need to record or sweep the density series override it.
     *
     * <p>This carries the same daily level that drives the regime detector — the exact series
     * {@code run3_sweep.py} needs to sweep the Schmitt marks without rebuilding density externally.</p>
     *
     * @param asOf           the UTC-midnight instant of the day this level represents
     * @param smoothedLevel  the trailing-median-smoothed daily mean density fed to the detector
     */
    default void onDensityLevel(java.time.Instant asOf, double smoothedLevel) {
        // default: observers that do not record the density series ignore it
    }

    /** A sink that drops every observation — the default when a consumer wants only the fire-stream. */
    static PipelineObserver noOp() {
        return observation -> {
            // intentionally ignores the observation: fire-stream-only consumer
        };
    }
}
