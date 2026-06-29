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

    /** A sink that drops every observation — the default when a consumer wants only the fire-stream. */
    static PipelineObserver noOp() {
        return observation -> {
            // intentionally ignores the observation: fire-stream-only consumer
        };
    }
}
