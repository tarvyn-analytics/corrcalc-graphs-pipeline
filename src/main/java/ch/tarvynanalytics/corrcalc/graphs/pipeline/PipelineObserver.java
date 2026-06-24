package ch.tarvynanalytics.corrcalc.graphs.pipeline;

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

    /** A sink that drops every observation — the default when a consumer wants only the fire-stream. */
    static PipelineObserver noOp() {
        return observation -> {
            // intentionally ignores the observation: fire-stream-only consumer
        };
    }
}
