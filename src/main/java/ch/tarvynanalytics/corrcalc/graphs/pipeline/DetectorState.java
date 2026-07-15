package ch.tarvynanalytics.corrcalc.graphs.pipeline;

/**
 * The detector's lifecycle state at a single transition, derived deterministically from a
 * {@link PipelineObservation} (see {@link PipelineObservation#lifecycle()}). It is surfaced so a reader
 * or consumer can see "armed and watching" vs "just fired" vs "held in refractory" at a glance, without
 * re-deriving it from the raw CUSUM/gate facts.
 *
 * <p>{@code CALIBRATING} is intentionally absent: observations are only emitted <em>after</em>
 * calibration, so the pre-stream warm-up never appears as an observed state (the {@code config}
 * record's {@code calibration}/{@code calmBars} cover that phase instead).</p>
 */
public enum DetectorState {
    /** Calibrated and watching: the firing arm has not breached (or breached only with the level gate shut). */
    ARMED,
    /** This transition opened an alert. */
    FIRED,
    /** Breached with the level gate open but no new alert: already fired this regime (refractory). */
    DEBOUNCED
}
