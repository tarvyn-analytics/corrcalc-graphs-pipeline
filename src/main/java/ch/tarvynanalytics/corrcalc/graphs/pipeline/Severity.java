package ch.tarvynanalytics.corrcalc.graphs.pipeline;

/**
 * The human-facing severity tier of a single transition, derived deterministically from a
 * {@link PipelineObservation}'s CUSUM {@link PipelineObservation#activation() activation} (and whether
 * it fired). It exists so the readable output can flag "approaching" transitions without the consumer
 * re-deriving thresholds — the WATCH/WARN escalation the owner asked for. This is presentation policy,
 * not engine state: the fire-stream ({@link SignalSink}) is unaffected.
 *
 * <ul>
 *   <li>{@link #CALM} — activation below the watch floor; nothing to look at.</li>
 *   <li>{@link #WATCH} — activation crossed {@link PipelineObservation#WATCH_FRACTION}; the meter is
 *       climbing.</li>
 *   <li>{@link #WARN} — activation crossed {@link PipelineObservation#WARN_FRACTION}; close to firing.</li>
 *   <li>{@link #FIRE} — the transition actually opened an alert.</li>
 * </ul>
 */
public enum Severity {
    /** Activation below the watch floor — nothing to look at. */
    CALM,
    /** The alarm meter is climbing (activation past the watch fraction). */
    WATCH,
    /** Close to firing (activation past the warn fraction). */
    WARN,
    /** The transition opened an alert. */
    FIRE
}
