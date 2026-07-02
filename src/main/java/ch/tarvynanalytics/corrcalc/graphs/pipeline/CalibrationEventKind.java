package ch.tarvynanalytics.corrcalc.graphs.pipeline;

/**
 * The finite calibration-lifecycle vocabulary (H2 design §3.2) — mirrors {@link ReasonCode}: a
 * bounded enum whose human phrase is a fixed table, never generated text.
 */
public enum CalibrationEventKind {
    /** The cold start finished: {@code K} clean bars admitted, scoring begins. */
    PROMOTED_TO_LIVE,
    /** Alerts are suspended for a fresh warm-up (only after a regime-shift timeout). */
    DEMOTED_TO_CALIBRATING,
    /** The drift meta-monitor opened a new epoch and the detector was re-baselined. */
    RECALIBRATED,
    /** No clean bar was admitted for the timeout span — re-baselined on the raw trailing window. */
    REGIME_TIMEOUT,
    /** A new calibration epoch opened (the σ-guard form — variance re-scaled without a mean drift). */
    EPOCH_OPENED
}
