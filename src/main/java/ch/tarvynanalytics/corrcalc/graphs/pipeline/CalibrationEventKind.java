package ch.tarvynanalytics.corrcalc.graphs.pipeline;

/**
 * The finite calibration-lifecycle vocabulary — mirrors {@link ReasonCode}: a
 * bounded enum whose human phrase is a fixed table ({@link #phrase()}), never generated text.
 */
public enum CalibrationEventKind {
    /** The cold start finished: {@code K} clean bars admitted, scoring begins. */
    PROMOTED_TO_LIVE("calibrated — scoring live"),
    /** Alerts are suspended for a fresh warm-up (only after a regime-shift timeout). */
    DEMOTED_TO_CALIBRATING("re-warming — alerts suspended"),
    /** The drift meta-monitor opened a new epoch and the detector was re-baselined. */
    RECALIBRATED("calm baseline drifted — recalibrated"),
    /** No clean bar was admitted for the timeout span — re-baselined on the raw trailing window. */
    REGIME_TIMEOUT("nothing calm for too long — re-baselined on the raw window"),
    /** A new calibration epoch opened (the σ-guard form — variance re-scaled without a mean drift). */
    EPOCH_OPENED("calm variance re-scaled — new epoch");

    private final String phrase;

    CalibrationEventKind(String phrase) {
        this.phrase = phrase;
    }

    /**
     * The single canned human phrase for this kind — the entire "narration" surface of the
     * calibration lifecycle (the raw before/after numbers live on the event, not in the text).
     *
     * @return a short, fixed plain-language phrase
     */
    public String phrase() {
        return phrase;
    }
}
