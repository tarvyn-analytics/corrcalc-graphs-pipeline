package ch.tarvynanalytics.corrcalc.graphs.pipeline;

/**
 * The finite vocabulary of why a snapshot's return was dropped before it ever reached the engine —
 * mirrors {@link ReasonCode}/{@link CalibrationEventKind}/{@link RegimeEventKind}: a bounded enum
 * whose human phrase is a fixed table ({@link #phrase()}), never generated text. Reported once per
 * dropped bar via {@link PipelineObserver#onDroppedBar}.
 */
public enum DroppedBarReason {

    /** No within-session predecessor to return against (a new {@code SessionPolicy} session key). */
    SESSION_BOUNDARY("first snapshot of a new session -- no return crosses the boundary"),

    /** The re-arm calendar backstop expired mid-question; the detector re-primed and scored nothing. */
    REARM_EXPIRED("re-arm backstop expired -- the detector re-primed and scored no transition");

    private final String phrase;

    DroppedBarReason(String phrase) {
        this.phrase = phrase;
    }

    /**
     * The single canned human phrase for this reason.
     *
     * @return a short, fixed plain-language phrase
     */
    public String phrase() {
        return phrase;
    }
}
