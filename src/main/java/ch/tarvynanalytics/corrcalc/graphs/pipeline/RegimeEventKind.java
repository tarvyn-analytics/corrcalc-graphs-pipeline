package ch.tarvynanalytics.corrcalc.graphs.pipeline;

/**
 * The finite vocabulary of a regime-state edge on the continuous-tape backbone (H2R-2 design §3, §6.1)
 * — mirrors {@link CalibrationEventKind}/{@link ReasonCode}: a bounded enum whose human phrase is a
 * fixed table ({@link #phrase()}), never generated text. A {@code RegimeStateDetector} down/up-crossing
 * is reported as one of these; the paired {@link #FUSION_ONSET}/{@link #CALM_ONSET} bracket one fused
 * regime, and {@link #OPEN_AT_EOF} marks a regime still fused when the tape ends (reported honestly,
 * not force-closed — design §5/§8.5).
 */
public enum RegimeEventKind {

    /** The smoothed density crossed up through {@code hi} with persistence — a fused regime opened. */
    FUSION_ONSET("regime opened — fused"),
    /** The smoothed density crossed down through {@code lo} with persistence — the all-clear. */
    CALM_ONSET("regime cleared — calm"),
    /** A fused regime was still open when the tape ended (not force-closed). */
    OPEN_AT_EOF("regime still open at end of tape");

    private final String phrase;

    RegimeEventKind(String phrase) {
        this.phrase = phrase;
    }

    /**
     * The single canned human phrase for this kind — the whole "narration" surface of a regime edge
     * (the raw density/dwell numbers live on the {@link RegimeEvent}, not in the text).
     *
     * @return a short, fixed plain-language phrase
     */
    public String phrase() {
        return phrase;
    }

    /**
     * The product {@link SignalKind} a regime edge publishes as (design §5.3: an onset <em>is</em> a
     * fusion, a down-crossing <em>is</em> the all-clear). {@link #OPEN_AT_EOF} publishes nothing — it is
     * an observability marker, not a fire — so it has no signal kind.
     *
     * @return {@link SignalKind#FUSION} for an onset, {@link SignalKind#DEFUSION} for a calm-onset
     * @throws IllegalStateException if called on {@link #OPEN_AT_EOF} (not a product fire)
     */
    public SignalKind toSignalKind() {
        return switch (this) {
            case FUSION_ONSET -> SignalKind.FUSION;
            case CALM_ONSET -> SignalKind.DEFUSION;
            case OPEN_AT_EOF -> throw new IllegalStateException("OPEN_AT_EOF is not a product fire");
        };
    }
}
