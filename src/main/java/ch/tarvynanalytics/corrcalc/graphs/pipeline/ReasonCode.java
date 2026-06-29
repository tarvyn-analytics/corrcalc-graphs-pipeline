package ch.tarvynanalytics.corrcalc.graphs.pipeline;

/**
 * The <strong>bounded</strong> vocabulary of reasons a transition is (or is not) firing — the
 * structured "why" the owner asked for, deliberately built as a <em>finite enum</em> rather than a
 * free-form narrator. Each transition's decision decomposes into a handful of these gate/magnitude
 * facts ({@link PipelineObservation#reasonCodes()}); the human sentence is a fixed one-phrase-per-code
 * lookup ({@link #phrase()}). Cost is O(codes), not O(market situations) — so new regimes never need
 * new prose. This is the H1 "narrator substitute" (see the meta-repo ROADMAP).
 *
 * <p>The codes are machine-readable facts; rendering them to text is the only place a phrase lives,
 * which keeps a future structured (NDJSON) stream and the human log in lock-step.</p>
 */
public enum ReasonCode {
    /** An alert opened on the upper (fusion / structure-tightened) arm. */
    FIRE_FUSION("FUSION fired — structure tightened (exit / risk-off)"),
    /** An alert opened on the lower (de-fusion / structure-loosened) arm. */
    FIRE_DEFUSION("DE-FUSION fired — structure loosened (re-entry)"),
    /** The structural move was at least one calm sigma above normal. */
    MAG_GE_1SIGMA("≥1σ move"),
    /** The structural move was at least two calm sigmas above normal. */
    MAG_GE_2SIGMA("≥2σ move"),
    /** The structural move was at least three calm sigmas above normal. */
    MAG_GE_3SIGMA("≥3σ move"),
    /** The density level gate is open (density ≥ the calibrated L). */
    LEVEL_GATE_OPEN("level-gate open (density ≥ L)"),
    /** The thresholded graph is (essentially) fully connected. */
    DENSITY_SATURATED("web fully connected"),
    /** Nearly the whole universe is in a single connected component. */
    COMPONENTS_COLLAPSED("collapsed into one block"),
    /** The firing CUSUM arm has crossed its decision threshold h. */
    CUSUM_BREACH("alarm meter past threshold"),
    /** The meter is past threshold but no alert opened: the density gate is shut. */
    BLOCKED_BY_LEVEL_GATE("held back — density below L"),
    /** Both gates are open but no alert opened: already fired this regime (refractory). */
    DEBOUNCED("already fired this regime (debounced)"),
    /** The meter is climbing but has not yet breached. */
    BUILDING("alarm meter building");

    private final String phrase;

    ReasonCode(String phrase) {
        this.phrase = phrase;
    }

    /**
     * The single canned human phrase for this code — the entire "narration" surface of the product.
     *
     * @return a short, fixed plain-language phrase
     */
    public String phrase() {
        return phrase;
    }
}
