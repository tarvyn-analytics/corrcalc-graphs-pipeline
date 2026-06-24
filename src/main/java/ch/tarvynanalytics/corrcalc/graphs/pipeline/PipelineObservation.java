package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import ch.tarvynanalytics.graphs.algos.model.ChangeMetrics;

import java.time.Instant;

/**
 * One per-transition observation of the pipeline's internal state — the unit of the
 * <em>observation seam</em> (distinct from the product fire-stream {@link SignalSink}). The engine
 * produces exactly one of these for <strong>every</strong> scored transition after calibration,
 * whether or not it fired; an {@link ObservationPolicy} chosen by the consumer decides which of them
 * actually reach the consumer's {@link PipelineObserver}. This is what makes "push every tick vs only
 * fires vs a magnitude-thresholded subset" a consumer configuration rather than an engine decision.
 *
 * <p>The product fire-stream is unaffected: a fired transition still produces a {@link StructuralSignal}
 * through the {@link SignalFilter}/{@link SignalSink} chain <em>and</em> an observation here with
 * {@link #fired()} {@code == true}. "No fire" is never censored on this seam — a non-firing transition
 * is a real observation, not a silent zero.</p>
 *
 * @param asOf              the timestamp of the matrix that produced this transition
 * @param market           the market label (e.g. {@code "crypto"})
 * @param timescale        which timescale stream produced it ({@code "daily"} / {@code "intraday"})
 * @param metrics          the S3 change metrics for this transition (weighted change, density, etc.)
 * @param cusumSPlus       the upper-arm (fusion) CUSUM accumulator after this transition ({@code >= 0})
 * @param cusumSMinus      the lower-arm (de-fusion) CUSUM accumulator after this transition ({@code >= 0})
 * @param fired            whether this transition opened an alert on the configured firing arm
 * @param firedKind        the fire direction when {@link #fired()}, otherwise {@code null}
 * @param decisionThreshold the CUSUM decision interval {@code h} (in calm-sigma units), {@code > 0}
 */
public record PipelineObservation(
        Instant asOf,
        String market,
        String timescale,
        ChangeMetrics metrics,
        double cusumSPlus,
        double cusumSMinus,
        boolean fired,
        SignalKind firedKind,
        double decisionThreshold) {

    /** Validates the metric block and the decision threshold (the gauge denominator). */
    public PipelineObservation {
        if (metrics == null) {
            throw new IllegalArgumentException("metrics must not be null");
        }
        if (!(decisionThreshold > 0.0)) {
            throw new IllegalArgumentException("decisionThreshold must be > 0 [" + decisionThreshold + "]");
        }
    }

    /**
     * The raw magnitude gauge: the primary weighted edge-change {@code mean |Δr|} for this transition —
     * "how big was the structural move". {@link Double#NaN} on a NaN-change gap (e.g. a degenerate
     * window), which is treated as below any finite threshold by the magnitude policies.
     *
     * @return the weighted change for this transition
     */
    public double magnitude() {
        return metrics.weightedChange();
    }

    /**
     * The activation gauge in {@code [0, ∞)}: {@code max(S+, S−) / h} — the fraction of the CUSUM
     * decision threshold this transition reached, i.e. "how close it came to firing" ({@code >= 1.0}
     * once a fire is structurally possible). A scale-free knob a consumer can threshold on without
     * knowing the per-market change scale.
     *
     * @return the CUSUM activation fraction
     */
    public double activation() {
        return Math.max(cusumSPlus, cusumSMinus) / decisionThreshold;
    }
}
