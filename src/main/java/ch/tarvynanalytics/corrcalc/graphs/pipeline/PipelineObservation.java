package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import ch.tarvynanalytics.graphs.algos.Calibration;
import ch.tarvynanalytics.graphs.algos.model.ChangeMetrics;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

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
 * @param cusumSMinus      the lower-arm CUSUM accumulator on the change metric after this transition
 *                         ({@code >= 0}; informational — not the re-entry signal)
 * @param recoveryGauge    the de-fusion recovery gauge — the trailing fraction of the gauge window the
 *                         density has spent back in the calm band — at this transition; in {@code [0, 1]},
 *                         running {@code 0 → 1} as structure heals after a fusion, or {@link Double#NaN}
 *                         when de-fusion is uncalibrated. The all-clear track the dashboard shows.
 * @param fired            whether this transition opened an alert on the configured firing arm
 * @param firedKind        the fire direction when {@link #fired()}, otherwise {@code null}
 * @param decisionThreshold the CUSUM decision interval {@code h} (in calm-sigma units), {@code > 0}
 * @param calmMu            the calm-window mean of the weighted-change series (the "normal" move size)
 * @param calmSigma         the calm-window standard deviation of the weighted-change series (already
 *                          sigma-floored upstream); {@code <= 0} or NaN makes {@link #zScore()} NaN
 * @param calmMuDensity     the calm-window mean of the density series ({@link Calibration#muDensity()},
 *                          wired straight through); {@link Double#NaN} when the calibration in force never
 *                          computed it (e.g. a hand-built {@code Calibration} via its 3-arg constructor)
 * @param calmSigmaDensity  the calm-window standard deviation of the density series
 *                          ({@link Calibration#sigmaDensity()}, wired straight through); {@link Double#NaN}
 *                          under the same condition as {@link #calmMuDensity()}
 * @param levelGate         the absolute density level gate {@code L} (a percentile of calm density)
 * @param contributors      the top-k asset pairs that moved most this transition (by {@code |Δr|},
 *                          descending), the "who" behind the move; never {@code null} but possibly
 *                          empty (a data gap, a degenerate window, or attribution disabled with k=0)
 */
public record PipelineObservation(
        Instant asOf,
        String market,
        String timescale,
        ChangeMetrics metrics,
        double cusumSPlus,
        double cusumSMinus,
        double recoveryGauge,
        boolean fired,
        SignalKind firedKind,
        double decisionThreshold,
        double calmMu,
        double calmSigma,
        double calmMuDensity,
        double calmSigmaDensity,
        double levelGate,
        List<PairContribution> contributors) {

    /** Activation past this fraction of {@code h} is {@link Severity#WATCH}. */
    public static final double WATCH_FRACTION = 0.5;
    /** Activation past this fraction of {@code h} is {@link Severity#WARN}. */
    public static final double WARN_FRACTION = 0.8;

    /** Validates the metric block and the decision threshold, and defensively copies the contributors. */
    public PipelineObservation {
        if (metrics == null) {
            throw new IllegalArgumentException("metrics must not be null");
        }
        if (!(decisionThreshold > 0.0)) {
            throw new IllegalArgumentException("decisionThreshold must be > 0 [" + decisionThreshold + "]");
        }
        contributors = contributors == null ? List.of() : List.copyOf(contributors);
    }

    /**
     * Pre-{@code calmMuDensity}/{@code calmSigmaDensity} arity; delegates with
     * {@link Double#NaN} for both — a value that can never be mistaken for a real calm statistic,
     * unlike a finite sentinel (a prior deprecated-delegate defect in {@code ChangeMetrics} defaulted a
     * count to {@code -1} and shipped it as if it were data; NaN cannot repeat that mistake here).
     *
     * @deprecated since 1.3.0; loses the density calm pair — carry {@link #calmMuDensity()} and
     *             {@link #calmSigmaDensity()} through the 16-arg canonical constructor instead
     */
    @Deprecated(since = "1.3.0")
    public PipelineObservation(Instant asOf, String market, String timescale, ChangeMetrics metrics,
            double cusumSPlus, double cusumSMinus, double recoveryGauge, boolean fired, SignalKind firedKind,
            double decisionThreshold, double calmMu, double calmSigma, double levelGate,
            List<PairContribution> contributors) {
        this(asOf, market, timescale, metrics, cusumSPlus, cusumSMinus, recoveryGauge, fired, firedKind,
                decisionThreshold, calmMu, calmSigma, Double.NaN, Double.NaN, levelGate, contributors);
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

    /**
     * The structural move in calm-sigma units: {@code (weightedChange − μ) / σ} — "how many standard
     * deviations above the usual move size this transition was". {@link Double#NaN} on a NaN-change gap
     * or a non-positive/NaN {@link #calmSigma()} (degenerate calibration).
     *
     * @return the z-score of the weighted change against the calm baseline
     */
    public double zScore() {
        if (!(calmSigma > 0.0) || Double.isNaN(magnitude())) {
            return Double.NaN;
        }
        return (magnitude() - calmMu) / calmSigma;
    }

    /**
     * Whether the absolute density level gate is open: {@code densityLevel ≥ L}. A NaN density is
     * treated as below any finite gate (closed), matching the detector's gate semantics.
     *
     * @return {@code true} if the density gate would admit a fire this transition
     */
    public boolean levelGateOpen() {
        double density = metrics.densityLevel();
        return !Double.isNaN(density) && density >= levelGate;
    }

    /**
     * The detector's {@link DetectorState lifecycle state} at this transition: {@link DetectorState#FIRED}
     * if it opened an alert, {@link DetectorState#DEBOUNCED} if the firing arm breached with the level
     * gate open but no new alert opened (refractory — already fired this regime), else
     * {@link DetectorState#ARMED} (watching; a breach with the gate shut stays ARMED and is explained by
     * {@link ReasonCode#BLOCKED_BY_LEVEL_GATE}).
     *
     * @return the detector lifecycle state of this transition
     */
    public DetectorState lifecycle() {
        if (fired) {
            return DetectorState.FIRED;
        }
        if (cusumSPlus >= decisionThreshold && levelGateOpen()) {
            return DetectorState.DEBOUNCED;
        }
        return DetectorState.ARMED;
    }

    /**
     * The human-facing {@link Severity} tier, derived from {@link #activation()} (or {@link #fired()}).
     *
     * @return the severity tier of this transition
     */
    public Severity severity() {
        if (fired) {
            return Severity.FIRE;
        }
        double a = activation();
        if (a >= WARN_FRACTION) {
            return Severity.WARN;
        }
        if (a >= WATCH_FRACTION) {
            return Severity.WATCH;
        }
        return Severity.CALM;
    }

    /**
     * The bounded decision-trace for this transition — the finite set of {@link ReasonCode} facts that
     * explain the fire/no-fire outcome (the narrator substitute). Includes the magnitude bucket, the
     * gate/density/component facts, the CUSUM-breach fact, and — when the meter is hot but nothing
     * fired — <em>why</em> ({@link ReasonCode#BLOCKED_BY_LEVEL_GATE} vs {@link ReasonCode#DEBOUNCED}).
     *
     * @return the reason codes in reading order (never {@code null}; possibly empty for a quiet bar)
     */
    public List<ReasonCode> reasonCodes() {
        List<ReasonCode> codes = new ArrayList<>();
        if (Double.isNaN(magnitude())) {
            codes.add(ReasonCode.DATA_GAP);
        }
        if (fired) {
            codes.add(firedKind == SignalKind.DEFUSION ? ReasonCode.FIRE_DEFUSION : ReasonCode.FIRE_FUSION);
        }
        double z = zScore();
        if (z >= 3.0) {
            codes.add(ReasonCode.MAG_GE_3SIGMA);
        } else if (z >= 2.0) {
            codes.add(ReasonCode.MAG_GE_2SIGMA);
        } else if (z >= 1.0) {
            codes.add(ReasonCode.MAG_GE_1SIGMA);
        }
        boolean levelOpen = levelGateOpen();
        if (levelOpen) {
            codes.add(ReasonCode.LEVEL_GATE_OPEN);
        }
        if (metrics.densityLevel() >= 0.999) {
            codes.add(ReasonCode.DENSITY_SATURATED);
        }
        if (metrics.largestComponentFraction() >= 0.99) {
            codes.add(ReasonCode.COMPONENTS_COLLAPSED);
        }
        boolean breached = cusumSPlus >= decisionThreshold;   // v1 fires on the upper arm only
        if (breached) {
            codes.add(ReasonCode.CUSUM_BREACH);
        }
        if (!fired) {
            if (breached && !levelOpen) {
                codes.add(ReasonCode.BLOCKED_BY_LEVEL_GATE);
            } else if (breached) {
                codes.add(ReasonCode.DEBOUNCED);
            } else if (activation() >= WATCH_FRACTION) {
                codes.add(ReasonCode.BUILDING);
            }
        }
        return codes;
    }
}
