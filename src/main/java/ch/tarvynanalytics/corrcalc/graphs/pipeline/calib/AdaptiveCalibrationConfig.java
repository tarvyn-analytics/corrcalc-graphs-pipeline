package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

/**
 * The adaptive-calibration tuning (H2 numerics spec Q2/Q3) — the quietness-rule + drift constants,
 * config not code (pipeline invariant 4), per asset × timescale. The drift constants are stored as
 * <strong>multiples of the epoch σ</strong> so one default transfers across assets (the meta-monitor
 * is scale-free).
 *
 * @param warmupBars             {@code K} — admitted clean bars before {@code CALIBRATING → LIVE}
 *                               promotion (cold start, spec 2.4)
 * @param learnThreshold         {@code zLearn} — the admit-to-baseline per-bar {@code |z|} ceiling,
 *                               strictly below the alarm accumulation (the dead zone, spec 2.1)
 * @param coolDownBars           admission freeze after an alert clears (the refractory tail, spec 2.3)
 * @param driftDelta             Page-Hinkley tolerance {@code δ} in σ-epoch multiples (spec Q3)
 * @param driftLambda            Page-Hinkley threshold {@code λ} in σ-epoch multiples (spec Q3)
 * @param driftSigmaRatioLo      σ-guard lower band edge as a ratio of σ-epoch (spec Q3)
 * @param driftSigmaRatioHi      σ-guard upper band edge as a ratio of σ-epoch (spec Q3)
 * @param regimeShiftTimeoutBars force an epoch if no bar is admitted for this long (starvation
 *                               fallback, spec 2.5)
 * @param trailingWindowBars     {@code M} — the robust median/MAD estimator's admitted-bar window
 *                               (spec 2.2)
 * @param sigmaFloorFrac         {@code Q3 σ-floor}: floor the trailing σ̂ at this fraction of a
 *                               long-window reference σ (spec H2R-1 Q3) so a brief calm patch cannot
 *                               collapse the CUSUM yardstick into the calm-regime fire metronome
 *                               (RUN-1 failure mode 2). {@code 0} disables the relative floor (the
 *                               exact pre-Q3 behaviour); the settled crypto value is {@code 0.5}
 * @param sigmaRefWindow         the admitted-bar window over which the reference σ is measured
 *                               ({@code >= 2}; the settled crypto ≈ 30 days). Ignored when
 *                               {@code sigmaFloorFrac == 0}
 */
public record AdaptiveCalibrationConfig(
        int warmupBars,
        double learnThreshold,
        int coolDownBars,
        double driftDelta,
        double driftLambda,
        double driftSigmaRatioLo,
        double driftSigmaRatioHi,
        int regimeShiftTimeoutBars,
        int trailingWindowBars,
        double sigmaFloorFrac,
        int sigmaRefWindow) {

    /**
     * The pre-Q3 constructor: the relative σ-floor disabled ({@code sigmaFloorFrac == 0}), behaviour
     * byte-for-byte identical to before the floor existed. Kept so existing callers and tests are
     * unaffected; the crypto factories opt into the floor via the canonical constructor.
     */
    public AdaptiveCalibrationConfig(int warmupBars, double learnThreshold, int coolDownBars,
                                     double driftDelta, double driftLambda, double driftSigmaRatioLo,
                                     double driftSigmaRatioHi, int regimeShiftTimeoutBars,
                                     int trailingWindowBars) {
        this(warmupBars, learnThreshold, coolDownBars, driftDelta, driftLambda, driftSigmaRatioLo,
                driftSigmaRatioHi, regimeShiftTimeoutBars, trailingWindowBars, 0.0, 2);
    }

    /** Validates the knobs, throwing {@link IllegalArgumentException} with the offending value bracketed. */
    public AdaptiveCalibrationConfig {
        if (warmupBars < 2) {
            throw new IllegalArgumentException("warmupBars must be >= 2 [" + warmupBars + "]");
        }
        if (!(learnThreshold > 0.0)) {
            throw new IllegalArgumentException("learnThreshold must be > 0 [" + learnThreshold + "]");
        }
        if (coolDownBars < 0) {
            throw new IllegalArgumentException("coolDownBars must be >= 0 [" + coolDownBars + "]");
        }
        if (!(driftDelta >= 0.0)) {
            throw new IllegalArgumentException("driftDelta must be >= 0 [" + driftDelta + "]");
        }
        if (!(driftLambda > 0.0)) {
            throw new IllegalArgumentException("driftLambda must be > 0 [" + driftLambda + "]");
        }
        if (!(driftSigmaRatioLo > 0.0) || !(driftSigmaRatioHi > driftSigmaRatioLo)) {
            throw new IllegalArgumentException("sigma-ratio band must satisfy 0 < lo < hi ["
                    + driftSigmaRatioLo + ", " + driftSigmaRatioHi + "]");
        }
        if (regimeShiftTimeoutBars < 1) {
            throw new IllegalArgumentException(
                    "regimeShiftTimeoutBars must be >= 1 [" + regimeShiftTimeoutBars + "]");
        }
        if (trailingWindowBars < 2) {
            throw new IllegalArgumentException(
                    "trailingWindowBars must be >= 2 [" + trailingWindowBars + "]");
        }
        if (warmupBars > trailingWindowBars) {
            // The estimator window must be able to hold a full warm-up: a prior-seeded source only
            // hands over from the prior once warmupBars admitted bars fit in the window.
            throw new IllegalArgumentException("warmupBars must be <= trailingWindowBars ["
                    + warmupBars + " > " + trailingWindowBars + "]");
        }
        if (!(sigmaFloorFrac >= 0.0)) {
            throw new IllegalArgumentException("sigmaFloorFrac must be >= 0 [" + sigmaFloorFrac + "]");
        }
        if (sigmaRefWindow < 2) {
            throw new IllegalArgumentException("sigmaRefWindow must be >= 2 [" + sigmaRefWindow + "]");
        }
    }

    /**
     * The crypto intraday defaults (1-min cadence): 24 h warm-up, 48 h freeze/estimator windows
     * matched to the recovery-gauge scale, 14-day starvation timeout (numerics spec §0). The Q3
     * σ-floor is enabled ({@code frac=0.5} over a {@code 43200}-bar ≈ 30-day reference window) — the
     * one universe-independent H2R-1 amendment, hardening the demoted CUSUM annotation against the
     * calm-regime fire metronome (design §5.2, §7).
     *
     * @return the intraday tuning
     */
    public static AdaptiveCalibrationConfig cryptoIntraday() {
        return new AdaptiveCalibrationConfig(1440, 2.5, 2880, 0.25, 5.0, 0.5, 2.0, 20160, 2880, 0.5, 43200);
    }

    /**
     * The crypto daily defaults: the validated 45-bar calm-block length as both warm-up and
     * estimator window, 90-day starvation timeout (numerics spec §0). The Q3 σ-floor is enabled
     * ({@code frac=0.5} over a 30-bar ≈ 30-day reference window).
     *
     * @return the daily tuning
     */
    public static AdaptiveCalibrationConfig cryptoDaily() {
        return new AdaptiveCalibrationConfig(45, 2.5, 2, 0.25, 5.0, 0.5, 2.0, 90, 45, 0.5, 30);
    }
}
