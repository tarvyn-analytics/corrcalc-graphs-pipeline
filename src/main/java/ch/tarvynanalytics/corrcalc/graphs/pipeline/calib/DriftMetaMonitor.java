package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

/**
 * The drift meta-monitor: a two-sided <strong>Page-Hinkley</strong> test on
 * the <em>admitted-calm</em> {@code c_t} series — it watches the calm statistics themselves, so it
 * opens a calibration epoch only when the calm baseline has durably moved (the detector fires on
 * events; the meta-monitor re-baselines on shifts) — plus the σ-ratio guard for a variance regime
 * change a mean-only test misses. Both arms accumulate signed excess past the tolerance
 * {@code δ = driftDelta·σ_epoch} against a running-min reference and open when the excess passes
 * {@code λ = driftLambda·σ_epoch}. O(1) state; single-writer.
 */
final class DriftMetaMonitor {

    /** What the last admitted bar triggered. */
    enum Trigger {
        /** No epoch opens. */
        NONE,
        /** The Page-Hinkley mean-drift arms crossed λ. */
        DRIFT,
        /** The estimator's σ̂ left the ratio band for the sustain span. */
        SIGMA
    }

    private final AdaptiveCalibrationConfig config;
    private final int sigmaSustainBars;

    private double muEpoch;
    private double phSigma;
    private double sigmaEpoch;
    private double mUp;
    private double minUp;
    private double mDown;
    private double minDown;
    private int sigmaOutsideBand;

    /**
     * @param config           the drift constants (δ, λ in σ-epoch multiples; the ratio band)
     * @param sigmaSustainBars admitted bars σ̂ must stay outside the band before the guard opens
     *                         (spec default: the warm-up length {@code K})
     */
    DriftMetaMonitor(AdaptiveCalibrationConfig config, int sigmaSustainBars) {
        this.config = config;
        this.sigmaSustainBars = sigmaSustainBars;
    }

    /**
     * (Re-)baselines the monitor at an epoch open: captures the epoch's calm references and resets
     * the Page-Hinkley arms (a new baseline invalidates the old accumulation — Q1's rule, applied
     * to the meta level). The two scales are deliberately distinct: PH accumulates raw deviations
     * of the admitted series, so its tolerance {@code δ}/threshold {@code λ} must scale with that
     * series' <em>sample</em> dispersion — the robust MAD-σ̂ understates it on the gate-truncated,
     * right-skewed calm stream, and a MAD-scaled {@code λ} is crossed by chance (measured 88–510
     * false epochs per event tape). The σ-ratio guard stays a like-for-like MAD comparison.
     *
     * @param muEpoch    the admitted window's arithmetic mean (the PH reference)
     * @param phSigma    the admitted window's sample standard deviation (the PH {@code δ}/{@code λ} scale)
     * @param sigmaEpoch the epoch's MAD-σ̂ (the σ-guard's ratio reference)
     */
    void open(double muEpoch, double phSigma, double sigmaEpoch) {
        this.muEpoch = muEpoch;
        this.phSigma = phSigma;
        this.sigmaEpoch = sigmaEpoch;
        mUp = 0.0;
        minUp = 0.0;
        mDown = 0.0;
        minDown = 0.0;
        sigmaOutsideBand = 0;
    }

    /**
     * Advances the monitor with one admitted-calm bar.
     *
     * @param c            the admitted {@code weightedChange}
     * @param sigmaCurrent the estimator's current σ̂ (the σ-guard input)
     * @return what this bar triggered
     */
    Trigger observeAdmitted(double c, double sigmaCurrent) {
        double delta = config.driftDelta() * phSigma;
        double lambda = config.driftLambda() * phSigma;
        mUp += c - muEpoch - delta;
        minUp = Math.min(minUp, mUp);
        mDown += muEpoch - c - delta;
        minDown = Math.min(minDown, mDown);
        if (mUp - minUp > lambda || mDown - minDown > lambda) {
            return Trigger.DRIFT;
        }
        boolean outside = sigmaCurrent < config.driftSigmaRatioLo() * sigmaEpoch
                || sigmaCurrent > config.driftSigmaRatioHi() * sigmaEpoch;
        sigmaOutsideBand = outside ? sigmaOutsideBand + 1 : 0;
        if (sigmaOutsideBand >= sigmaSustainBars) {
            return Trigger.SIGMA;
        }
        return Trigger.NONE;
    }

    /** The current upper-arm Page-Hinkley excess (test hook — pins the hand trace). */
    double phUp() {
        return mUp - minUp;
    }
}
