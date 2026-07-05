package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.CalibrationEvent;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.CalibrationEventKind;
import ch.tarvynanalytics.graphs.algos.Calibration;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Optional;

/**
 * The adaptive {@link CalibrationSource} (H2 numerics spec Q2/Q3): the online automation of the
 * validated walk-forward calm block. Composes five proven rules — the <strong>learn gate</strong>
 * ({@code |z| ≤ zLearn} against the current yardstick, plus density at-or-below the live epoch's
 * level gate: the learn-free/alarm-free dead zone sits between the gate and the alarm), the
 * <strong>freeze</strong> during alerts + cool-down (never learn the run-up — the covid rule), the
 * <strong>median/MAD trailing-window estimator</strong> (50% breakdown point;
 * {@code σ̂ = 1.4826·MAD} floored by {@code epsilonSigma}), the <strong>drift meta-monitor</strong>
 * (Page-Hinkley + σ-guard on the admitted series, opening epochs through the detector's
 * {@code recalibrate}), and the <strong>starvation timeout</strong> (re-baseline on the raw
 * trailing window when nothing has been admitted for too long, then re-warm). The detector's
 * baseline moves only at epoch events; the rolling estimate is the gate's yardstick.
 *
 * <p>The gates need a yardstick that <em>exists</em>, so they apply only while {@code LIVE} (the
 * spec's gate is defined against "the current live {@code (μ̂, σ̂)}"). A cold-start warm-up admits
 * every clean bar ungated — the direct analogue of the offline calm block, whose 45-day window is
 * taken whole, with the median/MAD breakdown point as the contamination guard; gating a 2-sample
 * estimate instead locks the band onto wherever the first bars landed. A prior-seeded run keeps the
 * operator-vouched prior as the frozen yardstick until {@code warmupBars} admitted bars make the
 * rolling estimate self-standing — a 1-sample median must never clobber a vouched baseline (its
 * MAD is 0, flooring σ̂ to ε on a scale that disarms the gate entirely). The drift meta-monitor
 * likewise runs only on a self-standing estimate.</p>
 */
final class AdaptiveCalibration implements CalibrationSource {

    static final String MODE = "adaptive";

    private final AdaptiveCalibrationConfig config;
    private final DetectorConfig detectorConfig;
    private final DriftMetaMonitor drift;
    private final CalibrationArtifact prior;

    // Admitted-bar trailing window (the calm set), plus the raw window the starvation escape uses.
    private final double[] cAdmitted;
    private final double[] dAdmitted;
    private final Instant[] tAdmitted;
    private int admittedCount;
    private int admittedHead;
    private final double[] cRaw;
    private final double[] dRaw;
    private int rawCount;
    private int rawHead;

    // Q3 σ-floor: a long admitted-c reference window (running sum/sumSq for an O(1) sample σ), used to
    // floor σ̂ at sigmaFloorFrac·σ_ref so a brief calm patch can't collapse the yardstick. Null when off.
    private final double[] cRef;
    private double refSum;
    private double refSumSq;
    private int refCount;
    private int refHead;

    private boolean live;
    private boolean promotedOnce;
    private long epochId;
    private int admittedThisWarmup;
    private int barsSinceAdmission;
    private int freezeRemaining;
    private Calibration epoch;          // the detector-facing calibration (moves only at epoch events)
    private double muHat;               // rolling estimates (the learn gate's yardstick)
    private double meanHat;             // the admitted window's arithmetic mean — the PH reference
    private double stdHat;              // the admitted window's sample std — the PH δ/λ scale
    private double sigmaHat;
    private double levelHat;
    private double muDensityHat;
    private double sigmaDensityHat;
    private final ArrayDeque<CalibrationEvent> events = new ArrayDeque<>();

    /**
     * @param config         the adaptive tuning
     * @param detectorConfig the detector tuning supplying {@code epsilonSigma} + the level percentile
     * @param prior          an optional operator-vouched artifact: when supplied the source starts
     *                       {@code LIVE} on it immediately and the trailing window refines it
     *                       (the pragmatic v1 seeding path); {@code null} for a cold start
     */
    AdaptiveCalibration(AdaptiveCalibrationConfig config, DetectorConfig detectorConfig,
                        CalibrationArtifact prior) {
        if (config == null) {
            throw new IllegalArgumentException("adaptive config must not be null");
        }
        if (detectorConfig == null) {
            throw new IllegalArgumentException("detector config must not be null");
        }
        this.config = config;
        this.detectorConfig = detectorConfig;
        this.drift = new DriftMetaMonitor(config, config.warmupBars());
        this.prior = prior;
        int m = config.trailingWindowBars();
        this.cAdmitted = new double[m];
        this.dAdmitted = new double[m];
        this.tAdmitted = new Instant[m];
        this.cRaw = new double[m];
        this.dRaw = new double[m];
        this.cRef = config.sigmaFloorFrac() > 0.0 ? new double[config.sigmaRefWindow()] : null;
        if (prior != null) {
            this.epoch = prior.calibration();
            this.epochId = prior.epochId();
            this.live = true;
            this.promotedOnce = true;
            // The vouched prior is the yardstick until the trailing window is self-standing.
            this.muHat = epoch.mu();
            this.meanHat = epoch.mu();
            this.stdHat = epoch.sigma();   // a vouched prior carries no sample std; σ approximates it
            this.sigmaHat = epoch.sigma();
            this.levelHat = epoch.level();
            this.muDensityHat = epoch.muDensity();
            this.sigmaDensityHat = epoch.sigmaDensity();
            drift.open(epoch.mu(), epoch.sigma(), epoch.sigma());
        }
    }

    @Override
    public void observe(Instant asOf, double weightedChange, double density) {
        ingest(asOf, weightedChange, density, false);
    }

    @Override
    public void observeDetection(Instant asOf, double weightedChange, double density, boolean alarmActive) {
        ingest(asOf, weightedChange, density, alarmActive);
    }

    @Override
    public boolean isReady() {
        return promotedOnce;
    }

    @Override
    public boolean live() {
        return live;
    }

    @Override
    public Calibration calibration() {
        if (!promotedOnce) {
            throw new IllegalStateException("adaptive calibration not ready: [" + admittedThisWarmup
                    + "] of [" + config.warmupBars() + "] warm-up bars admitted");
        }
        return epoch;
    }

    @Override
    public Optional<CalibrationEvent> pollEvent() {
        return Optional.ofNullable(events.poll());
    }

    @Override
    public CalibrationProvenance provenance() {
        return new CalibrationProvenance(MODE, epochId, oldestAdmitted(), newestAdmitted());
    }

    @Override
    public CalibrationArtifact artifact(String market, String timescale) {
        if (admittedCount < 2) {
            // Nothing learned yet: a prior-seeded run persists the prior it still runs on; a cold
            // start has no calibration to persist at all (calibration() throws below).
            if (prior != null) {
                return prior;
            }
            calibration();   // throws the not-ready IllegalStateException with the warm-up counts
        }
        return new CalibrationArtifact(CalibrationArtifact.SCHEMA_VERSION, market, timescale, epochId,
                oldestAdmitted(), newestAdmitted(), admittedCount, calibration());
    }

    private void ingest(Instant asOf, double c, double d, boolean alarmActive) {
        if (Double.isNaN(c) || Double.isNaN(d)) {
            starve(asOf);   // a data gap is a non-admission (spec 2.5: "or a data pathology")
            return;
        }
        pushRaw(c, d);
        // Frozen bars never count toward the starvation timeout: a freeze is a pending regime
        // question (in-fire, the fused-awaiting-re-arm span, the refractory tail), not a dark
        // gate — force-re-baselining mid-question would answer a pending all-clear against moved
        // goalposts (the may2021 leak). The timeout escape watches gate rejections only.
        if (alarmActive) {
            freezeRemaining = config.coolDownBars();   // in-fire: frozen, and the refractory tail restarts
            return;
        }
        if (freezeRemaining > 0) {
            freezeRemaining--;
            return;
        }
        // The gates need a live yardstick: a warm-up (cold start or post-timeout) admits every
        // clean bar, exactly as the offline calm block takes its window whole.
        if (live && Math.abs((c - muHat) / sigmaHat) > config.learnThreshold()) {
            starve(asOf);   // the dead zone: not learned, not (by itself) alarming
            return;
        }
        if (live && d > epoch.level()) {
            starve(asOf);   // density-stable rule: above the live epoch's level gate is not calm
            return;
        }
        admit(asOf, c, d);
    }

    private void admit(Instant asOf, double c, double d) {
        barsSinceAdmission = 0;
        pushRef(c);
        cAdmitted[admittedHead] = c;
        dAdmitted[admittedHead] = d;
        tAdmitted[admittedHead] = asOf;
        admittedHead = (admittedHead + 1) % cAdmitted.length;
        admittedCount = Math.min(admittedCount + 1, cAdmitted.length);
        // A prior stays the frozen yardstick until the window can stand on its own; without one the
        // rolling estimate refreshes on every admission (it is only consulted once live/promoting).
        boolean selfStanding = prior == null || admittedCount >= config.warmupBars();
        if (selfStanding) {
            reestimate();
        }
        if (!live) {
            admittedThisWarmup++;
            if (admittedThisWarmup >= config.warmupBars()) {
                promote(asOf);
            }
            return;
        }
        if (!selfStanding) {
            return;   // never open an epoch from an estimate that is still the prior itself
        }
        switch (drift.observeAdmitted(c, sigmaHat)) {
            case DRIFT -> openEpoch(asOf, CalibrationEventKind.RECALIBRATED);
            case SIGMA -> openEpoch(asOf, CalibrationEventKind.EPOCH_OPENED);
            case NONE -> {
                // no drift; the epoch baseline stands
            }
        }
    }

    private void promote(Instant asOf) {
        live = true;
        double muBefore = epoch == null ? Double.NaN : epoch.mu();
        double sigmaBefore = epoch == null ? Double.NaN : epoch.sigma();
        if (promotedOnce) {
            epochId++;   // a re-promotion after a timeout re-baselines a fresh epoch
        }
        promotedOnce = true;
        epoch = estimateCalibration();
        // The PH reference is the admitted window's arithmetic MEAN, not the calibration median:
        // PH accumulates raw deviations, and on a half-bounded skewed series mean − median can
        // exceed δ persistently — a median reference ratchets PH into perpetual false epochs.
        // Its δ/λ scale is likewise the window's SAMPLE std, not the MAD-σ̂ (which understates
        // the truncated-skewed dispersion and lets λ be crossed by chance).
        drift.open(meanHat, stdHat, sigmaHat);
        events.add(new CalibrationEvent(CalibrationEventKind.PROMOTED_TO_LIVE, epochId,
                muBefore, epoch.mu(), sigmaBefore, epoch.sigma(), asOf));
    }

    private void openEpoch(Instant asOf, CalibrationEventKind kind) {
        double muBefore = epoch.mu();
        double sigmaBefore = epoch.sigma();
        epochId++;
        epoch = estimateCalibration();
        drift.open(meanHat, stdHat, sigmaHat);
        events.add(new CalibrationEvent(kind, epochId, muBefore, epoch.mu(),
                sigmaBefore, epoch.sigma(), asOf));
    }

    /** A bar was not admitted; the starvation timeout is the escape from a permanently dark gate. */
    private void starve(Instant asOf) {
        barsSinceAdmission++;
        if (!live || barsSinceAdmission < config.regimeShiftTimeoutBars() || rawCount < 2) {
            return;
        }
        rebaselineAndRewarm(asOf);
    }

    @Override
    public void onRegimeExpired(Instant asOf) {
        // The backstop expired an unresolved question: the same "sustained new regime" verdict the
        // starvation timeout reaches, arrived at by the cadence instead of the counter — same move.
        if (!live || rawCount < 2) {
            return;   // already re-warming (or nothing raw to re-baseline on): the expiry is moot
        }
        rebaselineAndRewarm(asOf);
    }

    /**
     * Gives up on the stale baseline and re-baselines on the raw trailing window (regardless of the
     * learn gate), then demotes for a fresh warm-up: the market never looked calm against it.
     */
    private void rebaselineAndRewarm(Instant asOf) {
        double muBefore = epoch.mu();
        double sigmaBefore = epoch.sigma();
        epochId++;
        epoch = estimate(window(cRaw, rawCount, rawHead), window(dRaw, rawCount, rawHead));
        drift.open(epoch.mu(), epoch.sigma(), epoch.sigma());   // superseded by the re-promotion's open
        events.add(new CalibrationEvent(CalibrationEventKind.REGIME_TIMEOUT, epochId,
                muBefore, epoch.mu(), sigmaBefore, epoch.sigma(), asOf));
        events.add(new CalibrationEvent(CalibrationEventKind.DEMOTED_TO_CALIBRATING, epochId,
                epoch.mu(), epoch.mu(), epoch.sigma(), epoch.sigma(), asOf));
        live = false;
        admittedThisWarmup = 0;
        barsSinceAdmission = 0;
        // The admitted window carries the stale regime — restart it so the gate re-learns afresh.
        admittedCount = 0;
        admittedHead = 0;
    }

    private void pushRaw(double c, double d) {
        cRaw[rawHead] = c;
        dRaw[rawHead] = d;
        rawHead = (rawHead + 1) % cRaw.length;
        rawCount = Math.min(rawCount + 1, cRaw.length);
    }

    /** Recomputes the rolling estimates over the admitted window (order statistics — spec 2.2). */
    private void reestimate() {
        double[] c = window(cAdmitted, admittedCount, admittedHead);
        double[] d = window(dAdmitted, admittedCount, admittedHead);
        meanHat = mean(c);
        stdHat = floored(sampleStd(c, meanHat));
        muHat = RobustStats.median(c);
        sigmaHat = sigmaFloored(floored(RobustStats.MAD_TO_SIGMA * RobustStats.mad(c, muHat)));
        levelHat = RobustStats.nearestRankPercentile(d, detectorConfig.levelPctile());
        muDensityHat = RobustStats.median(d);
        sigmaDensityHat = floored(RobustStats.MAD_TO_SIGMA * RobustStats.mad(d, muDensityHat));
    }

    private Calibration estimateCalibration() {
        return new Calibration(muHat, sigmaHat, levelHat, muDensityHat, sigmaDensityHat);
    }

    private Calibration estimate(double[] c, double[] d) {
        double mu = RobustStats.median(c);
        double sigma = floored(RobustStats.MAD_TO_SIGMA * RobustStats.mad(c, mu));
        double level = RobustStats.nearestRankPercentile(d, detectorConfig.levelPctile());
        double muD = RobustStats.median(d);
        double sigmaD = floored(RobustStats.MAD_TO_SIGMA * RobustStats.mad(d, muD));
        return new Calibration(mu, sigma, level, muD, sigmaD);
    }

    /** The exact existing degenerate-calm floor (spec 2.5): a zero spread flips to {@code epsilonSigma}. */
    private double floored(double sigma) {
        return sigma == 0.0 ? detectorConfig.epsilonSigma() : sigma;
    }

    /**
     * The Q3 relative σ-floor (spec H2R-1 Q3): floor the trailing σ̂ at {@code sigmaFloorFrac} of a
     * long-window reference σ, so a brief calm patch cannot collapse the yardstick into the
     * calm-regime fire metronome (RUN-1 failure mode 2). A no-op when the floor is disabled
     * ({@code cRef == null}) or the reference window has fewer than two admitted bars.
     */
    private double sigmaFloored(double sigma) {
        if (cRef == null || refCount < 2) {
            return sigma;
        }
        double variance = (refSumSq - refSum * refSum / refCount) / (refCount - 1);
        double sigmaRef = variance > 0.0 ? Math.sqrt(variance) : 0.0;
        return Math.max(sigma, config.sigmaFloorFrac() * sigmaRef);
    }

    /** Pushes one admitted weighted-change into the long σ-reference ring, keeping running sum/sumSq O(1). */
    private void pushRef(double c) {
        if (cRef == null) {
            return;
        }
        if (refCount == cRef.length) {
            double evicted = cRef[refHead];
            refSum -= evicted;
            refSumSq -= evicted * evicted;
        } else {
            refCount++;
        }
        cRef[refHead] = c;
        refSum += c;
        refSumSq += c * c;
        refHead = (refHead + 1) % cRef.length;
    }

    private static double mean(double[] values) {
        double sum = 0.0;
        for (double v : values) {
            sum += v;
        }
        return sum / values.length;
    }

    private static double sampleStd(double[] values, double mean) {
        if (values.length < 2) {
            return 0.0;
        }
        double ss = 0.0;
        for (double v : values) {
            double d = v - mean;
            ss += d * d;
        }
        return Math.sqrt(ss / (values.length - 1));
    }

    private static double[] window(double[] ring, int count, int head) {
        double[] out = new double[count];
        for (int i = 0; i < count; i++) {
            out[i] = ring[Math.floorMod(head - count + i, ring.length)];
        }
        return out;
    }

    private Instant oldestAdmitted() {
        return admittedCount == 0 ? null
                : tAdmitted[Math.floorMod(admittedHead - admittedCount, tAdmitted.length)];
    }

    private Instant newestAdmitted() {
        return admittedCount == 0 ? null
                : tAdmitted[Math.floorMod(admittedHead - 1, tAdmitted.length)];
    }

    // ---- test hooks (package-private): the oracle pins the rolling estimates directly ----

    double muHat() {
        return muHat;
    }

    double sigmaHat() {
        return sigmaHat;
    }

    int admittedCount() {
        return admittedCount;
    }
}
