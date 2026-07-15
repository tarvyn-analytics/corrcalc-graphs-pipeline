package ch.tarvynanalytics.corrcalc.graphs.pipeline.replay;

import java.nio.file.Path;
import java.time.LocalDate;

/**
 * The tuning of one paced replay run — every knob the {@link PacedReplay} engine reads, parsed from
 * the CLI. The data location is passed separately (it is the CLI's positional argument); this record
 * holds only the run parameters. Thresholds/windows are <em>not</em> here: those are market
 * configuration resolved from {@link #market()} + {@link #timescale()} (a new market is wired, not
 * coded — pipeline invariant 4).
 *
 * @param event          the event id selecting the {@code <SYMBOL>_<freq>_<event>.csv} bar files
 * @param market         the market label (config selector + the published signal's {@code market})
 * @param timescale      which timescale stream: {@code "intraday"} or {@code "daily"}
 * @param speed          the simulated-time : real-time ratio; higher replays faster ({@code > 0})
 * @param maxStepMs      cap on the per-bar sleep so session gaps don't stall the view ({@code >= 0})
 * @param calmBars       window-points used to calibrate the detector, or {@code null} to auto-pick (~40%)
 * @param limit          stop after this many detection points, or {@code null} for the whole series
 * @param heartbeatEvery log a calm heartbeat every N detection points ({@code >= 1})
 * @param universePath   explicit universe CSV, or {@code null} to default to {@code <dir>/<event>_universe.csv}
 * @param from           earliest UTC bar date to keep (inclusive), or {@code null} for no lower bound
 * @param to             latest UTC bar date to keep (inclusive), or {@code null} for no upper bound
 * @param calibrationMode     how the detector is calibrated: {@code "leading-warmup"} (the pragmatic
 *                            leading prefix, the default), {@code "calm-block"} (primed from a
 *                            persisted walk-forward artifact — requires {@code calibrationArtifact}),
 *                            or {@code "adaptive"} (the online walk-forward automation — quietness
 *                            gate, robust estimator, drift meta-monitor; the product default once
 *                            validated)
 * @param calibrationArtifact the persisted {@code CalibrationArtifact} JSON: required for
 *                            {@code calm-block} (the baseline), optional for {@code adaptive} (an
 *                            operator-vouched prior to start LIVE on), rejected for
 *                            {@code leading-warmup}
 * @param saveCalibration     where to persist the run's resulting calibration artifact, or
 *                            {@code null} to not save
 * @param fireMode            which detector drives the product fire-stream: {@code "cusum"} (the
 *                            default adaptive-CUSUM detector — the n=8 in-span product) or
 *                            {@code "regime"} (the level+hysteresis regime backbone on the
 *                            daily-smoothed density — the continuous-tape fire, with the CUSUM demoted
 *                            to annotation)
 * @param observeDensity      when {@code true}, the engine forwards each finalized smoothed daily
 *                            density level to {@link ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObserver#onDensityLevel}
 *                            ({@code --observe density} in the CLI; only meaningful in regime-backbone
 *                            fire mode; enables {@code density} NDJSON records in {@code --style ndjson})
 */
public record ReplayOptions(
        String event,
        String market,
        String timescale,
        double speed,
        long maxStepMs,
        Integer calmBars,
        Integer limit,
        int heartbeatEvery,
        Path universePath,
        LocalDate from,
        LocalDate to,
        String calibrationMode,
        Path calibrationArtifact,
        Path saveCalibration,
        String fireMode,
        boolean observeDensity) {

    /** The pragmatic leading-prefix calibration-mode label (the "quick look" default). */
    public static final String LEADING_WARMUP = "leading-warmup";
    /** The walk-forward calm-block mode label. */
    public static final String CALM_BLOCK = "calm-block";
    /** The adaptive (online walk-forward) mode label. */
    public static final String ADAPTIVE = "adaptive";
    /** The default adaptive-CUSUM fire mode (the in-span n=8 product). */
    public static final String CUSUM = "cusum";
    /** The regime-backbone fire mode (the continuous-tape product). */
    public static final String REGIME = "regime";

    /** Validates the knobs, throwing {@link IllegalArgumentException} with the offending value bracketed. */
    public ReplayOptions {
        if (event == null || event.isBlank()) {
            throw new IllegalArgumentException("event must be non-blank [" + event + "]");
        }
        if (market == null || market.isBlank()) {
            throw new IllegalArgumentException("market must be non-blank [" + market + "]");
        }
        if (!"intraday".equals(timescale) && !"daily".equals(timescale)) {
            throw new IllegalArgumentException("timescale must be intraday or daily [" + timescale + "]");
        }
        if (!(speed > 0.0) || !Double.isFinite(speed)) {
            throw new IllegalArgumentException("speed must be a finite positive multiplier [" + speed + "]");
        }
        if (maxStepMs < 0) {
            throw new IllegalArgumentException("maxStepMs must be >= 0 [" + maxStepMs + "]");
        }
        if (calmBars != null && calmBars < 2) {
            throw new IllegalArgumentException("calmBars must be >= 2 when set [" + calmBars + "]");
        }
        if (limit != null && limit < 1) {
            throw new IllegalArgumentException("limit must be >= 1 when set [" + limit + "]");
        }
        if (heartbeatEvery < 1) {
            throw new IllegalArgumentException("heartbeatEvery must be >= 1 [" + heartbeatEvery + "]");
        }
        if (!LEADING_WARMUP.equals(calibrationMode) && !CALM_BLOCK.equals(calibrationMode)
                && !ADAPTIVE.equals(calibrationMode)) {
            throw new IllegalArgumentException("calibrationMode must be " + LEADING_WARMUP + ", "
                    + CALM_BLOCK + " or " + ADAPTIVE + " [" + calibrationMode + "]");
        }
        if (CALM_BLOCK.equals(calibrationMode) && calibrationArtifact == null) {
            throw new IllegalArgumentException(
                    "calm-block calibration requires a calibration artifact [null]");
        }
        if (LEADING_WARMUP.equals(calibrationMode) && calibrationArtifact != null) {
            throw new IllegalArgumentException("a calibration artifact is only read under calm-block; "
                    + "mode is [" + calibrationMode + "]");
        }
        if (!CUSUM.equals(fireMode) && !REGIME.equals(fireMode)) {
            throw new IllegalArgumentException("fireMode must be " + CUSUM + " or " + REGIME
                    + " [" + fireMode + "]");
        }
    }
}
