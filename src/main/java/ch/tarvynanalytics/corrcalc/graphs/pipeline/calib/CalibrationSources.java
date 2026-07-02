package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

import ch.tarvynanalytics.graphs.algos.DetectorConfig;

import java.nio.file.Path;

/**
 * Factory for the built-in {@link CalibrationSource} implementations (the implementations stay
 * package-private — pipeline invariant 7; mirrors the lib's {@code ChangeDetectors} recipe), plus
 * the narrow artifact-persistence door drivers use ({@link #load}/{@link #save} — the Jackson
 * {@code CalibrationStore} itself stays package-private).
 */
public final class CalibrationSources {

    private CalibrationSources() {
    }

    /**
     * The leading-warmup source: calibrate once on the first {@code calmBars} window-points, then
     * freeze — the engine's original behaviour and the explicit "quick look" mode (a lead time under
     * this source is start-point-dependent; see the H2 design).
     *
     * @param calmBars window-points to accumulate before calibrating ({@code >= 2})
     * @param config   the detector tuning supplying the level percentile and sigma floor
     * @return a new single-run source
     */
    public static CalibrationSource leadingWarmup(int calmBars, DetectorConfig config) {
        return new LeadingWarmupCalibration(calmBars, config);
    }

    /**
     * The calm-block source: ready before the stream starts, primed from a walk-forward
     * {@link CalibrationArtifact} (a calm window that precedes and is disjoint from the detection
     * span), so the detector calibrates on the first window-fill snapshot and a reported lead time
     * is never start-point-dependent.
     *
     * @param artifact the persisted walk-forward calibration to prime from
     * @return a new single-run source
     */
    public static CalibrationSource calmBlock(CalibrationArtifact artifact) {
        return new CalmBlockCalibration(artifact);
    }

    /**
     * The adaptive source (H2 numerics spec Q2/Q3): the online walk-forward automation — a
     * self-gating quietness rule admits calm bars to a robust median/MAD trailing estimator, a
     * Page-Hinkley drift meta-monitor opens new calibration epochs through the detector's
     * {@code recalibrate}, and the lifecycle is surfaced as bounded {@code CalibrationEvent}s.
     *
     * @param config         the adaptive tuning (per asset × timescale)
     * @param detectorConfig the detector tuning supplying {@code epsilonSigma} + the level percentile
     * @param prior          an operator-vouched artifact to start {@code LIVE} on immediately, or
     *                       {@code null} for a cold start ({@code CALIBRATING} until the warm-up
     *                       admits {@code warmupBars} clean bars)
     * @return a new single-run source
     */
    public static CalibrationSource adaptive(AdaptiveCalibrationConfig config,
                                             DetectorConfig detectorConfig, CalibrationArtifact prior) {
        return new AdaptiveCalibration(config, detectorConfig, prior);
    }

    /**
     * Loads a persisted artifact ({@code --calibration-artifact}).
     *
     * @param json the artifact file
     * @return the artifact, validated through its record constructor
     * @throws java.io.UncheckedIOException if the file cannot be read or parsed
     */
    public static CalibrationArtifact load(Path json) {
        return CalibrationStore.load(json);
    }

    /**
     * Persists an artifact ({@code --save-calibration}).
     *
     * @param artifact the artifact to write
     * @param json     the destination file (overwritten)
     * @throws java.io.UncheckedIOException if the file cannot be written
     */
    public static void save(CalibrationArtifact artifact, Path json) {
        CalibrationStore.save(artifact, json);
    }
}
