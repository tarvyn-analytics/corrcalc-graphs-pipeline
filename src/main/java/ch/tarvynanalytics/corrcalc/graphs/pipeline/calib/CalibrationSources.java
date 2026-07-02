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
