package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

import ch.tarvynanalytics.graphs.algos.Calibration;

import java.time.Instant;

/**
 * The calm-block {@link CalibrationSource}: primed from a caller-supplied walk-forward
 * {@link CalibrationArtifact} (a calm window calibrated offline, preceding and disjoint from the
 * detection span), so it is ready before the stream starts and the detector calibrates on the
 * <em>first</em> window-fill snapshot — no leading accumulation, no start-point dependence. This is
 * the mode that makes the walk-forward tape-test behaviour reachable through the replay CLI.
 */
final class CalmBlockCalibration implements CalibrationSource {

    static final String MODE = "calm-block";

    private final CalibrationArtifact artifact;

    CalmBlockCalibration(CalibrationArtifact artifact) {
        if (artifact == null) {
            throw new IllegalArgumentException("artifact must not be null");
        }
        this.artifact = artifact;
    }

    @Override
    public void observe(Instant asOf, double weightedChange, double density) {
        // externally calibrated: the stream never feeds the baseline (walk-forward discipline)
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public Calibration calibration() {
        return artifact.calibration();
    }

    @Override
    public CalibrationProvenance provenance() {
        return artifact.provenance(MODE);
    }

    @Override
    public CalibrationArtifact artifact(String market, String timescale) {
        return artifact;
    }
}
