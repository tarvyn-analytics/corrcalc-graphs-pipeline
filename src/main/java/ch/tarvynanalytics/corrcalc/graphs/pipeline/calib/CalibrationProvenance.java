package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

import java.time.Instant;

/**
 * The lightweight calibration provenance surfaced on the run context: which mode calibrated the
 * detector, which epoch is live, and what source window produced it. The full persisted form is
 * {@link CalibrationArtifact}; this is the few fields every observer may echo.
 *
 * @param calibrationMode how the baseline was selected: {@code "leading-warmup"},
 *                        {@code "calm-block"} or {@code "adaptive"}
 * @param epochId         the live calibration epoch ({@code 0} for a single-epoch run)
 * @param sourceFrom      calm-window start, or {@code null} when not yet known (a leading-warmup
 *                        run discovers its window from the stream)
 * @param sourceTo        calm-window end, or {@code null} when not yet known
 */
public record CalibrationProvenance(String calibrationMode, long epochId,
                                    Instant sourceFrom, Instant sourceTo) {

    /** Validates the mode label, throwing {@link IllegalArgumentException} when blank. */
    public CalibrationProvenance {
        if (calibrationMode == null || calibrationMode.isBlank()) {
            throw new IllegalArgumentException("calibrationMode must be non-blank [" + calibrationMode + "]");
        }
        if (epochId < 0) {
            throw new IllegalArgumentException("epochId must be >= 0 [" + epochId + "]");
        }
    }
}
