package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

import ch.tarvynanalytics.graphs.algos.Calibration;

import java.time.Instant;

/**
 * A versioned, persisted, provenance-stamped calibration — the lib's {@link Calibration} value
 * {@code (μ, σ, L, μ_D, σ_D)} plus the epoch id and the source window that produced it (H2 design
 * item i). This is the walk-forward currency: a calm block calibrated offline (or an adaptive run's
 * final epoch) is saved as this artifact and primes a later run's calm-block source, so a reported
 * lead time or all-clear is never start-point-dependent. Persistence is the package-private
 * {@link CalibrationStore} (Jackson at the CGP edge — the lib defines the <em>value</em>, the
 * pipeline defines the <em>persisted artifact</em>; the lib stays dependency-free).
 *
 * @param schemaVersion the artifact format version; always {@link #SCHEMA_VERSION} for artifacts
 *                      written by this build (a reader rejects versions it does not understand)
 * @param market        the market the calm window was measured on (e.g. {@code "crypto"})
 * @param timescale     the timescale of the calm series: {@code "intraday"} or {@code "daily"}
 *                      (never mix timescales — the artifact is as timescale-bound as the detector)
 * @param epochId       monotonic per adaptive lifecycle; {@code 0} for a single-epoch offline
 *                      calibration (leading-warmup / calm-block)
 * @param sourceFrom    calm-window start (inclusive) — the source window's provenance
 * @param sourceTo      calm-window end (exclusive)
 * @param sourceSamples clean window-points that fed the estimate
 * @param calibration   the lib calibration value {@code (μ, σ, L, μ_D, σ_D)} the detector runs on
 */
public record CalibrationArtifact(
        int schemaVersion,
        String market,
        String timescale,
        long epochId,
        Instant sourceFrom,
        Instant sourceTo,
        int sourceSamples,
        Calibration calibration) {

    /** The artifact format version this build reads and writes. */
    public static final int SCHEMA_VERSION = 1;

    /** Validates the artifact, throwing {@link IllegalArgumentException} with the offending value bracketed. */
    public CalibrationArtifact {
        if (schemaVersion != SCHEMA_VERSION) {
            throw new IllegalArgumentException("unsupported artifact schemaVersion [" + schemaVersion
                    + "]; this build understands [" + SCHEMA_VERSION + "]");
        }
        if (market == null || market.isBlank()) {
            throw new IllegalArgumentException("market must be non-blank [" + market + "]");
        }
        if (!"intraday".equals(timescale) && !"daily".equals(timescale)) {
            throw new IllegalArgumentException("timescale must be intraday or daily [" + timescale + "]");
        }
        if (epochId < 0) {
            throw new IllegalArgumentException("epochId must be >= 0 [" + epochId + "]");
        }
        if (sourceFrom == null || sourceTo == null) {
            throw new IllegalArgumentException(
                    "source window must be stamped [" + sourceFrom + ", " + sourceTo + "]");
        }
        if (!sourceFrom.isBefore(sourceTo)) {
            throw new IllegalArgumentException(
                    "source window must be non-empty [" + sourceFrom + " >= " + sourceTo + "]");
        }
        if (sourceSamples < 2) {
            throw new IllegalArgumentException(
                    "sourceSamples must be >= 2 to estimate a spread [" + sourceSamples + "]");
        }
        if (calibration == null) {
            throw new IllegalArgumentException("calibration must not be null");
        }
    }

    /**
     * The lightweight provenance view of this artifact for {@code RunContext} / the observation
     * stream.
     *
     * @param calibrationMode the run's calibration mode label (e.g. {@code "calm-block"})
     * @return the provenance carrying this artifact's epoch and source window
     */
    public CalibrationProvenance provenance(String calibrationMode) {
        return new CalibrationProvenance(calibrationMode, epochId, sourceFrom, sourceTo);
    }
}
