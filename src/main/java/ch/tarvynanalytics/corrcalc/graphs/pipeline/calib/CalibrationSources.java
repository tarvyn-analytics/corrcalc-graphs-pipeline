package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

import ch.tarvynanalytics.graphs.algos.DetectorConfig;

/**
 * Factory for the built-in {@link CalibrationSource} implementations (the implementations stay
 * package-private — pipeline invariant 7; mirrors the lib's {@code ChangeDetectors} recipe).
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
}
