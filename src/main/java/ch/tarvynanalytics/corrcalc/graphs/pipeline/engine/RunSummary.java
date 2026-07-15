package ch.tarvynanalytics.corrcalc.graphs.pipeline.engine;

/**
 * The outcome of one pipeline run, accumulated by the {@link PipelineEngine}.
 *
 * @param detectionPoints     transitions scored after calibration (a NaN-priming first matrix is not one)
 * @param fires               transitions that fired the configured arm
 * @param published           fires that passed the {@code SignalFilter} and reached the {@code SignalSink}
 * @param observationsEmitted observations forwarded across the observation seam (after the policy gate)
 * @param calmBars            window-points consumed to calibrate the detector
 */
public record RunSummary(long detectionPoints, long fires, long published, long observationsEmitted,
                         int calmBars) {
}
