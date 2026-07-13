package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

import ch.tarvynanalytics.graphs.algos.Calibration;
import ch.tarvynanalytics.graphs.algos.ChangeDetectors;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * The leading-warmup {@link CalibrationSource}: accumulates the first {@code calmBars} window-points'
 * calm statistics, then calibrates once via {@link ChangeDetectors#calibrate} and freezes — the exact
 * behaviour the engine hardcoded before the seam existed (extracted verbatim from the pre-seam engine).
 * This is replay's pragmatic "quick look": the baseline is whatever the series starts with, so a
 * reported lead time is start-point-dependent — the rigorous modes are the walk-forward calm-block
 * and adaptive sources.
 */
final class LeadingWarmupCalibration implements CalibrationSource {

    static final String MODE = "leading-warmup";

    private final int calmBars;
    private final DetectorConfig config;
    private final List<Double> calmChange = new ArrayList<>();
    private final List<Double> calmDensity = new ArrayList<>();
    private Instant firstAsOf;
    private Instant lastAsOf;
    private Calibration result;

    LeadingWarmupCalibration(int calmBars, DetectorConfig config) {
        if (calmBars < 2) {
            throw new IllegalArgumentException("calmBars must be >= 2 [" + calmBars + "]");
        }
        if (config == null) {
            throw new IllegalArgumentException("detector config must not be null");
        }
        this.calmBars = calmBars;
        this.config = config;
    }

    @Override
    public void observe(Instant asOf, double weightedChange, double density) {
        if (firstAsOf == null) {
            firstAsOf = asOf;
        }
        lastAsOf = asOf;
        calmChange.add(weightedChange);
        calmDensity.add(density);
    }

    @Override
    public boolean isReady() {
        return calmChange.size() >= calmBars;
    }

    @Override
    public Calibration calibration() {
        if (!isReady()) {
            throw new IllegalStateException("calibration not ready: [" + calmChange.size()
                    + "] of [" + calmBars + "] calm points observed");
        }
        if (result == null) {
            result = ChangeDetectors.calibrate(toArray(calmChange), toArray(calmDensity), config);
        }
        return result;
    }

    @Override
    public CalibrationProvenance provenance() {
        // epoch 0: leading-warmup calibrates once and freezes; the window is discovered from the stream.
        return new CalibrationProvenance(MODE, 0L, firstAsOf, lastAsOf);
    }

    @Override
    public CalibrationArtifact artifact(String market, String timescale) {
        return new CalibrationArtifact(CalibrationArtifact.SCHEMA_VERSION, market, timescale, 0L,
                firstAsOf, lastAsOf, calmChange.size(), calibration());
    }

    private static double[] toArray(List<Double> values) {
        double[] out = new double[values.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = values.get(i);
        }
        return out;
    }
}
