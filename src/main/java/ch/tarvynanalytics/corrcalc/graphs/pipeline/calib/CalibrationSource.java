package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

import ch.tarvynanalytics.graphs.algos.Calibration;

import java.time.Instant;

/**
 * How the engine obtains the detector's calm-window {@link Calibration} — the strategy seam that
 * makes "when and from what the detector is calibrated" a run configuration instead of a hardcoded
 * engine phase (H2 design §2.1). The engine feeds every pre-detection window-point's calm statistics
 * through {@link #observe} and consults {@link #isReady()}; once ready, it builds the detector from
 * {@link #calibration()} and stops observing. One instance per engine run; single-writer, like the
 * engine that drives it.
 *
 * <p>The three product modes are implementations of this seam: the leading-warmup prefix (replay's
 * pragmatic "quick look" — {@link CalibrationSources#leadingWarmup}), a caller-supplied walk-forward
 * calm-block artifact, and the adaptive online estimator (H2 design §3.2). The engine path is shared
 * by replay and live (replay = live except source + pace), so a source plugged here is exercised
 * identically by both.</p>
 */
public interface CalibrationSource {

    /**
     * Feeds one pre-detection window-point's calm statistics.
     *
     * @param asOf           the window-point timestamp
     * @param weightedChange the S3 weighted change at this point ({@link Double#NaN} for the first
     *                       point of a stream/session — no predecessor)
     * @param density        the S3 density level at this point
     */
    void observe(Instant asOf, double weightedChange, double density);

    /**
     * Whether enough calm data exists to calibrate — the engine's calibration boundary. Once this
     * turns {@code true} the engine calls {@link #calibration()} and starts detecting; it is never
     * consulted again for this run.
     *
     * @return {@code true} once {@link #calibration()} may be called
     */
    boolean isReady();

    /**
     * The calm-window calibration once ready.
     *
     * @return the calibration to build the detector from
     * @throws IllegalStateException if {@link #isReady()} is {@code false}
     */
    Calibration calibration();

    /**
     * This source's provenance for the run context: the mode label, the live epoch, and the calm
     * source window when known (a leading-warmup source discovers its window from the stream, so
     * the window is {@code null} until it has observed data).
     *
     * @return the provenance to echo on {@code RunContext}
     */
    CalibrationProvenance provenance();

    /**
     * The persisted form of this source's current calibration — the walk-forward currency a later
     * run's calm-block source primes from ({@code --save-calibration}).
     *
     * @param market    the market label to stamp on the artifact
     * @param timescale the timescale label to stamp on the artifact
     * @return the artifact carrying {@link #calibration()} + this source's window provenance
     * @throws IllegalStateException if {@link #isReady()} is {@code false}
     */
    CalibrationArtifact artifact(String market, String timescale);
}
