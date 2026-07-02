package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.CalibrationEvent;
import ch.tarvynanalytics.graphs.algos.Calibration;

import java.time.Instant;
import java.util.Optional;

/**
 * How the engine obtains the detector's calm-window {@link Calibration} — the strategy seam that
 * makes "when and from what the detector is calibrated" a run configuration instead of a hardcoded
 * engine phase (H2 design §2.1). The engine feeds every pre-detection window-point's calm statistics
 * through {@link #observe} and consults {@link #isReady()}; once ready, it builds the detector from
 * {@link #calibration()} and switches to feeding {@link #observeDetection} — the frozen sources
 * ignore that, the adaptive source keeps learning through it and surfaces new epochs via
 * {@link #pollEvent()}. One instance per engine run; single-writer, like the engine that drives it.
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
     * Feeds one <em>post-calibration</em> scored transition's calm statistics — the online half of
     * the seam. The adaptive source keeps its quietness gate, estimator and drift meta-monitor
     * running on these; the frozen sources (leading-warmup, calm-block) ignore them. The default is
     * a no-op.
     *
     * @param asOf           the transition timestamp
     * @param weightedChange the S3 weighted change at this transition
     * @param density        the S3 density level at this transition
     * @param alarmActive    the in-fire condition (fired, or the firing arm breached with the level
     *                       gate open) — the adaptive freeze rule's input: never learn the run-up
     */
    default void observeDetection(Instant asOf, double weightedChange, double density,
                                  boolean alarmActive) {
        // frozen sources never learn from the detection span
    }

    /**
     * The re-arm cadence expired an unresolved regime question (the calendar backstop: a fusion
     * whose aftermath never returned to the old calm band). The freeze rule protected the baseline
     * while the all-clear was <em>pending</em>; expiry ends the question, and an expired-unresolved
     * one is exactly the sustained new regime the starvation timeout re-baselines on — the adaptive
     * source force-re-baselines on its raw trailing window and re-warms (otherwise the stale frozen
     * baseline re-fires on the still-elevated structure every backstop period: a fire metronome).
     * Frozen sources ignore this (the default): their calibration is the operator's to change.
     *
     * @param asOf the stream timestamp of the expiring bar
     */
    default void onRegimeExpired(Instant asOf) {
        // frozen sources keep their operator-vouched calibration
    }

    /**
     * Whether enough calm data exists to calibrate — the engine's calibration boundary. Once this
     * turns {@code true} the engine calls {@link #calibration()} and starts detecting; it is never
     * consulted again for this run.
     *
     * @return {@code true} once {@link #calibration()} may be called
     */
    boolean isReady();

    /**
     * Whether the source currently vouches for alerts. Frozen sources are live from the moment they
     * are ready (the default); the adaptive source demotes to a no-alerts re-warm-up after a
     * regime-shift starvation timeout (its baseline was force-rebuilt from unvetted bars), during
     * which the engine suppresses the product fire-stream.
     *
     * @return {@code true} while alerts are vouched for
     */
    default boolean live() {
        return isReady();
    }

    /**
     * The next pending calibration-lifecycle event, if any — a bounded record the engine forwards to
     * the observer once per event ({@link CalibrationEvent}; H1 tie-in, never free-form text). The
     * adaptive source queues one per promotion/recalibration/timeout; frozen sources never emit
     * (the default).
     *
     * @return the next pending event, or empty
     */
    default Optional<CalibrationEvent> pollEvent() {
        return Optional.empty();
    }

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
