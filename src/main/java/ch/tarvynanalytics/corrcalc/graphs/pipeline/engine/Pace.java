package ch.tarvynanalytics.corrcalc.graphs.pipeline.engine;

import java.time.Instant;

/**
 * A pacing hook the {@link PipelineDriver} calls between consecutive detection bars — the one knob that
 * separates a wall-clock-paced <em>replay</em> from a real-time <em>live</em> run. It keeps the driver
 * and engine independent of the replay clock: a replay passes the {@code ReplayClock} (which sleeps the
 * scaled inter-bar gap), a live feed passes {@link #none()} (no artificial delay — the source already
 * arrives in real time).
 */
@FunctionalInterface
public interface Pace {

    /**
     * Called between two consecutive detection bars, before the later one is processed.
     *
     * @param prev the previous detection bar's timestamp, or {@code null} for the first
     * @param cur  the current detection bar's timestamp
     */
    void between(Instant prev, Instant cur);

    /** No pacing — process bars as fast as the source yields them (the live default). */
    static Pace none() {
        return (prev, cur) -> {
            // live: the source paces itself, so the driver adds no delay
        };
    }
}
