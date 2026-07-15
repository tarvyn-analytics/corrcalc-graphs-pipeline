package ch.tarvynanalytics.corrcalc.graphs.pipeline.replay;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.engine.Pace;

import java.time.Duration;
import java.time.Instant;

/**
 * Paces a replay to wall-clock time scaled by a speed multiplier: between two consecutive bar
 * timestamps it sleeps {@code realGap / speed}, capped at {@code maxStepMs} so a long session gap
 * (the overnight hole the panel leaves between UTC days) never stalls the view. The duration maths is
 * the pure, side-effect-free {@link #stepMillis} (unit-tested directly); {@link #pace} is the thin
 * sleeping wrapper. {@link #noSleep()} yields an instant clock for tests.
 *
 * <p>It is the replay's {@link Pace}: the {@code PipelineDriver} calls {@link #between} between detection
 * bars, which a live run satisfies with {@link Pace#none()} instead.</p>
 */
public final class ReplayClock implements Pace {

    private final double speed;
    private final long maxStepMs;
    private final boolean sleep;

    private ReplayClock(double speed, long maxStepMs, boolean sleep) {
        this.speed = speed;
        this.maxStepMs = maxStepMs;
        this.sleep = sleep;
    }

    /**
     * A real clock that sleeps to pace the replay.
     *
     * @param speed     the simulated-time : real-time ratio ({@code > 0}; higher = faster)
     * @param maxStepMs the per-step sleep cap in milliseconds ({@code >= 0})
     * @return a sleeping clock
     * @throws IllegalArgumentException if {@code speed <= 0} / non-finite, or {@code maxStepMs < 0}
     */
    public static ReplayClock of(double speed, long maxStepMs) {
        if (!(speed > 0.0) || !Double.isFinite(speed)) {
            throw new IllegalArgumentException("speed must be a finite positive multiplier [" + speed + "]");
        }
        if (maxStepMs < 0) {
            throw new IllegalArgumentException("maxStepMs must be >= 0 [" + maxStepMs + "]");
        }
        return new ReplayClock(speed, maxStepMs, true);
    }

    /** A non-sleeping clock — replays as fast as possible (used by tests). */
    static ReplayClock noSleep() {
        return new ReplayClock(1.0, 0L, false);
    }

    /**
     * The pacing sleep, in milliseconds, between two consecutive timestamps under {@code speed},
     * capped at {@code maxStepMs}. Returns {@code 0} for a null/first timestamp or a non-positive gap.
     *
     * @param prev      the previous bar's timestamp, or {@code null} for the first
     * @param cur       the current bar's timestamp
     * @param speed     the simulated-time : real-time ratio
     * @param maxStepMs the per-step cap
     * @return the milliseconds to sleep
     */
    public static long stepMillis(Instant prev, Instant cur, double speed, long maxStepMs) {
        if (prev == null || cur == null) {
            return 0L;
        }
        long deltaMs = Duration.between(prev, cur).toMillis();
        if (deltaMs <= 0L) {
            return 0L;
        }
        long scaled = (long) Math.floor(deltaMs / speed);
        return Math.min(scaled, maxStepMs);
    }

    /**
     * Sleeps for the paced interval between {@code prev} and {@code cur}. A no-op on a non-sleeping
     * clock; an interruption is restored and returns promptly so the caller can stop.
     *
     * @param prev the previous bar's timestamp, or {@code null} for the first
     * @param cur  the current bar's timestamp
     */
    public void pace(Instant prev, Instant cur) {
        if (!sleep) {
            return;
        }
        long ms = stepMillis(prev, cur, speed, maxStepMs);
        if (ms > 0L) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** {@link Pace} adapter: the driver's between-bars hook is this clock's paced sleep. */
    @Override
    public void between(Instant prev, Instant cur) {
        pace(prev, cur);
    }

    /** The speed multiplier. */
    public double speed() {
        return speed;
    }

    /** The per-step sleep cap in milliseconds. */
    public long maxStepMs() {
        return maxStepMs;
    }
}
