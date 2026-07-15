package ch.tarvynanalytics.corrcalc.graphs.pipeline.replay;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReplayClockTest {

    private static final Instant T0 = Instant.parse("2021-05-18T00:00:00Z");

    @Test
    void stepMillis_NullPrev_ReturnsZero() {
        assertEquals(0L, ReplayClock.stepMillis(null, T0, 10.0, 2000L));
    }

    @Test
    void stepMillis_ScalesGapBySpeed() {
        // 60 s real gap at 500x → 120 ms, below the cap.
        assertEquals(120L, ReplayClock.stepMillis(T0, T0.plusSeconds(60), 500.0, 2000L));
    }

    @Test
    void stepMillis_LongGap_CapsAtMaxStep() {
        // a full day at 500x scales to 172_800 ms but is capped at 2000.
        assertEquals(2000L, ReplayClock.stepMillis(T0, T0.plusSeconds(86_400), 500.0, 2000L));
    }

    @Test
    void stepMillis_NonPositiveGap_ReturnsZero() {
        assertEquals(0L, ReplayClock.stepMillis(T0.plusSeconds(60), T0, 500.0, 2000L));
    }

    @Test
    void of_NonPositiveSpeed_Throws() {
        assertThrows(IllegalArgumentException.class, () -> ReplayClock.of(0.0, 2000L));
    }

    @Test
    void of_NegativeMaxStep_Throws() {
        assertThrows(IllegalArgumentException.class, () -> ReplayClock.of(10.0, -1L));
    }

    @Test
    void pace_NoSleepClock_ReturnsImmediately() {
        long start = System.nanoTime();
        ReplayClock.noSleep().pace(T0, T0.plusSeconds(3600));
        assertEquals(0L, ReplayClock.noSleep().maxStepMs());
        // no sleep happened: comfortably under any pacing interval.
        org.junit.jupiter.api.Assertions.assertTrue(System.nanoTime() - start < 500_000_000L);
    }

    @Test
    void pace_RealClock_SleepsUpToCap() {
        ReplayClock clock = ReplayClock.of(1.0, 5L);   // tiny 5 ms cap keeps the test fast
        long start = System.nanoTime();
        clock.pace(T0, T0.plusSeconds(60));            // would be 60 s, capped to 5 ms
        assertEquals(1.0, clock.speed());
        org.junit.jupiter.api.Assertions.assertTrue(System.nanoTime() - start < 2_000_000_000L);
    }
}
