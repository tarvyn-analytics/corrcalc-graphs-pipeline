package ch.tarvynanalytics.corrcalc.graphs.pipeline.engine;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.RearmConfig;
import ch.tarvynanalytics.graphs.algos.FireArm;
import ch.tarvynanalytics.graphs.algos.model.ChangeMetrics;
import ch.tarvynanalytics.graphs.algos.model.ChangeSignal;
import ch.tarvynanalytics.graphs.algos.model.FireDirection;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * H2 Q4 oracle for the re-arm cadence: hand-constructed signal sequences with known
 * fusion/all-clear/relaxation points, window-id trajectories pinned as literals (the naive-loop
 * reference is the hand trace in the numerics spec). {@code h=8}, {@code relaxFrac=0.25} ⇒ the
 * relaxation ceiling is {@code S⁺ < 2}; the level gate literal is {@code L=0.5}.
 */
class RearmCadenceTest {

    private static final double H = 8.0;
    private static final double LEVEL_GATE = 0.5;

    private static ChangeSignal sig(FireDirection dir, double sPlus, double density) {
        return new ChangeSignal(0L, new ChangeMetrics(0.1, density, 0.0, 1, 1.0, List.of()),
                sPlus, 0.0, 0.5, dir);
    }

    private static RearmCadence intraday(int coolDown) {
        return new RearmCadence(new RearmConfig(true, coolDown, 0.25, 5, 0),
                true, FireArm.UPPER, H, LEVEL_GATE);
    }

    private static RearmCadence daily(int sustain, int calendar) {
        return new RearmCadence(new RearmConfig(true, 2, 0.25, sustain, calendar),
                false, FireArm.UPPER, H, LEVEL_GATE);
    }

    @Test
    void intraday_MultiFusion_RearmsOnAllClearPlusCoolDown_FiresNTimes() {
        RearmCadence c = intraday(3);
        // Cycle 1: fusion, 2 calm bars, all-clear, cool-down 3 bars -> id advances to 1.
        c.observe(sig(FireDirection.FUSION, 9.0, 0.9));
        assertEquals(0, c.windowId(), "the fire bar feeds no trigger");
        c.observe(sig(FireDirection.NONE, 1.0, 0.3));
        c.observe(sig(FireDirection.NONE, 0.5, 0.3));
        c.observe(sig(FireDirection.DEFUSION, 0.2, 0.3));
        assertEquals(0, c.windowId(), "the all-clear starts the cool-down, not the re-arm");
        c.observe(sig(FireDirection.NONE, 0.1, 0.3));
        c.observe(sig(FireDirection.NONE, 0.1, 0.3));
        assertEquals(0, c.windowId(), "still cooling down");
        c.observe(sig(FireDirection.NONE, 0.1, 0.3));
        assertEquals(1, c.windowId(), "re-armed 3 bars after the all-clear");
        // Cycle 2: the re-armed detector fires again -> the same loop advances to 2.
        c.observe(sig(FireDirection.FUSION, 9.5, 0.9));
        c.observe(sig(FireDirection.DEFUSION, 0.2, 0.3));
        c.observe(sig(FireDirection.NONE, 0.1, 0.3));
        c.observe(sig(FireDirection.NONE, 0.1, 0.3));
        c.observe(sig(FireDirection.NONE, 0.1, 0.3));
        assertEquals(2, c.windowId(), "one re-arm per fusion→all-clear cycle");
    }

    @Test
    void intraday_CalmBetweenEvents_NoRearmNoClock() {
        // The FA-safety guard: after a completed cycle, pure calm must never advance the id — the
        // re-arm is downstream of a real fusion, not a free-running clock.
        RearmCadence c = intraday(0);
        c.observe(sig(FireDirection.FUSION, 9.0, 0.9));
        c.observe(sig(FireDirection.DEFUSION, 0.2, 0.3));   // cool-down 0: re-arms on the all-clear bar
        assertEquals(1, c.windowId());
        for (int i = 0; i < 10_000; i++) {
            c.observe(sig(FireDirection.NONE, 0.1, 0.3));
        }
        assertEquals(1, c.windowId(), "10k calm bars must not manufacture a re-arm");
    }

    @Test
    void daily_DefusionOff_RelaxRearm_RequiresSustainedDrainAndClosedGate() {
        RearmCadence c = daily(3, 0);
        c.observe(sig(FireDirection.FUSION, 9.0, 0.9));
        // S+ drained but the gate still open (density 0.9 >= L) -> no sustain credit.
        c.observe(sig(FireDirection.NONE, 1.0, 0.9));
        c.observe(sig(FireDirection.NONE, 1.0, 0.9));
        assertEquals(0, c.windowId());
        // Gate closes; two relaxed bars, then a hot bar (S+=3 >= 2) breaks the streak.
        c.observe(sig(FireDirection.NONE, 1.5, 0.3));
        c.observe(sig(FireDirection.NONE, 1.0, 0.3));
        c.observe(sig(FireDirection.NONE, 3.0, 0.3));
        assertEquals(0, c.windowId(), "the streak must be consecutive");
        // Three consecutive relaxed-and-closed bars -> re-arm.
        c.observe(sig(FireDirection.NONE, 1.0, 0.3));
        c.observe(sig(FireDirection.NONE, 0.5, 0.3));
        assertEquals(0, c.windowId());
        c.observe(sig(FireDirection.NONE, 0.2, 0.3));
        assertEquals(1, c.windowId(), "re-armed on the 3rd sustained relaxed bar");
    }

    @Test
    void daily_UnresolvedRegime_CalendarBackstopForcesRearm() {
        RearmCadence c = daily(3, 21);
        c.observe(sig(FireDirection.FUSION, 9.0, 0.9));
        // Density never subsides (a structural break): 20 elevated bars -> no event-driven re-arm.
        for (int i = 0; i < 20; i++) {
            c.observe(sig(FireDirection.NONE, 5.0, 0.9));
        }
        assertEquals(0, c.windowId());
        c.observe(sig(FireDirection.NONE, 5.0, 0.9));   // the 21st bar after the fire
        assertEquals(1, c.windowId(), "the backstop re-arms at fire + calendarRearmBars");
    }

    @Test
    void disabled_NeverAdvances() {
        RearmCadence c = new RearmCadence(RearmConfig.disabled(), true, FireArm.UPPER, H, LEVEL_GATE);
        c.observe(sig(FireDirection.FUSION, 9.0, 0.9));
        c.observe(sig(FireDirection.DEFUSION, 0.2, 0.3));
        for (int i = 0; i < 100; i++) {
            c.observe(sig(FireDirection.NONE, 0.1, 0.3));
        }
        assertEquals(0, c.windowId(), "disabled cadence keeps the pre-H2 one-fire behaviour");
    }
}
