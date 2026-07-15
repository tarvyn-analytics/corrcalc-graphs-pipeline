package ch.tarvynanalytics.corrcalc.graphs.pipeline.engine;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.RearmConfig;
import ch.tarvynanalytics.graphs.algos.FireArm;
import ch.tarvynanalytics.graphs.algos.model.ChangeSignal;
import ch.tarvynanalytics.graphs.algos.model.FireDirection;

/**
 * The continuous-stream re-arm cadence: decides <em>when</em> the engine
 * advances the detector's window id, so the GAL-28 {@code reset_ids} re-arm (clear the one-fire
 * debounce, reset the firing arm) happens once per resolved regime event — a multi-year stream then
 * fires once per event instead of once ever, while inter-event calm FA stays at the single-armed
 * detector's measured rate (re-arms are event-count-bounded, never a free-running clock).
 *
 * <p>Triggers (config, {@link RearmConfig}): the DEFUSION all-clear plus a cool-down (timescales
 * with de-fusion enabled); the S⁺-relaxation-with-gate-closed rule (de-fusion disabled); and the
 * calendar backstop for a regime that never cleanly resolves. Single-writer, driven by the engine's
 * listener with each scored signal; {@link #windowId()} applies from the <em>next</em> matrix.</p>
 */
final class RearmCadence {

    /** How a scored signal moved the cadence — what kind of re-arm (if any) this bar decided. */
    enum Rearm {
        /** No re-arm this bar. */
        NONE,
        /** The regime question resolved on its own evidence (all-clear + cool-down, or relaxation). */
        RESOLVED,
        /**
         * The calendar backstop expired an unresolved question: the aftermath never returned to the
         * old calm. The engine must let the calibration re-baseline — the freeze protected a
         * <em>pending</em> question, and an expired-unresolved one is exactly the sustained new
         * regime the starvation timeout re-baselines on (otherwise the stale frozen baseline
         * re-fires on the elevated structure every {@code calendarRearmBars}: a fire metronome).
         */
        EXPIRED
    }

    private final RearmConfig config;
    private final boolean defusionEnabled;
    private final FireArm fireArm;
    private final double h;
    private double levelGate;   // refreshed when an adaptive epoch recalibrates the detector

    private long windowId;
    private boolean awaitingRearm;   // a fusion fired; the debounce is latched until a trigger resolves it
    private long barsSinceFire;
    private int relaxSustained;
    private long coolDownRemaining = -1;   // >= 0 once an all-clear started the cool-down

    /**
     * @param config          the cadence tuning
     * @param defusionEnabled whether this timescale fires the DEFUSION all-clear (selects the
     *                        all-clear trigger vs the S⁺-relaxation fallback)
     * @param fireArm         which CUSUM arm fires (selects the arm the relaxation rule watches)
     * @param h               the CUSUM decision interval (the relaxation ceiling is {@code relaxFrac·h})
     * @param levelGate       the calm level gate {@code L} (the relaxation rule requires {@code density < L})
     */
    RearmCadence(RearmConfig config, boolean defusionEnabled, FireArm fireArm, double h, double levelGate) {
        this.config = config;
        this.defusionEnabled = defusionEnabled;
        this.fireArm = fireArm;
        this.h = h;
        this.levelGate = levelGate;
    }

    /** The window id to tag the next matrix with. */
    long windowId() {
        return windowId;
    }

    /**
     * Whether a fusion fired and its regime question is still unresolved (no all-clear/relaxation/
     * backstop re-arm yet). While this holds, the adaptive calibration must stay frozen: the
     * pending all-clear is a question asked against the baseline that fired, and re-baselining
     * mid-question moves the goalposts (the gauge band would re-derive from the post-event
     * structure, trivially satisfying the recovery it is supposed to measure).
     */
    boolean awaitingRearm() {
        return awaitingRearm;
    }

    /**
     * An adaptive calibration epoch re-derived the level gate: keep the relaxation rule's
     * gate-closed test on the live {@code L}, matching the detector it re-arms.
     *
     * @param levelGate the new calm level gate
     */
    void recalibrated(double levelGate) {
        this.levelGate = levelGate;
    }

    /** How many re-arms this cadence has performed. */
    long rearms() {
        return windowId;
    }

    /**
     * Advances the cadence with one scored signal (never {@code null}); a re-arm decided here takes
     * effect on the next {@link #windowId()}.
     *
     * @param signal the transition the detector just scored under the current window id
     * @return what kind of re-arm (if any) this bar decided
     */
    Rearm observe(ChangeSignal signal) {
        if (!config.enabled()) {
            return Rearm.NONE;
        }
        if (signal.fireDirection() == FireDirection.FUSION) {
            awaitingRearm = true;
            barsSinceFire = 0;
            relaxSustained = 0;
            coolDownRemaining = -1;
            return Rearm.NONE;   // the fire bar itself feeds no trigger
        }
        if (!awaitingRearm) {
            return Rearm.NONE;   // pure calm: no free-running re-arm clock (the FA-safety guard)
        }
        barsSinceFire++;
        if (defusionEnabled) {
            if (signal.fireDirection() == FireDirection.DEFUSION) {
                coolDownRemaining = config.coolDownBars();
            }
            if (coolDownRemaining >= 0 && coolDownRemaining-- == 0) {
                rearm();
                return Rearm.RESOLVED;
            }
        } else if (config.relaxSustainBars() > 0) {
            relaxSustained = relaxedGateClosed(signal) ? relaxSustained + 1 : 0;
            if (relaxSustained >= config.relaxSustainBars()) {
                rearm();
                return Rearm.RESOLVED;
            }
        }
        if (config.calendarRearmBars() > 0 && barsSinceFire >= config.calendarRearmBars()) {
            rearm();
            return Rearm.EXPIRED;
        }
        return Rearm.NONE;
    }

    /** The per-bar relaxation condition: the firing arm has drained AND density left the fired band. */
    private boolean relaxedGateClosed(ChangeSignal signal) {
        double firingArm = fireArm == FireArm.UPPER ? signal.sPlus() : signal.sMinus();
        double density = signal.metrics().densityLevel();
        return firingArm < config.relaxFrac() * h && density < levelGate;
    }

    /**
     * A session boundary re-armed the detector itself ({@code onSessionBoundary} clears the debounce
     * and both arms), so any pending trigger state is stale — clear it. The id is left monotonic (ids
     * only ever need to differ).
     */
    void onSessionBoundary() {
        awaitingRearm = false;
        relaxSustained = 0;
        coolDownRemaining = -1;
        barsSinceFire = 0;
    }

    private void rearm() {
        windowId++;
        awaitingRearm = false;
        relaxSustained = 0;
        coolDownRemaining = -1;
    }
}
