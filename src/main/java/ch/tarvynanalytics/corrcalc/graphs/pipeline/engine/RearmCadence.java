package ch.tarvynanalytics.corrcalc.graphs.pipeline.engine;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.RearmConfig;
import ch.tarvynanalytics.graphs.algos.FireArm;
import ch.tarvynanalytics.graphs.algos.model.ChangeSignal;
import ch.tarvynanalytics.graphs.algos.model.FireDirection;

/**
 * The continuous-stream re-arm cadence (H2 numerics spec Q4): decides <em>when</em> the engine
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

    private final RearmConfig config;
    private final boolean defusionEnabled;
    private final FireArm fireArm;
    private final double h;
    private final double levelGate;

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

    /** How many re-arms this cadence has performed. */
    long rearms() {
        return windowId;
    }

    /**
     * Advances the cadence with one scored signal (never {@code null}); a re-arm decided here takes
     * effect on the next {@link #windowId()}.
     *
     * @param signal the transition the detector just scored under the current window id
     */
    void observe(ChangeSignal signal) {
        if (!config.enabled()) {
            return;
        }
        if (signal.fireDirection() == FireDirection.FUSION) {
            awaitingRearm = true;
            barsSinceFire = 0;
            relaxSustained = 0;
            coolDownRemaining = -1;
            return;   // the fire bar itself feeds no trigger
        }
        if (!awaitingRearm) {
            return;   // pure calm: no free-running re-arm clock (the FA-safety guard)
        }
        barsSinceFire++;
        if (defusionEnabled) {
            if (signal.fireDirection() == FireDirection.DEFUSION) {
                coolDownRemaining = config.coolDownBars();
            }
            if (coolDownRemaining >= 0 && coolDownRemaining-- == 0) {
                rearm();
                return;
            }
        } else if (config.relaxSustainBars() > 0) {
            relaxSustained = relaxedGateClosed(signal) ? relaxSustained + 1 : 0;
            if (relaxSustained >= config.relaxSustainBars()) {
                rearm();
                return;
            }
        }
        if (config.calendarRearmBars() > 0 && barsSinceFire >= config.calendarRearmBars()) {
            rearm();
        }
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
