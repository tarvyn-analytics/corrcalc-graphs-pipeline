package ch.tarvynanalytics.corrcalc.graphs.pipeline;

/**
 * A small stateful anti-flap for the <em>displayed</em> {@link Severity} tier. Escalation is immediate
 * (cross the watch/warn fraction and the tier rises at once), but de-escalation requires
 * {@link PipelineObservation#activation() activation} to fall a {@link #MARGIN} below the entry
 * fraction — so a transition oscillating around a boundary does not flicker between tiers.
 *
 * <p>{@link Severity#FIRE} is a per-bar <em>event</em> override (an actual fire), never part of the
 * sticky state: a fired bar displays FIRE but the hysteresis tier keeps tracking the WATCH/WARN reading,
 * so FIRE does not "stick" after the fire bar. This is a human-readability aid used only by
 * {@link ReadableObserver}; the raw, instantaneous {@link PipelineObservation#severity()} and the NDJSON
 * {@code severity} field are unchanged, so the structured stream stays a faithful per-transition record
 * and a machine consumer applies its own smoothing. Single-writer (one per readable stream).</p>
 */
final class SeverityHysteresis {

    /** How far below the entry fraction activation must fall before a tier de-escalates. */
    static final double MARGIN = 0.1;

    private Severity tier = Severity.CALM;   // sticky over {CALM, WATCH, WARN} only

    /**
     * Returns the displayed tier for this transition, applying hysteresis to the WATCH/WARN tiers.
     *
     * @param o the observation
     * @return the anti-flapped severity tier to display ({@link Severity#FIRE} on a fired bar)
     */
    Severity step(PipelineObservation o) {
        double activation = o.activation();
        Severity raw = rawTier(activation);
        if (raw.ordinal() > tier.ordinal()) {
            tier = raw;                                              // escalate immediately
        } else if (raw.ordinal() < tier.ordinal() && belowExitBand(activation)) {
            tier = raw;                                             // de-escalate only past the lower band
        }
        return o.fired() ? Severity.FIRE : tier;
    }

    /** The instantaneous tier from activation, over {CALM, WATCH, WARN} (never FIRE — that is an event). */
    private static Severity rawTier(double activation) {
        if (activation >= PipelineObservation.WARN_FRACTION) {
            return Severity.WARN;
        }
        if (activation >= PipelineObservation.WATCH_FRACTION) {
            return Severity.WATCH;
        }
        return Severity.CALM;
    }

    /** Whether activation has dropped a {@link #MARGIN} below the current tier's entry fraction. */
    private boolean belowExitBand(double activation) {
        double entry = tier == Severity.WARN
                ? PipelineObservation.WARN_FRACTION
                : PipelineObservation.WATCH_FRACTION;
        return activation < entry - MARGIN;
    }
}
