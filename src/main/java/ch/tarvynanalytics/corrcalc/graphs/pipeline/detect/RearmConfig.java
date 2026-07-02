package ch.tarvynanalytics.corrcalc.graphs.pipeline.detect;

/**
 * The continuous-stream <strong>re-arm cadence</strong> tuning (H2 numerics spec Q4) — when the
 * engine advances the detector's window id (the GAL-28 {@code reset_ids} re-arm: clears the
 * one-fire debounce and resets the firing arm) so a multi-year stream fires once per regime event
 * instead of once ever. All constants are configuration, never literals in the cadence body
 * (pipeline invariant 4).
 *
 * <p>Three event-driven triggers, in precedence order:</p>
 * <ol>
 *   <li><strong>All-clear</strong> (timescales with de-fusion enabled): re-arm
 *       {@code coolDownBars} after a DEFUSION fire. FA-safe by the was-fused latch — no all-clear
 *       without a real fusion, so re-arms are event-count-bounded, not time-bounded.</li>
 *   <li><strong>S⁺-relaxation</strong> (de-fusion disabled, e.g. daily): re-arm once the firing arm
 *       has stayed below {@code relaxFrac·h} <em>and</em> the level gate has closed
 *       ({@code density < L}) for {@code relaxSustainBars} consecutive bars — the accumulation that
 *       fired has drained and density left the elevated band.</li>
 *   <li><strong>Calendar backstop</strong>: force a re-arm {@code calendarRearmBars} after the fire
 *       if neither event-driven trigger has resolved (a structural break that never returns to the
 *       old calm density). {@code 0} disables the backstop.</li>
 * </ol>
 *
 * @param enabled           master switch; {@link #disabled()} keeps the one-fire-per-run behaviour
 * @param coolDownBars      bars to wait after a DEFUSION all-clear before re-arming ({@code >= 0})
 * @param relaxFrac         the firing-arm relaxation ceiling as a fraction of {@code h}
 *                          ({@code 0 < relaxFrac < 1})
 * @param relaxSustainBars  consecutive relaxed-and-gate-closed bars required ({@code 0} disables
 *                          the relaxation trigger)
 * @param calendarRearmBars the hard backstop in bars after a fire ({@code 0} disables it)
 */
public record RearmConfig(boolean enabled, int coolDownBars, double relaxFrac,
                          int relaxSustainBars, int calendarRearmBars) {

    /** Validates the knobs, throwing {@link IllegalArgumentException} with the offending value bracketed. */
    public RearmConfig {
        if (coolDownBars < 0) {
            throw new IllegalArgumentException("coolDownBars must be >= 0 [" + coolDownBars + "]");
        }
        if (!(relaxFrac > 0.0) || !(relaxFrac < 1.0)) {
            throw new IllegalArgumentException("relaxFrac must be in (0, 1) [" + relaxFrac + "]");
        }
        if (relaxSustainBars < 0) {
            throw new IllegalArgumentException("relaxSustainBars must be >= 0 [" + relaxSustainBars + "]");
        }
        if (calendarRearmBars < 0) {
            throw new IllegalArgumentException("calendarRearmBars must be >= 0 [" + calendarRearmBars + "]");
        }
    }

    /**
     * No re-arm: the detector debounces after its first fire for the whole run — the pre-H2
     * single-event behaviour, and the default for a bare {@link TimescaleConfig}.
     *
     * @return the disabled cadence
     */
    public static RearmConfig disabled() {
        return new RearmConfig(false, 0, 0.25, 0, 0);
    }
}
