package ch.tarvynanalytics.corrcalc.graphs.pipeline.backtest;

import java.time.Instant;

/**
 * One row of the n=8 lead table — the regression target, with the same columns as the spike's
 * {@code crypto_lead_table.csv}. Nullable boxed fields are the spike's empty cells (a miss has no
 * alert timestamp and no density-at-alert; {@code lead_hours} exists only when both timescales fire).
 *
 * @param event                   the event name
 * @param universeSize            the number of symbols that passed the liquidity filter
 * @param calmBlock               the {@code "calm_start .. calm_end"} string
 * @param eventWindow             the {@code "start .. end"} string
 * @param tDaily                  the daily alert timestamp, or {@code null} on a daily miss
 * @param tIntraday               the intraday alert timestamp, or {@code null} on an intraday miss
 * @param dailyDensityAtAlert     the daily density at the alert, or {@code null} on a miss
 * @param intradayDensityAtAlert  the intraday density at the alert, or {@code null} on a miss
 * @param dailyL                  the daily level gate {@code L} (present whenever the timescale was scored)
 * @param intradayL               the intraday level gate {@code L}
 * @param leadHours               wall-clock hours the intraday alert led the daily, or {@code null} if not both fired
 * @param dailyMiss               whether the daily detector missed
 * @param intradayMiss            whether the intraday detector missed
 * @param skipReason              the reason the whole event was skipped, or {@code null} if scored
 */
public record LeadTableRow(
        String event,
        int universeSize,
        String calmBlock,
        String eventWindow,
        Instant tDaily,
        Instant tIntraday,
        Double dailyDensityAtAlert,
        Double intradayDensityAtAlert,
        Double dailyL,
        Double intradayL,
        Double leadHours,
        boolean dailyMiss,
        boolean intradayMiss,
        String skipReason) {

    /** Builds a skipped row (universe too small or series too short): both timescales miss. */
    public static LeadTableRow skipped(String event, int universeSize, String reason) {
        return new LeadTableRow(event, universeSize, null, null, null, null, null, null,
                null, null, null, true, true, reason);
    }
}
