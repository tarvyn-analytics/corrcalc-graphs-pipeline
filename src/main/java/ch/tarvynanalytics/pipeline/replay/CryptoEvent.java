package ch.tarvynanalytics.pipeline.replay;

import java.time.LocalDate;

/**
 * One crypto regime event with its event window and the leak-free calm block that strictly
 * precedes it (walk-forward calibration discipline). A direct port of one entry of the spike's
 * {@code crypto_universe.EVENTS}; calm blocks are 45 calendar days ending 21 days before the event
 * start (the settled recalibration that de-contaminates the calm baseline from pre-event fusion).
 *
 * @param name      the event identifier (matches the {@code event} column of {@code crypto_lead_table.csv})
 * @param start     the event-window start date (inclusive, UTC)
 * @param end       the event-window end date (inclusive, UTC)
 * @param calmStart the calm-block start date (inclusive, UTC)
 * @param calmEnd   the calm-block end date (inclusive, UTC)
 */
public record CryptoEvent(String name, LocalDate start, LocalDate end,
                          LocalDate calmStart, LocalDate calmEnd) {

    /** The {@code "calm_start .. calm_end"} string as written in the lead table. */
    public String calmBlock() {
        return calmStart + " .. " + calmEnd;
    }

    /** The {@code "start .. end"} event-window string as written in the lead table. */
    public String eventWindow() {
        return start + " .. " + end;
    }
}
