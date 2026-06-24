package ch.tarvynanalytics.corrcalc.graphs.pipeline.data;

import java.time.Instant;
import java.util.List;

/**
 * An aligned panel of log returns over one symbol set at one sampling frequency — the input the
 * S1 rolling-correlation engine consumes, one bar (row) at a time.
 *
 * <p>Row {@code t} is the return bar ending at {@code timestamps[t]}; {@code returns[t][s]} is the
 * log return of {@code symbols[s]} for that bar. {@code sessionId[t]} is the bar's session bucket
 * (a UTC calendar day for crypto intraday); a return never crosses a session boundary, so the
 * first bar of each session produces no return and never appears here. A daily panel has every bar
 * in session {@code 0} (consecutive daily closes never gap a session).</p>
 *
 * @param timestamps the bar-end timestamp of each row, ascending
 * @param symbols    the column order (variable set), stable across the panel
 * @param returns    {@code returns[t][s]} = the log return of {@code symbols[s]} at bar {@code t}
 * @param sessionId  the session bucket of each row (parallel to {@code timestamps})
 */
public record ReturnPanel(List<Instant> timestamps, String[] symbols, double[][] returns, int[] sessionId) {

    /** The number of return bars (rows) in the panel. */
    public int barCount() {
        return timestamps.size();
    }

    /** The number of symbols (columns). */
    public int symbolCount() {
        return symbols.length;
    }
}
