package ch.tarvynanalytics.corrcalc.graphs.pipeline.data;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Decides session membership for the streaming {@link ReturnBuilder}: two consecutive snapshots are in
 * the same session iff they share a {@link #sessionKey(Instant)}. The first snapshot of a session has no
 * within-session predecessor, so it yields no return (the gap into it is never correlated) — exactly the
 * rule {@code ReturnPanels} applies in batch.
 *
 * <p>This is only about <em>which returns exist</em>, not about resetting the rolling-correlation window:
 * the spike (and {@code SeriesBuilder}) slide the window over the contiguous return rows <em>without</em>
 * a window reset between sessions, so {@code ReturnBuilder} mirrors that — it drops the cross-session
 * return and nothing more.</p>
 */
public enum SessionPolicy {

    /** Intraday: a new session begins on each UTC calendar day (crypto's 24/7 day boundary). */
    INTRADAY_UTC_DAY {
        @Override
        Object sessionKey(Instant timestamp) {
            return LocalDate.ofInstant(timestamp, ZoneOffset.UTC);
        }
    },

    /** Daily: a single session for the whole stream (consecutive daily closes never gap a session). */
    DAILY_SINGLE {
        @Override
        Object sessionKey(Instant timestamp) {
            return SINGLE_SESSION;
        }
    };

    private static final Object SINGLE_SESSION = new Object();

    /** The session bucket of {@code timestamp}; equal keys for consecutive snapshots mean same session. */
    abstract Object sessionKey(Instant timestamp);
}
