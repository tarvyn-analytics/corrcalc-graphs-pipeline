package ch.tarvynanalytics.corrcalc.graphs.pipeline.replay;

import java.nio.file.Path;
import java.time.LocalDate;

/**
 * The tuning of one paced replay run — every knob the {@link PacedReplay} engine reads, parsed from
 * the CLI. The data location is passed separately (it is the CLI's positional argument); this record
 * holds only the run parameters. Thresholds/windows are <em>not</em> here: those are market
 * configuration resolved from {@link #market()} + {@link #timescale()} (a new market is wired, not
 * coded — pipeline invariant 4).
 *
 * @param event          the event id selecting the {@code <SYMBOL>_<freq>_<event>.csv} bar files
 * @param market         the market label (config selector + the published signal's {@code market})
 * @param timescale      which timescale stream: {@code "intraday"} or {@code "daily"}
 * @param speed          the simulated-time : real-time ratio; higher replays faster ({@code > 0})
 * @param maxStepMs      cap on the per-bar sleep so session gaps don't stall the view ({@code >= 0})
 * @param calmBars       window-points used to calibrate the detector, or {@code null} to auto-pick (~40%)
 * @param limit          stop after this many detection points, or {@code null} for the whole series
 * @param heartbeatEvery log a calm heartbeat every N detection points ({@code >= 1})
 * @param universePath   explicit universe CSV, or {@code null} to default to {@code <dir>/<event>_universe.csv}
 * @param from           earliest UTC bar date to keep (inclusive), or {@code null} for no lower bound
 * @param to             latest UTC bar date to keep (inclusive), or {@code null} for no upper bound
 */
public record ReplayOptions(
        String event,
        String market,
        String timescale,
        double speed,
        long maxStepMs,
        Integer calmBars,
        Integer limit,
        int heartbeatEvery,
        Path universePath,
        LocalDate from,
        LocalDate to) {

    /** Validates the knobs, throwing {@link IllegalArgumentException} with the offending value bracketed. */
    public ReplayOptions {
        if (event == null || event.isBlank()) {
            throw new IllegalArgumentException("event must be non-blank [" + event + "]");
        }
        if (market == null || market.isBlank()) {
            throw new IllegalArgumentException("market must be non-blank [" + market + "]");
        }
        if (!"intraday".equals(timescale) && !"daily".equals(timescale)) {
            throw new IllegalArgumentException("timescale must be intraday or daily [" + timescale + "]");
        }
        if (!(speed > 0.0) || !Double.isFinite(speed)) {
            throw new IllegalArgumentException("speed must be a finite positive multiplier [" + speed + "]");
        }
        if (maxStepMs < 0) {
            throw new IllegalArgumentException("maxStepMs must be >= 0 [" + maxStepMs + "]");
        }
        if (calmBars != null && calmBars < 2) {
            throw new IllegalArgumentException("calmBars must be >= 2 when set [" + calmBars + "]");
        }
        if (limit != null && limit < 1) {
            throw new IllegalArgumentException("limit must be >= 1 when set [" + limit + "]");
        }
        if (heartbeatEvery < 1) {
            throw new IllegalArgumentException("heartbeatEvery must be >= 1 [" + heartbeatEvery + "]");
        }
    }
}
