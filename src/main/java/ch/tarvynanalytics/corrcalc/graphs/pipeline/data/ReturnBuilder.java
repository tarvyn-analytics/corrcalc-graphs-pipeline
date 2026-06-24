package ch.tarvynanalytics.corrcalc.graphs.pipeline.data;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.MarketSnapshot;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * The streaming counterpart of {@link ReturnPanels}: turns a stream of aligned {@link MarketSnapshot}
 * cross-sections into log-return bars, one snapshot at a time. This is the live {@code ReturnBuilder}
 * the README diagram names — the inbound stage between a {@code MarketDataSource} and the
 * {@code PipelineEngine}.
 *
 * <p><strong>Exactly the batch contract, incrementally.</strong> The return definition is
 * {@code log(close / prevClose)} per symbol ({@link ReturnPanels#logReturn}); the first snapshot of each
 * session (per the {@link SessionPolicy}) has no within-session predecessor and so yields
 * {@link Optional#empty()} — no return crosses a session boundary. It deliberately does <em>not</em>
 * reset any rolling window (that is the engine's concern, and the spike slides the window across
 * sessions): feeding {@code PriceSnapshots.align(...)} through this builder reproduces
 * {@code ReturnPanels.buildIntraday/buildDaily} bar-for-bar (pinned by {@code ReturnBuilderTest}).</p>
 *
 * <p><strong>Single-writer</strong>, like the engine it feeds: one ingest thread calls {@link #accept}.</p>
 */
public final class ReturnBuilder {

    private final String[] symbols;
    private final SessionPolicy policy;
    private double[] prevCloses;
    private Object prevSessionKey;

    /**
     * @param symbols the universe column order (must match every {@link MarketSnapshot}'s {@code closes})
     * @param policy  the session policy selecting where returns are dropped
     * @throws IllegalArgumentException if {@code symbols} is null/empty or {@code policy} is null
     */
    public ReturnBuilder(String[] symbols, SessionPolicy policy) {
        if (symbols == null || symbols.length == 0) {
            throw new IllegalArgumentException("symbols must be non-empty");
        }
        if (policy == null) {
            throw new IllegalArgumentException("session policy must not be null");
        }
        this.symbols = symbols.clone();
        this.policy = policy;
    }

    /**
     * Ingests the next aligned cross-section and returns the log-return bar for the transition from the
     * previous in-session snapshot to this one, or {@link Optional#empty()} for the first snapshot of a
     * session (no within-session predecessor).
     *
     * @param snapshot the next aligned cross-section
     * @return the return bar, or empty when no return crosses into this snapshot
     * @throws IllegalArgumentException if {@code snapshot} is null or its close count differs from the universe size
     */
    public Optional<ReturnBar> accept(MarketSnapshot snapshot) {
        if (snapshot == null) {
            throw new IllegalArgumentException("snapshot must not be null");
        }
        double[] closes = snapshot.closes();
        if (closes.length != symbols.length) {
            throw new IllegalArgumentException("snapshot has [" + closes.length + "] closes, expected ["
                    + symbols.length + "]");
        }
        Object key = policy.sessionKey(snapshot.timestamp());
        if (prevCloses == null || !key.equals(prevSessionKey)) {
            prevCloses = closes;
            prevSessionKey = key;
            return Optional.empty();   // first snapshot of a session: no return crosses into it
        }
        double[] returns = new double[symbols.length];
        for (int s = 0; s < symbols.length; s++) {
            returns[s] = ReturnPanels.logReturn(prevCloses[s], closes[s]);
        }
        prevCloses = closes;
        prevSessionKey = key;
        return Optional.of(new ReturnBar(snapshot.timestamp(), returns));
    }

    /**
     * Counts how many of {@code snapshots} would produce a return bar under {@code policy} (the rest are
     * session-firsts). Lets a caller size the calibration prefix without materializing the returns; uses
     * the same session rule as {@link #accept}.
     *
     * @param snapshots the time-ordered aligned cross-sections
     * @param policy    the session policy
     * @return the number of return bars the stream would yield
     */
    public static int countReturns(List<MarketSnapshot> snapshots, SessionPolicy policy) {
        if (snapshots == null || policy == null) {
            throw new IllegalArgumentException("snapshots and policy must not be null");
        }
        int count = 0;
        Object prevKey = null;
        for (MarketSnapshot snapshot : snapshots) {
            Object key = policy.sessionKey(snapshot.timestamp());
            if (prevKey != null && key.equals(prevKey)) {
                count++;
            }
            prevKey = key;
        }
        return count;
    }

    /**
     * One log-return bar emitted by {@link ReturnBuilder} — the inbound counterpart of a single
     * {@code ReturnPanel} row, ready to feed straight into {@code PipelineEngine.onReturns}.
     *
     * @param asOf    the bar-end timestamp (the snapshot that produced the return)
     * @param returns one log return per universe symbol, in column order
     */
    public record ReturnBar(Instant asOf, double[] returns) {
    }
}
