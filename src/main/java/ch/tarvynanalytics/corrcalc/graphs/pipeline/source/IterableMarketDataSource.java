package ch.tarvynanalytics.corrcalc.graphs.pipeline.source;

import java.util.Iterator;
import java.util.Optional;

/**
 * An in-memory {@link MarketDataSource} over a fixed, time-ordered sequence of {@link MarketSnapshot}s —
 * the in-process implementation behind the replay driver and the tests. It never blocks: {@link #poll()}
 * walks the supplied snapshots and reports end-of-stream when they are exhausted. A network connector
 * (S2) is the blocking, live counterpart of this same SPI.
 */
public final class IterableMarketDataSource implements MarketDataSource {

    private final String[] universe;
    private final Iterator<MarketSnapshot> snapshots;

    /**
     * @param universe  the symbol column order this source streams
     * @param snapshots the time-ordered aligned cross-sections to replay
     * @throws IllegalArgumentException if either argument is {@code null} or {@code universe} is empty
     */
    public IterableMarketDataSource(String[] universe, Iterable<MarketSnapshot> snapshots) {
        if (universe == null || universe.length == 0) {
            throw new IllegalArgumentException("universe must be non-empty");
        }
        if (snapshots == null) {
            throw new IllegalArgumentException("snapshots must not be null");
        }
        this.universe = universe.clone();
        this.snapshots = snapshots.iterator();
    }

    @Override
    public String[] universe() {
        return universe.clone();
    }

    @Override
    public Optional<MarketSnapshot> poll() {
        return snapshots.hasNext() ? Optional.of(snapshots.next()) : Optional.empty();
    }
}
