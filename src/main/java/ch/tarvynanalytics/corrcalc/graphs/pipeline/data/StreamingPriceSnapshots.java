package ch.tarvynanalytics.corrcalc.graphs.pipeline.data;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.MarketSnapshot;

import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * The streaming counterpart of {@code loadPrices + }{@link PriceSnapshots#align}: aligns per-symbol
 * CSV bars into the same common-timestamp {@link MarketSnapshot} intersection, but as a lazy k-way
 * merge over {@link BarCsvCursor}s — constant memory in the series length, so a multi-year minute
 * tape replays without holding a single symbol's bars (let alone all of them) in memory.
 *
 * <p>Semantics are pinned to the in-memory path (the equivalence oracle in
 * {@code StreamingPriceSnapshotsTest}): only timestamps present for <em>every</em> symbol become a
 * snapshot, ascending by time; same-timestamp duplicate rows collapse to the last one; the closed
 * UTC date-range filter matches {@link PriceBars#read}. The one deliberate difference: bars out of
 * time order fail loudly instead of being silently re-sorted (stored tapes are time-ordered by
 * contract — see {@link BarCsvCursor}).</p>
 *
 * <p>The returned {@link Iterable} opens fresh cursors per {@link Iterable#iterator() iterator()}
 * call, so it can be consumed more than once (a counting pre-pass, then the replay). Each iterator
 * closes its cursors at end-of-stream; abandoning one early leaks the readers until GC, so an owner
 * that may stop early should close it — the iterator is {@link AutoCloseable}, and
 * {@code IterableMarketDataSource.close()} forwards to it.</p>
 */
public final class StreamingPriceSnapshots {

    private StreamingPriceSnapshots() {
    }

    /**
     * Lazily aligns {@code <symbol>_<freq>_<event>.csv} bars under {@code dataDir} into the
     * common-timestamp snapshot stream.
     *
     * @param dataDir directory of {@code <SYMBOL>_<freq>_<event>.csv} bar files
     * @param event   the event id in the file names
     * @param freq    the frequency token in the file names ({@code 1m}/{@code 1d})
     * @param fromUtc earliest UTC date to keep (inclusive), or {@code null} for no lower bound
     * @param toUtc   latest UTC date to keep (inclusive), or {@code null} for no upper bound
     * @param symbols the ordered symbol set (column order of every snapshot)
     * @return a re-iterable, lazily-aligned snapshot stream (empty when {@code symbols} is empty)
     */
    public static Iterable<MarketSnapshot> align(Path dataDir, String event, String freq,
                                                 LocalDate fromUtc, LocalDate toUtc, String[] symbols) {
        if (dataDir == null || event == null || freq == null || symbols == null) {
            throw new IllegalArgumentException("dataDir, event, freq and symbols must be non-null");
        }
        String[] universe = symbols.clone();
        return () -> new AligningIterator(dataDir, event, freq, fromUtc, toUtc, universe);
    }

    /** The k-way sorted-intersection merge; closes its cursors at end-of-stream or on {@link #close()}. */
    private static final class AligningIterator implements Iterator<MarketSnapshot>, AutoCloseable {

        private final BarCsvCursor[] cursors;
        private MarketSnapshot next;
        private boolean closed;

        AligningIterator(Path dataDir, String event, String freq, LocalDate fromUtc, LocalDate toUtc,
                         String[] symbols) {
            this.cursors = new BarCsvCursor[symbols.length];
            try {
                for (int s = 0; s < symbols.length; s++) {
                    cursors[s] = new BarCsvCursor(
                            dataDir.resolve(symbols[s] + "_" + freq + "_" + event + ".csv"), fromUtc, toUtc);
                }
            } catch (RuntimeException e) {
                close();
                throw e;
            }
            if (symbols.length == 0 || !advanceAll()) {
                close();
            } else {
                this.next = alignOnce();
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public MarketSnapshot next() {
            if (next == null) {
                throw new NoSuchElementException("snapshot stream is exhausted");
            }
            MarketSnapshot out = next;
            next = advanceAll() ? alignOnce() : null;
            if (next == null) {
                close();
            }
            return out;
        }

        /** Steps every cursor one bar forward; {@code false} when any file is exhausted. */
        private boolean advanceAll() {
            for (BarCsvCursor cursor : cursors) {
                if (!cursor.advance()) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Advances lagging cursors until every cursor sits on the same timestamp — the next member of
         * the intersection — or {@code null} when a file runs out first.
         */
        private MarketSnapshot alignOnce() {
            while (true) {
                Instant high = cursors[0].bar().timestamp();
                boolean aligned = true;
                for (BarCsvCursor cursor : cursors) {
                    Instant ts = cursor.bar().timestamp();
                    if (ts.isAfter(high)) {
                        high = ts;
                        aligned = false;
                    } else if (ts.isBefore(high)) {
                        aligned = false;
                    }
                }
                if (aligned) {
                    double[] closes = new double[cursors.length];
                    for (int s = 0; s < cursors.length; s++) {
                        closes[s] = cursors[s].bar().close();
                    }
                    return new MarketSnapshot(high, closes);
                }
                for (BarCsvCursor cursor : cursors) {
                    while (cursor.bar().timestamp().isBefore(high)) {
                        if (!cursor.advance()) {
                            close();
                            return null;
                        }
                    }
                }
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            next = null;
            for (BarCsvCursor cursor : cursors) {
                if (cursor != null) {
                    cursor.close();
                }
            }
        }
    }
}
