package ch.tarvynanalytics.corrcalc.graphs.pipeline.data;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * A forward-only cursor over one {@link PriceBars}-format CSV: parses rows lazily (one buffered line at
 * a time, never the whole file), applies the same closed UTC date-range filter, collapses same-timestamp
 * duplicates to the <em>last</em> row (exactly what {@link PriceSnapshots#align}'s map overwrite did),
 * and yields a strictly time-ascending bar stream. A row whose timestamp goes <em>backwards</em> is
 * rejected loudly — the in-memory path silently re-sorted such input, but every stored tape is
 * time-ordered by contract ({@link PriceBars}), so disorder is data corruption, not a case to absorb.
 */
final class BarCsvCursor implements AutoCloseable {

    private final Path csv;
    private final BufferedReader reader;
    private final LocalDate fromUtc;
    private final LocalDate toUtc;
    private int lineNo;
    private Bar pending;   // the next in-range row, read ahead to collapse duplicates
    private Bar current;

    BarCsvCursor(Path csv, LocalDate fromUtc, LocalDate toUtc) {
        this.csv = csv;
        this.fromUtc = fromUtc;
        this.toUtc = toUtc;
        try {
            this.reader = Files.newBufferedReader(csv);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read price CSV [" + csv + "]", e);
        }
        this.pending = readNextInRange();
    }

    /**
     * Advances to the next bar, collapsing same-timestamp duplicates to the last one.
     *
     * @return {@code true} if a bar is available via {@link #bar()}, {@code false} at end-of-file
     */
    boolean advance() {
        if (pending == null) {
            current = null;
            return false;
        }
        Bar next = pending;
        Bar lookahead = readNextInRange();
        while (lookahead != null && lookahead.timestamp().equals(next.timestamp())) {
            next = lookahead;   // duplicate timestamp: the last row wins
            lookahead = readNextInRange();
        }
        if (lookahead != null && lookahead.timestamp().isBefore(next.timestamp())) {
            throw new IllegalArgumentException("bars out of time order in [" + csv + "]: ["
                    + lookahead.timestamp() + "] after [" + next.timestamp() + "]");
        }
        current = next;
        pending = lookahead;
        return true;
    }

    /** The bar the last successful {@link #advance()} landed on. */
    Bar bar() {
        return current;
    }

    private Bar readNextInRange() {
        String line;
        while ((line = readLine()) != null) {
            lineNo++;
            if (line.isBlank() || (lineNo == 1 && line.startsWith("timestamp"))) {
                continue;
            }
            Bar bar = PriceBars.parse(line, csv, lineNo - 1);
            LocalDate date = LocalDate.ofInstant(bar.timestamp(), ZoneOffset.UTC);
            if (fromUtc != null && date.isBefore(fromUtc)) {
                continue;
            }
            if (toUtc != null && date.isAfter(toUtc)) {
                continue;
            }
            return bar;
        }
        return null;
    }

    private String readLine() {
        try {
            return reader.readLine();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read price CSV [" + csv + "]", e);
        }
    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot close price CSV [" + csv + "]", e);
        }
    }
}
