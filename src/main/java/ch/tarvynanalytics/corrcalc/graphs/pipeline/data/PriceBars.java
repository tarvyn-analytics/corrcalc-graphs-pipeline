package ch.tarvynanalytics.corrcalc.graphs.pipeline.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads Binance-style OHLCV CSV bars (the format the crypto spike saved on disk):
 * a {@code timestamp,open,high,low,close,volume} header followed by rows whose timestamp is an
 * ISO-8601 instant (e.g. {@code 2020-03-12T02:17:00Z}) and whose close is column 4. Only the
 * timestamp and close are retained (returns are close-to-close).
 *
 * <p>Hand-rolled parsing keeps the pipeline free of a CSV dependency, matching the family's
 * lean-dependency culture. Bars are returned in file order (the spike's files are time-ordered).</p>
 */
public final class PriceBars {

    private PriceBars() {
    }

    /**
     * Reads all bars from a CSV file, optionally restricted to a closed UTC date range.
     *
     * @param csv        the CSV file path
     * @param fromUtc    earliest UTC date to keep (inclusive), or {@code null} for no lower bound
     * @param toUtc      latest UTC date to keep (inclusive), or {@code null} for no upper bound
     * @return the bars in file order, within the date range
     * @throws UncheckedIOException if the file cannot be read
     * @throws IllegalArgumentException if a data row is malformed
     */
    public static List<Bar> read(Path csv, LocalDate fromUtc, LocalDate toUtc) {
        List<String> lines;
        try {
            lines = Files.readAllLines(csv);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read price CSV [" + csv + "]", e);
        }
        List<Bar> bars = new ArrayList<>(Math.max(0, lines.size() - 1));
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            if (line.isBlank()) {
                continue;
            }
            if (i == 0 && line.startsWith("timestamp")) {
                continue;   // header
            }
            Bar bar = parse(line, csv, i);
            LocalDate date = LocalDate.ofInstant(bar.timestamp(), ZoneOffset.UTC);
            if (fromUtc != null && date.isBefore(fromUtc)) {
                continue;
            }
            if (toUtc != null && date.isAfter(toUtc)) {
                continue;
            }
            bars.add(bar);
        }
        return bars;
    }

    /** Parses one {@code timestamp,open,high,low,close,volume} data row (shared with the streaming cursor). */
    static Bar parse(String line, Path csv, int lineNo) {
        // timestamp,open,high,low,close,volume -- close is field index 4.
        int field = 0;
        int start = 0;
        Instant ts = null;
        double close = Double.NaN;
        for (int p = 0; p <= line.length(); p++) {
            if (p == line.length() || line.charAt(p) == ',') {
                String value = line.substring(start, p);
                if (field == 0) {
                    ts = Instant.parse(value);
                } else if (field == 4) {
                    close = Double.parseDouble(value);
                }
                field++;
                start = p + 1;
            }
        }
        if (ts == null || field < 5 || Double.isNaN(close)) {
            throw new IllegalArgumentException(
                    "malformed price row in [" + csv + "] line " + (lineNo + 1) + ": [" + line + "]");
        }
        return new Bar(ts, close);
    }
}
