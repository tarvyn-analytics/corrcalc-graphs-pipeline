package ch.tarvynanalytics.corrcalc.graphs.pipeline.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads a universe symbol list — a single-column CSV with a {@code symbol} header followed by one
 * symbol per line, the exact shape of the committed {@code <event>_universe.csv} fixtures. The order
 * is the column order of every correlation matrix in the run, so it is preserved as read.
 *
 * <p>Hand-rolled, matching the family's lean-dependency culture and {@link PriceBars}' style
 * (filesystem read, {@link UncheckedIOException} on IO failure, validation messages carry the
 * offending path/value in brackets).</p>
 */
public final class UniverseCsv {

    private UniverseCsv() {
    }

    /**
     * Reads the ordered symbol list from a universe CSV.
     *
     * @param csv the universe CSV path
     * @return the symbols in file order (an immutable list)
     * @throws UncheckedIOException     if the file cannot be read
     * @throws IllegalArgumentException if the file holds no symbols
     */
    public static List<String> read(Path csv) {
        List<String> lines;
        try {
            lines = Files.readAllLines(csv);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read universe CSV [" + csv + "]", e);
        }
        List<String> symbols = new ArrayList<>();
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.isEmpty()) {
                continue;
            }
            if (i == 0 && line.equalsIgnoreCase("symbol")) {
                continue;   // header
            }
            symbols.add(line);
        }
        if (symbols.isEmpty()) {
            throw new IllegalArgumentException("universe CSV [" + csv + "] holds no symbols");
        }
        return List.copyOf(symbols);
    }
}
