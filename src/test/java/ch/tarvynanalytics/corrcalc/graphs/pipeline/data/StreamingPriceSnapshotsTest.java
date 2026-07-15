package ch.tarvynanalytics.corrcalc.graphs.pipeline.data;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.MarketSnapshot;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The streaming aligner is proven against the in-memory path it replaces —
 * {@code PriceBars.read + PriceSnapshots.align} is the independent oracle, element-wise, on seeded
 * data with per-symbol gaps, duplicates and date filters.
 */
class StreamingPriceSnapshotsTest {

    @TempDir
    Path dir;

    @Test
    void align_SeededGappySeries_MatchesInMemoryOracleElementWise() throws IOException {
        String[] symbols = {"AAA", "BBB", "CCC"};
        Random rnd = new Random(42);
        // 200 minutes; each symbol keeps ~85% of them, so the intersection is gappy and irregular
        for (String symbol : symbols) {
            StringBuilder csv = new StringBuilder("timestamp,open,high,low,close,volume\n");
            for (int m = 0; m < 200; m++) {
                if (rnd.nextDouble() < 0.85) {
                    csv.append(row(minute(m), 100 + rnd.nextDouble()));
                }
            }
            Files.writeString(file(symbol), csv.toString());
        }

        assertEquals(oracle(symbols, null, null), streamed(symbols, null, null));
    }

    @Test
    void align_DateRangeFilter_MatchesInMemoryOracle() throws IOException {
        String[] symbols = {"AAA", "BBB"};
        for (String symbol : symbols) {
            StringBuilder csv = new StringBuilder("timestamp,open,high,low,close,volume\n");
            for (int d = 0; d < 9; d++) {
                csv.append(row(Instant.parse("2021-05-01T12:00:00Z").plusSeconds(d * 86400L), 100 + d));
            }
            Files.writeString(file(symbol), csv.toString());
        }
        LocalDate from = LocalDate.parse("2021-05-03");
        LocalDate to = LocalDate.parse("2021-05-07");

        List<MarketSnapshot> expected = oracle(symbols, from, to);
        assertEquals(expected, streamed(symbols, from, to));
        assertEquals(5, expected.size(), "the closed range keeps exactly May 3rd..7th");
    }

    @Test
    void align_DuplicateTimestampRows_LastCloseWins_MatchingTheOracle() throws IOException {
        String[] symbols = {"AAA", "BBB"};
        Files.writeString(file("AAA"), "timestamp,open,high,low,close,volume\n"
                + row(minute(0), 100.0)
                + row(minute(0), 101.5)   // duplicate: this close must win
                + row(minute(1), 102.0));
        Files.writeString(file("BBB"), "timestamp,open,high,low,close,volume\n"
                + row(minute(0), 50.0)
                + row(minute(1), 51.0));

        List<MarketSnapshot> streamed = streamed(symbols, null, null);
        assertEquals(oracle(symbols, null, null), streamed);
        assertEquals(101.5, streamed.get(0).closes()[0], 0.0);
    }

    @Test
    void align_OutOfOrderRows_FailLoudly() throws IOException {
        Files.writeString(file("AAA"), "timestamp,open,high,low,close,volume\n"
                + row(minute(5), 100.0)
                + row(minute(3), 99.0));
        Files.writeString(file("BBB"), "timestamp,open,high,low,close,volume\n"
                + row(minute(3), 50.0)
                + row(minute(5), 51.0));

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> streamed(new String[]{"AAA", "BBB"}, null, null));
        assertTrue(e.getMessage().contains("out of time order"), e.getMessage());
        assertTrue(e.getMessage().contains(minute(3).toString()), e.getMessage());
    }

    @Test
    void align_EmptyIntersection_YieldsNoSnapshots() throws IOException {
        Files.writeString(file("AAA"), "timestamp,open,high,low,close,volume\n" + row(minute(0), 100.0));
        Files.writeString(file("BBB"), "timestamp,open,high,low,close,volume\n" + row(minute(1), 50.0));

        assertEquals(List.of(), streamed(new String[]{"AAA", "BBB"}, null, null));
    }

    @Test
    void align_EmptySymbols_YieldsNoSnapshots() {
        assertEquals(List.of(), streamed(new String[]{}, null, null));
    }

    @Test
    void align_MissingFile_ThrowsTheSameUncheckedIOAsTheInMemoryPath() {
        assertThrows(UncheckedIOException.class, () -> streamed(new String[]{"NOPE"}, null, null));
    }

    @Test
    void align_IsReIterable_SecondPassEqualsFirst() throws IOException {
        String[] symbols = {"AAA", "BBB"};
        for (String symbol : symbols) {
            Files.writeString(file(symbol), "timestamp,open,high,low,close,volume\n"
                    + row(minute(0), 100.0) + row(minute(1), 101.0) + row(minute(2), 102.0));
        }
        Iterable<MarketSnapshot> aligned = StreamingPriceSnapshots.align(dir, "test", "1m", null, null, symbols);

        List<MarketSnapshot> first = collect(aligned);
        List<MarketSnapshot> second = collect(aligned);
        assertEquals(3, first.size());
        assertEquals(first, second);
    }

    private Path file(String symbol) {
        return dir.resolve(symbol + "_1m_test.csv");
    }

    private static Instant minute(int m) {
        return Instant.parse("2021-05-01T00:00:00Z").plusSeconds(60L * m);
    }

    private static String row(Instant ts, double close) {
        return ts + ",1,2,0.5," + close + ",1000\n";
    }

    private List<MarketSnapshot> oracle(String[] symbols, LocalDate from, LocalDate to) {
        Map<String, List<Bar>> prices = new LinkedHashMap<>();
        for (String symbol : symbols) {
            prices.put(symbol, PriceBars.read(file(symbol), from, to));
        }
        return PriceSnapshots.align(prices, symbols);
    }

    private List<MarketSnapshot> streamed(String[] symbols, LocalDate from, LocalDate to) {
        return collect(StreamingPriceSnapshots.align(dir, "test", "1m", from, to, symbols));
    }

    private static List<MarketSnapshot> collect(Iterable<MarketSnapshot> aligned) {
        List<MarketSnapshot> out = new ArrayList<>();
        aligned.forEach(out::add);
        return out;
    }
}
