package ch.tarvynanalytics.corrcalc.graphs.pipeline.backtest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Shared loader + asserter for the committed n=8 regression fixtures (the golden
 * {@code crypto_lead_table.csv} and the per-event universe lists), used by both the
 * fixture-driven {@link CryptoLeadTableRegressionTest} and the opt-in raw-data
 * {@link CryptoFullBacktestDriverTest} so both check a produced {@link LeadTableRow} against the
 * golden row the same way.
 */
final class LeadTableFixtures {

    private LeadTableFixtures() {
    }

    /** Loads the golden lead table keyed by event name. */
    static Map<String, GoldenRow> golden() {
        Map<String, GoldenRow> map = new HashMap<>();
        List<String> lines = resourceLines("/fixtures/crypto_lead_table.csv");
        Header header = new Header(lines.get(0));
        for (int i = 1; i < lines.size(); i++) {
            if (lines.get(i).isBlank()) {
                continue;
            }
            String[] f = lines.get(i).split(",", -1);
            map.put(f[header.idx("event")], new GoldenRow(
                    Integer.parseInt(f[header.idx("universe_size")]),
                    parseTs(f[header.idx("t_daily")]),
                    parseTs(f[header.idx("t_intraday")]),
                    parseNullableD(f[header.idx("daily_density_at_alert")]),
                    parseNullableD(f[header.idx("intraday_density_at_alert")]),
                    parseNullableD(f[header.idx("daily_L")]),
                    parseNullableD(f[header.idx("intraday_L")]),
                    parseNullableD(f[header.idx("lead_hours")]),
                    Boolean.parseBoolean(f[header.idx("daily_miss")]),
                    Boolean.parseBoolean(f[header.idx("intraday_miss")])));
        }
        return map;
    }

    /** Loads the committed per-event universe symbol list (column order). */
    static List<String> universe(String event) {
        List<String> lines = resourceLines("/fixtures/" + event + "_universe.csv");
        List<String> symbols = new ArrayList<>();
        for (int i = 1; i < lines.size(); i++) {   // skip header
            if (!lines.get(i).isBlank()) {
                symbols.add(lines.get(i).trim());
            }
        }
        return symbols;
    }

    /** Asserts a produced row reproduces the golden row column-for-column (timestamps to the second). */
    static void assertMatches(GoldenRow expected, LeadTableRow actual, String event) {
        assertEquals(expected.universeSize(), actual.universeSize(), event + " universe size");
        assertEquals(expected.dailyMiss(), actual.dailyMiss(), event + " daily miss");
        assertEquals(expected.intradayMiss(), actual.intradayMiss(), event + " intraday miss");
        assertTimestamp(expected.tDaily(), actual.tDaily(), event + " t_daily");
        assertTimestamp(expected.tIntraday(), actual.tIntraday(), event + " t_intraday");
        assertNullableDouble(expected.dailyDensity(), actual.dailyDensityAtAlert(), 1e-12, event + " daily density@alert");
        assertNullableDouble(expected.intradayDensity(), actual.intradayDensityAtAlert(), 1e-12, event + " intraday density@alert");
        assertNullableDouble(expected.dailyL(), actual.dailyL(), 1e-12, event + " daily L");
        assertNullableDouble(expected.intradayL(), actual.intradayL(), 1e-12, event + " intraday L");
        assertNullableDouble(expected.leadHours(), actual.leadHours(), 1e-9, event + " lead hours");
    }

    private static void assertTimestamp(Instant expected, Instant actual, String message) {
        if (expected == null) {
            assertNull(actual, message);
        } else {
            assertEquals(expected.getEpochSecond(), actual.getEpochSecond(), message);
        }
    }

    private static void assertNullableDouble(Double expected, Double actual, double tol, String message) {
        if (expected == null) {
            assertNull(actual, message);
        } else {
            assertEquals(expected, actual, tol, message);
        }
    }

    private static Double parseNullableD(String s) {
        return s == null || s.isBlank() ? null : Double.parseDouble(s.trim());
    }

    private static Instant parseTs(String s) {
        if (s == null || s.isBlank()) {
            return null;
        }
        return LocalDateTime.parse(s.trim().replace(' ', 'T')).toInstant(ZoneOffset.UTC);
    }

    static List<String> resourceLines(String path) {
        try (InputStream in = LeadTableFixtures.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("missing test fixture [" + path + "] — run export_pipeline_fixture.py");
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                List<String> lines = new ArrayList<>();
                String line;
                while ((line = reader.readLine()) != null) {
                    lines.add(line);
                }
                return lines;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read fixture [" + path + "]", e);
        }
    }

    /** The golden lead-table row for one event. */
    record GoldenRow(int universeSize, Instant tDaily, Instant tIntraday,
                     Double dailyDensity, Double intradayDensity, Double dailyL, Double intradayL,
                     Double leadHours, boolean dailyMiss, boolean intradayMiss) {
    }

    /** A CSV header index lookup. */
    static final class Header {
        private final Map<String, Integer> index = new HashMap<>();

        Header(String headerLine) {
            String[] cols = headerLine.split(",", -1);
            for (int i = 0; i < cols.length; i++) {
                index.put(cols[i].trim(), i);
            }
        }

        int idx(String column) {
            Integer i = index.get(column);
            if (i == null) {
                throw new IllegalStateException("fixture missing column [" + column + "]");
            }
            return i;
        }
    }
}
