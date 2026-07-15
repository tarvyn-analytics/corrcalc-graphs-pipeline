package ch.tarvynanalytics.corrcalc.graphs.pipeline.data;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReturnPanelsTest {

    private static final double EPS = 1e-12;

    @Test
    void buildIntraday_TwoUtcDays_LosesOneReturnPerDayBoundary() {
        // Mirrors replay_crypto_panel.__main__: base at 23:58, 5 one-minute bars per symbol spanning
        // two UTC days (23:58, 23:59 | 00:00, 00:01, 00:02) -> exactly two sessions, and the first
        // bar of each UTC day yields no return.
        Instant base = Instant.parse("2024-01-01T23:58:00Z");
        String[] symbols = {"AUSDT", "BUSDT", "CUSDT"};
        Map<String, List<Bar>> prices = new LinkedHashMap<>();
        for (int idx = 0; idx < symbols.length; idx++) {
            List<Bar> bars = new java.util.ArrayList<>();
            for (int i = 0; i < 5; i++) {
                bars.add(new Bar(base.plus(i, ChronoUnit.MINUTES), 100.0 + i + 10.0 * idx));
            }
            prices.put(symbols[idx], bars);
        }

        ReturnPanel panel = ReturnPanels.buildIntraday(prices, symbols);

        // 5 common timestamps across 2 UTC days -> 5 - 2 = 3 return bars; 2 distinct sessions.
        assertEquals(3, panel.barCount());
        assertEquals(3, panel.symbolCount());
        assertEquals(2, (int) java.util.Arrays.stream(panel.sessionId()).distinct().count());
        assertArrayEquals(new int[]{0, 1, 1}, panel.sessionId());
        // first return bar is 23:59 (within day 0); the 00:00 boundary bar is dropped.
        assertEquals(Instant.parse("2024-01-01T23:59:00Z"), panel.timestamps().get(0));
        assertEquals(Instant.parse("2024-01-02T00:01:00Z"), panel.timestamps().get(1));
        // AUSDT 23:58->23:59 is log(101/100).
        assertEquals(Math.log(101.0 / 100.0), panel.returns()[0][0], EPS);
    }

    @Test
    void buildIntraday_IntersectsCommonTimestampsAcrossSymbols() {
        String[] symbols = {"AUSDT", "BUSDT"};
        Instant t0 = Instant.parse("2024-03-01T12:00:00Z");
        Map<String, List<Bar>> prices = new LinkedHashMap<>();
        prices.put("AUSDT", List.of(
                new Bar(t0, 10.0), new Bar(t0.plus(1, ChronoUnit.MINUTES), 11.0),
                new Bar(t0.plus(2, ChronoUnit.MINUTES), 12.0)));
        // BUSDT is missing the t0+1 bar -> that timestamp is not common and produces no bar.
        prices.put("BUSDT", List.of(
                new Bar(t0, 20.0), new Bar(t0.plus(2, ChronoUnit.MINUTES), 24.0)));

        ReturnPanel panel = ReturnPanels.buildIntraday(prices, symbols);

        // common timestamps {t0, t0+2}; same UTC day -> one session, one return bar (t0 -> t0+2).
        assertEquals(1, panel.barCount());
        assertEquals(Instant.parse("2024-03-01T12:02:00Z"), panel.timestamps().get(0));
        assertEquals(Math.log(12.0 / 10.0), panel.returns()[0][0], EPS);
        assertEquals(Math.log(24.0 / 20.0), panel.returns()[0][1], EPS);
    }

    @Test
    void buildIntraday_EmptySymbolSet_YieldsEmptyPanel() {
        ReturnPanel panel = ReturnPanels.buildIntraday(Map.of(), new String[0]);
        assertEquals(0, panel.barCount());
        assertEquals(0, panel.symbolCount());
    }

    @Test
    void buildDaily_ConsecutiveCloses_NoSessionReset() {
        String[] symbols = {"AUSDT"};
        Map<String, List<Bar>> prices = new LinkedHashMap<>();
        prices.put("AUSDT", List.of(
                new Bar(Instant.parse("2024-01-01T00:00:00Z"), 100.0),
                new Bar(Instant.parse("2024-01-02T00:00:00Z"), 110.0),
                new Bar(Instant.parse("2024-01-03T00:00:00Z"), 99.0)));

        ReturnPanel panel = ReturnPanels.buildDaily(prices, symbols);

        assertEquals(2, panel.barCount());
        assertTrue(java.util.Arrays.stream(panel.sessionId()).allMatch(s -> s == 0));
        assertEquals(Math.log(110.0 / 100.0), panel.returns()[0][0], EPS);
        assertEquals(Math.log(99.0 / 110.0), panel.returns()[1][0], EPS);
    }
}
