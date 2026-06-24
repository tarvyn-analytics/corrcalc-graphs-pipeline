package ch.tarvynanalytics.corrcalc.graphs.pipeline.data;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.MarketSnapshot;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReturnBuilderTest {

    private static final double EPS = 1e-15;

    // The oracle: streaming align -> ReturnBuilder must reproduce the batch ReturnPanels bar-for-bar.

    @Test
    void accept_AlignedIntradaySnapshots_MatchBuildIntradayBarForBar() {
        String[] symbols = {"AUSDT", "BUSDT", "CUSDT"};
        // ~3 UTC days of 1-minute bars (start near a day boundary so several boundaries fall inside).
        Map<String, List<Bar>> prices = intradayPrices(symbols, "2024-01-01T23:55:00Z", 3500, 17L);

        ReturnPanel panel = ReturnPanels.buildIntraday(prices, symbols);
        List<ReturnBuilder.ReturnBar> streamed = stream(prices, symbols, SessionPolicy.INTRADAY_UTC_DAY);

        assertSameSeries(panel, streamed);
    }

    @Test
    void accept_AlignedDailySnapshots_MatchBuildDailyBarForBar() {
        String[] symbols = {"AUSDT", "BUSDT"};
        Map<String, List<Bar>> prices = dailyPrices(symbols, 40, 5L);

        ReturnPanel panel = ReturnPanels.buildDaily(prices, symbols);
        List<ReturnBuilder.ReturnBar> streamed = stream(prices, symbols, SessionPolicy.DAILY_SINGLE);

        assertSameSeries(panel, streamed);
    }

    @Test
    void accept_FirstSnapshotOfEachSession_YieldsEmpty() {
        String[] symbols = {"AUSDT"};
        ReturnBuilder builder = new ReturnBuilder(symbols, SessionPolicy.INTRADAY_UTC_DAY);

        assertTrue(builder.accept(snap("2024-01-01T23:59:00Z", 100.0)).isEmpty(), "first ever bar: no return");
        assertTrue(builder.accept(snap("2024-01-02T00:00:00Z", 101.0)).isEmpty(), "first bar of new UTC day");
        Optional<ReturnBuilder.ReturnBar> within = builder.accept(snap("2024-01-02T00:01:00Z", 102.0));
        assertTrue(within.isPresent(), "second bar within a day yields a return");
        assertEquals(Math.log(102.0 / 101.0), within.get().returns()[0], EPS);
    }

    @Test
    void countReturns_EqualsTheNumberOfEmittedBars() {
        String[] symbols = {"AUSDT", "BUSDT"};
        Map<String, List<Bar>> prices = intradayPrices(symbols, "2024-01-01T23:50:00Z", 600, 3L);
        List<MarketSnapshot> snaps = PriceSnapshots.align(prices, symbols);

        int counted = ReturnBuilder.countReturns(snaps, SessionPolicy.INTRADAY_UTC_DAY);
        assertEquals(stream(prices, symbols, SessionPolicy.INTRADAY_UTC_DAY).size(), counted);
    }

    @Test
    void accept_WrongCloseCount_Throws() {
        ReturnBuilder builder = new ReturnBuilder(new String[]{"A", "B"}, SessionPolicy.DAILY_SINGLE);
        assertThrows(IllegalArgumentException.class,
                () -> builder.accept(new MarketSnapshot(Instant.EPOCH, new double[]{1.0})));
    }

    @Test
    void constructor_RejectsNullsAndEmpty() {
        assertThrows(IllegalArgumentException.class, () -> new ReturnBuilder(new String[0], SessionPolicy.DAILY_SINGLE));
        assertThrows(IllegalArgumentException.class, () -> new ReturnBuilder(new String[]{"A"}, null));
        assertThrows(IllegalArgumentException.class,
                () -> ReturnBuilder.countReturns(null, SessionPolicy.DAILY_SINGLE));
    }

    private static void assertSameSeries(ReturnPanel panel, List<ReturnBuilder.ReturnBar> streamed) {
        assertEquals(panel.barCount(), streamed.size(), "same number of return bars");
        assertTrue(panel.barCount() > 5, "fixture should produce a non-trivial series");
        for (int t = 0; t < panel.barCount(); t++) {
            assertEquals(panel.timestamps().get(t), streamed.get(t).asOf(), "bar " + t + " timestamp");
            for (int s = 0; s < panel.symbolCount(); s++) {
                assertEquals(panel.returns()[t][s], streamed.get(t).returns()[s], EPS, "bar " + t + " sym " + s);
            }
        }
    }

    private static List<ReturnBuilder.ReturnBar> stream(Map<String, List<Bar>> prices, String[] symbols,
                                                        SessionPolicy policy) {
        ReturnBuilder builder = new ReturnBuilder(symbols, policy);
        List<ReturnBuilder.ReturnBar> out = new ArrayList<>();
        for (MarketSnapshot snap : PriceSnapshots.align(prices, symbols)) {
            builder.accept(snap).ifPresent(out::add);
        }
        return out;
    }

    private static Map<String, List<Bar>> intradayPrices(String[] symbols, String start, int bars, long seed) {
        Random rng = new Random(seed);
        Instant t0 = Instant.parse(start);
        Map<String, List<Bar>> prices = new LinkedHashMap<>();
        for (String symbol : symbols) {
            List<Bar> series = new ArrayList<>();
            double price = 100.0;
            for (int i = 0; i < bars; i++) {
                price *= Math.exp(0.001 * rng.nextGaussian());
                series.add(new Bar(t0.plus(i, ChronoUnit.MINUTES), price));
            }
            prices.put(symbol, series);
        }
        return prices;
    }

    private static Map<String, List<Bar>> dailyPrices(String[] symbols, int bars, long seed) {
        Random rng = new Random(seed);
        Instant t0 = Instant.parse("2024-01-01T00:00:00Z");
        Map<String, List<Bar>> prices = new LinkedHashMap<>();
        for (String symbol : symbols) {
            List<Bar> series = new ArrayList<>();
            double price = 100.0;
            for (int i = 0; i < bars; i++) {
                price *= Math.exp(0.02 * rng.nextGaussian());
                series.add(new Bar(t0.plus(i, ChronoUnit.DAYS), price));
            }
            prices.put(symbol, series);
        }
        return prices;
    }

    private static MarketSnapshot snap(String ts, double close) {
        return new MarketSnapshot(Instant.parse(ts), new double[]{close});
    }
}
