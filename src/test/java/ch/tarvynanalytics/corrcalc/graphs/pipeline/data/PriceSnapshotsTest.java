package ch.tarvynanalytics.corrcalc.graphs.pipeline.data;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.MarketSnapshot;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PriceSnapshotsTest {

    @Test
    void align_KeepsOnlyTimestampsCommonToEverySymbol_InColumnOrder() {
        String[] symbols = {"AUSDT", "BUSDT"};
        Instant t0 = Instant.parse("2024-03-01T12:00:00Z");
        Map<String, List<Bar>> prices = new LinkedHashMap<>();
        prices.put("AUSDT", List.of(
                new Bar(t0, 10.0), new Bar(t0.plus(1, ChronoUnit.MINUTES), 11.0),
                new Bar(t0.plus(2, ChronoUnit.MINUTES), 12.0)));
        // BUSDT is missing the t0+1 bar -> that timestamp is not common.
        prices.put("BUSDT", List.of(
                new Bar(t0, 20.0), new Bar(t0.plus(2, ChronoUnit.MINUTES), 24.0)));

        List<MarketSnapshot> snapshots = PriceSnapshots.align(prices, symbols);

        assertEquals(2, snapshots.size());                        // {t0, t0+2}
        assertEquals(t0, snapshots.get(0).timestamp());
        assertEquals(t0.plus(2, ChronoUnit.MINUTES), snapshots.get(1).timestamp());
        assertArrayEquals(new double[]{10.0, 20.0}, snapshots.get(0).closes());   // column order = symbols
        assertArrayEquals(new double[]{12.0, 24.0}, snapshots.get(1).closes());
    }

    @Test
    void align_EmptyUniverse_YieldsEmpty() {
        assertEquals(0, PriceSnapshots.align(Map.of(), new String[0]).size());
    }
}
