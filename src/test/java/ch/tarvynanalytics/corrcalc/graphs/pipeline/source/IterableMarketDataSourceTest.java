package ch.tarvynanalytics.corrcalc.graphs.pipeline.source;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IterableMarketDataSourceTest {

    private static final String[] UNIVERSE = {"A", "B"};

    @Test
    void poll_WalksSnapshotsInOrderThenReportsEndOfStream() throws InterruptedException {
        MarketSnapshot s0 = new MarketSnapshot(Instant.EPOCH, new double[]{1.0, 2.0});
        MarketSnapshot s1 = new MarketSnapshot(Instant.EPOCH.plusSeconds(60), new double[]{3.0, 4.0});
        MarketDataSource source = new IterableMarketDataSource(UNIVERSE, List.of(s0, s1));

        assertEquals(Optional.of(s0), source.poll());
        assertEquals(Optional.of(s1), source.poll());
        assertTrue(source.poll().isEmpty(), "exhausted source reports end-of-stream");
        assertTrue(source.poll().isEmpty(), "and stays exhausted");
    }

    @Test
    void universe_IsACopy() {
        IterableMarketDataSource source = new IterableMarketDataSource(UNIVERSE, List.of());
        String[] u = source.universe();
        u[0] = "MUT";
        assertArrayEquals(new String[]{"A", "B"}, source.universe());
    }

    @Test
    void constructor_RejectsNullsAndEmptyUniverse() {
        assertThrows(IllegalArgumentException.class, () -> new IterableMarketDataSource(new String[0], List.of()));
        assertThrows(IllegalArgumentException.class, () -> new IterableMarketDataSource(UNIVERSE, null));
    }
}
