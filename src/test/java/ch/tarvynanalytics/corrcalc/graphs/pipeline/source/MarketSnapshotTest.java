package ch.tarvynanalytics.corrcalc.graphs.pipeline.source;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MarketSnapshotTest {

    @Test
    void constructor_DefensivelyCopiesCloses() {
        double[] closes = {1.0, 2.0, 3.0};
        MarketSnapshot snapshot = new MarketSnapshot(Instant.EPOCH, closes);

        closes[0] = 99.0;   // mutate the source array after construction
        assertEquals(1.0, snapshot.closes()[0], "snapshot must not see the post-construction mutation");
        assertEquals(3, snapshot.symbolCount());
    }

    @Test
    void constructor_RejectsNulls() {
        assertThrows(IllegalArgumentException.class, () -> new MarketSnapshot(null, new double[]{1.0}));
        assertThrows(IllegalArgumentException.class, () -> new MarketSnapshot(Instant.EPOCH, null));
    }
}
