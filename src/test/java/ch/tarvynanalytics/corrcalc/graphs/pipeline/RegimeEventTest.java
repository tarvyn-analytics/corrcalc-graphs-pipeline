package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RegimeEventTest {

    private static final Instant ONSET = Instant.parse("2021-04-19T00:00:00Z");
    private static final Instant CLOSE = Instant.parse("2021-11-07T00:00:00Z");

    @Test
    void fusedDwell_IsAsOfMinusOnset() {
        RegimeEvent close = new RegimeEvent(CLOSE, RegimeEventKind.CALM_ONSET, 0.4, 0.07, ONSET);

        assertEquals(202L, close.fusedDwell().toDays(), "the may2021+china regime is one 202-day span");
    }

    @Test
    void fusedDwell_AtOnset_IsZero() {
        RegimeEvent onset = new RegimeEvent(ONSET, RegimeEventKind.FUSION_ONSET, 0.9, 0.33, ONSET);

        assertEquals(0L, onset.fusedDwell().toDays());
        assertEquals(0.33, onset.confidence(), 1e-12);
    }

    @Test
    void nullComponents_Throw() {
        assertThrows(IllegalArgumentException.class,
                () -> new RegimeEvent(null, RegimeEventKind.FUSION_ONSET, 0.9, 0.3, ONSET));
        assertThrows(IllegalArgumentException.class,
                () -> new RegimeEvent(ONSET, null, 0.9, 0.3, ONSET));
        assertThrows(IllegalArgumentException.class,
                () -> new RegimeEvent(ONSET, RegimeEventKind.FUSION_ONSET, 0.9, 0.3, null));
    }
}
