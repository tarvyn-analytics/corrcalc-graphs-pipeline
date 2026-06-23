package ch.tarvynanalytics.pipeline.replay;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CryptoEventsTest {

    @Test
    void all_HasTheEightEventsInLeadTableOrder() {
        assertEquals(8, CryptoEvents.ALL.size());
        assertEquals("covid_crash_2020", CryptoEvents.ALL.get(0).name());
        assertEquals("yen_carry_unwind_2024", CryptoEvents.ALL.get(7).name());
    }

    @Test
    void everyEvent_HasACalmBlockThatStrictlyPrecedesItsWindow() {
        for (CryptoEvent event : CryptoEvents.ALL) {
            assertTrue(event.calmEnd().isBefore(event.start()),
                    "calm block must precede the event window for " + event.name());
            assertTrue(event.calmStart().isBefore(event.calmEnd()), event.name());
            assertTrue(!event.end().isBefore(event.start()), event.name());
        }
    }

    @Test
    void byName_ReturnsTheEvent_AndThrowsOnUnknown() {
        CryptoEvent ftx = CryptoEvents.byName("ftx_collapse_2022");
        assertEquals(LocalDate.of(2022, 11, 7), ftx.start());
        assertEquals("2022-09-02 .. 2022-10-17", ftx.calmBlock());
        assertEquals("2022-11-07 .. 2022-11-11", ftx.eventWindow());
        assertThrows(IllegalArgumentException.class, () -> CryptoEvents.byName("no_such_event"));
    }
}
