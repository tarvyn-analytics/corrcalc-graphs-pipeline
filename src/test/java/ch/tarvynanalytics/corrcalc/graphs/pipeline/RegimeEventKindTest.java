package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RegimeEventKindTest {

    @Test
    void everyKind_HasANonBlankPhrase() {
        for (RegimeEventKind kind : RegimeEventKind.values()) {
            assertFalse(kind.phrase().isBlank(), kind + " must carry a phrase");
        }
    }

    @Test
    void toSignalKind_MapsOnsetsToTheTwoFireKinds() {
        assertEquals(SignalKind.FUSION, RegimeEventKind.FUSION_ONSET.toSignalKind());
        assertEquals(SignalKind.DEFUSION, RegimeEventKind.CALM_ONSET.toSignalKind());
    }

    @Test
    void toSignalKind_OpenAtEof_IsNotAProductFire() {
        // OPEN_AT_EOF is an observability marker, not a fire: it has no product signal kind.
        assertThrows(IllegalStateException.class, RegimeEventKind.OPEN_AT_EOF::toSignalKind);
    }
}
