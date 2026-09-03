package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;

class DroppedBarReasonTest {

    @Test
    void everyReason_HasANonBlankPhrase() {
        for (DroppedBarReason reason : DroppedBarReason.values()) {
            assertFalse(reason.phrase().isBlank(), reason + " must carry a phrase");
        }
    }
}
