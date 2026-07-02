package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

import ch.tarvynanalytics.graphs.algos.Calibration;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Validation of the persisted artifact + the provenance view (H2 design item i). */
class CalibrationArtifactTest {

    private static final Instant FROM = Instant.parse("2021-03-01T00:00:00Z");
    private static final Instant TO = Instant.parse("2021-04-15T00:00:00Z");
    private static final Calibration CAL = new Calibration(0.09, 0.03, 0.167, 0.21, 0.05);

    private static CalibrationArtifact valid() {
        return new CalibrationArtifact(1, "crypto", "intraday", 0L, FROM, TO, 1440, CAL);
    }

    @Test
    void constructor_ValidArtifact_ExposesComponents() {
        CalibrationArtifact a = valid();
        assertEquals(1, a.schemaVersion());
        assertEquals("crypto", a.market());
        assertEquals(CAL, a.calibration());
    }

    @Test
    void provenance_CarriesModeEpochAndWindow() {
        CalibrationProvenance p = valid().provenance("calm-block");
        assertEquals(new CalibrationProvenance("calm-block", 0L, FROM, TO), p);
    }

    @Test
    void constructor_RejectsBadValues_WithTheValueBracketed() {
        assertThrows(IllegalArgumentException.class,
                () -> new CalibrationArtifact(2, "crypto", "intraday", 0L, FROM, TO, 1440, CAL));
        assertThrows(IllegalArgumentException.class,
                () -> new CalibrationArtifact(1, " ", "intraday", 0L, FROM, TO, 1440, CAL));
        assertThrows(IllegalArgumentException.class,
                () -> new CalibrationArtifact(1, "crypto", "weekly", 0L, FROM, TO, 1440, CAL));
        assertThrows(IllegalArgumentException.class,
                () -> new CalibrationArtifact(1, "crypto", "intraday", -1L, FROM, TO, 1440, CAL));
        assertThrows(IllegalArgumentException.class,
                () -> new CalibrationArtifact(1, "crypto", "intraday", 0L, null, TO, 1440, CAL));
        assertThrows(IllegalArgumentException.class,
                () -> new CalibrationArtifact(1, "crypto", "intraday", 0L, TO, FROM, 1440, CAL));
        assertThrows(IllegalArgumentException.class,
                () -> new CalibrationArtifact(1, "crypto", "intraday", 0L, FROM, TO, 1, CAL));
        assertThrows(IllegalArgumentException.class,
                () -> new CalibrationArtifact(1, "crypto", "intraday", 0L, FROM, TO, 1440, null));
    }

    @Test
    void provenance_RejectsBlankModeAndNegativeEpoch() {
        assertThrows(IllegalArgumentException.class, () -> new CalibrationProvenance(" ", 0L, FROM, TO));
        assertThrows(IllegalArgumentException.class, () -> new CalibrationProvenance("adaptive", -1L, FROM, TO));
    }
}
