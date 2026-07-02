package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

import ch.tarvynanalytics.graphs.algos.Calibration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The calm-block source: ready before the stream, primed from the walk-forward artifact (PR-3). */
class CalmBlockCalibrationTest {

    private static final Instant FROM = Instant.parse("2021-03-01T00:00:00Z");
    private static final Instant TO = Instant.parse("2021-04-15T00:00:00Z");
    private static final Calibration CAL = new Calibration(0.0906, 0.0333, 0.167, 0.21, 0.05);
    private static final CalibrationArtifact ARTIFACT = new CalibrationArtifact(
            CalibrationArtifact.SCHEMA_VERSION, "crypto", "intraday", 0L, FROM, TO, 1440, CAL);

    @Test
    void calmBlock_IsReadyImmediately_WithTheArtifactCalibration() {
        CalibrationSource source = CalibrationSources.calmBlock(ARTIFACT);
        assertTrue(source.isReady(), "an externally-calibrated source needs no leading accumulation");
        assertEquals(CAL, source.calibration());
    }

    @Test
    void observe_IsIgnored_TheStreamNeverFeedsTheBaseline() {
        CalibrationSource source = CalibrationSources.calmBlock(ARTIFACT);
        source.observe(TO.plusSeconds(60), 99.0, 1.0);   // an in-event bar must not contaminate
        assertEquals(CAL, source.calibration());
    }

    @Test
    void provenance_IsTheArtifactsWindowUnderCalmBlockMode() {
        assertEquals(new CalibrationProvenance("calm-block", 0L, FROM, TO),
                CalibrationSources.calmBlock(ARTIFACT).provenance());
    }

    @Test
    void artifact_ReturnsTheSuppliedArtifact() {
        assertEquals(ARTIFACT, CalibrationSources.calmBlock(ARTIFACT).artifact("crypto", "intraday"));
    }

    @Test
    void calmBlock_NullArtifact_Throws() {
        assertThrows(IllegalArgumentException.class, () -> CalibrationSources.calmBlock(null));
    }

    @Test
    void loadSave_PublicDoor_RoundTrips(@TempDir Path dir) {
        Path json = dir.resolve("artifact.json");
        CalibrationSources.save(ARTIFACT, json);
        assertEquals(ARTIFACT, CalibrationSources.load(json));
    }
}
