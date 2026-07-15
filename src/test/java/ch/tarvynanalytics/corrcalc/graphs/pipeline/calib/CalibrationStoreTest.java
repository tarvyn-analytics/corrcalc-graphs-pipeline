package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

import ch.tarvynanalytics.graphs.algos.Calibration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip contract: a known artifact round-trips save → load to an equal record, the schema version
 * is pinned, and the NaN de-fusion-uncalibrated marker survives persistence.
 */
class CalibrationStoreTest {

    @TempDir
    Path dir;

    private static CalibrationArtifact artifact() {
        // The walk-forward may-2021 shape: a calm block ending 21 days pre-event, defusion calibrated.
        return new CalibrationArtifact(CalibrationArtifact.SCHEMA_VERSION, "crypto", "intraday", 0L,
                Instant.parse("2021-03-01T00:00:00Z"), Instant.parse("2021-04-15T00:00:00Z"), 1440,
                new Calibration(0.0906, 0.0333, 0.167, 0.21, 0.05));
    }

    @Test
    void save_ThenLoad_RoundTripsToAnEqualArtifact() {
        Path json = dir.resolve("calm.json");
        CalibrationArtifact original = artifact();
        CalibrationStore.save(original, json);
        assertEquals(original, CalibrationStore.load(json));
    }

    @Test
    void save_DefusionUncalibratedNaN_SurvivesTheRoundTrip() {
        Path json = dir.resolve("fusion-only.json");
        CalibrationArtifact original = new CalibrationArtifact(1, "crypto", "daily", 3L,
                Instant.parse("2020-01-04T00:00:00Z"), Instant.parse("2020-02-18T00:00:00Z"), 45,
                new Calibration(0.0647, 0.0258, 0.167));   // muDensity/sigmaDensity = NaN marker
        CalibrationStore.save(original, json);
        CalibrationArtifact loaded = CalibrationStore.load(json);
        assertEquals(original, loaded);
        assertTrue(Double.isNaN(loaded.calibration().muDensity()));
    }

    @Test
    void load_UnsupportedSchemaVersion_Rejects() throws Exception {
        Path json = dir.resolve("v99.json");
        String v99 = Files.readString(write(artifact())).replace("\"schemaVersion\" : 1", "\"schemaVersion\" : 99");
        Files.writeString(json, v99);
        UncheckedIOException e = assertThrows(UncheckedIOException.class, () -> CalibrationStore.load(json));
        assertTrue(e.getCause().getMessage().contains("schemaVersion [99]"));
    }

    @Test
    void load_MissingFile_ThrowsWithThePath() {
        Path json = dir.resolve("absent.json");
        UncheckedIOException e = assertThrows(UncheckedIOException.class, () -> CalibrationStore.load(json));
        assertTrue(e.getMessage().contains("absent.json"));
    }

    @Test
    void save_NullArtifact_Throws() {
        assertThrows(IllegalArgumentException.class,
                () -> CalibrationStore.save(null, dir.resolve("x.json")));
    }

    private Path write(CalibrationArtifact a) {
        Path json = dir.resolve("tmp.json");
        CalibrationStore.save(a, json);
        return json;
    }
}
