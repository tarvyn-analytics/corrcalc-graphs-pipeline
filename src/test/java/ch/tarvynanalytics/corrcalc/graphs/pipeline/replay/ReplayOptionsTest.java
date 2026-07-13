package ch.tarvynanalytics.corrcalc.graphs.pipeline.replay;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the calibration-mode validation matrix: which of the three modes accept, require or reject
 * an artifact (the artifact is the calm-block baseline, an optional adaptive
 * prior, and meaningless under leading-warmup).
 */
class ReplayOptionsTest {

    private static final Path ARTIFACT = Path.of("calm.json");

    @Test
    void constructor_AdaptiveMode_AcceptsWithAndWithoutAPriorArtifact() {
        assertDoesNotThrow(() -> options(ReplayOptions.ADAPTIVE, null));
        assertDoesNotThrow(() -> options(ReplayOptions.ADAPTIVE, ARTIFACT));
    }

    @Test
    void constructor_CalmBlock_RequiresTheArtifact() {
        assertDoesNotThrow(() -> options(ReplayOptions.CALM_BLOCK, ARTIFACT));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> options(ReplayOptions.CALM_BLOCK, null));
        assertTrue(e.getMessage().contains("artifact"), e.getMessage());
    }

    @Test
    void constructor_LeadingWarmup_RejectsAnArtifact() {
        assertDoesNotThrow(() -> options(ReplayOptions.LEADING_WARMUP, null));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> options(ReplayOptions.LEADING_WARMUP, ARTIFACT));
        assertTrue(e.getMessage().contains("leading-warmup"), e.getMessage());
    }

    @Test
    void constructor_UnknownMode_ThrowsWithTheOffendingValue() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> options("psychic", null));
        assertTrue(e.getMessage().contains("[psychic]"), e.getMessage());
    }

    @Test
    void constructor_FireMode_AcceptsCusumAndRegimeRejectsOthers() {
        assertDoesNotThrow(() -> options(ReplayOptions.LEADING_WARMUP, null, ReplayOptions.CUSUM));
        assertDoesNotThrow(() -> options(ReplayOptions.LEADING_WARMUP, null, ReplayOptions.REGIME));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> options(ReplayOptions.LEADING_WARMUP, null, "quantum"));
        assertTrue(e.getMessage().contains("[quantum]"), e.getMessage());
    }

    private static ReplayOptions options(String calibrationMode, Path artifact) {
        return options(calibrationMode, artifact, ReplayOptions.CUSUM);
    }

    private static ReplayOptions options(String calibrationMode, Path artifact, String fireMode) {
        return new ReplayOptions("synthetic", "crypto", "intraday",
                1000.0, 0L, null, null, 1, null, null, null, calibrationMode, artifact, null, fireMode, false);
    }
}
