package ch.tarvynanalytics.pipeline;

import ch.tarvynanalytics.corrcalc.lib.stream.RollingCorrelationEngine;
import ch.tarvynanalytics.corrcalc.lib.stream.RollingCorrelations;
import ch.tarvynanalytics.graphs.algos.Calibration;
import ch.tarvynanalytics.graphs.algos.ChangeDetector;
import ch.tarvynanalytics.graphs.algos.ChangeDetectors;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StructuralSignalTest {

    @Test
    void constructor_DefensivelyCopiesUniverse_SoTheSignalIsImmutable() {
        List<String> universe = new ArrayList<>(List.of("BTCUSDT", "ETHUSDT"));
        StructuralSignal signal = new StructuralSignal(
                Instant.parse("2022-11-08T16:11:00Z"), "crypto", "intraday", universe,
                SignalKind.FUSION, 0.043, 9.4, 0.0, 0.71, 0.12, 2, 0.72,
                StructuralSignal.Validity.accepted(), null);

        universe.add("MUTATED");   // mutating the source list must not affect the signal

        assertEquals(List.of("BTCUSDT", "ETHUSDT"), signal.universe());
        assertEquals(SignalKind.FUSION, signal.kind());
        assertEquals("crypto", signal.market());
        assertNull(signal.leadVsDailyHours());
        assertFalse(signal.validity().filtered());
        assertNull(signal.validity().score());
        assertThrows(UnsupportedOperationException.class, () -> signal.universe().add("x"));
    }

    @Test
    void nullUniverse_BecomesEmptyList() {
        StructuralSignal signal = new StructuralSignal(
                Instant.EPOCH, "crypto", "daily", null, SignalKind.DEFUSION,
                0.0, 0.0, 0.0, 1.0, 0.0, 1, 1.0, StructuralSignal.Validity.accepted(), 12.5);
        assertTrue(signal.universe().isEmpty());
        assertEquals(12.5, signal.leadVsDailyHours());
    }

    /**
     * Compile-and-link smoke check that the pipeline resolves BOTH private upstream artifacts:
     * S1 (corrcalc-lib-core, the rolling engine) and S3 (graphs-algos-lib, the change detector).
     * If either GitHub-Packages dependency is missing, this test does not compile.
     */
    @Test
    void pipelineResolvesBothUpstreamLibraries_S1EngineAndS3Detector() {
        RollingCorrelationEngine engine = RollingCorrelations.pearson(
                new String[]{"A", "B"}, 2, (seq, asOf, pearson, labels) -> { /* snapshot sink */ });
        engine.onBar(Instant.EPOCH, new double[]{0.01, -0.02});

        DetectorConfig config = DetectorConfig.crypto();
        Calibration calibration = ChangeDetectors.calibrate(
                new double[]{0.01, 0.02, 0.015}, new double[]{0.4, 0.5, 0.45}, config);
        ChangeDetector detector = ChangeDetectors.create(2, config, calibration);

        assertNull(detector.onMatrix(new double[][]{{1.0, 0.3}, {0.3, 1.0}}),
                "the first matrix has no predecessor, so it yields no transition signal");
        assertEquals(1.5, config.k());
    }
}
