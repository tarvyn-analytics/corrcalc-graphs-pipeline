package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.engine.RunSummary;
import ch.tarvynanalytics.graphs.algos.model.ChangeMetrics;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReadableObserverTest {

    @Test
    void legend_DefinesTheFields() {
        String legend = ReadableObserver.legend();
        assertTrue(legend.contains("LEGEND"));
        assertTrue(legend.contains("density"));
        assertTrue(legend.contains("act="));
    }

    @Test
    void calibrationBanner_ShowsMuSigmaLevelAndThreshold() {
        String banner = ReadableObserver.calibrationBanner(
                obs(1.0, 1.0, 0.81, 0.0647, 0.0258, 0.167, 34.0, true));
        assertTrue(banner.contains("μ=0.0647"), banner);
        assertTrue(banner.contains("σ=0.0258"), banner);
        assertTrue(banner.contains("L=0.167"), banner);
        assertTrue(banner.contains("h=8.00"), banner);
    }

    @Test
    void renderLine_FireLine_FlagsSeverityAndReasons() {
        String line = ReadableObserver.renderLine(obs(1.0, 1.0, 0.81, 0.0647, 0.0258, 0.167, 34.0, true));
        assertTrue(line.contains("FIRE"), line);
        assertTrue(line.contains("FUSION fired"), line);
        assertTrue(line.contains("density=1.000"), line);
        assertTrue(line.contains("z=+"), line);
    }

    @Test
    void renderLine_UsesSuppliedDisplayTier() {
        // raw severity here is CALM (activation 1/8); the overload shows the supplied (hysteresis) tier
        String line = ReadableObserver.renderLine(obs(0.4, 0.5, 0.05, 0.05, 0.02, 0.5, 1.0, false), Severity.WARN);
        assertTrue(line.contains("WARN"), line);
    }

    @Test
    void renderLine_BlockedLine_ExplainsWhichGateHeldItBack() {
        // S+ huge but density below L: the reader should see WHY nothing fired
        String line = ReadableObserver.renderLine(obs(0.936, 0.9, 0.0006, 0.0013, 0.0014, 1.0, 206.0, false));
        assertTrue(line.contains("held back"), line);
    }

    @Test
    void onObservation_RendersBothSeverities_AndRejectsNull() {
        ReadableObserver observer = new ReadableObserver();
        observer.onObservation(obs(1.0, 1.0, 0.81, 0.0647, 0.0258, 0.167, 34.0, true));    // FIRE -> WARN path
        observer.onObservation(obs(0.4, 0.5, 0.05, 0.05, 0.02, 0.5, 1.0, false));          // CALM -> INFO path
        assertThrows(IllegalArgumentException.class, () -> observer.onObservation(null));
    }

    @Test
    void configBanner_ShowsConfigAndProvenance() {
        String banner = ReadableObserver.configBanner(new RunContext(
                "crypto", "daily", "replay", new ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationProvenance("leading-warmup", 0L, null, null), 14, 0.5, 1.5, 8.0, 99.0, "UPPER", 18, 60.0));
        assertTrue(banner.contains("CONFIG"), banner);
        assertTrue(banner.contains("mode=replay"), banner);
        assertTrue(banner.contains("calibration=leading-warmup"), banner);
        assertTrue(banner.contains("window=14"), banner);
        assertTrue(banner.contains("fireArm=UPPER"), banner);
    }

    @Test
    void onStart_LogsConfigBanner_AndRejectsNull() {
        ReadableObserver observer = new ReadableObserver();
        observer.onStart(new RunContext(
                "crypto", "daily", "replay", new ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationProvenance("leading-warmup", 0L, null, null), 14, 0.5, 1.5, 8.0, 99.0, "UPPER", 18, 60.0));
        assertThrows(IllegalArgumentException.class, () -> observer.onStart(null));
    }

    @Test
    void digestBlock_SummarizesTheRun() {
        RunDigest d = new RunDigest();
        d.add(obs(1.0, 1.0, 0.81, 0.0647, 0.0258, 0.167, 34.0, true));
        String block = ReadableObserver.digestBlock(d, new RunSummary(5, 1, 1, 5, 18));
        assertTrue(block.contains("DIGEST"), block);
        assertTrue(block.contains("FIRE=1"), block);
        assertTrue(block.contains("fires=1"), block);
        assertTrue(block.contains("biggest="), block);
    }

    @Test
    void renderLine_NonCalmBar_ShowsWhoMoved() {
        PipelineObservation fire = new PipelineObservation(Instant.parse("2021-02-12T00:00:00Z"), "crypto", "daily",
                new ChangeMetrics(0.81, 1.0, 0.1, 1, 1.0, List.of(2)), 34.0, 0.0, 0.9, true, SignalKind.FUSION,
                8.0, 0.0647, 0.0258, 0.167, List.of(new PairContribution("ETH", "BNB", 0.42)));
        String line = ReadableObserver.renderLine(fire);
        assertTrue(line.contains("who=ETH–BNB"), line);
    }

    @Test
    void renderLine_CalmBar_OmitsWho() {
        PipelineObservation calm = new PipelineObservation(Instant.parse("2021-02-12T00:00:00Z"), "crypto", "daily",
                new ChangeMetrics(0.05, 0.5, 0.1, 1, 0.5, List.of(2)), 1.0, 0.0, 0.9, false, null,
                8.0, 0.05, 0.02, 0.5, List.of(new PairContribution("ETH", "BNB", 0.01)));
        String line = ReadableObserver.renderLine(calm);
        assertTrue(line.contains("CALM"), line);
        assertFalse(line.contains("who="), line);
    }

    @Test
    void digestBlock_ShowsBiggestMoveContributors() {
        RunDigest d = new RunDigest();
        d.add(new PipelineObservation(Instant.parse("2021-02-12T00:00:00Z"), "crypto", "daily",
                new ChangeMetrics(0.81, 1.0, 0.1, 1, 1.0, List.of(2)), 34.0, 0.0, 0.9, true, SignalKind.FUSION,
                8.0, 0.0647, 0.0258, 0.167, List.of(new PairContribution("ETH", "BNB", 0.42))));
        String block = ReadableObserver.digestBlock(d, new RunSummary(5, 1, 1, 5, 18));
        assertTrue(block.contains("(ETH–BNB)"), block);
    }

    private static PipelineObservation obs(double density, double largestFraction, double weightedChange,
                                           double mu, double sigma, double level, double sPlus, boolean fired) {
        ChangeMetrics m = new ChangeMetrics(weightedChange, density, 0.1, 1, largestFraction, List.of(2));
        return new PipelineObservation(Instant.parse("2021-02-12T00:00:00Z"), "crypto", "daily",
                m, sPlus, 0.0, 0.9, fired, fired ? SignalKind.FUSION : null, 8.0, mu, sigma, level, List.of());
    }

    @Test
    void renderLine_ShowsRecoveryGauge() {
        String line = ReadableObserver.renderLine(obs(0.4, 0.5, 0.05, 0.05, 0.02, 0.5, 1.0, false));
        assertTrue(line.contains("rec=0.90"), line);   // the helper sets gauge 0.9
    }

    @Test
    void digestBlock_ShowsPeakRecoveryAndAllClear() {
        RunDigest d = new RunDigest();
        d.add(new PipelineObservation(Instant.parse("2021-05-18T00:00:00Z"), "crypto", "intraday",
                new ChangeMetrics(2.0, 1.0, 0.1, 1, 1.0, List.of(2)),
                10.0, 0.0, 0.0, true, SignalKind.FUSION, 8.0, 0.05, 0.02, 0.5, List.of()));
        d.add(new PipelineObservation(Instant.parse("2021-05-20T00:00:00Z"), "crypto", "intraday",
                new ChangeMetrics(0.0001, 0.05, 0.1, 1, 0.2, List.of(2)),
                0.0, 0.0, 0.95, true, SignalKind.DEFUSION, 8.0, 0.05, 0.02, 0.5, List.of()));
        String block = ReadableObserver.digestBlock(d, new RunSummary(2, 2, 2, 2, 18));
        assertTrue(block.contains("peak recovery=0.95"), block);
        assertTrue(block.contains("all-clear=2021-05-20T00:00:00Z (+48h after fusion)"), block);
    }
}
