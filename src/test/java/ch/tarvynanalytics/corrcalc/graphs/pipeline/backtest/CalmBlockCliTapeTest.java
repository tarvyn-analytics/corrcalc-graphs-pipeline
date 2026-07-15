package ch.tarvynanalytics.corrcalc.graphs.pipeline.backtest;

import ch.tarvynanalytics.graphs.algos.Calibration;
import ch.tarvynanalytics.graphs.algos.ChangeDetectors;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationArtifact;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationSources;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.cli.PipelineCli;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.Bar;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.PriceBars;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.ReturnPanel;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.ReturnPanels;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.DensityChangeSeries;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.SeriesBuilder;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.TimescaleConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Opt-in acceptance (the owner's headline): the walk-forward tape-test verdicts
 * ({@link DefusionGaugeRecoveryTapeTest} — every genuinely-recovering event sounds the all-clear,
 * the choppy {@code may2021_selloff} stays suppressed) must reproduce <strong>through the replay
 * CLI</strong> under {@code --calibration calm-block}. Per event: the calm-block artifact is
 * calibrated offline on the event's leak-free calm window (exactly the real walk-forward
 * calibration, {@link ChangeDetectors#calibrate} over the {@link SeriesBuilder} series), persisted,
 * and handed to the CLI; the full product path (S1→S3 detector with the de-fusion gauge)
 * then replays the recovery tape and the NDJSON fire stream is asserted. <strong>Skipped
 * unless</strong> {@code -Dcrypto.data.dir=...} points at the (gitignored) raw bars.
 *
 * <pre>{@code
 * ./mvnw test -Dtest=CalmBlockCliTapeTest \
 *     -Dcrypto.data.dir=/path/to/crypto-data
 * }</pre>
 */
@EnabledIfSystemProperty(named = "crypto.data.dir", matches = ".+")
class CalmBlockCliTapeTest {

    private static final String INTRADAY_FREQ = "1m";

    @TempDir
    Path work;

    @Test
    void calmBlockCli_FiresAllClearsOnRecoveries_AndSuppressesMay2021() throws IOException {
        Path dataDir = Path.of(System.getProperty("crypto.data.dir"));
        int goodFired = 0;
        int goodTotal = 0;
        boolean may2021FiredAllClear = false;

        for (CryptoEvent event : CryptoEvents.ALL) {
            CliVerdict v = replayThroughCli(dataDir, event);
            if (v == null) {
                System.out.printf("%-24s SKIP (data-thin)%n", event.name());
                continue;
            }
            System.out.printf("%-24s fusion=%s  allClear=%s%n", event.name(), v.fusion, v.allClear);
            assertTrue(v.fusion, event.name() + " should fire a FUSION on its regime event");
            if (event.name().equals("may2021_selloff")) {
                may2021FiredAllClear = v.allClear;
            } else {
                goodTotal++;
                if (v.allClear) {
                    goodFired++;
                }
            }
        }

        assertEquals(goodTotal, goodFired,
                "every genuinely-recovering event should sound the all-clear through the CLI");
        assertTrue(goodTotal >= 5, "expected the recovering-event base (got " + goodTotal + ")");
        assertFalse(may2021FiredAllClear,
                "may2021 (choppy non-recovery) must stay suppressed through the CLI");
    }

    /** Calibrates the event's calm-block artifact offline, then replays the tape through the CLI. */
    private CliVerdict replayThroughCli(Path dataDir, CryptoEvent event) throws IOException {
        List<String> universe = LeadTableFixtures.universe(event.name());
        CalibrationArtifact artifact = calmBlockArtifact(dataDir, event, universe);
        if (artifact == null) {
            return null;
        }
        Path artifactPath = work.resolve(event.name() + "_calm.json");
        CalibrationSources.save(artifact, artifactPath);
        Path universePath = work.resolve(event.name() + "_universe.csv");
        Files.writeString(universePath, "symbol\n" + String.join("\n", universe) + "\n");

        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        int code = PipelineCli.run(new String[]{
                "replay", dataDir.toString(),
                "--event", event.name(),
                "--market", "crypto",
                "--timescale", "intraday",
                "--speed", "1e9",
                "--max-step-ms", "0",
                "--style", "ndjson",
                "--observe", "fires",
                "--universe", universePath.toString(),
                "--calibration", "calm-block",
                "--calibration-artifact", artifactPath.toString(),
                // Walk-forward discipline (invariant 9): the detection span starts AFTER the calm
                // window — replaying the calm window itself would score (and fire on) the very data
                // the baseline was calibrated on.
                "--from", event.calmEnd().plusDays(1).toString(),
                "--to", event.end().plusDays(70).toString()},
                new PrintStream(stdout, true, StandardCharsets.UTF_8),
                new PrintStream(stderr, true, StandardCharsets.UTF_8));
        String errText = stderr.toString(StandardCharsets.UTF_8);
        if (code != 0 && errText.contains("series too short")) {
            // Data-thin on this machine (e.g. yen_carry_2024: WAVES delists mid-recovery, emptying the
            // aligned post-calm span) — the same skip the in-process tape test takes.
            return null;
        }
        assertEquals(0, code, errText);

        String ndjson = stdout.toString(StandardCharsets.UTF_8);
        return new CliVerdict(
                ndjson.contains("\"firedKind\":\"FUSION\""),
                ndjson.contains("\"firedKind\":\"DEFUSION\""));
    }

    /**
     * The real walk-forward calibration: the density/weighted-change series over the event's calm
     * window only (leak-free: it precedes and is disjoint from the detection span), through the
     * pinned S1→S3 wiring — the same computation {@code DefusionGaugeRecoveryTapeTest} uses.
     */
    private static CalibrationArtifact calmBlockArtifact(Path dataDir, CryptoEvent event,
                                                         List<String> universe) {
        Map<String, List<Bar>> prices = new LinkedHashMap<>();
        for (String symbol : universe) {
            Path csv = dataDir.resolve(symbol + "_" + INTRADAY_FREQ + "_" + event.name() + ".csv");
            try {
                prices.put(symbol, PriceBars.read(csv, event.calmStart(), event.calmEnd()));
            } catch (UncheckedIOException e) {
                return null;   // data-thin event on this machine
            }
        }
        ReturnPanel panel = ReturnPanels.buildIntraday(prices, universe.toArray(new String[0]));
        TimescaleConfig cfg = TimescaleConfig.cryptoIntraday();
        DensityChangeSeries series = SeriesBuilder.build(panel, cfg.window(), cfg.edgeThreshold());
        if (series.size() < 10) {
            return null;
        }
        Calibration cal = ChangeDetectors.calibrate(series.weightedChange(), series.density(), cfg.detector());
        LocalDate calmEndExclusive = event.calmEnd().plusDays(1);
        return new CalibrationArtifact(CalibrationArtifact.SCHEMA_VERSION, "crypto", "intraday", 0L,
                event.calmStart().atStartOfDay(ZoneOffset.UTC).toInstant(),
                calmEndExclusive.atStartOfDay(ZoneOffset.UTC).toInstant(),
                series.size(), cal);
    }

    private record CliVerdict(boolean fusion, boolean allClear) {
    }
}
