package ch.tarvynanalytics.corrcalc.graphs.pipeline.cli;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineCliTest {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();

    private Logger replayLogger;
    private ListAppender<ILoggingEvent> appender;

    @BeforeEach
    void attachAppender() {
        replayLogger = (Logger) LoggerFactory.getLogger("replay");
        appender = new ListAppender<>();
        appender.start();
        replayLogger.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        replayLogger.detachAppender(appender);
        System.clearProperty("cgp.log.target");
    }

    private int run(String... args) {
        return PipelineCli.run(args, new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8));
    }

    @Test
    void run_NoArgs_PrintsUsageExit2() {
        assertEquals(2, run());
        assertTrue(err.toString(StandardCharsets.UTF_8).contains("Usage:"));
    }

    @Test
    void run_Help_Exit0() {
        assertEquals(0, run("-h"));
        assertTrue(out.toString(StandardCharsets.UTF_8).contains("Usage:"));
    }

    @Test
    void run_UnknownCommand_Exit2() {
        assertEquals(2, run("frobnicate"));
    }

    @Test
    void run_ReplayHelp_Exit0() {
        assertEquals(0, run("replay", "-h"));
        assertTrue(out.toString(StandardCharsets.UTF_8).contains("--market"));
    }

    @Test
    void run_MissingDataDir_Exit2() {
        assertEquals(2, run("replay", "--event", "e", "--market", "crypto"));
    }

    @Test
    void run_MissingEvent_Exit2() {
        assertEquals(2, run("replay", "dir", "--market", "crypto"));
    }

    @Test
    void run_MissingMarket_Exit2() {
        assertEquals(2, run("replay", "dir", "--event", "e"));
    }

    @Test
    void run_UnknownOption_Exit2() {
        assertEquals(2, run("replay", "dir", "--event", "e", "--market", "crypto", "--bogus", "x"));
    }

    @Test
    void run_BadSpeed_Exit2() {
        assertEquals(2, run("replay", "dir", "--event", "e", "--market", "crypto", "--speed", "fast"));
    }

    @Test
    void run_BadDate_Exit2() {
        assertEquals(2, run("replay", "dir", "--event", "e", "--market", "crypto", "--from", "not-a-date"));
    }

    @Test
    void run_BadObserve_Exit2() {
        assertEquals(2, run("replay", "dir", "--event", "e", "--market", "crypto", "--observe", "bogus"));
        assertTrue(err.toString(StandardCharsets.UTF_8).contains("--observe"));
    }

    @Test
    void run_BadStyle_Exit2() {
        assertEquals(2, run("replay", "dir", "--event", "e", "--market", "crypto", "--style", "fancy"));
        assertTrue(err.toString(StandardCharsets.UTF_8).contains("--style"));
    }

    @Test
    void run_UnsupportedMarket_Exit2() {
        assertEquals(2, run("replay", "dir", "--event", "e", "--market", "forex", "--max-step-ms", "0"));
        assertTrue(err.toString(StandardCharsets.UTF_8).contains("unsupported market"));
    }

    @Test
    void run_MissingDataFiles_Exit1(@TempDir Path dir) {
        int code = run("replay", dir.toString(), "--event", "ghost", "--market", "crypto", "--max-step-ms", "0");
        assertEquals(1, code);
        assertTrue(err.toString(StandardCharsets.UTF_8).contains("io error"));
    }

    @Test
    void run_HappyPathDaily_StreamsAndExit0(@TempDir Path dir) throws IOException {
        String event = "demo";
        List<String> symbols = List.of("AAA", "BBB", "CCC");
        writeUniverse(dir, event, symbols);
        for (int s = 0; s < symbols.size(); s++) {
            writeDailyBars(dir, symbols.get(s), event, 24, 100L + s);
        }

        int code = run("replay", dir.toString(), "--event", event, "--market", "crypto",
                "--timescale", "daily", "--speed", "100000", "--max-step-ms", "0",
                "--observe", "activation>=0.5");

        assertEquals(0, code, err.toString(StandardCharsets.UTF_8));
        assertFalse(appender.list.isEmpty(), "the replay should have logged at least a start/summary line");
    }

    @Test
    void run_NdjsonStyle_WritesStructuredStreamToStdout(@TempDir Path dir) throws IOException {
        String event = "demo";
        List<String> symbols = List.of("AAA", "BBB", "CCC");
        writeUniverse(dir, event, symbols);
        for (int s = 0; s < symbols.size(); s++) {
            writeDailyBars(dir, symbols.get(s), event, 24, 100L + s);
        }

        int code = run("replay", dir.toString(), "--event", event, "--market", "crypto",
                "--timescale", "daily", "--speed", "100000", "--max-step-ms", "0", "--style", "ndjson");

        assertEquals(0, code, err.toString(StandardCharsets.UTF_8));
        String stdout = out.toString(StandardCharsets.UTF_8);
        assertTrue(stdout.contains("{\"rec\":\"calib\""), stdout);
        assertTrue(stdout.contains("{\"rec\":\"obs\""), stdout);
    }

    private static void writeUniverse(Path dir, String event, List<String> symbols) throws IOException {
        StringBuilder sb = new StringBuilder("symbol\n");
        for (String s : symbols) {
            sb.append(s).append('\n');
        }
        Files.writeString(dir.resolve(event + "_universe.csv"), sb.toString());
    }

    private static void writeDailyBars(Path dir, String symbol, String event, int bars, long seed) throws IOException {
        Random rng = new Random(seed);
        StringBuilder sb = new StringBuilder("timestamp,open,high,low,close,volume\n");
        Instant day = Instant.parse("2021-01-01T00:00:00Z");
        double price = 100.0;
        for (int t = 0; t < bars; t++) {
            price *= Math.exp(0.02 * rng.nextGaussian());
            sb.append(day.plusSeconds(86_400L * t)).append(",0,0,0,")
                    .append(price).append(",0\n");
        }
        Files.writeString(dir.resolve(symbol + "_1d_" + event + ".csv"), sb.toString());
    }
}
