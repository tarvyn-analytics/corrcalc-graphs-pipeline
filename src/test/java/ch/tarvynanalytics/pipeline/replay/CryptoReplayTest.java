package ch.tarvynanalytics.pipeline.replay;

import ch.tarvynanalytics.graphs.algos.DetectorConfig;
import ch.tarvynanalytics.pipeline.detect.TimescaleConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the full CSV→panel→S1→S3→alert {@link CryptoReplay} chain on tiny synthetic data (the
 * opt-in {@link CryptoFullReplayDriverTest} runs the same code over the real 2.4 GB dataset). Three
 * symbols share one monotonic price path, so every window correlates at +1 and the density stays
 * saturated at 1.0 throughout — the level baseline therefore cannot fire (no headroom), giving a
 * deterministic both-miss row with {@code L = 1.0}.
 */
class CryptoReplayTest {

    private static final List<String> UNIVERSE = List.of("AAAUSDT", "BBBUSDT", "CCCUSDT");

    private static final CryptoEvent EVENT = new CryptoEvent("mini_event",
            LocalDate.of(2024, 1, 6), LocalDate.of(2024, 1, 7),
            LocalDate.of(2024, 1, 1), LocalDate.of(2024, 1, 4));

    // tiny windows so a handful of bars produce a series; crypto detector constants otherwise.
    private static final TimescaleConfig CFG = new TimescaleConfig(3, DetectorConfig.crypto());

    @Test
    void replayEvent_SaturatedIdenticalSeries_GivesBothMissRowWithUnitLevel(@TempDir Path dataDir) {
        // daily bars: one per day across calm + event, identical across symbols.
        List<Instant> dailyTimes = List.of(
                Instant.parse("2024-01-01T00:00:00Z"), Instant.parse("2024-01-02T00:00:00Z"),
                Instant.parse("2024-01-03T00:00:00Z"), Instant.parse("2024-01-04T00:00:00Z"),
                Instant.parse("2024-01-06T00:00:00Z"), Instant.parse("2024-01-07T00:00:00Z"));
        // intraday bars: four 1-minute bars on two calm days and two event days.
        List<Instant> intradayTimes = new java.util.ArrayList<>();
        for (String day : List.of("2024-01-01", "2024-01-02", "2024-01-06", "2024-01-07")) {
            Instant dayStart = Instant.parse(day + "T00:00:00Z");
            for (int m = 0; m < 4; m++) {
                intradayTimes.add(dayStart.plus(m, ChronoUnit.MINUTES));
            }
        }
        for (String symbol : UNIVERSE) {
            writeBars(dataDir, symbol, "1d", dailyTimes);
            writeBars(dataDir, symbol, "1m", intradayTimes);
        }

        LeadTableRow row = CryptoReplay.replayEvent(dataDir, EVENT, UNIVERSE, CFG, CFG);

        assertEquals(3, row.universeSize());
        assertTrue(row.dailyMiss(), "saturated density has no headroom for the level baseline");
        assertTrue(row.intradayMiss());
        assertEquals(1.0, row.dailyL(), 1e-12);     // density is constant at 1.0 -> level gate at 1.0
        assertEquals(1.0, row.intradayL(), 1e-12);
        assertNull(row.leadHours());
        assertNull(row.skipReason());
        assertEquals("2024-01-06 .. 2024-01-07", row.eventWindow());
    }

    private static void writeBars(Path dir, String symbol, String freq, List<Instant> times) {
        StringBuilder csv = new StringBuilder("timestamp,open,high,low,close,volume\n");
        for (int i = 0; i < times.size(); i++) {
            double close = 100.0 + i;   // monotonic: identical across symbols, always changing
            csv.append(times.get(i)).append(",0,0,0,").append(close).append(",1\n");
        }
        try {
            Files.writeString(dir.resolve(symbol + "_" + freq + "_" + EVENT.name() + ".csv"), csv.toString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
