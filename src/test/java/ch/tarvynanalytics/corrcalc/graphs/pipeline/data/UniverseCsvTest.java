package ch.tarvynanalytics.corrcalc.graphs.pipeline.data;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UniverseCsvTest {

    @Test
    void read_HeaderAndBlankLines_ReturnsOrderedSymbols(@TempDir Path dir) throws IOException {
        Path csv = dir.resolve("u.csv");
        Files.writeString(csv, "symbol\nBTCUSDT\nETHUSDT\n\nXRPUSDT\n");

        assertEquals(List.of("BTCUSDT", "ETHUSDT", "XRPUSDT"), UniverseCsv.read(csv));
    }

    @Test
    void read_NoHeaderJustSymbols_KeepsFirstLine(@TempDir Path dir) throws IOException {
        Path csv = dir.resolve("u.csv");
        Files.writeString(csv, "BTCUSDT\nETHUSDT\n");

        assertEquals(List.of("BTCUSDT", "ETHUSDT"), UniverseCsv.read(csv));
    }

    @Test
    void read_OnlyHeader_Throws(@TempDir Path dir) throws IOException {
        Path csv = dir.resolve("u.csv");
        Files.writeString(csv, "symbol\n");

        assertThrows(IllegalArgumentException.class, () -> UniverseCsv.read(csv));
    }

    @Test
    void read_MissingFile_ThrowsUncheckedIo(@TempDir Path dir) {
        assertThrows(UncheckedIOException.class, () -> UniverseCsv.read(dir.resolve("nope.csv")));
    }
}
