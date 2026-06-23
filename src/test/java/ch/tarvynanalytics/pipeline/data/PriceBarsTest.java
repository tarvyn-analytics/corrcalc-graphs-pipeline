package ch.tarvynanalytics.pipeline.data;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PriceBarsTest {

    private static final String CSV = """
            timestamp,open,high,low,close,volume
            2020-01-04T00:00:00Z,0.03417000,0.03425000,0.03417000,0.03419000,551798.50000000
            2020-01-05T00:01:00Z,0.03417000,0.03420000,0.03417000,0.03420000,17402.30000000
            2020-01-06T00:02:00Z,0.03417000,0.03420000,0.03417000,0.03421000,1000.00000000
            """;

    @Test
    void read_KeepsTimestampAndClose_DropsOhlcv(@TempDir Path dir) throws IOException {
        Path csv = Files.writeString(dir.resolve("ADAUSDT.csv"), CSV);

        List<Bar> bars = PriceBars.read(csv, null, null);

        assertEquals(3, bars.size());
        assertEquals(Instant.parse("2020-01-04T00:00:00Z"), bars.get(0).timestamp());
        assertEquals(0.03419, bars.get(0).close(), 1e-12);   // close is column index 4, not open
        assertEquals(0.03421, bars.get(2).close(), 1e-12);
    }

    @Test
    void read_RestrictsToClosedUtcDateRange(@TempDir Path dir) throws IOException {
        Path csv = Files.writeString(dir.resolve("ADAUSDT.csv"), CSV);

        List<Bar> bars = PriceBars.read(csv, LocalDate.of(2020, 1, 5), LocalDate.of(2020, 1, 5));

        assertEquals(1, bars.size());
        assertEquals(Instant.parse("2020-01-05T00:01:00Z"), bars.get(0).timestamp());
    }

    @Test
    void read_MalformedRow_Throws(@TempDir Path dir) throws IOException {
        Path csv = Files.writeString(dir.resolve("bad.csv"), "timestamp,open,high,low,close,volume\n2020-01-04T00:00:00Z,0.1,0.1\n");
        assertThrows(IllegalArgumentException.class, () -> PriceBars.read(csv, null, null));
    }

    @Test
    void read_MissingFile_ThrowsUnchecked(@TempDir Path dir) {
        assertThrows(java.io.UncheckedIOException.class,
                () -> PriceBars.read(dir.resolve("nope.csv"), null, null));
    }
}
