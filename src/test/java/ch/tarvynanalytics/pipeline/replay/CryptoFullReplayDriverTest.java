package ch.tarvynanalytics.pipeline.replay;

import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/**
 * Opt-in full-pipeline regression: reproduces {@code crypto_lead_table.csv} by running the entire
 * {@code CSV → ReturnPanels → S1 → S3 → alert} chain ({@link CryptoReplay}) over the real saved
 * Binance bars, not the committed derived fixture. It is <strong>skipped unless</strong>
 * {@code -Dcrypto.data.dir=/path/to/spike/crypto-data} points at the (gitignored, 2.4 GB) raw data,
 * so CI never runs it — the deterministic in-CI regression is {@link CryptoLeadTableRegressionTest}.
 *
 * <pre>{@code
 * ./mvnw test -Dtest=CryptoFullReplayDriverTest \
 *     -Dcrypto.data.dir=../corrcalc-graphs-research-scratches/spike/crypto-data
 * }</pre>
 */
@EnabledIfSystemProperty(named = "crypto.data.dir", matches = ".+")
class CryptoFullReplayDriverTest {

    static Stream<String> events() {
        return CryptoEvents.ALL.stream().map(CryptoEvent::name);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("events")
    void fullPipelineReproducesCryptoLeadTableRow(String name) {
        Path dataDir = Path.of(System.getProperty("crypto.data.dir"));
        CryptoEvent event = CryptoEvents.byName(name);
        List<String> universe = LeadTableFixtures.universe(name);

        LeadTableRow row = CryptoReplay.replayEvent(dataDir, event, universe);

        LeadTableFixtures.assertMatches(LeadTableFixtures.golden().get(name), row, name);
    }
}
