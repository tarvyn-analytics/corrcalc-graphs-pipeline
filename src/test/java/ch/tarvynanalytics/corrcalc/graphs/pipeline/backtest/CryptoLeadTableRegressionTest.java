package ch.tarvynanalytics.corrcalc.graphs.pipeline.backtest;

import ch.tarvynanalytics.graphs.algos.Calibration;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * The n=8 end-to-end regression: the pipeline reproduces the spike's
 * {@code crypto_lead_table.csv} bit-for-bit using the density-level baseline detector. Driven by a
 * compact committed fixture — per event/timescale the calm calibration {@code (μ, σ, L)} plus the
 * event-window {@code (epoch_second, n_edges)} series — exported by
 * an offline exporter over the reference Python implementation (the 2.4 GB of raw
 * bars stays out of git). Because the CUSUM resets at the event-window start and density
 * {@code = n_edges/|P|} is integer-exact vs the Python engine, the calibration plus the event-window
 * slice fully determine every column. The full-pipeline reproduction from raw bars lives in the
 * opt-in {@link CryptoFullBacktestDriverTest}.
 */
class CryptoLeadTableRegressionTest {

    private static final Map<String, Manifest> MANIFEST = loadManifest();
    private static final Map<String, LeadTableFixtures.GoldenRow> GOLDEN = LeadTableFixtures.golden();

    static Stream<String> events() {
        return CryptoEvents.ALL.stream().map(CryptoEvent::name);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("events")
    void reproducesCryptoLeadTableRow(String name) {
        Manifest manifest = MANIFEST.get(name);
        CryptoEvent event = CryptoEvents.byName(name);

        TimescaleScoring daily = loadScoring(name, "daily", manifest.dailyNpairs, manifest.dailyCal);
        TimescaleScoring intraday = loadScoring(name, "intraday", manifest.intradayNpairs, manifest.intradayCal);

        LeadTableRow row = EventScorer.score(event, manifest.universeSize, daily, intraday, DetectorConfig.crypto());

        LeadTableFixtures.assertMatches(GOLDEN.get(name), row, name);
    }

    private TimescaleScoring loadScoring(String event, String timescale, int npairs, Calibration cal) {
        List<String> lines = LeadTableFixtures.resourceLines("/fixtures/" + event + "_" + timescale + ".csv");
        List<Instant> timestamps = new ArrayList<>();
        List<Double> density = new ArrayList<>();
        for (int i = 1; i < lines.size(); i++) {   // skip header
            if (lines.get(i).isBlank()) {
                continue;
            }
            String[] parts = lines.get(i).split(",");
            timestamps.add(Instant.ofEpochSecond(Long.parseLong(parts[0].trim())));
            density.add((double) Long.parseLong(parts[1].trim()) / npairs);
        }
        double[] densityArr = new double[density.size()];
        double[] changeArr = new double[density.size()];   // unused by the baseline scorer
        for (int i = 0; i < densityArr.length; i++) {
            densityArr[i] = density.get(i);
            changeArr[i] = Double.NaN;
        }
        return new TimescaleScoring(cal, List.copyOf(timestamps), densityArr, changeArr);
    }

    private static Map<String, Manifest> loadManifest() {
        Map<String, Manifest> map = new HashMap<>();
        List<String> lines = LeadTableFixtures.resourceLines("/fixtures/manifest.csv");
        LeadTableFixtures.Header header = new LeadTableFixtures.Header(lines.get(0));
        for (int i = 1; i < lines.size(); i++) {
            if (lines.get(i).isBlank()) {
                continue;
            }
            String[] f = lines.get(i).split(",", -1);
            map.put(f[header.idx("event")], new Manifest(
                    Integer.parseInt(f[header.idx("universe_size")]),
                    Integer.parseInt(f[header.idx("daily_npairs")]),
                    new Calibration(parseD(f[header.idx("daily_mu")]), parseD(f[header.idx("daily_sigma")]),
                            parseD(f[header.idx("daily_level")])),
                    Integer.parseInt(f[header.idx("intraday_npairs")]),
                    new Calibration(parseD(f[header.idx("intraday_mu")]), parseD(f[header.idx("intraday_sigma")]),
                            parseD(f[header.idx("intraday_level")]))));
        }
        return map;
    }

    private static double parseD(String s) {
        return Double.parseDouble(s.trim());
    }

    private record Manifest(int universeSize, int dailyNpairs, Calibration dailyCal,
                            int intradayNpairs, Calibration intradayCal) {
    }
}
