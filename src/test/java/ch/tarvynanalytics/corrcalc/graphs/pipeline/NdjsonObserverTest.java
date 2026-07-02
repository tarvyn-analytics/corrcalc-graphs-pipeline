package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.engine.RunSummary;
import ch.tarvynanalytics.graphs.algos.model.ChangeMetrics;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NdjsonObserverTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void calibRecord_SerializesCalibrationFields() throws Exception {
        JsonNode n = MAPPER.readTree(
                NdjsonObserver.calibRecord(obs(1.0, 1.0, 0.81, 0.0647, 0.0258, 0.167, 34.0, true)));
        assertEquals("calib", n.get("rec").asText());
        assertEquals(3, n.get("schema").asInt());
        assertEquals("crypto", n.get("market").asText());
        assertEquals("daily", n.get("timescale").asText());
        assertEquals(0.0647, n.get("mu").asDouble(), 1e-12);
        assertEquals(0.0258, n.get("sigma").asDouble(), 1e-12);
        assertEquals(0.167, n.get("levelGate").asDouble(), 1e-12);
        assertEquals(8.0, n.get("threshold").asDouble(), 1e-12);
    }

    @Test
    void obsRecord_FireLine_SerializesSeverityFiredKindAndReasons() throws Exception {
        // mu=0, sigma=1, change=2 -> z=2.0; sPlus=8=h -> activation 1.0 and a CUSUM breach; density/comp saturated
        JsonNode n = MAPPER.readTree(
                NdjsonObserver.obsRecord(obs(1.0, 1.0, 2.0, 0.0, 1.0, 0.167, 8.0, true)));
        assertEquals("obs", n.get("rec").asText());
        assertEquals("2021-02-12T00:00:00Z", n.get("asOf").asText());
        assertEquals("FIRE", n.get("severity").asText());
        assertEquals("FIRED", n.get("lifecycle").asText());
        assertTrue(n.get("fired").asBoolean());
        assertEquals("FUSION", n.get("firedKind").asText());
        assertEquals(1.0, n.get("density").asDouble(), 1e-12);
        assertEquals(2.0, n.get("magnitude").asDouble(), 1e-12);
        assertEquals(2.0, n.get("z").asDouble(), 1e-12);
        assertEquals(1.0, n.get("activation").asDouble(), 1e-12);
        assertTrue(n.get("levelGateOpen").asBoolean());
        assertEquals(List.of("FIRE_FUSION", "MAG_GE_2SIGMA", "LEVEL_GATE_OPEN", "DENSITY_SATURATED",
                "COMPONENTS_COLLAPSED", "CUSUM_BREACH"), reasons(n));
    }

    @Test
    void obsRecord_CarriesRecoveryGauge_AndDigestTracksPeakAndAllClear() throws Exception {
        JsonNode obs = MAPPER.readTree(
                NdjsonObserver.obsRecord(obs(0.4, 0.5, 0.05, 0.05, 0.02, 0.5, 1.0, false)));
        assertEquals(0.5, obs.get("recoveryGauge").asDouble(), 1e-12);   // the helper sets gauge 0.5

        RunDigest d = new RunDigest();
        d.add(fusionAt("2021-05-18T00:00:00Z"));      // first fusion arms the latch
        d.add(allClearAt("2021-05-20T00:00:00Z"));    // de-fusion 48h later
        JsonNode n = MAPPER.readTree(NdjsonObserver.digestRecord(d, new RunSummary(2, 2, 2, 2, 18)));
        assertEquals(0.95, n.get("maxRecoveryGauge").asDouble(), 1e-12);
        assertEquals("2021-05-20T00:00:00Z", n.get("firstAllClearAt").asText());
        assertEquals(48.0, n.get("timeToAllClearHours").asDouble(), 1e-12);
    }

    private static PipelineObservation fusionAt(String at) {
        return new PipelineObservation(Instant.parse(at), "crypto", "intraday",
                new ChangeMetrics(2.0, 1.0, 0.1, 1, 1.0, List.of(2)),
                10.0, 0.0, 0.0, true, SignalKind.FUSION, 8.0, 0.05, 0.02, 0.5, List.of());
    }

    private static PipelineObservation allClearAt(String at) {
        return new PipelineObservation(Instant.parse(at), "crypto", "intraday",
                new ChangeMetrics(0.0001, 0.05, 0.1, 1, 0.2, List.of(2)),
                0.0, 0.0, 0.95, true, SignalKind.DEFUSION, 8.0, 0.05, 0.02, 0.5, List.of());
    }

    @Test
    void obsRecord_NotFired_FiredKindIsNull() throws Exception {
        JsonNode n = MAPPER.readTree(
                NdjsonObserver.obsRecord(obs(0.4, 0.5, 0.05, 0.05, 0.02, 0.5, 1.0, false)));
        assertFalse(n.get("fired").asBoolean());
        assertTrue(n.get("firedKind").isNull(), n.toString());
        assertEquals("CALM", n.get("severity").asText());
    }

    @Test
    void obsRecord_DegenerateOrGappedMetrics_SerializeAsJsonNull() throws Exception {
        // NaN weighted-change (a gap) -> magnitude and z are JSON null, never a NaN token
        JsonNode gap = MAPPER.readTree(
                NdjsonObserver.obsRecord(obs(0.5, 0.5, Double.NaN, 0.0, 1.0, 0.5, 1.0, false)));
        assertTrue(gap.get("magnitude").isNull(), gap.toString());
        assertTrue(gap.get("z").isNull(), gap.toString());
        assertTrue(reasons(gap).contains("DATA_GAP"), gap.toString());
        // sigma <= 0 (degenerate calibration) -> z null even with a finite magnitude
        JsonNode degenerate = MAPPER.readTree(
                NdjsonObserver.obsRecord(obs(0.5, 0.5, 0.1, 0.0, 0.0, 0.5, 1.0, false)));
        assertTrue(degenerate.get("z").isNull(), degenerate.toString());
        assertEquals(0.1, degenerate.get("magnitude").asDouble(), 1e-12);
    }

    @Test
    void obsRecord_RoundTripsStringSpecials() throws Exception {
        // quote, backslash, newline, CR, tab and a control char; built from char codes (no escape runs)
        String special = "q\"" + (char) 92 + (char) 10 + (char) 13 + (char) 9 + (char) 8 + "c";
        JsonNode n = MAPPER.readTree(
                NdjsonObserver.obsRecord(obs(special, "daily", 0.5, 0.5, 0.1, 0.0, 1.0, 0.5, 1.0, false)));
        assertEquals(special, n.get("market").asText());
    }

    @Test
    void onObservation_EmitsCalibHeaderOnceThenObsPerTransition_AndRejectsNull() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        NdjsonObserver observer = new NdjsonObserver(new PrintStream(bos, true, StandardCharsets.UTF_8));

        observer.onObservation(obs(1.0, 1.0, 2.0, 0.0, 1.0, 0.167, 8.0, true));
        observer.onObservation(obs(0.4, 0.5, 0.05, 0.05, 0.02, 0.5, 1.0, false));

        String[] lines = bos.toString(StandardCharsets.UTF_8).split("\n");
        assertEquals(3, lines.length, bos.toString(StandardCharsets.UTF_8));
        assertEquals("calib", MAPPER.readTree(lines[0]).get("rec").asText());
        assertEquals("obs", MAPPER.readTree(lines[1]).get("rec").asText());
        assertEquals("obs", MAPPER.readTree(lines[2]).get("rec").asText());
        assertThrows(IllegalArgumentException.class, () -> observer.onObservation(null));
    }

    @Test
    void configRecord_SerializesStaticConfigAndProvenance() throws Exception {
        JsonNode n = MAPPER.readTree(NdjsonObserver.configRecord(new RunContext(
                "crypto", "daily", "replay", new ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationProvenance("leading-warmup", 0L, null, null), 14, 0.5, 1.5, 8.0, 99.0, "UPPER", 18, 60.0)));
        assertEquals("config", n.get("rec").asText());
        assertEquals("replay", n.get("mode").asText());
        assertEquals("leading-warmup", n.get("calibration").asText());
        assertEquals(14, n.get("window").asInt());
        assertEquals(0.5, n.get("edgeThreshold").asDouble(), 1e-12);
        assertEquals(1.5, n.get("cusumK").asDouble(), 1e-12);
        assertEquals(8.0, n.get("decisionInterval").asDouble(), 1e-12);
        assertEquals(99.0, n.get("levelPctile").asDouble(), 1e-12);
        assertEquals("UPPER", n.get("fireArm").asText());
        assertEquals(18, n.get("calmBars").asInt());
    }

    @Test
    void onStart_EmitsConfigLineBeforeCalibAndRejectsNull() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        NdjsonObserver observer = new NdjsonObserver(new PrintStream(bos, true, StandardCharsets.UTF_8));
        observer.onStart(new RunContext(
                "crypto", "daily", "replay", new ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationProvenance("leading-warmup", 0L, null, null), 14, 0.5, 1.5, 8.0, 99.0, "UPPER", 18, 60.0));
        observer.onObservation(obs(1.0, 1.0, 2.0, 0.0, 1.0, 0.167, 8.0, true));

        String[] lines = bos.toString(StandardCharsets.UTF_8).split("\n");
        assertEquals("config", MAPPER.readTree(lines[0]).get("rec").asText());
        assertEquals("calib", MAPPER.readTree(lines[1]).get("rec").asText());
        assertEquals("obs", MAPPER.readTree(lines[2]).get("rec").asText());
        assertThrows(IllegalArgumentException.class, () -> observer.onStart(null));
    }

    @Test
    void digestRecord_FoldsTheRunIntoOneObject() throws Exception {
        RunDigest d = new RunDigest();
        d.add(obs(1.0, 1.0, 2.0, 0.0, 1.0, 0.167, 8.0, true));    // FIRE, z=2.0, saturated
        d.add(obs(0.4, 0.5, 0.05, 0.0, 1.0, 0.5, 1.0, false));    // CALM
        JsonNode n = MAPPER.readTree(NdjsonObserver.digestRecord(d, new RunSummary(2, 1, 1, 2, 18)));

        assertEquals("digest", n.get("rec").asText());
        assertEquals("crypto", n.get("market").asText());
        assertEquals(2, n.get("detectionPoints").asInt());
        assertEquals(1, n.get("fires").asInt());
        assertEquals(18, n.get("calmBars").asInt());
        assertEquals(2, n.get("observed").asInt());
        assertEquals(1, n.get("bySeverity").get("FIRE").asInt());
        assertEquals(1, n.get("bySeverity").get("CALM").asInt());
        assertEquals(2.0, n.get("maxAbsZ").asDouble(), 1e-12);
        assertEquals(1.0, n.get("maxActivation").asDouble(), 1e-12);
        assertEquals(1, n.get("timeInFused").asInt());
        assertEquals(2.0, n.get("biggestMove").get("magnitude").asDouble(), 1e-12);
        assertEquals("2021-02-12T00:00:00Z", n.get("biggestMove").get("asOf").asText());
    }

    @Test
    void onComplete_EmitsDigestLineAndRejectsNull() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        NdjsonObserver observer = new NdjsonObserver(new PrintStream(bos, true, StandardCharsets.UTF_8));
        observer.onObservation(obs(1.0, 1.0, 2.0, 0.0, 1.0, 0.167, 8.0, true));
        observer.onComplete(new RunSummary(1, 1, 1, 1, 18));

        String[] lines = bos.toString(StandardCharsets.UTF_8).split("\n");
        assertEquals("digest", MAPPER.readTree(lines[lines.length - 1]).get("rec").asText());
        assertThrows(IllegalArgumentException.class, () -> observer.onComplete(null));
    }

    @Test
    void constructor_RejectsNullStream() {
        assertThrows(IllegalArgumentException.class, () -> new NdjsonObserver(null));
    }

    private static List<String> reasons(JsonNode obs) {
        List<String> out = new ArrayList<>();
        obs.get("reasons").forEach(j -> out.add(j.asText()));
        return out;
    }

    private static PipelineObservation obs(double density, double largestFraction, double weightedChange,
                                           double mu, double sigma, double level, double sPlus, boolean fired) {
        return obs("crypto", "daily", density, largestFraction, weightedChange, mu, sigma, level, sPlus, fired);
    }

    private static PipelineObservation obs(String market, String timescale, double density, double largestFraction,
                                           double weightedChange, double mu, double sigma, double level,
                                           double sPlus, boolean fired) {
        ChangeMetrics m = new ChangeMetrics(weightedChange, density, 0.1, 1, largestFraction, List.of(2));
        return new PipelineObservation(Instant.parse("2021-02-12T00:00:00Z"), market, timescale,
                m, sPlus, 0.0, 0.5, fired, fired ? SignalKind.FUSION : null, 8.0, mu, sigma, level, List.of());
    }

    private static PipelineObservation obsWithContributors(List<PairContribution> contributors) {
        ChangeMetrics m = new ChangeMetrics(2.0, 1.0, 0.1, 1, 1.0, List.of(2));
        return new PipelineObservation(Instant.parse("2021-02-12T00:00:00Z"), "crypto", "daily",
                m, 8.0, 0.0, 0.5, true, SignalKind.FUSION, 8.0, 0.0, 1.0, 0.167, contributors);
    }

    @Test
    void obsRecord_SerializesContributors_AsLabelledPairArray() throws Exception {
        JsonNode n = MAPPER.readTree(NdjsonObserver.obsRecord(obsWithContributors(List.of(
                new PairContribution("ETH", "BNB", 0.42),
                new PairContribution("BTC", "LTC", 0.30)))));
        JsonNode arr = n.get("contributors");
        assertEquals(2, arr.size());
        assertEquals("ETH", arr.get(0).get("a").asText());
        assertEquals("BNB", arr.get(0).get("b").asText());
        assertEquals(0.42, arr.get(0).get("absDelta").asDouble(), 1e-12);
        assertEquals("BTC", arr.get(1).get("a").asText());
        assertEquals("LTC", arr.get(1).get("b").asText());
    }

    @Test
    void obsRecord_NoContributors_IsEmptyArrayNotMissing() throws Exception {
        JsonNode n = MAPPER.readTree(NdjsonObserver.obsRecord(obs(0.4, 0.5, 0.05, 0.05, 0.02, 0.5, 1.0, false)));
        assertTrue(n.get("contributors").isArray(), n.toString());
        assertEquals(0, n.get("contributors").size());
    }

    @Test
    void digestRecord_BiggestMove_CarriesItsContributors() throws Exception {
        RunDigest d = new RunDigest();
        d.add(obsWithContributors(List.of(new PairContribution("ETH", "BNB", 0.42))));
        JsonNode n = MAPPER.readTree(NdjsonObserver.digestRecord(d, new RunSummary(1, 1, 1, 1, 18)));
        JsonNode arr = n.get("biggestMove").get("contributors");
        assertEquals(1, arr.size());
        assertEquals("ETH", arr.get(0).get("a").asText());
        assertEquals(0.42, arr.get(0).get("absDelta").asDouble(), 1e-12);
    }

    @Test
    void calibEventRecord_SerializesKindEpochAndBeforeAfter() throws Exception {
        JsonNode n = MAPPER.readTree(NdjsonObserver.calibEventRecord(new CalibrationEvent(
                CalibrationEventKind.RECALIBRATED, 2, 0.0100, 0.0132, 0.0043, 0.0051,
                Instant.parse("2021-05-19T13:00:00Z"))));
        assertEquals("calibEvent", n.get("rec").asText());
        assertEquals(3, n.get("schema").asInt());
        assertEquals("2021-05-19T13:00:00Z", n.get("asOf").asText());
        assertEquals("RECALIBRATED", n.get("kind").asText());
        assertEquals(2, n.get("epochId").asLong());
        assertEquals(0.0100, n.get("muBefore").asDouble(), 1e-12);
        assertEquals(0.0132, n.get("muAfter").asDouble(), 1e-12);
        assertEquals(0.0043, n.get("sigmaBefore").asDouble(), 1e-12);
        assertEquals(0.0051, n.get("sigmaAfter").asDouble(), 1e-12);
    }

    @Test
    void calibEventRecord_PromotionWithoutPriorBaseline_BeforeFieldsAreJsonNull() throws Exception {
        JsonNode n = MAPPER.readTree(NdjsonObserver.calibEventRecord(new CalibrationEvent(
                CalibrationEventKind.PROMOTED_TO_LIVE, 0, Double.NaN, 0.05, Double.NaN, 0.02,
                Instant.parse("2021-02-10T00:00:00Z"))));
        assertTrue(n.get("muBefore").isNull(), n.toString());
        assertTrue(n.get("sigmaBefore").isNull(), n.toString());
        assertEquals(0.05, n.get("muAfter").asDouble(), 1e-12);
    }

    @Test
    void onCalibrationEvent_EmitsOneCalibEventLine_AndRejectsNull() throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        NdjsonObserver observer = new NdjsonObserver(new PrintStream(bos, true, StandardCharsets.UTF_8));

        observer.onCalibrationEvent(new CalibrationEvent(
                CalibrationEventKind.EPOCH_OPENED, 1, 0.01, 0.01, 0.004, 0.006,
                Instant.parse("2021-05-19T13:00:00Z")));

        String[] lines = bos.toString(StandardCharsets.UTF_8).split("\n");
        assertEquals(1, lines.length, bos.toString(StandardCharsets.UTF_8));
        assertEquals("calibEvent", MAPPER.readTree(lines[0]).get("rec").asText());
        assertThrows(IllegalArgumentException.class, () -> observer.onCalibrationEvent(null));
    }

    @Test
    void configRecord_CarriesEpochProvenance() throws Exception {
        JsonNode n = MAPPER.readTree(NdjsonObserver.configRecord(new RunContext(
                "crypto", "intraday", "replay",
                new ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationProvenance(
                        "adaptive", 2L,
                        Instant.parse("2021-01-01T00:00:00Z"), Instant.parse("2021-01-15T00:00:00Z")),
                14, 0.5, 1.5, 8.0, 99.0, "UPPER", 18, 60.0)));
        assertEquals("adaptive", n.get("calibration").asText());
        assertEquals(2, n.get("epochId").asLong());
        assertEquals("2021-01-01T00:00:00Z", n.get("calibSourceFrom").asText());
        assertEquals("2021-01-15T00:00:00Z", n.get("calibSourceTo").asText());
    }

    @Test
    void configRecord_UnknownSourceWindow_SerializesAsJsonNull() throws Exception {
        JsonNode n = MAPPER.readTree(NdjsonObserver.configRecord(new RunContext(
                "crypto", "daily", "replay",
                new ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationProvenance("leading-warmup", 0L, null, null),
                14, 0.5, 1.5, 8.0, 99.0, "UPPER", 18, 60.0)));
        assertEquals(0, n.get("epochId").asLong());
        assertTrue(n.get("calibSourceFrom").isNull(), n.toString());
        assertTrue(n.get("calibSourceTo").isNull(), n.toString());
    }

    @Test
    void digestRecord_FoldsLifecycleCountsAndMuJourney() throws Exception {
        RunDigest d = new RunDigest();
        d.addCalibrationEvent(new CalibrationEvent(CalibrationEventKind.RECALIBRATED, 1,
                0.0100, 0.0132, 0.0043, 0.0051, Instant.parse("2021-05-19T13:00:00Z")));
        d.addCalibrationEvent(new CalibrationEvent(CalibrationEventKind.EPOCH_OPENED, 2,
                0.0132, 0.0140, 0.0051, 0.0060, Instant.parse("2021-06-01T00:00:00Z")));
        JsonNode n = MAPPER.readTree(NdjsonObserver.digestRecord(d, new RunSummary(1, 1, 1, 1, 18)));
        assertEquals(2, n.get("epochsOpened").asLong());
        assertEquals(1, n.get("recalibrations").asLong());
        assertEquals(0.0100, n.get("muJourney").get("from").asDouble(), 1e-12);
        assertEquals(0.0140, n.get("muJourney").get("to").asDouble(), 1e-12);
    }

    @Test
    void digestRecord_NoLifecycleEvents_MuJourneyIsJsonNull() throws Exception {
        RunDigest d = new RunDigest();
        d.add(obs(0.4, 0.5, 0.05, 0.05, 0.02, 0.5, 1.0, false));
        JsonNode n = MAPPER.readTree(NdjsonObserver.digestRecord(d, new RunSummary(1, 1, 1, 1, 18)));
        assertEquals(0, n.get("epochsOpened").asLong());
        assertEquals(0, n.get("recalibrations").asLong());
        assertTrue(n.get("muJourney").isNull(), n.toString());
    }
}
