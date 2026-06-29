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
        assertEquals(1, n.get("schema").asInt());
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
                "crypto", "daily", "replay", "leading-warmup", 14, 0.5, 1.5, 8.0, 99.0, "UPPER", 18, 60.0)));
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
                "crypto", "daily", "replay", "leading-warmup", 14, 0.5, 1.5, 8.0, 99.0, "UPPER", 18, 60.0));
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
                m, sPlus, 0.0, fired, fired ? SignalKind.FUSION : null, 8.0, mu, sigma, level);
    }
}
