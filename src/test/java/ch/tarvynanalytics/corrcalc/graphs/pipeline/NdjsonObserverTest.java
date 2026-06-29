package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import ch.tarvynanalytics.graphs.algos.model.ChangeMetrics;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NdjsonObserverTest {

    @Test
    void calibRecord_SerializesCalibrationFields() {
        String rec = NdjsonObserver.calibRecord(obs(1.0, 1.0, 0.81, 0.0647, 0.0258, 0.167, 34.0, true));
        assertEquals("{\"rec\":\"calib\",\"schema\":1,\"market\":\"crypto\",\"timescale\":\"daily\","
                + "\"mu\":0.0647,\"sigma\":0.0258,\"levelGate\":0.167,\"threshold\":8.0}", rec);
    }

    @Test
    void obsRecord_FireLine_SerializesSeverityFiredKindAndReasons() {
        // mu=0, sigma=1, change=2 -> z=2.0; sPlus=8=h -> activation 1.0 and a CUSUM breach; density/comp saturated
        String rec = NdjsonObserver.obsRecord(obs(1.0, 1.0, 2.0, 0.0, 1.0, 0.167, 8.0, true));
        assertEquals("{\"rec\":\"obs\",\"asOf\":\"2021-02-12T00:00:00Z\",\"market\":\"crypto\","
                + "\"timescale\":\"daily\",\"severity\":\"FIRE\",\"fired\":true,\"firedKind\":\"FUSION\","
                + "\"density\":1.0,\"magnitude\":2.0,\"z\":2.0,\"activation\":1.0,\"sPlus\":8.0,"
                + "\"sMinus\":0.0,\"largestComponent\":1.0,\"levelGateOpen\":true,"
                + "\"reasons\":[\"FIRE_FUSION\",\"MAG_GE_2SIGMA\",\"LEVEL_GATE_OPEN\",\"DENSITY_SATURATED\","
                + "\"COMPONENTS_COLLAPSED\",\"CUSUM_BREACH\"]}", rec);
    }

    @Test
    void obsRecord_NotFired_FiredKindIsNull() {
        String rec = NdjsonObserver.obsRecord(obs(0.4, 0.5, 0.05, 0.05, 0.02, 0.5, 1.0, false));
        assertTrue(rec.contains("\"fired\":false"), rec);
        assertTrue(rec.contains("\"firedKind\":null"), rec);
        assertTrue(rec.contains("\"severity\":\"CALM\""), rec);
    }

    @Test
    void obsRecord_DegenerateOrGappedMetrics_SerializeAsJsonNull() {
        // NaN weighted-change (a gap) -> magnitude and z are null, never "NaN"
        String rec = NdjsonObserver.obsRecord(obs(0.5, 0.5, Double.NaN, 0.0, 1.0, 0.5, 1.0, false));
        assertTrue(rec.contains("\"magnitude\":null"), rec);
        assertTrue(rec.contains("\"z\":null"), rec);
        // sigma <= 0 (degenerate calibration) -> z null even with a finite magnitude
        String degenerate = NdjsonObserver.obsRecord(obs(0.5, 0.5, 0.1, 0.0, 0.0, 0.5, 1.0, false));
        assertTrue(degenerate.contains("\"z\":null"), degenerate);
        assertTrue(degenerate.contains("\"magnitude\":0.1"), degenerate);
    }

    @Test
    void obsRecord_EscapesJsonStringSpecials() {
        // one of every escaped class: quote, backslash, newline, CR, tab, a non-special control char
        // (backspace -> ), then a plain char. Built from char codes so the source has no escape runs.
        String bs = String.valueOf((char) 92);
        String special = "q\"" + bs + (char) 10 + (char) 13 + (char) 9 + (char) 8 + "c";
        String rec = NdjsonObserver.obsRecord(obs(special, "daily", 0.5, 0.5, 0.1, 0.0, 1.0, 0.5, 1.0, false));
        String expected = "\"market\":\"q" + bs + "\"" + bs + bs + bs + "n" + bs + "r" + bs + "t" + bs + "u0008c\"";
        assertTrue(rec.contains(expected), rec);
    }

    @Test
    void onObservation_EmitsCalibHeaderOnceThenObsPerTransition_AndRejectsNull() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        NdjsonObserver observer = new NdjsonObserver(new PrintStream(bos, true, StandardCharsets.UTF_8));

        observer.onObservation(obs(1.0, 1.0, 2.0, 0.0, 1.0, 0.167, 8.0, true));
        observer.onObservation(obs(0.4, 0.5, 0.05, 0.05, 0.02, 0.5, 1.0, false));

        String[] lines = bos.toString(StandardCharsets.UTF_8).split("\n");
        assertEquals(3, lines.length, bos.toString(StandardCharsets.UTF_8));
        assertTrue(lines[0].startsWith("{\"rec\":\"calib\""), lines[0]);
        assertTrue(lines[1].contains("\"rec\":\"obs\""), lines[1]);
        assertTrue(lines[2].contains("\"rec\":\"obs\""), lines[2]);
        assertThrows(IllegalArgumentException.class, () -> observer.onObservation(null));
    }

    @Test
    void constructor_RejectsNullStream() {
        assertThrows(IllegalArgumentException.class, () -> new NdjsonObserver(null));
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
