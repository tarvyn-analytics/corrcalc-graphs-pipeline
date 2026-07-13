package ch.tarvynanalytics.corrcalc.graphs.pipeline.backtest;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.cli.PipelineCli;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Opt-in acceptance tape test: adaptive cold-start through the replay CLI, judged with
 * <strong>per-question semantics</strong> — each FUSION opens a regime question that exactly one
 * of three things closes: its own DEFUSION all-clear (answered), the next FUSION after a re-arm
 * (moved on), or the calendar-backstop expiry (expired unresolved). The replay INCLUDES the calm
 * lead-in (the whole point of adaptive is learning it online), so lead-in turbulence opens and
 * closes questions of its own — a whole-replay "did any DEFUSION happen" verdict is meaningless
 * here, and per-event recovery <em>equality</em> with {@code DefusionGaugeRecoveryTapeTest} is
 * structurally unmeasurable: the reference arms its gauge at the in-event <em>peak</em>-fusion
 * sample, while the engine anchors at its own (often precursor) first fire — the
 * {@code 2026-07-01} analysis' early-anchor gap. Tape-level recovery sanity belongs to RUN-1 (the
 * continuous multi-year replay), per the ROADMAP acceptance.
 *
 * <p>What this test pins:</p>
 * <ol>
 *   <li><strong>Detection:</strong> every event fires an event-anchored FUSION under adaptive
 *       cold-start (± the lead-table's intraday lead).</li>
 *   <li><strong>Specificity:</strong> may2021's event question is never answered by an all-clear
 *       (the headline suppression result).</li>
 *   <li><strong>Sensitivity of the mechanics:</strong> the adaptive-calibrated gauge resolves
 *       honest fusion→all-clear cycles timely (the walk-forward reference's ~48 h–2.5 d lag; ≤ 10 d bar) across the suite.</li>
 *   <li><strong>The latch invariant:</strong> no DEFUSION without a pending FUSION (FA safety).</li>
 * </ol>
 *
 * <p><strong>Skipped unless</strong> {@code -Dcrypto.data.dir=...} points at the (gitignored) raw
 * bars.</p>
 *
 * <pre>{@code
 * ./mvnw test -Dtest=AdaptiveCliTapeTest \
 *     -Dcrypto.data.dir=/path/to/crypto-data
 * }</pre>
 */
@EnabledIfSystemProperty(named = "crypto.data.dir", matches = ".+")
class AdaptiveCliTapeTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Events with truncated tapes: {@code yen_carry_unwind_2024} is data-thin (WAVES delists
     * mid-recovery — both reference tests skip its recovery), so only "a FUSION fires somewhere"
     * is asserted, exactly as before.
     */
    private static final Set<String> DATA_THIN = Set.of("yen_carry_unwind_2024");

    /** The intraday lead allowance around the lead-table dates when anchoring the event's fusion. */
    private static final Duration EVENT_LEAD = Duration.ofDays(7);
    private static final Duration EVENT_TAIL = Duration.ofDays(3);

    /** Honest all-clears land ~48 h–2.5 d after their fusion (walk-forward reference); ≤ 10 d is the bar. */
    private static final Duration TIMELY = Duration.ofDays(10);

    @TempDir
    Path work;

    @Test
    void adaptiveCli_ColdStart_PerQuestionVerdictsHold() throws IOException {
        Path dataDir = Path.of(System.getProperty("crypto.data.dir"));
        int timelyCycles = 0;

        for (CryptoEvent event : CryptoEvents.ALL) {
            List<Fire> fires = replayThroughCli(dataDir, event);
            if (fires == null) {
                System.out.printf("%-24s SKIP (data-thin)%n", event.name());
                continue;
            }
            assertLatchAlternation(event.name(), fires);
            timelyCycles += timelyResolvedCycles(fires);

            Fire eventFusion = eventFusion(fires, event);
            boolean answered = eventFusion != null && questionAnswered(fires, eventFusion);
            System.out.printf("%-24s eventFusion=%s  perQuestionAllClear=%s  fires=%d%n",
                    event.name(), eventFusion == null ? "none" : eventFusion.asOf(), answered, fires.size());

            if (DATA_THIN.contains(event.name())) {
                assertTrue(fires.stream().anyMatch(Fire::fusion),
                        event.name() + " (data-thin) should still fire a FUSION somewhere");
                continue;
            }
            assertNotNull(eventFusion,
                    event.name() + " should fire an event-anchored FUSION under adaptive cold-start");
            if (event.name().equals("may2021_selloff")) {
                assertFalse(answered,
                        "may2021 (choppy non-recovery): its regime question must never be answered");
            }
        }

        assertTrue(timelyCycles >= 5,
                "the adaptive-calibrated gauge should resolve honest cycles timely (got " + timelyCycles + ")");
    }

    /** One fired signal in stream order: a FUSION opens a question, a DEFUSION answers the open one. */
    private record Fire(Instant asOf, boolean fusion) {
    }

    /** The first FUSION inside the event's lead-table window ± the intraday lead allowance. */
    private static Fire eventFusion(List<Fire> fires, CryptoEvent event) {
        Instant from = event.start().atStartOfDay(ZoneOffset.UTC).toInstant().minus(EVENT_LEAD);
        Instant to = event.end().plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant().plus(EVENT_TAIL);
        return fires.stream()
                .filter(Fire::fusion)
                .filter(f -> !f.asOf().isBefore(from) && f.asOf().isBefore(to))
                .findFirst().orElse(null);
    }

    /**
     * Whether the question this fusion opened was answered: with {@code --observe fires} the very
     * next fired signal after a FUSION is either its DEFUSION (answered) or the next FUSION after
     * a re-arm (moved on — relaxation or backstop expiry; neither emits a fired signal).
     */
    private static boolean questionAnswered(List<Fire> fires, Fire fusion) {
        int i = fires.indexOf(fusion) + 1;
        return i < fires.size() && !fires.get(i).fusion();
    }

    /** Counts answered questions whose all-clear landed within the {@link #TIMELY} bar. */
    private static int timelyResolvedCycles(List<Fire> fires) {
        int cycles = 0;
        for (int i = 0; i + 1 < fires.size(); i++) {
            if (fires.get(i).fusion() && !fires.get(i + 1).fusion()
                    && Duration.between(fires.get(i).asOf(), fires.get(i + 1).asOf()).compareTo(TIMELY) <= 0) {
                cycles++;
            }
        }
        return cycles;
    }

    /** The FA-safety latch: no all-clear before the first fusion, never two answers to one question. */
    private static void assertLatchAlternation(String name, List<Fire> fires) {
        boolean pending = false;
        for (Fire fire : fires) {
            if (fire.fusion()) {
                pending = true;
            } else {
                assertTrue(pending, name + ": DEFUSION at " + fire.asOf() + " without a pending FUSION");
                pending = false;
            }
        }
    }

    /**
     * Replays calm window + event + recovery through the CLI in one continuous adaptive run and
     * returns the fired signals in stream order, or {@code null} when the tape is data-thin on
     * this machine (the same skip the calm-block test takes).
     */
    private List<Fire> replayThroughCli(Path dataDir, CryptoEvent event) throws IOException {
        List<String> universe = LeadTableFixtures.universe(event.name());
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
                "--calibration", "adaptive",
                "--from", event.calmStart().toString(),
                "--to", event.end().plusDays(70).toString()},
                new PrintStream(stdout, true, StandardCharsets.UTF_8),
                new PrintStream(stderr, true, StandardCharsets.UTF_8));
        String errText = stderr.toString(StandardCharsets.UTF_8);
        if (code != 0 && errText.contains("series too short")) {
            return null;   // data-thin on this machine — the same skip the calm-block test takes
        }
        assertEquals(0, code, errText);
        return parseFires(stdout.toString(StandardCharsets.UTF_8));
    }

    private static List<Fire> parseFires(String ndjson) throws IOException {
        List<Fire> fires = new ArrayList<>();
        for (String line : ndjson.split("\n")) {
            if (!line.startsWith("{")) {
                continue;
            }
            JsonNode n = MAPPER.readTree(line);
            if ("obs".equals(n.path("rec").asText()) && n.hasNonNull("firedKind")) {
                fires.add(new Fire(Instant.parse(n.get("asOf").asText()),
                        "FUSION".equals(n.get("firedKind").asText())));
            }
        }
        return fires;
    }
}
