package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.engine.RunSummary;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.PrintStream;
import java.util.List;

/**
 * The structured machine-readable {@link PipelineObserver} — the {@code --style ndjson} counterpart to
 * the human {@link ReadableObserver}. It emits one JSON object per line (newline-delimited JSON): a
 * one-time {@code calib} record carrying what "normal" was, then one {@code obs} record per scored
 * transition carrying the same facts the human renderers show ({@link PipelineObservation#severity()
 * severity}, {@link PipelineObservation#zScore() z}, {@link PipelineObservation#activation() activation},
 * the {@link ReasonCode} decision-trace, the gate state). This is the primary product stream and the
 * stable contract a downstream visualization/exporter (S5) consumes; the human logs are a thin
 * formatter over the very same {@link PipelineObservation} facts.
 *
 * <p>Records are written verbatim to the injected {@link PrintStream} (the CLI passes {@code stdout}),
 * <em>not</em> through SLF4J — logback's level/timestamp/colour pattern would corrupt the stream. The
 * CLI routes its diagnostic logging to {@code stderr} in this mode so the {@code stdout} stream pipes
 * clean (data on {@code stdout}, logs on {@code stderr}). Serialization uses Jackson: this consumer/edge
 * module is the place dependencies are allowed (the asset-agnostic libraries never see the stream), so
 * the wire format leans on the standard JSON library rather than bespoke code. Non-finite doubles
 * ({@link Double#NaN}/±∞, e.g. {@code z} on a degenerate calibration) serialize as JSON {@code null}.
 * Single-writer, like the engine that drives it (the {@code headerShown} latch is not synchronised).</p>
 */
public final class NdjsonObserver implements PipelineObserver {

    /** The record-schema version; bump on any incompatible change to the emitted fields. */
    public static final int SCHEMA = 2;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PrintStream out;
    private final RunDigest digest = new RunDigest();
    private boolean headerShown;

    /**
     * @param out the stream each NDJSON record line is written to (the CLI passes {@code stdout})
     */
    public NdjsonObserver(PrintStream out) {
        if (out == null) {
            throw new IllegalArgumentException("out must not be null");
        }
        this.out = out;
    }

    @Override
    public void onStart(RunContext context) {
        if (context == null) {
            throw new IllegalArgumentException("context must not be null");
        }
        emit(configRecord(context));
    }

    @Override
    public void onObservation(PipelineObservation observation) {
        if (observation == null) {
            throw new IllegalArgumentException("observation must not be null");
        }
        if (!headerShown) {
            emit(calibRecord(observation));
            headerShown = true;
        }
        digest.add(observation);
        emit(obsRecord(observation));
    }

    @Override
    public void onComplete(RunSummary summary) {
        if (summary == null) {
            throw new IllegalArgumentException("summary must not be null");
        }
        emit(digestRecord(digest, summary));
    }

    private void emit(String line) {
        out.print(line);
        out.print('\n');
        out.flush();
    }

    /**
     * The leading {@code config} record — the static run context (config + provenance) known before the
     * stream, the machine counterpart of {@link ReadableObserver#configBanner}. The <em>learned</em>
     * calibration (μ, σ, L) is not here; it arrives in the {@code calib} record with the first observation.
     *
     * @param c the run context
     * @return the config record as a single JSON object
     */
    public static String configRecord(RunContext c) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("rec", "config");
        n.put("schema", SCHEMA);
        n.put("market", c.market());
        n.put("timescale", c.timescale());
        n.put("mode", c.mode());
        n.put("calibration", c.calibration().calibrationMode());
        n.put("window", c.window());
        putNum(n, "edgeThreshold", c.edgeThreshold());
        putNum(n, "cusumK", c.cusumK());
        putNum(n, "decisionInterval", c.decisionInterval());
        putNum(n, "levelPctile", c.levelPctile());
        n.put("fireArm", c.fireArm());
        n.put("calmBars", c.calmBars());
        putNum(n, "speed", c.speed());
        return n.toString();
    }

    /**
     * The one-time {@code calib} record — what "normal" was, read off the first observation's carried
     * calibration (the machine counterpart of {@link ReadableObserver#calibrationBanner}).
     *
     * @param o any post-calibration observation
     * @return the calibration record as a single JSON object
     */
    public static String calibRecord(PipelineObservation o) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("rec", "calib");
        n.put("schema", SCHEMA);
        n.put("market", o.market());
        n.put("timescale", o.timescale());
        putNum(n, "mu", o.calmMu());
        putNum(n, "sigma", o.calmSigma());
        putNum(n, "levelGate", o.levelGate());
        putNum(n, "threshold", o.decisionThreshold());
        return n.toString();
    }

    /**
     * One {@code obs} record — a self-contained serialization of every fact this transition carries.
     *
     * @param o the observation to serialize
     * @return the observation record as a single JSON object
     */
    public static String obsRecord(PipelineObservation o) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("rec", "obs");
        n.put("asOf", o.asOf().toString());
        n.put("market", o.market());
        n.put("timescale", o.timescale());
        n.put("severity", o.severity().name());
        n.put("lifecycle", o.lifecycle().name());
        n.put("fired", o.fired());
        if (o.firedKind() == null) {
            n.putNull("firedKind");
        } else {
            n.put("firedKind", o.firedKind().name());
        }
        putNum(n, "density", o.metrics().densityLevel());
        putNum(n, "magnitude", o.magnitude());
        putNum(n, "z", o.zScore());
        putNum(n, "activation", o.activation());
        putNum(n, "sPlus", o.cusumSPlus());
        putNum(n, "sMinus", o.cusumSMinus());
        putNum(n, "recoveryGauge", o.recoveryGauge());
        putNum(n, "largestComponent", o.metrics().largestComponentFraction());
        n.put("levelGateOpen", o.levelGateOpen());
        ArrayNode reasons = n.putArray("reasons");
        for (ReasonCode code : o.reasonCodes()) {
            reasons.add(code.name());
        }
        putContributors(n, o.contributors());
        return n.toString();
    }

    /**
     * The terminal {@code digest} record — the run folded into one object: counts by severity, the peak
     * activation and σ-move, the single biggest move (and when), fired/published, and a time-in-fused
     * proxy. The counts are over the observations this observer received (see {@link RunDigest}).
     *
     * @param d       the accumulated figures
     * @param summary the run outcome
     * @return the digest record as a single JSON object
     */
    public static String digestRecord(RunDigest d, RunSummary summary) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("rec", "digest");
        n.put("schema", SCHEMA);
        putNullable(n, "market", d.market());
        putNullable(n, "timescale", d.timescale());
        n.put("detectionPoints", summary.detectionPoints());
        n.put("fires", summary.fires());
        n.put("published", summary.published());
        n.put("calmBars", summary.calmBars());
        n.put("observed", d.observed());
        n.put("firedObserved", d.fired());
        ObjectNode sev = n.putObject("bySeverity");
        for (Severity s : Severity.values()) {
            sev.put(s.name(), d.count(s));
        }
        putNum(n, "maxActivation", d.maxActivation());
        putNum(n, "maxAbsZ", d.maxAbsZ());
        n.put("timeInFused", d.timeInFused());
        putNum(n, "maxRecoveryGauge", d.maxRecoveryGauge());
        if (d.firstAllClearAt() == null) {
            n.putNull("firstAllClearAt");
        } else {
            n.put("firstAllClearAt", d.firstAllClearAt().toString());
        }
        var timeToAllClear = d.timeToAllClear();
        if (timeToAllClear == null) {
            n.putNull("timeToAllClearHours");
        } else {
            n.put("timeToAllClearHours", timeToAllClear.toMinutes() / 60.0);
        }
        if (d.biggestMoveAt() == null) {
            n.putNull("biggestMove");
        } else {
            ObjectNode bm = n.putObject("biggestMove");
            putNum(bm, "magnitude", d.biggestMove());
            bm.put("asOf", d.biggestMoveAt().toString());
            putContributors(bm, d.biggestMoveContributors());
        }
        return n.toString();
    }

    /** Serializes the "who moved" attribution as a {@code contributors:[{a,b,absDelta}]} array. */
    private static void putContributors(ObjectNode parent, List<PairContribution> contributors) {
        ArrayNode arr = parent.putArray("contributors");
        for (PairContribution c : contributors) {
            ObjectNode cn = arr.addObject();
            cn.put("a", c.a());
            cn.put("b", c.b());
            putNum(cn, "absDelta", c.absDelta());
        }
    }

    /** Puts a finite double, or JSON {@code null} for {@link Double#NaN}/±∞ (no {@code NaN} token). */
    private static void putNum(ObjectNode n, String field, double v) {
        if (Double.isFinite(v)) {
            n.put(field, v);
        } else {
            n.putNull(field);
        }
    }

    /** Puts a string, or JSON {@code null} when absent (no observation was ever received). */
    private static void putNullable(ObjectNode n, String field, String value) {
        if (value == null) {
            n.putNull(field);
        } else {
            n.put(field, value);
        }
    }
}
