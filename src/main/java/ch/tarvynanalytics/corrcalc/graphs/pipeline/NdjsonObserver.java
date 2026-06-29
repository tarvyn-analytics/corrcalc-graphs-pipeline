package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.PrintStream;

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
    public static final int SCHEMA = 1;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PrintStream out;
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
    public void onObservation(PipelineObservation observation) {
        if (observation == null) {
            throw new IllegalArgumentException("observation must not be null");
        }
        if (!headerShown) {
            emit(calibRecord(observation));
            headerShown = true;
        }
        emit(obsRecord(observation));
    }

    private void emit(String line) {
        out.print(line);
        out.print('\n');
        out.flush();
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
        putNum(n, "largestComponent", o.metrics().largestComponentFraction());
        n.put("levelGateOpen", o.levelGateOpen());
        ArrayNode reasons = n.putArray("reasons");
        for (ReasonCode code : o.reasonCodes()) {
            reasons.add(code.name());
        }
        return n.toString();
    }

    /** Puts a finite double, or JSON {@code null} for {@link Double#NaN}/±∞ (no {@code NaN} token). */
    private static void putNum(ObjectNode n, String field, double v) {
        if (Double.isFinite(v)) {
            n.put(field, v);
        } else {
            n.putNull(field);
        }
    }
}
