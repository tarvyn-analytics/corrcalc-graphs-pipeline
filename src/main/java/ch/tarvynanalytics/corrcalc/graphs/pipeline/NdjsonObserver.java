package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import java.io.PrintStream;
import java.util.List;
import java.util.Locale;

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
 * clean (data on {@code stdout}, logs on {@code stderr}). JSON is hand-rolled: the records are flat and
 * finite, the repo carries no JSON dependency, and the bounded shape mirrors the bounded reason-code
 * philosophy. Non-finite doubles ({@link Double#NaN}/±∞, e.g. {@code z} on a degenerate calibration)
 * serialize as JSON {@code null}; finite doubles use {@link Double#toString} so the value round-trips.
 * Single-writer, like the engine that drives it (the {@code headerShown} latch is not synchronised).</p>
 */
public final class NdjsonObserver implements PipelineObserver {

    /** The record-schema version; bump on any incompatible change to the emitted fields. */
    public static final int SCHEMA = 1;

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
        StringBuilder sb = new StringBuilder(128).append('{');
        str(sb, "rec", "calib").append(',');
        raw(sb, "schema", Integer.toString(SCHEMA)).append(',');
        str(sb, "market", o.market()).append(',');
        str(sb, "timescale", o.timescale()).append(',');
        num(sb, "mu", o.calmMu()).append(',');
        num(sb, "sigma", o.calmSigma()).append(',');
        num(sb, "levelGate", o.levelGate()).append(',');
        num(sb, "threshold", o.decisionThreshold());
        return sb.append('}').toString();
    }

    /**
     * One {@code obs} record — a self-contained serialization of every fact this transition carries.
     *
     * @param o the observation to serialize
     * @return the observation record as a single JSON object
     */
    public static String obsRecord(PipelineObservation o) {
        StringBuilder sb = new StringBuilder(256).append('{');
        str(sb, "rec", "obs").append(',');
        str(sb, "asOf", o.asOf().toString()).append(',');
        str(sb, "market", o.market()).append(',');
        str(sb, "timescale", o.timescale()).append(',');
        str(sb, "severity", o.severity().name()).append(',');
        bool(sb, "fired", o.fired()).append(',');
        nullableStr(sb, "firedKind", o.firedKind() == null ? null : o.firedKind().name()).append(',');
        num(sb, "density", o.metrics().densityLevel()).append(',');
        num(sb, "magnitude", o.magnitude()).append(',');
        num(sb, "z", o.zScore()).append(',');
        num(sb, "activation", o.activation()).append(',');
        num(sb, "sPlus", o.cusumSPlus()).append(',');
        num(sb, "sMinus", o.cusumSMinus()).append(',');
        num(sb, "largestComponent", o.metrics().largestComponentFraction()).append(',');
        bool(sb, "levelGateOpen", o.levelGateOpen()).append(',');
        reasons(sb, o.reasonCodes());
        return sb.append('}').toString();
    }

    // --- JSON writers (hand-rolled; the records are flat and finite) ---

    private static StringBuilder str(StringBuilder sb, String key, String value) {
        return string(key(sb, key), value);
    }

    private static StringBuilder nullableStr(StringBuilder sb, String key, String value) {
        key(sb, key);
        return value == null ? sb.append("null") : string(sb, value);
    }

    private static StringBuilder raw(StringBuilder sb, String key, String literal) {
        return key(sb, key).append(literal);
    }

    private static StringBuilder bool(StringBuilder sb, String key, boolean value) {
        return key(sb, key).append(value);
    }

    private static StringBuilder num(StringBuilder sb, String key, double value) {
        key(sb, key);
        return Double.isFinite(value) ? sb.append(Double.toString(value)) : sb.append("null");
    }

    private static StringBuilder reasons(StringBuilder sb, List<ReasonCode> codes) {
        key(sb, "reasons").append('[');
        for (int i = 0; i < codes.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            string(sb, codes.get(i).name());
        }
        return sb.append(']');
    }

    private static StringBuilder key(StringBuilder sb, String key) {
        return string(sb, key).append(':');
    }

    private static StringBuilder string(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> appendChar(sb, c);
            }
        }
        return sb.append('"');
    }

    private static void appendChar(StringBuilder sb, char c) {
        if (c < 0x20) {
            sb.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
        } else {
            sb.append(c);
        }
    }
}
