package ch.tarvynanalytics.corrcalc.graphs.pipeline;

/**
 * The AI signal-validity filter hook (build-design §5.3, feature #1a): a pluggable stage that sits
 * <em>between</em> the S3 detector and the {@link SignalSink} fan-out and assesses whether a fired
 * signal is genuine. The default is a no-op that accepts everything; an AI-backed implementation is
 * a later deliverable that directly targets the n=8 false-alarm weakness. The verdict populates the
 * published signal's {@link StructuralSignal.Validity} block.
 */
@FunctionalInterface
public interface SignalFilter {

    /**
     * Assesses a candidate signal.
     *
     * @param signal the candidate structural-change signal
     * @return the validity verdict ({@code filtered=true} suppresses it; an optional score)
     */
    StructuralSignal.Validity assess(StructuralSignal signal);

    /** The no-op default: accept every signal, no score. */
    static SignalFilter acceptAll() {
        return signal -> StructuralSignal.Validity.accepted();
    }
}
