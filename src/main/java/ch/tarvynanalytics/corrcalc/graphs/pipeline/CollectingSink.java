package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An in-memory {@link SignalSink} that retains every published signal, in publish order. Used by the
 * tests and the n=8 regression driver to assert what the pipeline emitted; also the simplest
 * reference adapter for the delivery seam.
 */
public final class CollectingSink implements SignalSink {

    private final List<StructuralSignal> signals = new ArrayList<>();

    @Override
    public void publish(StructuralSignal signal) {
        signals.add(signal);
    }

    /** The signals published so far, in order (an unmodifiable view). */
    public List<StructuralSignal> signals() {
        return Collections.unmodifiableList(signals);
    }

    /** The number of signals published so far. */
    public int count() {
        return signals.size();
    }
}
