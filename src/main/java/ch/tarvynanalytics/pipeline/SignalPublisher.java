package ch.tarvynanalytics.pipeline;

import java.util.Optional;

/**
 * Ties the output stages together: a detected {@link StructuralSignal} is passed through the
 * {@link SignalFilter}, the verdict is attached to the signal, and unless the filter suppressed it
 * the signal is published to the {@link SignalSink}. This is the {@code filter → sink} tail of the
 * {@code S2 → S1 → S3 → filter → sink} pipeline (build-design §5.2/§5.3).
 */
public final class SignalPublisher {

    private final SignalFilter filter;
    private final SignalSink sink;

    /**
     * @param filter the validity filter (use {@link SignalFilter#acceptAll()} for the no-op default)
     * @param sink   the delivery sink (e.g. a {@link FanOutSink} over several adapters)
     * @throws IllegalArgumentException if either argument is {@code null}
     */
    public SignalPublisher(SignalFilter filter, SignalSink sink) {
        if (filter == null || sink == null) {
            throw new IllegalArgumentException("filter and sink must be non-null");
        }
        this.filter = filter;
        this.sink = sink;
    }

    /**
     * Filters, tags and (unless suppressed) publishes a detected signal.
     *
     * @param detected the raw signal from the detector (its validity block is replaced)
     * @return the published signal carrying the verdict, or empty if the filter suppressed it
     */
    public Optional<StructuralSignal> publish(StructuralSignal detected) {
        StructuralSignal.Validity verdict = filter.assess(detected);
        StructuralSignal tagged = detected.withValidity(verdict);
        if (verdict.filtered()) {
            return Optional.empty();
        }
        sink.publish(tagged);
        return Optional.of(tagged);
    }
}
