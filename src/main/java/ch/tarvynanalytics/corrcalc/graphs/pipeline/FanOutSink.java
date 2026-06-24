package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import java.util.List;

/**
 * A {@link SignalSink} that fans one signal out to every configured downstream sink, in order. This
 * is the multi-sink delivery seam (build-design §5.2): wire a Kafka sink, a Slack sink and an email
 * sink behind one {@code FanOutSink} and the pipeline publishes once. The list of delegates is
 * defensively copied and immutable.
 */
public final class FanOutSink implements SignalSink {

    private final List<SignalSink> delegates;

    /**
     * @param delegates the downstream sinks to publish to, in order
     * @throws IllegalArgumentException if {@code delegates} or any element is {@code null}
     */
    public FanOutSink(List<SignalSink> delegates) {
        if (delegates == null || delegates.stream().anyMatch(d -> d == null)) {
            throw new IllegalArgumentException("delegate sinks must be non-null");
        }
        this.delegates = List.copyOf(delegates);
    }

    @Override
    public void publish(StructuralSignal signal) {
        for (SignalSink delegate : delegates) {
            delegate.publish(signal);
        }
    }
}
