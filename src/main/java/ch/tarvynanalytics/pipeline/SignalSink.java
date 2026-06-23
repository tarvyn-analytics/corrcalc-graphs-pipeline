package ch.tarvynanalytics.pipeline;

/**
 * The output delivery seam (build-design §5.2, feature #2): a sink the pipeline publishes a
 * {@link StructuralSignal} to. Multi-sink from day one — concrete adapters (Kafka, REST callback,
 * email, Slack, Teams) are later deliverables; {@link FanOutSink} composes several, and
 * {@link CollectingSink} is the in-memory sink the tests and the regression driver consume. This is
 * what makes the product "infrastructure-first": a published signal, not a UI.
 */
@FunctionalInterface
public interface SignalSink {

    /**
     * Publishes one structural-change signal.
     *
     * @param signal the signal to deliver
     */
    void publish(StructuralSignal signal);
}
