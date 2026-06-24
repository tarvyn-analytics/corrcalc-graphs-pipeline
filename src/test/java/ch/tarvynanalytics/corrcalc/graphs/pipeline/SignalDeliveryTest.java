package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SignalDeliveryTest {

    private static StructuralSignal sample(String universeHead) {
        return new StructuralSignal(Instant.EPOCH, "crypto", "intraday", List.of(universeHead, "ETHUSDT"),
                SignalKind.FUSION, 0.04, 9.0, 0.0, 0.7, 0.1, 2, 0.7,
                StructuralSignal.Validity.accepted(), null);
    }

    @Test
    void publish_AcceptAllFilter_FansOutToEverySink() {
        CollectingSink a = new CollectingSink();
        CollectingSink b = new CollectingSink();
        SignalPublisher publisher = new SignalPublisher(SignalFilter.acceptAll(), new FanOutSink(List.of(a, b)));

        Optional<StructuralSignal> published = publisher.publish(sample("BTCUSDT"));

        assertTrue(published.isPresent());
        assertEquals(1, a.count());
        assertEquals(1, b.count());
        assertSame(published.get(), a.signals().get(0));
        assertEquals(SignalKind.FUSION, a.signals().get(0).kind());
    }

    @Test
    void publish_FilterSuppresses_NothingReachesTheSink_AndVerdictIsAttached() {
        CollectingSink sink = new CollectingSink();
        // a filter that suppresses any BTC-led signal, with a score
        SignalFilter filter = s -> s.universe().contains("BTCUSDT")
                ? new StructuralSignal.Validity(true, 0.1)
                : StructuralSignal.Validity.accepted();
        SignalPublisher publisher = new SignalPublisher(filter, sink);

        assertTrue(publisher.publish(sample("BTCUSDT")).isEmpty());
        assertEquals(0, sink.count());

        Optional<StructuralSignal> kept = publisher.publish(sample("SOLUSDT"));
        assertTrue(kept.isPresent());
        assertEquals(1, sink.count());
    }

    @Test
    void fanOutSink_RejectsNullDelegates() {
        assertThrows(IllegalArgumentException.class, () -> new FanOutSink(null));
        assertThrows(IllegalArgumentException.class,
                () -> new FanOutSink(java.util.Arrays.asList(new CollectingSink(), null)));
    }

    @Test
    void signalPublisher_RejectsNullArguments() {
        assertThrows(IllegalArgumentException.class, () -> new SignalPublisher(null, new CollectingSink()));
        assertThrows(IllegalArgumentException.class, () -> new SignalPublisher(SignalFilter.acceptAll(), null));
    }
}
