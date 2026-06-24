package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LoggingSinkTest {

    private Logger signalLogger;
    private ListAppender<ILoggingEvent> appender;

    @BeforeEach
    void attachAppender() {
        signalLogger = (Logger) LoggerFactory.getLogger("signal");
        appender = new ListAppender<>();
        appender.start();
        signalLogger.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        signalLogger.detachAppender(appender);
    }

    @Test
    void publish_FusionWithLead_LogsHighlightedWarnWithLead() {
        new LoggingSink().publish(signal(SignalKind.FUSION, 46.9));

        assertEquals(1, appender.list.size());
        ILoggingEvent event = appender.list.get(0);
        assertEquals(Level.WARN, event.getLevel());
        String message = event.getFormattedMessage();
        assertTrue(message.contains("FUSION"), message);
        assertTrue(message.contains("lead=+46.9h"), message);
    }

    @Test
    void publish_DefusionWithoutLead_LogsDefusionBannerAndNoLead() {
        new LoggingSink().publish(signal(SignalKind.DEFUSION, null));

        ILoggingEvent event = appender.list.get(0);
        assertEquals(Level.WARN, event.getLevel());
        String message = event.getFormattedMessage();
        assertTrue(message.contains("DEFUSION"), message);
        assertFalse(message.contains("lead="), message);
    }

    @Test
    void publish_Null_Throws() {
        assertThrows(IllegalArgumentException.class, () -> new LoggingSink().publish(null));
    }

    private static StructuralSignal signal(SignalKind kind, Double leadHours) {
        return new StructuralSignal(
                Instant.parse("2021-05-18T01:06:00Z"), "crypto", "intraday",
                List.of("BTCUSDT", "ETHUSDT"), kind,
                0.0823, 8.41, 0.0, 1.0, 0.25, 1, 1.0,
                StructuralSignal.Validity.accepted(), leadHours);
    }
}
