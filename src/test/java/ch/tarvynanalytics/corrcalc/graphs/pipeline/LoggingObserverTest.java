package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObservationTest.observation;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LoggingObserverTest {

    private Logger observationLogger;
    private ListAppender<ILoggingEvent> appender;

    @BeforeEach
    void attachAppender() {
        observationLogger = (Logger) LoggerFactory.getLogger("observation");
        appender = new ListAppender<>();
        appender.start();
        observationLogger.addAppender(appender);
    }

    @AfterEach
    void detachAppender() {
        observationLogger.detachAppender(appender);
    }

    @Test
    void onObservation_Calm_LogsInfoHeartbeatWithoutFireMarker() {
        new LoggingObserver().onObservation(observation(0.02, 1.0, 0.0, false));

        assertEquals(1, appender.list.size());
        ILoggingEvent event = appender.list.get(0);
        assertEquals(Level.INFO, event.getLevel());
        String message = event.getFormattedMessage();
        assertTrue(message.contains("density="), message);
        assertFalse(message.contains("<=="), message);
    }

    @Test
    void onObservation_Fired_TagsTheDirectionInline() {
        new LoggingObserver().onObservation(observation(0.20, 8.5, 0.0, true));

        String message = appender.list.get(0).getFormattedMessage();
        assertTrue(message.contains("<== FUSION"), message);
    }

    @Test
    void onObservation_Null_Throws() {
        assertThrows(IllegalArgumentException.class, () -> new LoggingObserver().onObservation(null));
    }
}
