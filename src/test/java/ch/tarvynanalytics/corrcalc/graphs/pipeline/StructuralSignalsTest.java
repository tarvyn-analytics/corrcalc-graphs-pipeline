package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import ch.tarvynanalytics.graphs.algos.model.ChangeMetrics;
import ch.tarvynanalytics.graphs.algos.model.ChangeSignal;
import ch.tarvynanalytics.graphs.algos.model.FireDirection;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class StructuralSignalsTest {

    @Test
    void fromChangeSignal_MapsEveryMetricAndCusumArm() {
        ChangeMetrics metrics = new ChangeMetrics(0.043, 0.71, 0.12, 2, 0.72, List.of(2, 1));
        ChangeSignal signal = new ChangeSignal(7, metrics, 9.4, 0.5, Double.NaN, FireDirection.FUSION);

        StructuralSignal s = StructuralSignals.fromChangeSignal(
                signal, Instant.parse("2022-11-08T16:11:00Z"), "crypto", "intraday",
                List.of("BTCUSDT", "ETHUSDT", "SOLUSDT"), SignalKind.FUSION, 46.9);

        assertEquals(0.043, s.changeMetric());
        assertEquals(0.71, s.levelDensity());
        assertEquals(0.12, s.edgeXorFraction());
        assertEquals(9.4, s.cusumSPlus());
        assertEquals(0.5, s.cusumSMinus());
        assertEquals(2, s.nComponents());
        assertEquals(0.72, s.largestComponentFraction());
        assertEquals(46.9, s.leadVsDailyHours());
        assertEquals(SignalKind.FUSION, s.kind());
        assertEquals(List.of("BTCUSDT", "ETHUSDT", "SOLUSDT"), s.universe());
        assertFalse(s.validity().filtered());
        assertNull(s.validity().score());
    }
}
