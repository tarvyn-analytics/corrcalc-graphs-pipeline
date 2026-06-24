package ch.tarvynanalytics.corrcalc.graphs.pipeline.detect;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TimescaleConfigTest {

    @Test
    void cryptoIntraday_Is480BarWindowWithCryptoDetectorConstants() {
        TimescaleConfig cfg = TimescaleConfig.cryptoIntraday();
        assertEquals(480, cfg.window());
        assertEquals(1.5, cfg.detector().k());
        assertEquals(8.0, cfg.detector().h());
        assertEquals(99.0, cfg.detector().levelPctile());
        assertEquals(0.5, cfg.edgeThreshold());
    }

    @Test
    void cryptoDaily_Is14BarWindowSharingTheCryptoDetectorConstants() {
        TimescaleConfig cfg = TimescaleConfig.cryptoDaily();
        assertEquals(14, cfg.window());
        assertEquals(1.5, cfg.detector().k());
        assertEquals(0.5, cfg.edgeThreshold());
    }
}
