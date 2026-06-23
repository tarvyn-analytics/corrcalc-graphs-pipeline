package ch.tarvynanalytics.pipeline.replay;

import ch.tarvynanalytics.graphs.algos.Calibration;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;
import ch.tarvynanalytics.graphs.algos.FireArm;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventScorerTest {

    private static final DetectorConfig CFG = new DetectorConfig(0.0, 2.0, 50.0, 0.5, 1.0, FireArm.UPPER);
    private static final Calibration CAL = new Calibration(0.0, 1.0, 0.0);

    private static final CryptoEvent EVENT = new CryptoEvent("test_event",
            LocalDate.of(2022, 1, 10), LocalDate.of(2022, 1, 12),
            LocalDate.of(2021, 12, 1), LocalDate.of(2022, 1, 1));

    private static TimescaleScoring scoring(Instant start, int stepMinutes, double[] density) {
        List<Instant> ts = new java.util.ArrayList<>();
        double[] change = new double[density.length];
        for (int i = 0; i < density.length; i++) {
            ts.add(start.plusSeconds((long) i * stepMinutes * 60));
            change[i] = 0.0;
        }
        return new TimescaleScoring(CAL, ts, density, change);
    }

    @Test
    void score_BothFire_IntradayEarlier_ReportsPositiveLeadAndNoMisses() {
        // intraday fires at index 2 (00:00 + 2h), daily fires at index 2 (a day later) -> intraday leads.
        TimescaleScoring intraday = scoring(Instant.parse("2022-01-10T00:00:00Z"), 60, new double[]{1, 1, 1, 1});
        TimescaleScoring daily = scoring(Instant.parse("2022-01-11T00:00:00Z"), 1440, new double[]{1, 1, 1, 1});

        LeadTableRow row = EventScorer.score(EVENT, 20, daily, intraday, CFG);

        assertFalse(row.dailyMiss());
        assertFalse(row.intradayMiss());
        assertEquals(Instant.parse("2022-01-10T02:00:00Z"), row.tIntraday());
        assertEquals(Instant.parse("2022-01-13T00:00:00Z"), row.tDaily());
        // t_daily - t_intraday = 2022-01-13 00:00 - 2022-01-10 02:00 = 70 h.
        assertEquals(70.0, row.leadHours(), 1e-9);
        assertEquals(1.0, row.intradayDensityAtAlert());
        assertEquals(0.0, row.intradayL());   // L = calibration level
        assertNull(row.skipReason());
    }

    @Test
    void score_IntradayDetectsDailyMisses_LeadIsNullAndDailyMissTrue() {
        TimescaleScoring intraday = scoring(Instant.parse("2022-01-10T00:00:00Z"), 60, new double[]{1, 1, 1, 1});
        TimescaleScoring daily = scoring(Instant.parse("2022-01-11T00:00:00Z"), 1440, new double[]{0, 0, 0, 0});

        LeadTableRow row = EventScorer.score(EVENT, 20, daily, intraday, CFG);

        assertTrue(row.dailyMiss());
        assertFalse(row.intradayMiss());
        assertNull(row.tDaily());
        assertNull(row.leadHours());
        assertNull(row.dailyDensityAtAlert());
        assertEquals(0.0, row.dailyL());   // L is present even on a miss (calibration was computed)
    }

    @Test
    void skipped_BuildsBothMissRowWithReason() {
        LeadTableRow row = LeadTableRow.skipped("tiny_event", 2, "universe_too_small");
        assertTrue(row.dailyMiss());
        assertTrue(row.intradayMiss());
        assertEquals("universe_too_small", row.skipReason());
        assertNull(row.tDaily());
    }
}
