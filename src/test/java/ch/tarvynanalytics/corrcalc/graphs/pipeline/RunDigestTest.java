package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import ch.tarvynanalytics.graphs.algos.model.ChangeMetrics;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunDigestTest {

    @Test
    void add_FoldsSeverityPeaksAndBiggestMove() {
        RunDigest d = new RunDigest();
        d.add(obs("2021-02-10T00:00:00Z", 0.4, 0.5, 0.05, 0.0, 1.0, 0.5, 1.0, false));  // CALM, z=0.05
        d.add(obs("2021-02-12T00:00:00Z", 1.0, 1.0, 2.0, 0.0, 1.0, 0.167, 8.0, true));  // FIRE, z=2.0, saturated

        assertEquals(2, d.observed());
        assertEquals(1, d.count(Severity.CALM));
        assertEquals(1, d.count(Severity.FIRE));
        assertEquals(0, d.count(Severity.WARN));
        assertEquals(1, d.fired());
        assertEquals("crypto", d.market());
        assertEquals("daily", d.timescale());
        assertEquals(2.0, d.biggestMove(), 1e-12);
        assertEquals(Instant.parse("2021-02-12T00:00:00Z"), d.biggestMoveAt());
        assertEquals(1.0, d.maxActivation(), 1e-12);
        assertEquals(2.0, d.maxAbsZ(), 1e-12);
        assertEquals(1, d.timeInFused());   // only the density>=0.999 transition
    }

    @Test
    void add_IgnoresNaNForPeaksAndBiggestMove() {
        RunDigest d = new RunDigest();
        d.add(obs("2021-02-10T00:00:00Z", 0.5, 0.5, Double.NaN, 0.0, 1.0, 0.5, 1.0, false));

        assertEquals(1, d.observed());
        assertNull(d.biggestMoveAt());
        assertTrue(Double.isNaN(d.biggestMove()));
        assertEquals(0.0, d.maxAbsZ(), 1e-12);   // z is NaN on a gap -> not counted
    }

    @Test
    void newDigest_HasZeroCountsAndNoMarket() {
        RunDigest d = new RunDigest();
        assertEquals(0, d.observed());
        assertEquals(0, d.count(Severity.WARN));
        assertNull(d.market());
        assertEquals(List.of(), d.biggestMoveContributors());
    }

    @Test
    void biggestMove_CarriesItsContributors_ReplacedWhenABiggerMoveArrives() {
        RunDigest d = new RunDigest();
        d.add(obsWithContributors("2021-02-10T00:00:00Z", 0.5, List.of(new PairContribution("A", "B", 0.5))));
        d.add(obsWithContributors("2021-02-12T00:00:00Z", 2.0, List.of(new PairContribution("ETH", "BNB", 2.0))));
        assertEquals(2.0, d.biggestMove(), 1e-12);
        assertEquals(List.of(new PairContribution("ETH", "BNB", 2.0)), d.biggestMoveContributors());
    }

    private static PipelineObservation obs(String asOf, double density, double largestFraction,
            double weightedChange, double mu, double sigma, double level, double sPlus, boolean fired) {
        ChangeMetrics m = new ChangeMetrics(weightedChange, density, 0.1, 1, largestFraction, List.of(2));
        return new PipelineObservation(Instant.parse(asOf), "crypto", "daily",
                m, sPlus, 0.0, Double.NaN, fired, fired ? SignalKind.FUSION : null, 8.0, mu, sigma, level, List.of());
    }

    private static PipelineObservation obsWithContributors(String asOf, double weightedChange,
            List<PairContribution> contributors) {
        ChangeMetrics m = new ChangeMetrics(weightedChange, 1.0, 0.1, 1, 1.0, List.of(2));
        return new PipelineObservation(Instant.parse(asOf), "crypto", "daily",
                m, 8.0, 0.0, Double.NaN, true, SignalKind.FUSION, 8.0, 0.0, 1.0, 0.167, contributors);
    }

    @Test
    void add_TracksPeakRecoveryGaugeAndTimeToAllClear() {
        RunDigest d = new RunDigest();
        d.add(gauge("2021-05-18T00:00:00Z", 0.30, true, SignalKind.FUSION));   // first fusion arms the latch
        d.add(gauge("2021-05-19T00:00:00Z", 0.70, false, null));               // recovering
        d.add(gauge("2021-05-20T00:00:00Z", 0.95, true, SignalKind.DEFUSION)); // all-clear 48h later
        assertEquals(0.95, d.maxRecoveryGauge(), 1e-12);
        assertEquals(Instant.parse("2021-05-20T00:00:00Z"), d.firstAllClearAt());
        assertEquals(48, d.timeToAllClear().toHours());
    }

    @Test
    void timeToAllClear_NullWhenNoFusionOrNoAllClear() {
        RunDigest d = new RunDigest();
        d.add(gauge("2021-05-20T00:00:00Z", 0.95, true, SignalKind.DEFUSION));  // all-clear with no prior fusion
        assertNull(d.timeToAllClear());
    }

    private static PipelineObservation gauge(String asOf, double recoveryGauge, boolean fired, SignalKind kind) {
        ChangeMetrics m = new ChangeMetrics(0.01, 0.2, 0.1, 1, 0.2, List.of(2));
        return new PipelineObservation(Instant.parse(asOf), "crypto", "intraday",
                m, 0.0, 0.0, recoveryGauge, fired, kind, 8.0, 0.05, 0.02, 0.5, List.of());
    }

    @Test
    void addCalibrationEvent_CountsEpochOpensAndTheRecalibrationSubset() {
        RunDigest d = new RunDigest();
        d.addCalibrationEvent(event(CalibrationEventKind.RECALIBRATED, 1, 0.0100, 0.0132));
        d.addCalibrationEvent(event(CalibrationEventKind.EPOCH_OPENED, 2, 0.0132, 0.0132));
        d.addCalibrationEvent(event(CalibrationEventKind.REGIME_TIMEOUT, 3, 0.0132, 0.0200));

        assertEquals(3, d.epochsOpened());
        assertEquals(1, d.recalibrations());
    }

    @Test
    void addCalibrationEvent_LifecycleTransitions_DoNotCountAsEpochOpens() {
        RunDigest d = new RunDigest();
        d.addCalibrationEvent(event(CalibrationEventKind.PROMOTED_TO_LIVE, 0, Double.NaN, 0.0500));
        d.addCalibrationEvent(event(CalibrationEventKind.DEMOTED_TO_CALIBRATING, 0, 0.0500, 0.0500));

        assertEquals(0, d.epochsOpened());
        assertEquals(0, d.recalibrations());
    }

    @Test
    void addCalibrationEvent_MuJourney_FirstBeforeToLatestAfter() {
        RunDigest d = new RunDigest();
        d.addCalibrationEvent(event(CalibrationEventKind.RECALIBRATED, 1, 0.0100, 0.0132));
        d.addCalibrationEvent(event(CalibrationEventKind.RECALIBRATED, 2, 0.0132, 0.0140));

        assertEquals(0.0100, d.muFirstBefore(), 1e-12);
        assertEquals(0.0140, d.muLastAfter(), 1e-12);
    }

    @Test
    void newDigest_NoCalibrationEvents_NaNJourneyAndZeroCounts() {
        RunDigest d = new RunDigest();
        assertEquals(0, d.epochsOpened());
        assertEquals(0, d.recalibrations());
        assertTrue(Double.isNaN(d.muFirstBefore()));
        assertTrue(Double.isNaN(d.muLastAfter()));
    }

    private static CalibrationEvent event(CalibrationEventKind kind, long epochId, double muBefore, double muAfter) {
        return new CalibrationEvent(kind, epochId, muBefore, muAfter, 0.0043, 0.0051,
                Instant.parse("2021-05-19T13:00:00Z"));
    }
}
