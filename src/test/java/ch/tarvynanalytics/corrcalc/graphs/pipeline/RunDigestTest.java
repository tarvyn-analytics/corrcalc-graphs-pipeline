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
                m, sPlus, 0.0, fired, fired ? SignalKind.FUSION : null, 8.0, mu, sigma, level, List.of());
    }

    private static PipelineObservation obsWithContributors(String asOf, double weightedChange,
            List<PairContribution> contributors) {
        ChangeMetrics m = new ChangeMetrics(weightedChange, 1.0, 0.1, 1, 1.0, List.of(2));
        return new PipelineObservation(Instant.parse(asOf), "crypto", "daily",
                m, 8.0, 0.0, true, SignalKind.FUSION, 8.0, 0.0, 1.0, 0.167, contributors);
    }
}
