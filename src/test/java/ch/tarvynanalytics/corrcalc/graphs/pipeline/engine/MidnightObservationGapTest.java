package ch.tarvynanalytics.corrcalc.graphs.pipeline.engine;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.CollectingSink;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.DroppedBarReason;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObservation;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObserver;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationArtifact;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationSources;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.ReturnBuilder;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.SessionPolicy;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.TimescaleConfig;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.IterableMarketDataSource;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.MarketDataSource;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.MarketSnapshot;
import ch.tarvynanalytics.graphs.algos.Calibration;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FMCSM-154: {@code SessionPolicy.INTRADAY_UTC_DAY} buckets by UTC calendar day, so
 * {@link ReturnBuilder#accept} returns {@code Optional.empty()} for the first snapshot of every day
 * (no within-session predecessor to return against) and {@link PipelineDriver#run} never calls
 * {@code engine.onReturns} for that bar — no S1 snapshot, no S3 detection, no {@link PipelineObservation}.
 * This is deliberately <strong>not</strong> fixed by making the return cross the session boundary (that
 * would change every later bar's numbers and break the "no return crosses a session boundary"
 * invariant); instead {@code PipelineDriver.run} now calls {@link PipelineEngine#onDroppedBar} on the
 * branch where the bar is absent (CGP-45), so the consumer at least hears that a bar existed and
 * produced no return. The pipeline itself still fabricates nothing and writes nothing for that bar.
 *
 * <p>This test drives the <em>actual production wiring</em>: {@link ReturnBuilder} over
 * {@link SessionPolicy#INTRADAY_UTC_DAY} feeding {@link PipelineDriver#run}, exactly the shape
 * {@code MarketEngine.drive(..)} in the product repo builds. It pins the new hook's contract at the
 * second UTC day's midnight, once the engine is already calibrated and observing every other bar: the
 * hook fires there with the right timestamp and reason, and no {@link PipelineObservation} exists for
 * that instant. (The very first snapshot of the whole stream, the first day's own midnight, drops for
 * the identical reason — it too has no within-session predecessor — but the engine has not calibrated
 * yet at that point, so there would be no observation to miss either way; this test isolates the
 * steady-state case FMCSM-154 is about.)
 */
class MidnightObservationGapTest {

    private static final String[] SYMBOLS = {"S0", "S1", "S2", "S3"};
    private static final Instant DAY1 = Instant.parse("2024-01-01T00:00:00Z");
    private static final Instant DAY2 = Instant.parse("2024-01-02T00:00:00Z");

    @Test
    void run_TwoUtcDaysOfMinuteBars_DroppedBarHookFiresOnceAtSecondDayMidnightWithNoObservation() {
        // A generous level gate (density is normalized in [0,1]) keeps calm random-walk noise from
        // ever firing -- this test isolates the return-drop mechanism from CUSUM/rearm behaviour.
        CalibrationArtifact artifact = new CalibrationArtifact(CalibrationArtifact.SCHEMA_VERSION,
                "crypto", "intraday", 0L, DAY1.minus(1, ChronoUnit.DAYS), DAY1, 1440,
                new Calibration(0.0, 1.0, 100.0));

        List<PipelineObservation> observed = new ArrayList<>();
        List<Instant> droppedAt = new ArrayList<>();
        List<DroppedBarReason> droppedReasons = new ArrayList<>();
        PipelineObserver observer = new PipelineObserver() {
            @Override
            public void onObservation(PipelineObservation observation) {
                observed.add(observation);
            }

            @Override
            public void onDroppedBar(Instant asOf, DroppedBarReason reason) {
                droppedAt.add(asOf);
                droppedReasons.add(reason);
            }
        };

        PipelineEngine engine = PipelineEngine.builder(SYMBOLS, new TimescaleConfig(60, DetectorConfig.crypto()))
                .calmBars(2)
                .market("crypto").timescale("intraday")
                .sink(new CollectingSink())
                .observer(observer)
                .calibrationSource(CalibrationSources.calmBlock(artifact))
                .build();

        // Exactly the product's wiring: ReturnBuilder over the UTC-day session policy, driven by
        // PipelineDriver.run -- see MarketEngine.drive(..) in the product repo.
        ReturnBuilder returnBuilder = new ReturnBuilder(SYMBOLS, SessionPolicy.INTRADAY_UTC_DAY);
        MarketDataSource source = twoDaysOfMinuteBars(11L);

        PipelineDriver.run(source, returnBuilder, engine, Pace.none());

        // Both UTC days' own first bar drop for the same reason (DAY1 pre-calibration, DAY2 the
        // steady-state case this ticket is about) -- see the class javadoc for why DAY1 also appears.
        assertEquals(List.of(DAY1, DAY2), droppedAt,
                "expected exactly two dropped bars, one per UTC day's first minute");
        assertEquals(List.of(DroppedBarReason.SESSION_BOUNDARY, DroppedBarReason.SESSION_BOUNDARY),
                droppedReasons);

        assertTrue(observed.stream().noneMatch(o -> o.asOf().equals(DAY2)),
                "no PipelineObservation should exist at " + DAY2 + " -- ReturnBuilder drops it as the "
                        + "first snapshot of the new UTC-day session before it ever reaches the engine");
    }

    /** Calm 1-minute random-walk closes spanning two full UTC days, no gaps, aligned at :00 each minute. */
    private static MarketDataSource twoDaysOfMinuteBars(long seed) {
        Random rng = new Random(seed);
        double[] price = new double[SYMBOLS.length];
        Arrays.fill(price, 100.0);
        List<MarketSnapshot> snapshots = new ArrayList<>();
        for (int m = 0; m < 2 * 24 * 60; m++) {
            double[] closes = new double[SYMBOLS.length];
            for (int s = 0; s < SYMBOLS.length; s++) {
                price[s] *= Math.exp(0.001 * rng.nextGaussian());
                closes[s] = price[s];
            }
            snapshots.add(new MarketSnapshot(DAY1.plus(m, ChronoUnit.MINUTES), closes));
        }
        return new IterableMarketDataSource(SYMBOLS, snapshots);
    }
}
