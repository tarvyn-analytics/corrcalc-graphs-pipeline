package ch.tarvynanalytics.corrcalc.graphs.pipeline.engine;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.CollectingSink;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.DroppedBarReason;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.ObservationPolicy;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObservation;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObserver;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.RearmConfig;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.TimescaleConfig;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CGP-191: {@code RearmCadence}'s calendar backstop re-primes the S3 detector ({@code onSessionBoundary}
 * plus a dropped predecessor) exactly like a real session boundary, but nothing ever told the observer —
 * the re-primed bar's {@code onMatrix} returns {@code null}, {@link PipelineEngine.Listener#detect} took
 * the early-return branch, and no {@link PipelineObservation} was ever built for it. Per Stream C's
 * ruling (option B) the re-prime itself stays; the engine now reports the swallowed bar via
 * {@link PipelineObserver#onDroppedBar} with {@link DroppedBarReason#REARM_EXPIRED} on the same
 * never-gated path {@code SESSION_BOUNDARY} uses (that path is driven by {@code PipelineDriver}, not
 * this engine, and is unaffected here -- see {@code MidnightObservationGapTest}).
 *
 * <p>De-fusion and the S⁺-relaxation trigger are both off ({@code DetectorConfig.crypto()}'s default
 * de-fusion, {@code relaxSustainBars=0}), so the calendar backstop is the <em>only</em> way this cadence
 * ever re-arms -- the fire is deterministic (a fixed post-calibration random-walk tape driven through a
 * sustained shared shock), and the backstop's countdown is exact.</p>
 */
class RearmExpiredDroppedBarTest {

    private static final String[] SYMBOLS = {"S0", "S1", "S2", "S3"};
    private static final int WINDOW = 12;
    private static final int CALM_ROWS = 48;
    private static final int SHOCK_ROWS = 24;
    private static final int TAIL_CALM_ROWS = 60;
    private static final int CALENDAR_REARM_BARS = 40;

    @Test
    void detect_CalendarBackstopExpires_DropsExactlyOneBarAsRearmExpiredThenResumesObserving() {
        TimescaleConfig cfg = new TimescaleConfig(WINDOW, DetectorConfig.crypto(),
                new RearmConfig(true, 0, 0.25, 0, CALENDAR_REARM_BARS));

        List<PipelineObservation> observations = new ArrayList<>();
        List<Instant> droppedAt = new ArrayList<>();
        List<DroppedBarReason> droppedReasons = new ArrayList<>();
        PipelineObserver observer = new PipelineObserver() {
            @Override
            public void onObservation(PipelineObservation observation) {
                observations.add(observation);
            }

            @Override
            public void onDroppedBar(Instant asOf, DroppedBarReason reason) {
                droppedAt.add(asOf);
                droppedReasons.add(reason);
            }
        };
        PipelineEngine engine = PipelineEngine.builder(SYMBOLS, cfg)
                .calmBars(24)
                .market("crypto").timescale("intraday")
                .observationPolicy(ObservationPolicy.all())
                .sink(new CollectingSink())
                .observer(observer)
                .build();

        // Every bar fed while still pre-calibration never reaches detect(); only the detect-phase
        // bars (calibratedBefore == true) are candidates for either an observation or a drop.
        List<Instant> detectPhaseAsOf = new ArrayList<>();
        for (Bar bar : tape()) {
            boolean detectPhase = engine.isCalibrated();
            engine.onReturns(bar.asOf(), bar.returns());
            if (detectPhase) {
                detectPhaseAsOf.add(bar.asOf());
            }
        }

        assertEquals(1, droppedAt.size(), "the calendar backstop must swallow exactly one bar: " + droppedAt);
        assertEquals(List.of(DroppedBarReason.REARM_EXPIRED), droppedReasons);

        int fireIdx = -1;
        for (int i = 0; i < observations.size(); i++) {
            if (observations.get(i).fired()) {
                fireIdx = i;
                break;
            }
        }
        assertTrue(fireIdx >= 0, "the shock block must fire a fusion");

        // Every detect-phase bar up to the drop is observed 1:1 (nothing swallowed before it), so
        // fireIdx also indexes the fire bar within detectPhaseAsOf.
        int dropIdx = detectPhaseAsOf.indexOf(droppedAt.get(0));
        assertEquals(fireIdx + CALENDAR_REARM_BARS + 1, dropIdx,
                "the swallowed bar must sit exactly calendarRearmBars scored bars after the fire "
                        + "-- the bar that decides EXPIRED, plus the one bar it re-primes");

        // (a)+(b): the callback names exactly the swallowed bar's asOf, and no observation exists for it.
        assertEquals(detectPhaseAsOf.get(dropIdx), droppedAt.get(0));
        assertTrue(observations.stream().noneMatch(o -> o.asOf().equals(droppedAt.get(0))),
                "no PipelineObservation should exist for the dropped bar " + droppedAt.get(0));
        // The EXPIRED-deciding bar itself (immediately before the drop) still observes normally.
        assertTrue(observations.stream().anyMatch(o -> o.asOf().equals(detectPhaseAsOf.get(dropIdx - 1))),
                "the EXPIRED-deciding bar keeps its own observation");
        // (c): the very next bar observes again.
        assertTrue(observations.stream().anyMatch(o -> o.asOf().equals(detectPhaseAsOf.get(dropIdx + 1))),
                "observing must resume on the bar right after the swallowed one");

        // Every detect-phase bar is accounted for by exactly one of {observed, the one drop} -- no
        // other bar is silently lost.
        List<Instant> expectedObserved = new ArrayList<>(detectPhaseAsOf);
        expectedObserved.remove(droppedAt.get(0));
        assertEquals(expectedObserved, observations.stream().map(PipelineObservation::asOf).toList());
    }

    /** Calm warm-up (window fill + calibration), a sustained shared shock that fires once, then a
     * long calm tail so the calendar backstop's countdown runs to completion and beyond. */
    private static List<Bar> tape() {
        Random rng = new Random(7L);
        Instant t0 = Instant.parse("2021-05-01T00:00:00Z");
        int rows = CALM_ROWS + SHOCK_ROWS + TAIL_CALM_ROWS;
        List<Bar> bars = new ArrayList<>(rows);
        for (int t = 0; t < rows; t++) {
            Instant asOf = t0.plus(t, ChronoUnit.MINUTES);
            double[] row = new double[SYMBOLS.length];
            if (t < CALM_ROWS || t >= CALM_ROWS + SHOCK_ROWS) {
                for (int s = 0; s < SYMBOLS.length; s++) {
                    row[s] = 0.2 * rng.nextGaussian();
                }
            } else {
                double shared = 5.0 * rng.nextGaussian();
                for (int s = 0; s < SYMBOLS.length; s++) {
                    row[s] = shared;
                }
            }
            bars.add(new Bar(asOf, row));
        }
        return bars;
    }

    private record Bar(Instant asOf, double[] returns) {
    }
}
