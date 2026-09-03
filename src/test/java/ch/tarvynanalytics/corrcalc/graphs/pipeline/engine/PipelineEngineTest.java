package ch.tarvynanalytics.corrcalc.graphs.pipeline.engine;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.CalibrationEvent;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.CalibrationEventKind;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.CollectingSink;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.ObservationPolicy;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PairContribution;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObservation;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObserver;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SignalKind;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.AdaptiveCalibrationConfig;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationArtifact;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationProvenance;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationSource;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationSources;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.RearmConfig;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.TimescaleConfig;
import ch.tarvynanalytics.graphs.algos.Calibration;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineEngineTest {

    private static final TimescaleConfig CFG = new TimescaleConfig(12, DetectorConfig.crypto());
    private static final int WINDOW = 12;

    @Test
    void onReturns_CalmThenFused_FiresPublishesAndObservesEverything() {
        CollectingSink sink = new CollectingSink();
        List<PipelineObservation> observed = new ArrayList<>();
        PipelineEngine engine = builder(24).sink(sink).observer(observed::add)
                .observationPolicy(ObservationPolicy.all()).build();

        Returns r = calmThenFused(48, 24, 4, 42L);
        drive(engine, r);

        RunSummary s = engine.summary();
        assertTrue(s.fires() >= 1, "expected at least one fusion fire");
        assertEquals(s.fires(), s.published(), "acceptAll publishes every fire");
        assertEquals(s.published(), sink.count(), "every published fire reaches the sink");
        assertEquals(SignalKind.FUSION, sink.signals().get(0).kind());
        assertEquals("crypto", sink.signals().get(0).market());
        // the "all" policy forwards one observation per scored transition
        assertEquals(s.detectionPoints(), s.observationsEmitted());
        assertEquals(s.detectionPoints(), observed.size());
        assertTrue(observed.stream().anyMatch(PipelineObservation::fired), "a fire is present in the series");
    }

    @Test
    void onReturns_FiresOnlyPolicy_ForwardsOnlyFiresButStillPublishesAll() {
        CollectingSink sink = new CollectingSink();
        List<PipelineObservation> observed = new ArrayList<>();
        PipelineEngine engine = builder(24).sink(sink).observer(observed::add)
                .observationPolicy(ObservationPolicy.firesOnly()).build();

        drive(engine, calmThenFused(48, 24, 4, 42L));

        RunSummary s = engine.summary();
        assertTrue(s.detectionPoints() > s.fires(), "most transitions do not fire");
        assertEquals(s.fires(), s.observationsEmitted(), "firesOnly forwards exactly the fires");
        assertEquals((int) s.fires(), observed.size());
        assertTrue(observed.stream().allMatch(PipelineObservation::fired));
        assertEquals(s.fires(), sink.count(), "the product fire-stream is unaffected by the observation policy");
    }

    @Test
    void onReturns_PurelyCalm_NoFiresButObservesTransitions() {
        CollectingSink sink = new CollectingSink();
        PipelineEngine engine = builder(60).sink(sink).build();

        drive(engine, calmThenFused(160, 0, 4, 7L));

        RunSummary s = engine.summary();
        assertEquals(0L, s.fires());
        assertEquals(0, sink.count());
        assertTrue(s.detectionPoints() > 0);
    }

    @Test
    void onSessionBoundary_PostCalibration_ClearsWindowAndDetectorPredecessor() {
        PipelineEngine engine = builder(10).sink(new CollectingSink()).build();

        // 30 calm bars: 19 snapshots, first 10 calibrate, then 9 detection points.
        Returns calm = calmThenFused(30, 0, 4, 3L);
        drive(engine, calm);
        long before = engine.summary().detectionPoints();
        assertTrue(engine.isCalibrated());
        assertTrue(before > 0);

        engine.onSessionBoundary();

        // Re-warm: exactly WINDOW post-boundary bars produce one snapshot, which is the re-primed
        // predecessor (no transition) — so detection points must NOT advance yet.
        Returns post = calmThenFused(WINDOW + 1, 0, 4, 9L);
        Instant t = Instant.parse("2021-06-01T00:00:00Z");
        for (int i = 0; i < WINDOW; i++) {
            engine.onReturns(t.plusSeconds(60L * i), post.rows[i]);
        }
        assertEquals(before, engine.summary().detectionPoints(), "window cleared: no detection on the re-prime");

        engine.onReturns(t.plusSeconds(60L * WINDOW), post.rows[WINDOW]);
        assertEquals(before + 1, engine.summary().detectionPoints(), "next bar yields the first post-gap transition");
    }

    @Test
    void onReturns_PopulatesTopKContributors_LabelledFromTheRunSymbols() {
        List<PipelineObservation> observed = new ArrayList<>();
        PipelineEngine engine = builder(24).sink(new CollectingSink()).observer(observed::add)
                .observationPolicy(ObservationPolicy.all()).build();

        drive(engine, calmThenFused(48, 24, 4, 42L));

        PipelineObservation withContrib = observed.stream()
                .filter(o -> !o.contributors().isEmpty()).findFirst().orElseThrow();
        assertTrue(withContrib.contributors().size() <= 3, "default top-k caps at 3");
        Set<String> syms = Set.of("S0", "S1", "S2", "S3");
        for (PairContribution c : withContrib.contributors()) {
            assertTrue(syms.contains(c.a()) && syms.contains(c.b()), c.toString());
            assertNotEquals(c.a(), c.b());
        }
    }

    @Test
    void contributorsTopK_Zero_DisablesAttribution() {
        List<PipelineObservation> observed = new ArrayList<>();
        PipelineEngine engine = builder(24).sink(new CollectingSink()).observer(observed::add)
                .observationPolicy(ObservationPolicy.all()).contributorsTopK(0).build();

        drive(engine, calmThenFused(48, 24, 4, 42L));

        assertFalse(observed.isEmpty());
        assertTrue(observed.stream().allMatch(o -> o.contributors().isEmpty()));
        assertThrows(IllegalArgumentException.class, () -> builder(24).contributorsTopK(-1));
    }

    @Test
    void onReturns_Limit_StopsRequestingAfterNDetectionPoints() {
        PipelineEngine engine = builder(20).sink(new CollectingSink()).limit(3).build();

        drive(engine, calmThenFused(120, 0, 4, 5L));

        assertTrue(engine.stopRequested());
        assertEquals(3L, engine.summary().detectionPoints());
    }

    @Test
    void calibrationSource_ReadyImmediately_CalibratesOnTheFirstSnapshotAndScoresTheRest() {
        // A walk-forward-style source (ready before the stream, e.g. from a calm-block artifact)
        // must prime the detector on the FIRST window-fill snapshot, so every later snapshot is a
        // scored transition — the seam the calm-block mode plugs into.
        ch.tarvynanalytics.graphs.algos.Calibration external =
                new ch.tarvynanalytics.graphs.algos.Calibration(0.01, 0.02, 0.9, 0.3, 0.05);
        ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationSource ready =
                new ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationSource() {
                    @Override
                    public void observe(Instant asOf, double weightedChange, double density) {
                        // an externally-calibrated source ignores the stream
                    }

                    @Override
                    public boolean isReady() {
                        return true;
                    }

                    @Override
                    public ch.tarvynanalytics.graphs.algos.Calibration calibration() {
                        return external;
                    }

                    @Override
                    public ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationProvenance provenance() {
                        return new ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationProvenance(
                                "calm-block", 0L, null, null);
                    }

                    @Override
                    public ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationArtifact artifact(
                            String market, String timescale) {
                        throw new UnsupportedOperationException("not persisted in this test");
                    }
                };
        PipelineEngine engine = builder(24).sink(new CollectingSink()).calibrationSource(ready).build();

        Returns r = calmThenFused(WINDOW + 5, 0, 4, 11L);
        for (int t = 0; t < WINDOW; t++) {
            engine.onReturns(r.timestamps.get(t), r.rows[t]);
        }
        assertTrue(engine.isCalibrated(), "ready source calibrates on the first window-fill snapshot");
        assertEquals(external, engine.calibration().orElseThrow());
        assertEquals(0L, engine.summary().detectionPoints(), "the first snapshot primes, not scores");

        for (int t = WINDOW; t < r.rows.length; t++) {
            engine.onReturns(r.timestamps.get(t), r.rows[t]);
        }
        assertEquals(5L, engine.summary().detectionPoints(), "every post-prime snapshot is scored");
    }

    @Test
    void onReturns_CalmDensityPair_SurvivesFromCalibrationToEveryObservation() {
        // CGP-45: PipelineObservation now exposes the density calm pair straight from the Calibration
        // the run is scored against -- wired through, never recomputed. muDensity/sigmaDensity are
        // distinguishable from every other number in this test (mu, sigma, level) so a wiring mistake
        // (e.g. swapped arguments) would show up as a mismatch, not a coincidental pass.
        Calibration external = new Calibration(0.01, 0.02, 0.9, 0.42, 0.07);
        CalibrationSource ready = new CalibrationSource() {
            @Override
            public void observe(Instant asOf, double weightedChange, double density) {
                // an externally-calibrated source ignores the stream
            }

            @Override
            public boolean isReady() {
                return true;
            }

            @Override
            public Calibration calibration() {
                return external;
            }

            @Override
            public CalibrationProvenance provenance() {
                return new CalibrationProvenance("calm-block", 0L, null, null);
            }

            @Override
            public CalibrationArtifact artifact(String market, String timescale) {
                throw new UnsupportedOperationException("not persisted in this test");
            }
        };
        List<PipelineObservation> observed = new ArrayList<>();
        PipelineEngine engine = builder(24).sink(new CollectingSink()).observer(observed::add)
                .observationPolicy(ObservationPolicy.all()).calibrationSource(ready).build();

        drive(engine, calmThenFused(WINDOW + 5, 0, 4, 11L));

        assertTrue(observed.size() > 0, "the calm-block source calibrates on the first window-fill snapshot");
        for (PipelineObservation obs : observed) {
            assertEquals(external.muDensity(), obs.calmMuDensity());
            assertEquals(external.sigmaDensity(), obs.calmSigmaDensity());
        }
    }

    @Test
    void onReturns_MultiFusionStream_RearmsOnAllClearAndFiresPerEvent() {
        // H2 Q4 acceptance in miniature: two well-separated fusion→recovery cycles through the real
        // engine; with the re-arm cadence enabled the SECOND event fires too (pre-H2: once ever).
        // Small defusion gauge (N_g=4, θ=0.75) + cool-down 2 keep the trace hand-checkable.
        CollectingSink sink = new CollectingSink();
        PipelineEngine engine = PipelineEngine.builder(syms(4), rearmCfg(true)).calmBars(24)
                .market("crypto").timescale("intraday").observer(PipelineObserver.noOp())
                .sink(sink).build();

        drive(engine, cycles(36, 10, 40, 10, 20, 21L));

        List<SignalKind> kinds = sink.signals().stream().map(s -> s.kind()).toList();
        assertEquals(2, kinds.stream().filter(k -> k == SignalKind.FUSION).count(),
                "one fire per regime event, re-armed between: " + kinds);
        assertTrue(kinds.contains(SignalKind.DEFUSION), "the all-clear drives the re-arm: " + kinds);
        assertTrue(kinds.lastIndexOf(SignalKind.FUSION) > kinds.indexOf(SignalKind.DEFUSION),
                "fusion → all-clear → re-armed fusion, in order: " + kinds);
    }

    @Test
    void detect_FusedAwaitingRearmSpan_FreezesTheCalibrationSource() {
        // The may2021-class suppression guard: between a FUSION and its
        // re-arm resolution, every calm statistic handed to the calibration source must carry the
        // freeze flag — the baseline may not move while an all-clear is pending against it.
        List<Instant> asOfs = new ArrayList<>();
        List<Boolean> frozen = new ArrayList<>();
        CalibrationSource delegate = CalibrationSources.leadingWarmup(24, rearmCfg(true).detector());
        CalibrationSource recording = new CalibrationSource() {
            @Override
            public void observe(Instant asOf, double weightedChange, double density) {
                delegate.observe(asOf, weightedChange, density);
            }

            @Override
            public void observeDetection(Instant asOf, double weightedChange, double density,
                                         boolean alarmActive) {
                asOfs.add(asOf);
                frozen.add(alarmActive);
            }

            @Override
            public boolean isReady() {
                return delegate.isReady();
            }

            @Override
            public Calibration calibration() {
                return delegate.calibration();
            }

            @Override
            public CalibrationProvenance provenance() {
                return delegate.provenance();
            }

            @Override
            public CalibrationArtifact artifact(String market, String timescale) {
                return delegate.artifact(market, timescale);
            }
        };
        CollectingSink sink = new CollectingSink();
        PipelineEngine engine = PipelineEngine.builder(syms(4), rearmCfg(true)).calmBars(24)
                .market("crypto").timescale("intraday").observer(PipelineObserver.noOp())
                .sink(sink).calibrationSource(recording).build();

        drive(engine, cycles(36, 10, 40, 10, 20, 21L));

        Instant fusionAt = sink.signals().stream().filter(s -> s.kind() == SignalKind.FUSION)
                .findFirst().orElseThrow().asOf();
        Instant allClearAt = sink.signals().stream().filter(s -> s.kind() == SignalKind.DEFUSION)
                .findFirst().orElseThrow().asOf();
        for (int i = 0; i < asOfs.size(); i++) {
            Instant t = asOfs.get(i);
            if (!t.isBefore(fusionAt) && !t.isAfter(allClearAt)) {
                assertTrue(frozen.get(i), "frozen throughout the fused span, bar " + t);
            }
        }
        int beforeFusion = asOfs.indexOf(fusionAt);
        assertTrue(beforeFusion > 0 && !frozen.get(beforeFusion - 1),
                "armed calm before the fusion is not frozen");
        assertFalse(frozen.get(frozen.size() - 1),
                "after the re-arm resolves, calm bars learn again");
    }

    @Test
    void detect_BackstopExpiry_TellsTheCalibrationSourceToRebaseline() {
        // The backstop-expiry wiring: when the calendar backstop expires an unresolved
        // question (a fusion whose aftermath never recovers), the engine must hand the expiry to
        // the calibration source — the adaptive one re-baselines; a frozen one ignores it.
        List<Instant> expiries = new ArrayList<>();
        CalibrationSource delegate = CalibrationSources.leadingWarmup(24, rearmCfg(true).detector());
        CalibrationSource recording = new CalibrationSource() {
            @Override
            public void observe(Instant asOf, double weightedChange, double density) {
                delegate.observe(asOf, weightedChange, density);
            }

            @Override
            public void onRegimeExpired(Instant asOf) {
                expiries.add(asOf);
            }

            @Override
            public boolean isReady() {
                return delegate.isReady();
            }

            @Override
            public Calibration calibration() {
                return delegate.calibration();
            }

            @Override
            public CalibrationProvenance provenance() {
                return delegate.provenance();
            }

            @Override
            public CalibrationArtifact artifact(String market, String timescale) {
                return delegate.artifact(market, timescale);
            }
        };
        DetectorConfig detector = rearmCfg(true).detector();
        TimescaleConfig backstopCfg = new TimescaleConfig(WINDOW, detector,
                new RearmConfig(true, 2, 0.25, 5, 30));
        CollectingSink sink = new CollectingSink();
        PipelineEngine engine = PipelineEngine.builder(syms(4), backstopCfg).calmBars(24)
                .market("crypto").timescale("intraday").observer(PipelineObserver.noOp())
                .sink(sink).calibrationSource(recording).build();

        drive(engine, cycles(36, 60, 20, 0, 0, 21L));   // one fusion, 60 elevated bars: never recovers

        // The wiring under test: the unresolved question expires via the backstop and reaches the
        // source exactly once (no DEFUSION ever fires on this tape — the span never recovers, so
        // the only re-arm path left is the calendar expiry). The adaptive source's response to the
        // expiry is pinned separately in AdaptiveQuietnessGateTest.
        assertEquals(1, expiries.size(),
                "the unresolved question must expire via the backstop and reach the source once");
        assertTrue(sink.signals().stream().noneMatch(s -> s.kind() == SignalKind.DEFUSION),
                "nothing recovered on this tape: no all-clear may precede the expiry");
    }

    @Test
    void detect_AdaptiveCalibrationAcrossBackstopExpiry_MatchesPreW1CachingByteForByte() {
        // W1's no-op proof (design/23 3.8d), extended to AdaptiveCalibration: drive the identical
        // tape through two otherwise-identical engines -- one on the real source, one wrapped to
        // reconstruct the pre-W1 drain-only caching (PreW1CachingCalibrationSource). VD-5 --
        // AdaptiveCalibration.calibration() never moves without synchronously queuing a matching
        // event, pinned across AdaptiveQuietnessGateTest -- plus the fact that nothing else touches
        // the source between two detect() calls means the two must produce byte-identical
        // observation and calibration-event streams.
        AdaptiveCalibrationConfig adaptiveCfg =
                new AdaptiveCalibrationConfig(6, 1e6, 0, 1e6, 1e6, 1e-6, 1e6, 1000, 64);
        DetectorConfig detector = rearmCfg(true).detector();
        TimescaleConfig backstopCfg = new TimescaleConfig(WINDOW, detector,
                new RearmConfig(true, 2, 0.25, 5, 30));
        Returns tape = cycles(36, 60, 20, 0, 0, 21L);   // one fusion, 60 elevated bars: never recovers

        RecordedRun real = driveWithSource(backstopCfg, tape,
                CalibrationSources.adaptive(adaptiveCfg, detector, null));
        RecordedRun preW1 = driveWithSource(backstopCfg, tape,
                new PreW1CachingCalibrationSource(CalibrationSources.adaptive(adaptiveCfg, detector, null)));

        // Pin the actual traversal, not just a count: cold-start PROMOTED_TO_LIVE, then the backstop
        // expiry's REGIME_TIMEOUT -> DEMOTED_TO_CALIBRATING -> re-promotion. A fixture change that
        // collapsed this to the cold-start hop alone must fail here, not just stay green on a count.
        assertEquals(List.of(CalibrationEventKind.PROMOTED_TO_LIVE, CalibrationEventKind.REGIME_TIMEOUT,
                CalibrationEventKind.DEMOTED_TO_CALIBRATING, CalibrationEventKind.PROMOTED_TO_LIVE),
                real.events().stream().map(CalibrationEvent::kind).toList());
        assertEquals(real.events(), preW1.events());
        assertEquals(real.observations(), preW1.observations());
    }

    /** The observer-forwarded event + observation streams of one recorded run. */
    private record RecordedRun(List<CalibrationEvent> events, List<PipelineObservation> observations) {
    }

    private static RecordedRun driveWithSource(TimescaleConfig cfg, Returns tape, CalibrationSource source) {
        List<CalibrationEvent> events = new ArrayList<>();
        List<PipelineObservation> observations = new ArrayList<>();
        PipelineObserver observer = new PipelineObserver() {
            @Override
            public void onObservation(PipelineObservation observation) {
                observations.add(observation);
            }

            @Override
            public void onCalibrationEvent(CalibrationEvent event) {
                events.add(event);
            }
        };
        PipelineEngine engine = PipelineEngine.builder(syms(4), cfg).calmBars(24).market("crypto")
                .timescale("intraday").observer(observer).observationPolicy(ObservationPolicy.all())
                .sink(new CollectingSink()).calibrationSource(source).build();
        drive(engine, tape);
        return new RecordedRun(events, observations);
    }

    /**
     * Reconstructs the calibration-caching discipline {@code Listener.detect} used
     * <strong>before</strong> W1 (design/23 3.8d): {@link #calibration()} returns a value refreshed
     * only on a drained, non-demotion event (mirroring the old {@code drainCalibrationEvents}
     * re-read) or on the very first call (mirroring {@code calibrate()}'s one-time read), never on
     * the new unconditional per-bar top-of-{@code detect()} read. Every other seam delegates
     * untouched, so the wrapped source's own internal state evolves identically to an undecorated
     * run -- only what {@code Listener.detect} would have cached differs.
     */
    private static final class PreW1CachingCalibrationSource implements CalibrationSource {
        private final CalibrationSource delegate;
        private Calibration cached;
        private boolean cachedOnce;

        PreW1CachingCalibrationSource(CalibrationSource delegate) {
            this.delegate = delegate;
        }

        @Override
        public void observe(Instant asOf, double weightedChange, double density) {
            delegate.observe(asOf, weightedChange, density);
        }

        @Override
        public void observeDetection(Instant asOf, double weightedChange, double density,
                                     boolean alarmActive) {
            delegate.observeDetection(asOf, weightedChange, density, alarmActive);
        }

        @Override
        public void onRegimeExpired(Instant asOf) {
            delegate.onRegimeExpired(asOf);
        }

        @Override
        public boolean isReady() {
            return delegate.isReady();
        }

        @Override
        public boolean live() {
            return delegate.live();
        }

        @Override
        public Optional<CalibrationEvent> pollEvent() {
            Optional<CalibrationEvent> event = delegate.pollEvent();
            if (event.isPresent() && event.get().kind() != CalibrationEventKind.DEMOTED_TO_CALIBRATING) {
                cached = delegate.calibration();
            }
            return event;
        }

        @Override
        public Calibration calibration() {
            if (!cachedOnce) {
                cached = delegate.calibration();
                cachedOnce = true;
            }
            return cached;
        }

        @Override
        public CalibrationProvenance provenance() {
            return delegate.provenance();
        }

        @Override
        public CalibrationArtifact artifact(String market, String timescale) {
            return delegate.artifact(market, timescale);
        }
    }

    @Test
    void onReturns_MultiFusionStream_DisabledRearm_FiresOnceEver() {
        // The unchanged-behaviour guard: with the cadence disabled (bare TimescaleConfig), the
        // identical stream keeps the pre-H2 debounce — one fusion, ever.
        CollectingSink sink = new CollectingSink();
        PipelineEngine engine = PipelineEngine.builder(syms(4), rearmCfg(false)).calmBars(24)
                .market("crypto").timescale("intraday").observer(PipelineObserver.noOp())
                .sink(sink).build();

        drive(engine, cycles(36, 10, 40, 10, 20, 21L));

        List<SignalKind> kinds = sink.signals().stream().map(s -> s.kind()).toList();
        assertEquals(1, kinds.stream().filter(k -> k == SignalKind.FUSION).count(),
                "disabled cadence: the debounce holds for the whole run: " + kinds);
    }

    /** Crypto constants + a small de-fusion gauge (N_g=4, θ=0.75), re-arm cadence on or off. */
    private static TimescaleConfig rearmCfg(boolean rearmEnabled) {
        DetectorConfig base = DetectorConfig.crypto();
        DetectorConfig withDefusion = new DetectorConfig(base.k(), base.h(), base.levelPctile(),
                base.edgeThreshold(), base.epsilonSigma(), base.fireArm(),
                new ch.tarvynanalytics.graphs.algos.DefusionConfig(0.75, 0.75, 4, true));
        return rearmEnabled
                ? new TimescaleConfig(WINDOW, withDefusion,
                        new RearmConfig(true, 2, 0.25, 5, 0))
                : new TimescaleConfig(WINDOW, withDefusion);
    }

    /** Calm(calibrate) → fused → calm(recovery) → fused → calm: two separated regime cycles. */
    private static Returns cycles(int calm1, int fused1, int calm2, int fused2, int calm3, long seed) {
        Random rng = new Random(seed);
        int rows = calm1 + fused1 + calm2 + fused2 + calm3;
        double[][] returns = new double[rows][4];
        List<Instant> timestamps = new ArrayList<>();
        Instant t0 = Instant.parse("2021-05-01T00:00:00Z");
        for (int t = 0; t < rows; t++) {
            timestamps.add(t0.plusSeconds(60L * t));
            boolean fused = (t >= calm1 && t < calm1 + fused1)
                    || (t >= calm1 + fused1 + calm2 && t < calm1 + fused1 + calm2 + fused2);
            if (fused) {
                double shared = 5.0 * rng.nextGaussian();
                for (int s = 0; s < 4; s++) {
                    returns[t][s] = shared;
                }
            } else {
                for (int s = 0; s < 4; s++) {
                    returns[t][s] = 0.2 * rng.nextGaussian();
                }
            }
        }
        return new Returns(timestamps, returns);
    }

    @Test
    void detect_SourceOpensAnEpoch_RecalibratesTheDetectorAndForwardsTheEvent() {
        // The online half of the calibration seam: a source that opens an epoch after the
        // 5th scored transition must (a) have the fresh calibration installed on the RUNNING
        // detector — later observations echo the new baseline — and (b) have its bounded event
        // forwarded to the observer exactly once.
        Calibration first = new Calibration(0.01, 0.02, 0.9, 0.3, 0.05);
        Calibration second = new Calibration(0.05, 0.04, 0.95, 0.35, 0.06);
        CalibrationSource scripted = new CalibrationSource() {
            private int scored;
            private Calibration current = first;
            private CalibrationEvent pending;

            @Override
            public void observe(Instant asOf, double weightedChange, double density) {
                // ready before the stream: the calibration hop never feeds this source
            }

            @Override
            public void observeDetection(Instant asOf, double weightedChange, double density,
                                         boolean alarmActive) {
                if (++scored == 5) {
                    current = second;
                    pending = new CalibrationEvent(CalibrationEventKind.RECALIBRATED, 1L,
                            first.mu(), second.mu(), first.sigma(), second.sigma(), asOf);
                }
            }

            @Override
            public boolean isReady() {
                return true;
            }

            @Override
            public Calibration calibration() {
                return current;
            }

            @Override
            public Optional<CalibrationEvent> pollEvent() {
                CalibrationEvent event = pending;
                pending = null;
                return Optional.ofNullable(event);
            }

            @Override
            public CalibrationProvenance provenance() {
                return new CalibrationProvenance("adaptive", 0L, null, null);
            }

            @Override
            public CalibrationArtifact artifact(String market, String timescale) {
                throw new UnsupportedOperationException("not persisted in this test");
            }
        };
        List<CalibrationEvent> events = new ArrayList<>();
        List<PipelineObservation> observed = new ArrayList<>();
        PipelineObserver observer = new PipelineObserver() {
            @Override
            public void onObservation(PipelineObservation observation) {
                observed.add(observation);
            }

            @Override
            public void onCalibrationEvent(CalibrationEvent event) {
                events.add(event);
            }
        };
        PipelineEngine engine = builder(24).sink(new CollectingSink()).observer(observer)
                .observationPolicy(ObservationPolicy.all()).calibrationSource(scripted).build();

        drive(engine, calmThenFused(60, 0, 4, 13L));

        assertEquals(1, events.size(), "the epoch event is forwarded exactly once");
        assertEquals(CalibrationEventKind.RECALIBRATED, events.get(0).kind());
        assertEquals(first.mu(), observed.get(4).calmMu(), "the epoch bar itself was scored on the old baseline");
        assertEquals(second.mu(), observed.get(5).calmMu(), "the next transition echoes the recalibrated detector");
        assertTrue(observed.stream().skip(5).allMatch(o -> o.calmMu() == second.mu()));
    }

    @Test
    void detect_SourceNotLive_SuppressesTheFireStreamButKeepsTheObservation() {
        // A demoted source does not vouch for alerts: the raw fired fact stays on the observation
        // seam, but nothing reaches the product fire-stream (spec 2.4 "no alerts until re-armed").
        Calibration external = new Calibration(0.01, 0.02, 0.9, 0.3, 0.05);
        CalibrationSource demoted = new CalibrationSource() {
            @Override
            public void observe(Instant asOf, double weightedChange, double density) {
                // externally calibrated
            }

            @Override
            public boolean isReady() {
                return true;
            }

            @Override
            public boolean live() {
                return false;
            }

            @Override
            public Calibration calibration() {
                return external;
            }

            @Override
            public CalibrationProvenance provenance() {
                return new CalibrationProvenance("adaptive", 1L, null, null);
            }

            @Override
            public CalibrationArtifact artifact(String market, String timescale) {
                throw new UnsupportedOperationException("not persisted in this test");
            }
        };
        CollectingSink sink = new CollectingSink();
        List<PipelineObservation> observed = new ArrayList<>();
        PipelineEngine engine = builder(24).sink(sink).observer(observed::add)
                .observationPolicy(ObservationPolicy.all()).calibrationSource(demoted).build();

        drive(engine, calmThenFused(48, 24, 4, 42L));

        RunSummary s = engine.summary();
        assertTrue(s.fires() >= 1, "the detector still scores and fires internally");
        assertEquals(0L, s.published(), "an unvouched fire never reaches the product stream");
        assertEquals(0, sink.count());
        assertTrue(observed.stream().anyMatch(PipelineObservation::fired),
                "the raw fired fact is never censored on the observation seam");
    }

    @Test
    void onReturns_AdaptiveColdStart_PromotesAfterKAdmittedBarsThenScores() {
        // The real adaptive source through the real engine: the first window-point is a NaN gap
        // (never counted), the next K = 6 calm points warm it up, the promotion event is forwarded,
        // and every later snapshot is a scored transition.
        AdaptiveCalibrationConfig adaptive =
                new AdaptiveCalibrationConfig(6, 1e6, 0, 0.25, 1e6, 1e-6, 1e6, 1000000, 64);
        CalibrationSource source = CalibrationSources.adaptive(adaptive, DetectorConfig.crypto(), null);
        List<CalibrationEvent> events = new ArrayList<>();
        List<PipelineObservation> observed = new ArrayList<>();
        PipelineObserver observer = new PipelineObserver() {
            @Override
            public void onObservation(PipelineObservation observation) {
                observed.add(observation);
            }

            @Override
            public void onCalibrationEvent(CalibrationEvent event) {
                events.add(event);
            }
        };
        PipelineEngine engine = builder(24).sink(new CollectingSink()).observer(observer)
                .observationPolicy(ObservationPolicy.all()).calibrationSource(source).build();

        Returns r = calmThenFused(60, 0, 4, 17L);
        for (int t = 0; t < r.rows.length; t++) {
            engine.onReturns(r.timestamps.get(t), r.rows[t]);
            // 60 bars, window 12 → 49 window-points; point p lands after bar index WINDOW − 2 + p.
            // 1 NaN gap + 6 admitted = ready on point 7, i.e. after bar index WINDOW + 5.
            if (t < WINDOW + 5) {
                assertFalse(engine.isCalibrated(), "CALIBRATING until K admitted bars, bar " + t);
            }
        }

        assertTrue(engine.isCalibrated());
        assertEquals(1, events.size(), "one bounded promotion event");
        assertEquals(CalibrationEventKind.PROMOTED_TO_LIVE, events.get(0).kind());
        assertEquals(42L, engine.summary().detectionPoints(),
                "49 window-points − 1 gap − 6 warm-up − 1 prime = 42 scored transitions");
        assertEquals(42, observed.size());
    }

    @Test
    void build_RejectsBadCalmBarsAndMissingSink() {
        assertThrows(IllegalArgumentException.class, () -> builder(1).sink(new CollectingSink()).build());
        assertThrows(IllegalArgumentException.class, () -> PipelineEngine.builder(syms(4), CFG).calmBars(10).build());
        assertThrows(IllegalArgumentException.class, () -> PipelineEngine.builder(new String[0], CFG));
    }

    private static PipelineEngine.Builder builder(int calmBars) {
        return PipelineEngine.builder(syms(4), CFG).calmBars(calmBars).market("crypto").timescale("intraday")
                .observer(PipelineObserver.noOp());
    }

    private static void drive(PipelineEngine engine, Returns r) {
        for (int t = 0; t < r.rows.length; t++) {
            engine.onReturns(r.timestamps.get(t), r.rows[t]);
            if (engine.stopRequested()) {
                break;
            }
        }
    }

    private static String[] syms(int n) {
        String[] out = new String[n];
        for (int s = 0; s < n; s++) {
            out[s] = "S" + s;
        }
        return out;
    }

    /** Calm independent returns, then (optionally) a block of large identical coordinated shocks. */
    private static Returns calmThenFused(int calmRows, int fusedRows, int symbols, long seed) {
        Random rng = new Random(seed);
        int rows = calmRows + fusedRows;
        double[][] returns = new double[rows][symbols];
        List<Instant> timestamps = new ArrayList<>();
        Instant t0 = Instant.parse("2021-05-01T00:00:00Z");
        for (int t = 0; t < rows; t++) {
            timestamps.add(t0.plusSeconds(60L * t));
            if (t < calmRows) {
                for (int s = 0; s < symbols; s++) {
                    returns[t][s] = 0.2 * rng.nextGaussian();
                }
            } else {
                double shared = 5.0 * rng.nextGaussian();
                for (int s = 0; s < symbols; s++) {
                    returns[t][s] = shared;
                }
            }
        }
        return new Returns(timestamps, returns);
    }

    private record Returns(List<Instant> timestamps, double[][] rows) {
    }
}
