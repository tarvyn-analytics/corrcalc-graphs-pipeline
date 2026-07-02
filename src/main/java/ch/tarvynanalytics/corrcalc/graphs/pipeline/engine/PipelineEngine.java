package ch.tarvynanalytics.corrcalc.graphs.pipeline.engine;

import ch.tarvynanalytics.corrcalc.lib.matrix.DoubleMatrix;
import ch.tarvynanalytics.corrcalc.lib.stream.CorrelationStreamListener;
import ch.tarvynanalytics.corrcalc.lib.stream.RollingCorrelationEngine;
import ch.tarvynanalytics.corrcalc.lib.stream.RollingCorrelations;
import ch.tarvynanalytics.graphs.algos.Calibration;
import ch.tarvynanalytics.graphs.algos.ChangeDetector;
import ch.tarvynanalytics.graphs.algos.ChangeDetectors;
import ch.tarvynanalytics.graphs.algos.ChangeMetricsAnalyzer;
import ch.tarvynanalytics.graphs.algos.model.ChangeMetrics;
import ch.tarvynanalytics.graphs.algos.model.ChangeSignal;
import ch.tarvynanalytics.graphs.algos.model.FireDirection;
import ch.tarvynanalytics.graphs.algos.model.PairChange;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.CalibrationEvent;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.CalibrationEventKind;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.DetectorState;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.ObservationPolicy;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PairContribution;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationSource;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationSources;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObservation;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObserver;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SignalFilter;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SignalKind;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SignalPublisher;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SignalSink;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.StructuralSignal;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.StructuralSignals;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.TimescaleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * The pipeline orchestrator — the single composition point where the two Initiative-S primitives and
 * the output stages are wired together, independent of <em>where the data comes from</em> (a replay, a
 * live feed) and <em>how it is paced</em>. A driver feeds it aligned return bars; the engine owns the
 * rest:
 *
 * <ol>
 *   <li>the S1 {@link RollingCorrelationEngine} (one timescale), fed one return bar per {@link #onReturns};</li>
 *   <li>the in-stream calibration boundary: pre-detection window-points feed the run's
 *       {@link CalibrationSource} (default: the leading-warmup prefix over {@code calmBars} points);
 *       once the source is ready the S3 detector is created from its {@link Calibration} and primed;</li>
 *   <li>per transition after calibration: the product fire-stream — a fired transition becomes a
 *       {@link StructuralSignal} through the {@link SignalPublisher} ({@code SignalFilter → SignalSink});
 *       and the observation seam — every transition becomes a {@link PipelineObservation} gated by the
 *       consumer's {@link ObservationPolicy} and handed to the {@link PipelineObserver}.</li>
 * </ol>
 *
 * <p>The engine is <strong>pace-agnostic</strong>: it never sleeps and never logs the per-transition
 * heartbeat (that is the consumer's observer). It is <strong>single-writer</strong> — exactly one
 * thread may drive {@link #onReturns}/{@link #onSessionBoundary}, mirroring the S1/S3 streaming
 * contract. Build one via {@link #builder(String[], TimescaleConfig)}.</p>
 */
public final class PipelineEngine {

    private static final Logger LOG = LoggerFactory.getLogger("pipeline");

    private final RollingCorrelationEngine s1;
    private final Listener listener;

    private PipelineEngine(Builder b) {
        this.listener = new Listener(b);
        this.s1 = RollingCorrelations.pearson(b.symbols, b.cfg.window(), listener);
    }

    /**
     * Starts a builder for an engine over a fixed symbol set and timescale tuning.
     *
     * @param symbols the variable labels (column order), stable across the run
     * @param cfg     the timescale tuning (S1 window + S3 detector config)
     * @return a new builder
     */
    public static Builder builder(String[] symbols, TimescaleConfig cfg) {
        return new Builder(symbols, cfg);
    }

    /**
     * Feeds one aligned return bar into S1 (which, once the window has filled, drives calibration and
     * then detection). The inbound seam: a replay loop, a live connector, or a test calls this.
     *
     * @param asOf    the bar-end timestamp
     * @param returns one log return per variable, in the engine's column order
     */
    public void onReturns(Instant asOf, double[] returns) {
        s1.onBar(asOf, returns);
    }

    /**
     * Signals a session boundary (e.g. a UTC-day gap): clears the S1 window so no rolling correlation
     * spans the gap, and resets the S3 side — the detector drops its predecessor and both CUSUM arms
     * (post-calibration), or the calibration predecessor is dropped (pre-calibration). The engine
     * re-warms over the next session's window before it scores again.
     */
    public void onSessionBoundary() {
        s1.onSessionBoundary();
        listener.onSessionBoundary();
    }

    /** Whether calibration has completed and the engine is now scoring transitions. */
    public boolean isCalibrated() {
        return listener.detector != null;
    }

    /** Whether a stop condition (the configured detection-point limit) has been reached. */
    public boolean stopRequested() {
        return listener.stop;
    }

    /** The calm-window calibration once it has been computed, otherwise empty. */
    public Optional<Calibration> calibration() {
        return Optional.ofNullable(listener.calibrationResult);
    }

    /** A snapshot of the run counters so far. */
    public RunSummary summary() {
        return listener.summary();
    }

    private static double[][] toArray(DoubleMatrix matrix, int n) {
        double[][] out = new double[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                out[i][j] = matrix.get(i, j);
            }
        }
        return out;
    }

    /**
     * Maps the S3 detector's fire direction to the pipeline {@link SignalKind}. A de-fusion fire is a
     * {@link SignalKind#DEFUSION} all-clear; every other fire (the proven exit alarm) is a
     * {@link SignalKind#FUSION}. Only called when the signal fired.
     */
    private static SignalKind toSignalKind(FireDirection direction) {
        return direction == FireDirection.DEFUSION ? SignalKind.DEFUSION : SignalKind.FUSION;
    }

    private static String fmt(double v, int decimals) {
        if (Double.isNaN(v)) {
            return "NaN";
        }
        return String.format(Locale.ROOT, "%." + decimals + "f", v);
    }

    /**
     * Fluent builder for {@link PipelineEngine}. {@code calmBars} and a fire {@code sink} (or full
     * {@code publisher}) are required; the observer/policy default to a no-op observer over the full
     * series.
     */
    public static final class Builder {

        private final String[] symbols;
        private final TimescaleConfig cfg;
        private int calmBars = -1;
        private String market = "";
        private String timescale = "";
        private SignalPublisher publisher;
        private PipelineObserver observer = PipelineObserver.noOp();
        private ObservationPolicy policy = ObservationPolicy.all();
        private Integer limit;
        private int contributorsTopK = 3;
        private CalibrationSource calibrationSource;

        private Builder(String[] symbols, TimescaleConfig cfg) {
            if (symbols == null || symbols.length == 0) {
                throw new IllegalArgumentException("symbols must be non-empty");
            }
            if (cfg == null) {
                throw new IllegalArgumentException("timescale config must not be null");
            }
            this.symbols = symbols.clone();
            this.cfg = cfg;
        }

        /** Window-points used to calibrate the detector before detection begins ({@code >= 2}). */
        public Builder calmBars(int n) {
            this.calmBars = n;
            return this;
        }

        /** The market label carried on emitted signals/observations. */
        public Builder market(String m) {
            this.market = m;
            return this;
        }

        /** The timescale label ({@code "daily"} / {@code "intraday"}) carried on emitted events. */
        public Builder timescale(String t) {
            this.timescale = t;
            return this;
        }

        /** The product fire-stream as a no-op-filtered sink (convenience for the common case). */
        public Builder sink(SignalSink sink) {
            this.publisher = new SignalPublisher(SignalFilter.acceptAll(), sink);
            return this;
        }

        /** The product fire-stream with an explicit filter + sink. */
        public Builder publisher(SignalPublisher p) {
            this.publisher = p;
            return this;
        }

        /** The consumer's observation observer (default: {@link PipelineObserver#noOp()}). */
        public Builder observer(PipelineObserver o) {
            this.observer = o == null ? PipelineObserver.noOp() : o;
            return this;
        }

        /** The consumer's observation policy (default: {@link ObservationPolicy#all()}). */
        public Builder observationPolicy(ObservationPolicy p) {
            this.policy = p == null ? ObservationPolicy.all() : p;
            return this;
        }

        /** Stop after this many detection points, or {@code null} for the whole stream. */
        public Builder limit(Integer n) {
            if (n != null && n < 1) {
                throw new IllegalArgumentException("limit must be >= 1 when set [" + n + "]");
            }
            this.limit = n;
            return this;
        }

        /**
         * How many top {@code |Δr|} contributing pairs each observation carries (default {@code 3});
         * {@code 0} disables attribution and skips the extra per-transition pass.
         *
         * @param k the number of contributing pairs to attach, {@code >= 0}
         * @return this builder
         */
        public Builder contributorsTopK(int k) {
            if (k < 0) {
                throw new IllegalArgumentException("contributorsTopK must be >= 0 [" + k + "]");
            }
            this.contributorsTopK = k;
            return this;
        }

        /**
         * The run's calibration source (default: {@link CalibrationSources#leadingWarmup} over
         * {@code calmBars} window-points — the engine's original behaviour). The seam the walk-forward
         * calm-block and adaptive modes plug into (H2 design §3.2); it lives on the engine, not on a
         * driver, so replay and live exercise the identical calibration lifecycle.
         *
         * @param source the source, or {@code null} to keep the leading-warmup default
         * @return this builder
         */
        public Builder calibrationSource(CalibrationSource source) {
            this.calibrationSource = source;
            return this;
        }

        /** Builds the engine, validating the required knobs. */
        public PipelineEngine build() {
            if (calmBars < 2) {
                throw new IllegalArgumentException("calmBars must be >= 2 [" + calmBars + "]");
            }
            if (publisher == null) {
                throw new IllegalArgumentException("a fire sink/publisher is required (call sink(..) or publisher(..))");
            }
            if (calibrationSource == null) {
                calibrationSource = CalibrationSources.leadingWarmup(calmBars, cfg.detector());
            }
            return new PipelineEngine(this);
        }
    }

    /** Stateful per-snapshot listener: calibrate on the calm prefix, then detect + publish + observe. */
    private static final class Listener implements CorrelationStreamListener {

        private final int order;
        private final double tau;
        private final TimescaleConfig cfg;
        private final String market;
        private final String timescale;
        private final int calmBars;
        private final SignalPublisher publisher;
        private final PipelineObserver observer;
        private final ObservationPolicy policy;
        private final Integer limit;
        private final int contributorsTopK;
        private final List<String> universe;
        private final CalibrationSource calibrationSource;

        private double[][] prev;
        private double[][] detectPrev;
        private ChangeDetector detector;
        private RearmCadence rearm;
        private Calibration calibrationResult;
        private int calmBarsSeen;
        private long detectionPoints;
        private long fires;
        private long published;
        private long observationsEmitted;
        private boolean stop;

        Listener(Builder b) {
            this.order = b.symbols.length;
            this.tau = b.cfg.edgeThreshold();
            this.cfg = b.cfg;
            this.market = b.market;
            this.timescale = b.timescale;
            this.calmBars = b.calmBars;
            this.publisher = b.publisher;
            this.observer = b.observer;
            this.policy = b.policy;
            this.limit = b.limit;
            this.contributorsTopK = b.contributorsTopK;
            this.universe = List.of(b.symbols);
            this.calibrationSource = b.calibrationSource;
        }

        @Override
        public void onSnapshot(long seq, Instant asOf, DoubleMatrix pearson, String[] labels) {
            double[][] current = toArray(pearson, order);
            if (detector == null) {
                calibrate(asOf, current);
            } else {
                detect(asOf, current);
            }
        }

        private void calibrate(Instant asOf, double[][] current) {
            ChangeMetrics m = ChangeMetricsAnalyzer.analyze(prev == null ? current : prev, current, tau);
            calibrationSource.observe(asOf, prev == null ? Double.NaN : m.weightedChange(), m.densityLevel());
            prev = current;
            calmBarsSeen++;
            if (calibrationSource.isReady()) {
                calibrationResult = calibrationSource.calibration();
                detector = ChangeDetectors.create(order, cfg.detector(), calibrationResult);
                // The continuous-stream re-arm cadence (H2 Q4): every onMatrix is windowed; the cadence
                // advances the id once per resolved regime event (all-clear / relaxation / backstop).
                rearm = new RearmCadence(cfg.rearm(), cfg.detector().defusion().enabled(),
                        cfg.detector().fireArm(), cfg.detector().h(), calibrationResult.level());
                detector.onMatrix(current, rearm.windowId());   // prime the predecessor; no transition
                detectPrev = current;         // mirror the detector's predecessor for contributor attribution
                drainCalibrationEvents(false);   // forward a cold-start PROMOTED_TO_LIVE, if queued
                LOG.info("calibrated on {} calm points: mu={} sigma={} L={} -- detecting...",
                        calmBarsSeen, fmt(calibrationResult.mu(), 4), fmt(calibrationResult.sigma(), 4),
                        fmt(calibrationResult.level(), 3));
            }
        }

        private void detect(Instant asOf, double[][] current) {
            ChangeSignal sig = detector.onMatrix(current, rearm.windowId());
            if (sig == null) {
                detectPrev = current;   // first matrix after a session-boundary re-prime: no transition
                return;
            }
            RearmCadence.Rearm rearmed = rearm.observe(sig);
            detectionPoints++;
            List<PairContribution> contributors = contributors(detectPrev, current);
            detectPrev = current;
            SignalKind kind = sig.fired() ? toSignalKind(sig.fireDirection()) : null;
            PipelineObservation obs = new PipelineObservation(asOf, market, timescale, sig.metrics(),
                    sig.sPlus(), sig.sMinus(), sig.recoveryGauge(), sig.fired(), kind, cfg.detector().h(),
                    calibrationResult.mu(), calibrationResult.sigma(), calibrationResult.level(), contributors);
            // The online half of the calibration seam: the adaptive source learns from every scored
            // transition (frozen sources no-op). The freeze condition is the in-fire state PLUS the
            // whole fused-awaiting-re-arm span — the baseline may not move while an all-clear is
            // still pending against it (numerics spec Q2 amendment; the may2021 suppression guard).
            boolean freeze = obs.lifecycle() != DetectorState.ARMED || rearm.awaitingRearm();
            calibrationSource.observeDetection(asOf, sig.metrics().weightedChange(),
                    sig.metrics().densityLevel(), freeze);
            if (rearmed == RearmCadence.Rearm.EXPIRED) {
                // The backstop expired an unresolved question: the freeze protected a PENDING
                // all-clear; expiry ends it, so the adaptive source re-baselines and re-warms
                // (numerics spec Q4.1 amendment — otherwise the stale frozen baseline re-fires
                // on the elevated structure every backstop period). The detector-side question
                // state must close with it: recalibrate deliberately keeps the wasFused latch and
                // gauge, so without the boundary reset the expired question could still sound a
                // LATE all-clear against a re-derived band — the moved-goalposts leak again.
                detector.onSessionBoundary();
                detectPrev = null;   // the boundary drops the predecessor: the next matrix re-primes
                calibrationSource.onRegimeExpired(asOf);
            }
            drainCalibrationEvents(true);
            if (policy.emit(obs)) {
                observer.onObservation(obs);
                observationsEmitted++;
            }
            if (sig.fired()) {
                fires++;
                // A demoted source (post-timeout re-warm-up) does not vouch for alerts: the raw fact
                // stays on the observation stream, but nothing reaches the product fire-stream.
                if (calibrationSource.live()) {
                    StructuralSignal signal = StructuralSignals.fromChangeSignal(
                            sig, asOf, market, timescale, universe, kind, null);
                    if (publisher.publish(signal).isPresent()) {
                        published++;
                    }
                }
            }
            if (limit != null && detectionPoints >= limit) {
                stop = true;
            }
        }

        /**
         * Forwards every pending calibration-lifecycle event to the observer and, when the event
         * re-baselined the source ({@code RECALIBRATED}/{@code EPOCH_OPENED}/{@code REGIME_TIMEOUT}/a
         * re-promotion), installs the fresh calibration on the running detector — a controlled
         * rebuild through the lib's {@code recalibrate}, arms reset, matrix/latch/gauge preserved
         * (numerics spec Q1). A demotion event carries no new baseline, so it is forwarded only.
         *
         * @param recalibrate whether a running detector exists to re-baseline (false during the
         *                    initial calibration hop, where the detector is built from scratch)
         */
        private void drainCalibrationEvents(boolean recalibrate) {
            Optional<CalibrationEvent> pending;
            while ((pending = calibrationSource.pollEvent()).isPresent()) {
                CalibrationEvent event = pending.get();
                if (recalibrate && event.kind() != CalibrationEventKind.DEMOTED_TO_CALIBRATING) {
                    calibrationResult = calibrationSource.calibration();
                    detector.recalibrate(calibrationResult);
                    rearm.recalibrated(calibrationResult.level());
                    LOG.info("calibration epoch {}: {} mu {} -> {}", event.epochId(), event.kind(),
                            fmt(event.muBefore(), 4), fmt(event.muAfter(), 4));
                }
                observer.onCalibrationEvent(event);
            }
        }

        /**
         * The top-k labelled pairs that moved most over {@code prev -> current} — the S3 index-based
         * contributors ({@link ChangeMetricsAnalyzer#topContributors}) resolved to this run's symbols.
         * Empty when attribution is disabled ({@code k == 0}) or there is no predecessor yet.
         */
        private List<PairContribution> contributors(double[][] prevMatrix, double[][] current) {
            if (contributorsTopK <= 0 || prevMatrix == null) {
                return List.of();
            }
            List<PairChange> ranked = ChangeMetricsAnalyzer.topContributors(prevMatrix, current, contributorsTopK);
            List<PairContribution> out = new ArrayList<>(ranked.size());
            for (PairChange pc : ranked) {
                out.add(new PairContribution(universe.get(pc.i()), universe.get(pc.j()), pc.absDelta()));
            }
            return out;
        }

        void onSessionBoundary() {
            if (detector != null) {
                detector.onSessionBoundary();
                rearm.onSessionBoundary();
            } else {
                prev = null;   // drop the calibration predecessor so no change spans the gap
            }
        }

        RunSummary summary() {
            return new RunSummary(detectionPoints, fires, published, observationsEmitted, calmBars);
        }
    }
}
