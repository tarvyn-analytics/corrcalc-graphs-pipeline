package ch.tarvynanalytics.corrcalc.graphs.pipeline.engine;

import ch.tarvynanalytics.corrcalc.lib.matrix.DoubleMatrix;
import ch.tarvynanalytics.corrcalc.lib.stream.CorrelationStreamListener;
import ch.tarvynanalytics.corrcalc.lib.stream.RollingCorrelationEngine;
import ch.tarvynanalytics.corrcalc.lib.stream.RollingCorrelations;
import ch.tarvynanalytics.graphs.algos.Calibration;
import ch.tarvynanalytics.graphs.algos.ChangeDetector;
import ch.tarvynanalytics.graphs.algos.ChangeDetectors;
import ch.tarvynanalytics.graphs.algos.ChangeMetricsAnalyzer;
import ch.tarvynanalytics.graphs.algos.RegimeDetector;
import ch.tarvynanalytics.graphs.algos.RegimeDetectors;
import ch.tarvynanalytics.graphs.algos.model.ChangeMetrics;
import ch.tarvynanalytics.graphs.algos.model.ChangeSignal;
import ch.tarvynanalytics.graphs.algos.model.FireDirection;
import ch.tarvynanalytics.graphs.algos.model.PairChange;
import ch.tarvynanalytics.graphs.algos.model.RegimeSignal;
import ch.tarvynanalytics.graphs.algos.model.RegimeState;
import ch.tarvynanalytics.graphs.algos.model.RegimeTransition;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.CalibrationEvent;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.CalibrationEventKind;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.DetectorState;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.ObservationPolicy;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PairContribution;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationSource;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationSources;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObservation;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObserver;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.RegimeEvent;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.RegimeEventKind;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SignalFilter;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SignalKind;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SignalPublisher;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SignalSink;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.StructuralSignal;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.StructuralSignals;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SymbolVolatilityObservation;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.SymbolVolatilityBaseline;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.PairUniverse;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.RegimeSeries;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.RegimeTimescaleConfig;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.SymbolVolatilityConfig;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.SymbolVolatilitySeries;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.TimescaleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * The pipeline orchestrator — the single composition point where the two upstream library engines and
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
     * Feeds one aligned close cross-section to the optional per-symbol volatility channel. No-op
     * unless {@link Builder#symbolVolatility} was configured. Call once per snapshot, <em>before</em>
     * the same bar's {@link #onReturns}, on the single ingest thread ({@link PipelineDriver#run}
     * does exactly this). Once the channel is warm, the observer's
     * {@code onSymbolVolatility} is invoked from inside this call — therefore always before the
     * same bar's {@code onObservation}.
     *
     * <p>Session boundaries never reset this channel: its windows and predecessor state slide
     * straight through day boundaries; its only discontinuity treatment is the configured gap
     * mask.</p>
     *
     * @param asOf   the cross-section's instant (UTC)
     * @param closes one close per universe symbol, in the engine's column order
     */
    public void onCloses(Instant asOf, double[] closes) {
        listener.onCloses(asOf, closes);
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

    /**
     * Signals end-of-stream: in the regime-backbone fire mode, flushes the daily-density aggregator so
     * the trailing days are read, emits any final regime edge, and reports a regime still fused at the
     * tape end ({@link RegimeEventKind#OPEN_AT_EOF}) rather than force-closing it. A no-op in the
     * adaptive-CUSUM fire mode. The driver calls this once after the last bar, before the run summary.
     */
    public void finish() {
        listener.finish();
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
        private RegimeTimescaleConfig regime;
        private boolean observeDensity;
        private SymbolVolatilityConfig volatilityConfig;
        private SymbolVolatilityBaseline volatilityBaseline;
        private PairUniverse pairUniverse = PairUniverse.ALL;

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
         * calm-block and adaptive modes plug into; it lives on the engine, not on a
         * driver, so replay and live exercise the identical calibration lifecycle.
         *
         * @param source the source, or {@code null} to keep the leading-warmup default
         * @return this builder
         */
        public Builder calibrationSource(CalibrationSource source) {
            this.calibrationSource = source;
            return this;
        }

        /**
         * Enables the {@code --observe density} observation mode: in regime-backbone fire mode,
         * the engine forwards each finalized daily smoothed level to
         * {@link PipelineObserver#onDensityLevel} as it is emitted from the daily aggregator. The
         * {@link ch.tarvynanalytics.corrcalc.graphs.pipeline.NdjsonObserver} converts these to
         * {@code density} NDJSON records. No-op in the default adaptive-CUSUM fire mode (no regime agg).
         *
         * @return this builder
         */
        public Builder observeDensity(boolean enabled) {
            this.observeDensity = enabled;
            return this;
        }

        /**
         * Selects the <strong>regime-backbone</strong> fire mode: the continuous-tape fire is the
         * level+hysteresis Schmitt trigger on the daily-smoothed correlation density, and
         * the CUSUM detector is demoted to an annotation layer — it still scores and observes, but its
         * fires no longer reach the product fire-stream. One {@link StructuralSignal} is published per
         * regime edge ({@link RegimeEventKind#FUSION_ONSET} → {@link SignalKind#FUSION},
         * {@link RegimeEventKind#CALM_ONSET} → {@link SignalKind#DEFUSION}) and one {@link RegimeEvent} is
         * forwarded to the observer. Leaving this unset keeps the default <strong>adaptive-CUSUM</strong>
         * fire mode, byte-for-byte the n=8 regression path (pinned by the leading-warmup regression).
         *
         * @param regimeConfig the regime timescale tuning, or {@code null} to keep the CUSUM fire mode
         * @return this builder
         */
        public Builder regime(RegimeTimescaleConfig regimeConfig) {
            this.regime = regimeConfig;
            return this;
        }

        /**
         * Enables the optional <strong>per-symbol realized-volatility diagnostic channel</strong>:
         * the engine additionally accepts aligned close cross-sections via
         * {@link PipelineEngine#onCloses} and, once the channel is warm, forwards one
         * {@link SymbolVolatilityObservation} per bar to
         * {@link PipelineObserver#onSymbolVolatility} — alongside (and always before) the same
         * bar's matrix-level observation. Leaving this unset keeps the engine byte-identical to a
         * build without the channel; {@code onCloses} is then a no-op.
         *
         * @param config   the channel tuning (windows + gap mask)
         * @param baseline the frozen per-column baseline; its symbols must equal the engine
         *                 symbols in order (validated at {@link #build()})
         * @return this builder
         * @throws IllegalArgumentException if either argument is null
         */
        public Builder symbolVolatility(SymbolVolatilityConfig config, SymbolVolatilityBaseline baseline) {
            if (config == null || baseline == null) {
                throw new IllegalArgumentException(
                        "symbolVolatility requires both a config and a baseline");
            }
            this.volatilityConfig = config;
            this.volatilityBaseline = baseline;
            return this;
        }

        /**
         * The pair universe the density metric normalizes over (design/23 §3.5): {@code allPairs /
         * activePairs(asOf)} is applied at the regime read, the observation's density, the
         * calibration source's density argument, and the re-arm cadence's relaxation gate compare
         * (the cadence's {@code levelGate} is learned from the same normalized density, so its
         * compare must use it too — a raw compare would relax/re-arm early). Default
         * {@link PairUniverse#ALL}, under which the engine is byte-identical to a build without
         * this seam.
         *
         * <p>Two seams stay raw-world regardless: the non-regime fire-stream's
         * {@link StructuralSignal#levelDensity()} ({@link StructuralSignals#fromChangeSignal} carries
         * the detector's raw output for the firing transition, by design), and the S3 detector's own
         * internal level-gate compare inside {@code graphs-algos-lib}'s {@code CusumChangeDetector} —
         * that library has no {@link PairUniverse} concept, a lag design/23 §3.8d already accepts.</p>
         *
         * @param universe the seam, or {@code null} to keep the {@link PairUniverse#ALL} default
         * @return this builder
         */
        public Builder pairUniverse(PairUniverse universe) {
            this.pairUniverse = universe == null ? PairUniverse.ALL : universe;
            return this;
        }

        /** Builds the engine, validating the required knobs. */
        public PipelineEngine build() {
            if (calmBars < 2) {
                throw new IllegalArgumentException("calmBars must be >= 2 [" + calmBars + "]");
            }
            if (volatilityBaseline != null && !volatilityBaseline.symbols().equals(List.of(symbols))) {
                throw new IllegalArgumentException("symbol-volatility baseline symbols ["
                        + volatilityBaseline.symbols() + "] must equal the engine symbols ["
                        + List.of(symbols) + "] in order");
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
        private final long allPairs;
        private final PairUniverse pairUniverse;
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

        // Regime-backbone fire mode: null in the default adaptive-CUSUM mode. When present, the
        // daily-density Schmitt trigger is the fire and the CUSUM path (below) is demoted to annotation.
        private final RegimeSeries.DailyAggregator regimeAgg;
        private final RegimeDetector regimeDetector;
        private final double regimeHi;    // the Schmitt high mark — the pipeline owns it, for confidence
        private final double regimeLo;    // the Schmitt low mark
        private final boolean observeDensity;  // --observe density: forward each daily level to the observer

        // The optional per-symbol volatility channel: null unless Builder.symbolVolatility was set.
        private final SymbolVolatilitySeries volatilitySeries;

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
        // Regime-backbone state: the last regime, the open fusion's onset day, and the latest daily read.
        private RegimeState regimeState = RegimeState.CALM;
        private Instant regimeOnset;
        private Instant lastRegimeDay;
        private double lastRegimeLevel = Double.NaN;
        private boolean finished;

        Listener(Builder b) {
            this.order = b.symbols.length;
            this.allPairs = (long) order * (order - 1) / 2;
            this.pairUniverse = b.pairUniverse;
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
            if (b.regime != null) {
                this.regimeAgg = new RegimeSeries.DailyAggregator(b.regime.smoothWindow());
                this.regimeDetector = RegimeDetectors.create(b.regime.regime());
                this.regimeHi = b.regime.regime().hi();
                this.regimeLo = b.regime.regime().lo();
            } else {
                this.regimeAgg = null;
                this.regimeDetector = null;
                this.regimeHi = Double.NaN;
                this.regimeLo = Double.NaN;
            }
            this.observeDensity = b.observeDensity;
            this.volatilitySeries = b.volatilityConfig == null
                    ? null
                    : new SymbolVolatilitySeries(b.volatilityConfig, b.volatilityBaseline);
        }

        /** Whether this run drives the regime-backbone fire (vs the default adaptive-CUSUM fire). */
        private boolean regimeMode() {
            return regimeDetector != null;
        }

        /**
         * Advances the per-symbol volatility channel by one close cross-section and, once the
         * channel emits, forwards the bar to the observer. A strict no-op when the channel is not
         * configured.
         */
        void onCloses(Instant asOf, double[] closes) {
            if (volatilitySeries == null) {
                return;   // channel not configured: the engine stays byte-identical
            }
            double[] zScores = volatilitySeries.onCloses(asOf, closes);
            if (zScores != null) {
                observer.onSymbolVolatility(
                        new SymbolVolatilityObservation(asOf, market, timescale, universe, zScores));
            }
        }

        @Override
        public void onSnapshot(long seq, Instant asOf, DoubleMatrix pearson, String[] labels) {
            double[][] current = toArray(pearson, order);
            if (regimeMode()) {
                // The regime read is independent of the CUSUM calibration: it consumes the raw density
                // from the first window-fill, aggregated to daily means then smoothed.
                double density = normalizedDensity(asOf,
                        ChangeMetricsAnalyzer.analyze(current, current, tau).densityLevel());
                for (RegimeSeries.DailyLevel daily : regimeAgg.onDensity(asOf, density)) {
                    stepRegime(daily);
                }
            }
            if (detector == null) {
                calibrate(asOf, current);
            } else {
                detect(asOf, current);
            }
        }

        /**
         * Advances the regime Schmitt trigger by one smoothed daily level and, on a crossing, opens or
         * closes a fused regime — publishing one {@link StructuralSignal} and forwarding one
         * {@link RegimeEvent}. The down-crossing (calm onset) is the all-clear; the up-crossing opens a
         * regime. This is the whole continuous-tape fire: no re-arm clock, no freeze.
         */
        private void stepRegime(RegimeSeries.DailyLevel daily) {
            if (observeDensity) {
                observer.onDensityLevel(daily.day(), daily.level());
            }
            RegimeSignal rs = regimeDetector.step(daily.level());
            regimeState = rs.state();
            lastRegimeDay = daily.day();
            lastRegimeLevel = daily.level();
            if (rs.transition() == RegimeTransition.FUSION_ONSET) {
                regimeOnset = daily.day();
                emitRegimeEdge(daily.day(), RegimeEventKind.FUSION_ONSET, daily.level(), daily.day());
            } else if (rs.transition() == RegimeTransition.CALM_ONSET) {
                // regimeOnset is always set here: a calm onset can only follow a fusion onset.
                emitRegimeEdge(daily.day(), RegimeEventKind.CALM_ONSET, daily.level(), regimeOnset);
            }
        }

        /**
         * Forwards a regime edge to the observer and, for a fusion/calm onset, publishes the matching
         * {@link StructuralSignal} to the product fire-stream ({@link RegimeEventKind#OPEN_AT_EOF} is an
         * observability marker, so it is forwarded but never published).
         */
        private void emitRegimeEdge(Instant day, RegimeEventKind kind, double level, Instant onset) {
            observer.onRegimeEvent(new RegimeEvent(day, kind, level, confidenceFor(kind, level), onset));
            if (kind == RegimeEventKind.OPEN_AT_EOF) {
                return;
            }
            fires++;
            StructuralSignal signal = regimeSignal(day, kind.toSignalKind(), level);
            if (publisher.publish(signal).isPresent()) {
                published++;
            }
        }

        /**
         * How decisive a regime crossing was, in {@code [0, 1]}: the margin past the mark normalised by
         * the room beyond it — {@code (level−hi)/(1−hi)} at a fusion onset, {@code (lo−level)/lo} at a
         * calm onset (0 at the mark, 1 at a saturated / empty graph). This is the pipeline's reporting
         * choice (it owns the marks); the GAL trigger never computes it. {@code NaN} for an open-at-EOF
         * marker (no crossing happened).
         */
        private double confidenceFor(RegimeEventKind kind, double level) {
            return switch (kind) {
                case FUSION_ONSET -> clamp01((level - regimeHi) / (1.0 - regimeHi));
                case CALM_ONSET -> clamp01((regimeLo - level) / Math.max(regimeLo, 1e-9));
                case OPEN_AT_EOF -> Double.NaN;
            };
        }

        private static double clamp01(double v) {
            if (Double.isNaN(v)) {
                return Double.NaN;
            }
            return Math.max(0.0, Math.min(1.0, v));
        }

        /** A product signal for a regime edge: the smoothed density is the level; CUSUM fields are N/A. */
        private StructuralSignal regimeSignal(Instant day, SignalKind kind, double level) {
            return new StructuralSignal(day, market, timescale, universe, kind,
                    Double.NaN, Double.NaN, Double.NaN, level, Double.NaN, 0, Double.NaN,
                    StructuralSignal.Validity.accepted(), null);
        }

        private void calibrate(Instant asOf, double[][] current) {
            ChangeMetrics m = ChangeMetricsAnalyzer.analyze(prev == null ? current : prev, current, tau);
            calibrationSource.observe(asOf, prev == null ? Double.NaN : m.weightedChange(),
                    normalizedDensity(asOf, m.densityLevel()));
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
            // W1 (design/23 §3.8d): re-read the calibration once per scored bar, not only on a
            // drained event, so a live-L bridge that queues no CalibrationEvent (e.g. a
            // panel-membership change) still reaches this bar's gate. No-op for every existing run:
            // calibration() only moves inside a call that also queues a matching event, and
            // drainCalibrationEvents(true) below already re-reads it before this method returns — so
            // this read always agrees with what the prior bar's drain already cached (VD-5).
            calibrationResult = calibrationSource.calibration();
            // Hoisted above rearm.observe (design/23 3.5's 4th point): the relaxation rule's gate
            // compare is density < levelGate, and levelGate is learned in normalized units the
            // moment a PairUniverse is active (the calibration source observes normalizedDensity —
            // calibrate()/observeDetection above) -- so the cadence must be handed the same
            // normalized density, never the signal's raw metrics().densityLevel().
            ChangeMetrics metrics = withNormalizedDensity(asOf, sig.metrics());
            RearmCadence.Rearm rearmed = rearm.observe(sig, metrics.densityLevel());
            detectionPoints++;
            List<PairContribution> contributors = contributors(detectPrev, current);
            detectPrev = current;
            SignalKind kind = sig.fired() ? toSignalKind(sig.fireDirection()) : null;
            PipelineObservation obs = new PipelineObservation(asOf, market, timescale, metrics,
                    sig.sPlus(), sig.sMinus(), sig.recoveryGauge(), sig.fired(), kind, cfg.detector().h(),
                    calibrationResult.mu(), calibrationResult.sigma(), calibrationResult.level(), contributors);
            // The online half of the calibration seam: the adaptive source learns from every scored
            // transition (frozen sources no-op). The freeze condition is the in-fire state PLUS the
            // whole fused-awaiting-re-arm span — the baseline may not move while an all-clear is
            // still pending against it (the may2021-class suppression guard).
            boolean freeze = obs.lifecycle() != DetectorState.ARMED || rearm.awaitingRearm();
            calibrationSource.observeDetection(asOf, metrics.weightedChange(), metrics.densityLevel(), freeze);
            if (rearmed == RearmCadence.Rearm.EXPIRED) {
                // The backstop expired an unresolved question: the freeze protected a PENDING
                // all-clear; expiry ends it, so the adaptive source re-baselines and re-warms
                // (otherwise the stale frozen baseline re-fires
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
            // In the regime-backbone mode the CUSUM is demoted to annotation: its fired fact stays on the
            // observation stream (above) but never counts as a product fire or reaches the fire-stream —
            // the regime edge is the fire. In the default mode this is the product fire.
            if (sig.fired() && !regimeMode()) {
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
         * rebuild through the lib's {@code recalibrate}, arms reset, matrix/latch/gauge preserved.
         * A demotion event carries no new baseline, so it is forwarded only.
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
         * Rescales a raw density by {@code allPairs / activePairs(asOf)} when the {@link PairUniverse}
         * reports fewer pairs than this run's fixed {@code C(order,2)}; returns {@code raw} verbatim
         * otherwise (design/23 §3.5) — the default {@link PairUniverse#ALL} keeps every consumer
         * byte-identical. No clamp: a result {@code > 1.0} means the seam and the pair count disagree,
         * a defect to surface, not hide.
         */
        private double normalizedDensity(Instant asOf, double raw) {
            long active = pairUniverse.activePairs(asOf);
            return active <= 0 || active >= allPairs ? raw : raw * (double) allPairs / active;
        }

        /**
         * Rebuilds {@code metrics} with its {@link ChangeMetrics#densityLevel()} replaced by
         * {@link #normalizedDensity}; every other field carries through unchanged.
         */
        private ChangeMetrics withNormalizedDensity(Instant asOf, ChangeMetrics metrics) {
            return new ChangeMetrics(metrics.weightedChange(), normalizedDensity(asOf, metrics.densityLevel()),
                    metrics.edgeXor(), metrics.nComponents(), metrics.largestComponentFraction(),
                    metrics.componentSizes());
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
            // The regime read is a daily-cadence product that spans sessions by design: a UTC-day
            // boundary must NOT reset the aggregator or the Schmitt trigger (that is what lets one fused
            // regime run across many days). So the regime state is deliberately untouched here.
        }

        /**
         * End-of-stream: flush the daily aggregator so the open day is finalized and its trailing-median
         * smoothed level is emitted, step the trigger over it, then — if the regime is still fused —
         * report it open at EOF instead of force-closing it. Idempotent.
         */
        void finish() {
            if (!regimeMode() || finished) {
                return;
            }
            finished = true;
            for (RegimeSeries.DailyLevel daily : regimeAgg.flush()) {
                stepRegime(daily);
            }
            if (regimeState == RegimeState.FUSED && lastRegimeDay != null) {
                // regimeOnset is set whenever the state is FUSED (a fusion onset put it there).
                emitRegimeEdge(lastRegimeDay, RegimeEventKind.OPEN_AT_EOF, lastRegimeLevel, regimeOnset);
            }
        }

        RunSummary summary() {
            return new RunSummary(detectionPoints, fires, published, observationsEmitted, calmBars);
        }
    }
}
