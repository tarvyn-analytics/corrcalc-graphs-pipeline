package ch.tarvynanalytics.corrcalc.graphs.pipeline.replay;

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
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SignalFilter;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SignalKind;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SignalPublisher;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SignalSink;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.StructuralSignal;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.StructuralSignals;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.Bar;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.PriceBars;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.ReturnPanel;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.ReturnPanels;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.UniverseCsv;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.TimescaleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The live, wall-clock-paced replay engine — the visualization counterpart to the batch
 * {@code backtest} scorer. It streams one timescale's stored bars through the <em>real</em> product
 * pipeline (S1 {@link RollingCorrelations} → S3 {@link ChangeDetector}) at a configurable speed and,
 * per transition, logs the calm metric heartbeat while publishing every genuine fire as a
 * {@link StructuralSignal} to the configured {@link SignalSink} (e.g. the
 * {@code ch.tarvynanalytics.corrcalc.graphs.pipeline.LoggingSink}, which highlights it).
 *
 * <p>Single S1 pass with an in-stream calibration boundary: the first {@code calmBars} window-points
 * calibrate the detector (μ, σ, level on the calm slice), then the detector is created and primed and
 * detection runs — paced — over the remainder. Calibration here is a leading warm-up of the replayed
 * series itself (a pragmatic choice for a visual first impression), not the rigorous walk-forward calm
 * block the regression uses.</p>
 */
public final class PacedReplay {

    private static final Logger LOG = LoggerFactory.getLogger("replay");
    private static final String INTRADAY_FREQ = "1m";
    private static final String DAILY_FREQ = "1d";
    private static final double DEFAULT_CALM_FRACTION = 0.4;

    private PacedReplay() {
    }

    /**
     * Loads the panel for {@code opts} from {@code dataDir} and replays it through the pipeline,
     * pacing with {@code clock} and publishing fires to {@code sink}.
     *
     * @param dataDir directory of {@code <SYMBOL>_<freq>_<event>.csv} bar files
     * @param opts    the run parameters
     * @param sink    the delivery sink for fired signals
     * @param clock   the pacing clock ({@link ReplayClock#of})
     * @return a run summary
     */
    public static Summary run(Path dataDir, ReplayOptions opts, SignalSink sink, ReplayClock clock) {
        TimescaleConfig cfg = resolveConfig(opts.market(), opts.timescale());
        ReturnPanel panel = loadPanel(dataDir, opts, freqFor(opts.timescale()));
        return stream(panel, cfg, opts, sink, clock);
    }

    /** Builds the aligned return panel for one timescale from the per-symbol bar files. */
    static ReturnPanel loadPanel(Path dataDir, ReplayOptions opts, String freq) {
        Path universePath = opts.universePath() != null
                ? opts.universePath()
                : dataDir.resolve(opts.event() + "_universe.csv");
        List<String> universe = UniverseCsv.read(universePath);
        Map<String, List<Bar>> prices = new LinkedHashMap<>();
        for (String symbol : universe) {
            Path csv = dataDir.resolve(symbol + "_" + freq + "_" + opts.event() + ".csv");
            prices.put(symbol, PriceBars.read(csv, opts.from(), opts.to()));
        }
        String[] symbols = universe.toArray(new String[0]);
        return "intraday".equals(opts.timescale())
                ? ReturnPanels.buildIntraday(prices, symbols)
                : ReturnPanels.buildDaily(prices, symbols);
    }

    /**
     * Replays an already-built panel (the testable streaming core — no filesystem).
     *
     * @param panel the aligned single-timescale return panel
     * @param cfg   the resolved timescale tuning (window + detector config)
     * @param opts  the run parameters
     * @param sink  the delivery sink for fired signals
     * @param clock the pacing clock
     * @return a run summary
     */
    static Summary stream(ReturnPanel panel, TimescaleConfig cfg, ReplayOptions opts,
                          SignalSink sink, ReplayClock clock) {
        double[][] returns = panel.returns();
        int window = cfg.window();
        int expectedPoints = Math.max(0, returns.length - window + 1);
        if (expectedPoints < 3) {
            throw new IllegalArgumentException("series too short to replay: need >= 3 window-points "
                    + "(bars - window + 1), got [" + expectedPoints + "] from [" + returns.length
                    + "] bars at window [" + window + "]");
        }
        int calmBars = resolveCalmBars(opts.calmBars(), expectedPoints);

        SignalPublisher publisher = new SignalPublisher(SignalFilter.acceptAll(), sink);
        StreamState state = new StreamState(panel.symbols(), cfg, opts, calmBars, clock, publisher);

        LOG.info("replay {} {}/{}: {} bars -> {} window-points (window={}, tau={}), calmBars={}, speed={}x",
                opts.event(), opts.market(), opts.timescale(), returns.length, expectedPoints, window,
                fmt(cfg.edgeThreshold(), 2), calmBars, fmt(clock.speed(), 1));

        RollingCorrelationEngine engine = RollingCorrelations.pearson(panel.symbols(), window, state);
        List<Instant> timestamps = panel.timestamps();
        for (int t = 0; t < returns.length; t++) {
            engine.onBar(timestamps.get(t), returns[t]);
            if (state.shouldStop() || Thread.currentThread().isInterrupted()) {
                break;
            }
        }
        Summary summary = state.summary();
        LOG.info("replay complete: {} detection points, {} fires, {} published",
                summary.detectionPoints(), summary.fires(), summary.published());
        return summary;
    }

    private static TimescaleConfig resolveConfig(String market, String timescale) {
        if (!"crypto".equals(market)) {
            throw new IllegalArgumentException("unsupported market [" + market + "]; supported: crypto");
        }
        return "daily".equals(timescale) ? TimescaleConfig.cryptoDaily() : TimescaleConfig.cryptoIntraday();
    }

    private static String freqFor(String timescale) {
        return "daily".equals(timescale) ? DAILY_FREQ : INTRADAY_FREQ;
    }

    private static int resolveCalmBars(Integer override, int expectedPoints) {
        int max = expectedPoints - 1;   // leave at least one detection transition
        if (override != null) {
            if (override > max) {
                throw new IllegalArgumentException("calmBars [" + override + "] leaves no detection points; "
                        + "max is [" + max + "] for [" + expectedPoints + "] window-points");
            }
            return override;
        }
        int auto = (int) Math.round(expectedPoints * DEFAULT_CALM_FRACTION);
        return Math.max(2, Math.min(auto, max));
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

    private static double[] toArray(List<Double> values) {
        double[] out = new double[values.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = values.get(i);
        }
        return out;
    }

    private static String fmt(double v, int decimals) {
        if (Double.isNaN(v)) {
            return "NaN";
        }
        return String.format(Locale.ROOT, "%." + decimals + "f", v);
    }

    /** Stateful per-snapshot driver: calibrate on the calm prefix, then pace + detect + publish. */
    private static final class StreamState implements CorrelationStreamListener {

        private final int order;
        private final double tau;
        private final TimescaleConfig cfg;
        private final ReplayOptions opts;
        private final int calmBars;
        private final ReplayClock clock;
        private final SignalPublisher publisher;
        private final List<String> universe;

        private final List<Double> calmChange = new ArrayList<>();
        private final List<Double> calmDensity = new ArrayList<>();
        private double[][] prev;
        private ChangeDetector detector;
        private int calmIdx;
        private Instant prevTs;
        private long detectionPoints;
        private long fires;
        private long published;
        private boolean stop;

        StreamState(String[] symbols, TimescaleConfig cfg, ReplayOptions opts, int calmBars,
                    ReplayClock clock, SignalPublisher publisher) {
            this.order = symbols.length;
            this.tau = cfg.edgeThreshold();
            this.cfg = cfg;
            this.opts = opts;
            this.calmBars = calmBars;
            this.clock = clock;
            this.publisher = publisher;
            this.universe = List.of(symbols);
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
            calmDensity.add(m.densityLevel());
            calmChange.add(prev == null ? Double.NaN : m.weightedChange());
            prev = current;
            calmIdx++;
            if (calmIdx >= calmBars) {
                Calibration cal = ChangeDetectors.calibrate(toArray(calmChange), toArray(calmDensity), cfg.detector());
                detector = ChangeDetectors.create(order, cfg.detector(), cal);
                detector.onMatrix(current);   // prime the predecessor; first call yields no transition
                prevTs = asOf;
                LOG.info("calibrated on {} calm points: mu={} sigma={} L={} -- detecting...",
                        calmBars, fmt(cal.mu(), 4), fmt(cal.sigma(), 4), fmt(cal.level(), 3));
            }
        }

        private void detect(Instant asOf, double[][] current) {
            clock.pace(prevTs, asOf);
            ChangeSignal sig = detector.onMatrix(current);
            prevTs = asOf;
            if (sig == null) {
                return;
            }
            detectionPoints++;
            if (detectionPoints % opts.heartbeatEvery() == 0) {
                heartbeat(asOf, sig);
            }
            if (sig.fired()) {
                fires++;
                StructuralSignal signal = StructuralSignals.fromChangeSignal(
                        sig, asOf, opts.market(), opts.timescale(), universe, SignalKind.FUSION, null);
                if (publisher.publish(signal).isPresent()) {
                    published++;
                }
            }
            if (opts.limit() != null && detectionPoints >= opts.limit()) {
                stop = true;
            }
        }

        private void heartbeat(Instant asOf, ChangeSignal sig) {
            ChangeMetrics m = sig.metrics();
            LOG.info("{}  density={}  wD={}  S+={}  S-={}",
                    asOf, fmt(m.densityLevel(), 3), fmt(m.weightedChange(), 4),
                    fmt(sig.sPlus(), 2), fmt(sig.sMinus(), 2));
        }

        boolean shouldStop() {
            return stop;
        }

        Summary summary() {
            return new Summary(detectionPoints, fires, published, calmBars);
        }
    }

    /**
     * The outcome of one replay run.
     *
     * @param detectionPoints transitions scored after calibration
     * @param fires           transitions that fired
     * @param published       fires that passed the filter and reached the sink
     * @param calmBars        window-points used for calibration
     */
    public record Summary(long detectionPoints, long fires, long published, int calmBars) {
    }
}
