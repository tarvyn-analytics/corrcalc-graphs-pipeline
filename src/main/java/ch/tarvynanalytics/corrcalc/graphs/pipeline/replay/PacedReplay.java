package ch.tarvynanalytics.corrcalc.graphs.pipeline.replay;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.ObservationPolicy;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObserver;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SignalSink;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.Bar;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.PriceBars;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.ReturnPanel;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.ReturnPanels;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.UniverseCsv;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.TimescaleConfig;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.engine.PipelineEngine;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.engine.RunSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The live, wall-clock-paced replay <em>driver</em> — the visualization counterpart to the batch
 * {@code backtest} scorer. It loads one timescale's stored bars, builds the aligned return panel, and
 * feeds it bar-by-bar into a {@link PipelineEngine} (S1 {@code RollingCorrelations} → S3
 * {@code ChangeDetector}), pacing the <em>detection</em> phase to wall-clock time scaled by a speed
 * multiplier. Fires reach the configured {@link SignalSink} (the product); every transition reaches the
 * configured {@link PipelineObserver} through the consumer's {@link ObservationPolicy}.
 *
 * <p>This class owns only the two things a replay adds over the engine: <em>where the data comes
 * from</em> (the stored-bar panel) and <em>pacing</em>. The pipeline composition itself lives in
 * {@link PipelineEngine}. Calibration here is a leading warm-up of the replayed series (a pragmatic
 * choice for a visual first impression), not the rigorous walk-forward calm block the regression uses.</p>
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
     * pacing with {@code clock}, publishing fires to {@code sink} and every transition to
     * {@code observer} (gated by {@code policy}).
     *
     * @param dataDir  directory of {@code <SYMBOL>_<freq>_<event>.csv} bar files
     * @param opts     the run parameters
     * @param sink     the delivery sink for fired signals (the product fire-stream)
     * @param observer the observation observer for the full transition series
     * @param policy   the consumer's policy deciding which observations reach {@code observer}
     * @param clock    the pacing clock ({@link ReplayClock#of})
     * @return a run summary
     */
    public static RunSummary run(Path dataDir, ReplayOptions opts, SignalSink sink,
                                 PipelineObserver observer, ObservationPolicy policy, ReplayClock clock) {
        TimescaleConfig cfg = resolveConfig(opts.market(), opts.timescale());
        ReturnPanel panel = loadPanel(dataDir, opts, freqFor(opts.timescale()));
        return stream(panel, cfg, opts, sink, observer, policy, clock);
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
     * Replays an already-built panel through a {@link PipelineEngine} (the testable streaming core —
     * no filesystem). Pacing is applied only to the detection phase: the warm-up and the calm
     * calibration prefix run as fast as possible, then each post-calibration transition is paced.
     *
     * @param panel    the aligned single-timescale return panel
     * @param cfg      the resolved timescale tuning (window + detector config)
     * @param opts     the run parameters
     * @param sink     the delivery sink for fired signals
     * @param observer the observation observer for the full transition series
     * @param policy   the consumer's observation policy
     * @param clock    the pacing clock
     * @return a run summary
     */
    static RunSummary stream(ReturnPanel panel, TimescaleConfig cfg, ReplayOptions opts, SignalSink sink,
                             PipelineObserver observer, ObservationPolicy policy, ReplayClock clock) {
        double[][] returns = panel.returns();
        int window = cfg.window();
        int expectedPoints = Math.max(0, returns.length - window + 1);
        if (expectedPoints < 3) {
            throw new IllegalArgumentException("series too short to replay: need >= 3 window-points "
                    + "(bars - window + 1), got [" + expectedPoints + "] from [" + returns.length
                    + "] bars at window [" + window + "]");
        }
        int calmBars = resolveCalmBars(opts.calmBars(), expectedPoints);

        PipelineEngine engine = PipelineEngine.builder(panel.symbols(), cfg)
                .calmBars(calmBars)
                .market(opts.market())
                .timescale(opts.timescale())
                .sink(sink)
                .observer(observer)
                .observationPolicy(policy)
                .limit(opts.limit())
                .build();

        LOG.info("replay {} {}/{}: {} bars -> {} window-points (window={}, tau={}), calmBars={}, speed={}x",
                opts.event(), opts.market(), opts.timescale(), returns.length, expectedPoints, window,
                fmt(cfg.edgeThreshold(), 2), calmBars, fmt(clock.speed(), 1));

        List<Instant> timestamps = panel.timestamps();
        Instant prevTs = null;
        for (int t = 0; t < returns.length; t++) {
            Instant ts = timestamps.get(t);
            if (engine.isCalibrated()) {
                clock.pace(prevTs, ts);   // pace only the detection phase, as before
            }
            engine.onReturns(ts, returns[t]);
            prevTs = ts;
            if (engine.stopRequested() || Thread.currentThread().isInterrupted()) {
                break;
            }
        }
        RunSummary summary = engine.summary();
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

    private static String fmt(double v, int decimals) {
        if (Double.isNaN(v)) {
            return "NaN";
        }
        return String.format(Locale.ROOT, "%." + decimals + "f", v);
    }
}
