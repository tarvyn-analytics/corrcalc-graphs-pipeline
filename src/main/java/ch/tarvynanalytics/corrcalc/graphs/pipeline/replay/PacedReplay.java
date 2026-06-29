package ch.tarvynanalytics.corrcalc.graphs.pipeline.replay;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.ObservationPolicy;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObserver;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SignalSink;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.Bar;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.PriceBars;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.PriceSnapshots;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.ReturnBuilder;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.ReturnPanel;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.SessionPolicy;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.UniverseCsv;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.TimescaleConfig;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.engine.PipelineDriver;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.engine.PipelineEngine;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.engine.RunSummary;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.IterableMarketDataSource;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.MarketDataSource;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.MarketSnapshot;
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
 * {@code backtest} scorer. It loads one timescale's stored bars, aligns them into
 * {@link MarketSnapshot}s, and replays them through the real inbound seam:
 * {@code IterableMarketDataSource → ReturnBuilder → PipelineEngine}, driven by {@link PipelineDriver}
 * and paced by a {@link ReplayClock}. Stored bars are simply a {@link MarketDataSource}, so replay and a
 * future live feed differ only in the source and the pace.
 *
 * <p>This class owns only what a replay adds over the engine: <em>where the data comes from</em> (the
 * stored-bar source) and <em>pacing</em> (the detection phase, scaled by speed). Fires reach the
 * {@link SignalSink} (the product); every transition reaches the {@link PipelineObserver} through the
 * consumer's {@link ObservationPolicy}. Calibration here is a leading warm-up of the replayed series (a
 * pragmatic choice for a visual first impression), not the rigorous walk-forward calm block the
 * regression uses.</p>
 */
public final class PacedReplay {

    private static final Logger LOG = LoggerFactory.getLogger("replay");
    private static final String DAILY = "daily";
    private static final String INTRADAY_FREQ = "1m";
    private static final String DAILY_FREQ = "1d";
    private static final double DEFAULT_CALM_FRACTION = 0.4;

    private PacedReplay() {
    }

    /**
     * Loads the stored bars for {@code opts} from {@code dataDir} and replays them through the inbound
     * seam, pacing with {@code clock}, publishing fires to {@code sink} and every transition to
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
        SessionPolicy sessionPolicy = sessionPolicyFor(opts.timescale());
        String[] symbols = loadUniverse(dataDir, opts);
        Map<String, List<Bar>> prices = loadPrices(dataDir, opts, freqFor(opts.timescale()), symbols);
        List<MarketSnapshot> snapshots = PriceSnapshots.align(prices, symbols);

        int returnBars = ReturnBuilder.countReturns(snapshots, sessionPolicy);
        int expectedPoints = validateExpectedPoints(returnBars, cfg.window());
        int calmBars = resolveCalmBars(opts.calmBars(), expectedPoints);

        PipelineEngine engine = buildEngine(symbols, cfg, opts, calmBars, sink, observer, policy);
        logStart(opts, returnBars, expectedPoints, cfg, calmBars, clock.speed());

        MarketDataSource source = new IterableMarketDataSource(symbols, snapshots);
        ReturnBuilder builder = new ReturnBuilder(symbols, sessionPolicy);
        RunSummary summary = PipelineDriver.run(source, builder, engine, clock);
        logDone(summary);
        observer.onComplete(summary);
        return summary;
    }

    /**
     * Replays an already-built panel through a {@link PipelineEngine} (the no-filesystem streaming core
     * used by tests). Pacing is applied only to the detection phase: the warm-up and the calm
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
        int expectedPoints = validateExpectedPoints(returns.length, cfg.window());
        int calmBars = resolveCalmBars(opts.calmBars(), expectedPoints);

        PipelineEngine engine = buildEngine(panel.symbols(), cfg, opts, calmBars, sink, observer, policy);
        logStart(opts, returns.length, expectedPoints, cfg, calmBars, clock.speed());

        List<Instant> timestamps = panel.timestamps();
        Instant prevTs = null;
        for (int t = 0; t < returns.length; t++) {
            Instant ts = timestamps.get(t);
            if (engine.isCalibrated()) {
                clock.pace(prevTs, ts);   // pace only the detection phase
            }
            engine.onReturns(ts, returns[t]);
            prevTs = ts;
            if (engine.stopRequested() || Thread.currentThread().isInterrupted()) {
                break;
            }
        }
        RunSummary summary = engine.summary();
        logDone(summary);
        observer.onComplete(summary);
        return summary;
    }

    private static PipelineEngine buildEngine(String[] symbols, TimescaleConfig cfg, ReplayOptions opts,
                                              int calmBars, SignalSink sink, PipelineObserver observer,
                                              ObservationPolicy policy) {
        return PipelineEngine.builder(symbols, cfg)
                .calmBars(calmBars)
                .market(opts.market())
                .timescale(opts.timescale())
                .sink(sink)
                .observer(observer)
                .observationPolicy(policy)
                .limit(opts.limit())
                .build();
    }

    private static String[] loadUniverse(Path dataDir, ReplayOptions opts) {
        Path universePath = opts.universePath() != null
                ? opts.universePath()
                : dataDir.resolve(opts.event() + "_universe.csv");
        return UniverseCsv.read(universePath).toArray(new String[0]);
    }

    private static Map<String, List<Bar>> loadPrices(Path dataDir, ReplayOptions opts, String freq,
                                                     String[] symbols) {
        Map<String, List<Bar>> prices = new LinkedHashMap<>();
        for (String symbol : symbols) {
            Path csv = dataDir.resolve(symbol + "_" + freq + "_" + opts.event() + ".csv");
            prices.put(symbol, PriceBars.read(csv, opts.from(), opts.to()));
        }
        return prices;
    }

    private static int validateExpectedPoints(int returnBars, int window) {
        int expectedPoints = Math.max(0, returnBars - window + 1);
        if (expectedPoints < 3) {
            throw new IllegalArgumentException("series too short to replay: need >= 3 window-points "
                    + "(bars - window + 1), got [" + expectedPoints + "] from [" + returnBars
                    + "] return bars at window [" + window + "]");
        }
        return expectedPoints;
    }

    private static TimescaleConfig resolveConfig(String market, String timescale) {
        if (!"crypto".equals(market)) {
            throw new IllegalArgumentException("unsupported market [" + market + "]; supported: crypto");
        }
        return DAILY.equals(timescale) ? TimescaleConfig.cryptoDaily() : TimescaleConfig.cryptoIntraday();
    }

    private static String freqFor(String timescale) {
        return DAILY.equals(timescale) ? DAILY_FREQ : INTRADAY_FREQ;
    }

    private static SessionPolicy sessionPolicyFor(String timescale) {
        return DAILY.equals(timescale) ? SessionPolicy.DAILY_SINGLE : SessionPolicy.INTRADAY_UTC_DAY;
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

    private static void logStart(ReplayOptions opts, int returnBars, int expectedPoints, TimescaleConfig cfg,
                                 int calmBars, double speed) {
        if (!LOG.isInfoEnabled()) {
            return;
        }
        LOG.info("replay {} {}/{}: {} bars -> {} window-points (window={}, tau={}), calmBars={}, speed={}x",
                opts.event(), opts.market(), opts.timescale(), returnBars, expectedPoints, cfg.window(),
                fmt(cfg.edgeThreshold(), 2), calmBars, fmt(speed, 1));
    }

    private static void logDone(RunSummary summary) {
        LOG.info("replay complete: {} detection points, {} fires, {} published",
                summary.detectionPoints(), summary.fires(), summary.published());
    }

    private static String fmt(double v, int decimals) {
        if (Double.isNaN(v)) {
            return "NaN";
        }
        return String.format(Locale.ROOT, "%." + decimals + "f", v);
    }
}
