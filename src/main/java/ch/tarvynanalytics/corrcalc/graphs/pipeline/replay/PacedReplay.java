package ch.tarvynanalytics.corrcalc.graphs.pipeline.replay;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.ObservationPolicy;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObserver;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.RunContext;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SignalSink;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.AdaptiveCalibrationConfig;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationArtifact;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationSource;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.CalibrationSources;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.ReturnBuilder;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.ReturnPanel;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.SessionPolicy;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.StreamingPriceSnapshots;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.UniverseCsv;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.RegimeTimescaleConfig;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.TimescaleConfig;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.engine.PipelineDriver;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.engine.PipelineEngine;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.engine.RunSummary;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.IterableMarketDataSource;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.MarketDataSource;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.MarketSnapshot;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

/**
 * The live, wall-clock-paced replay <em>driver</em> — the visualization counterpart to the batch
 * {@code backtest} scorer. It streams one timescale's stored bars, lazily aligned into
 * {@link MarketSnapshot}s ({@link StreamingPriceSnapshots} — constant memory in the series length),
 * and replays them through the real inbound seam:
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
        // Streamed, never materialized: the aligner re-reads the files per pass (a counting pre-pass,
        // then the replay), so a multi-year minute tape runs in constant memory.
        Iterable<MarketSnapshot> snapshots = StreamingPriceSnapshots.align(dataDir, opts.event(),
                freqFor(opts.timescale()), opts.from(), opts.to(), symbols);

        int returnBars = ReturnBuilder.countReturns(snapshots, sessionPolicy);
        int expectedPoints = validateExpectedPoints(returnBars, cfg.window());
        int calmBars = resolveCalmBars(opts.calmBars(), expectedPoints);

        CalibrationSource calibrationSource = buildCalibrationSource(opts, cfg, calmBars);
        PipelineEngine engine = buildEngine(symbols, cfg, opts, calmBars, sink, observer, policy,
                calibrationSource);
        logStart(opts, returnBars, expectedPoints, cfg, calmBars, clock.speed());
        observer.onStart(runContext(opts, cfg, calmBars, clock.speed(), calibrationSource));

        RunSummary summary;
        try (MarketDataSource source = new IterableMarketDataSource(symbols, snapshots)) {
            ReturnBuilder builder = new ReturnBuilder(symbols, sessionPolicy);
            summary = PipelineDriver.run(source, builder, engine, clock);
        }
        logDone(summary);
        observer.onComplete(summary);
        saveCalibration(opts, engine, calibrationSource);
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

        CalibrationSource calibrationSource = buildCalibrationSource(opts, cfg, calmBars);
        PipelineEngine engine = buildEngine(panel.symbols(), cfg, opts, calmBars, sink, observer, policy,
                calibrationSource);
        logStart(opts, returns.length, expectedPoints, cfg, calmBars, clock.speed());
        observer.onStart(runContext(opts, cfg, calmBars, clock.speed(), calibrationSource));

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
        engine.finish();   // end-of-stream: flush the regime aggregator + report an open-at-EOF regime
        RunSummary summary = engine.summary();
        logDone(summary);
        observer.onComplete(summary);
        saveCalibration(opts, engine, calibrationSource);
        return summary;
    }

    /**
     * Builds the run's {@link CalibrationSource} from the requested mode: the leading-warmup prefix
     * (the default), a calm-block source primed from the persisted walk-forward artifact, or the
     * adaptive online source (optionally seeded from a prior artifact). A supplied artifact must
     * match this run's market + timescale (an intraday artifact never calibrates a daily detector;
     * never mix timescales).
     */
    private static CalibrationSource buildCalibrationSource(ReplayOptions opts, TimescaleConfig cfg,
                                                            int calmBars) {
        return switch (opts.calibrationMode()) {
            case ReplayOptions.CALM_BLOCK -> CalibrationSources.calmBlock(loadArtifact(opts));
            case ReplayOptions.ADAPTIVE -> CalibrationSources.adaptive(
                    adaptiveConfigFor(opts.timescale()), cfg.detector(),
                    opts.calibrationArtifact() == null ? null : loadArtifact(opts));
            default -> CalibrationSources.leadingWarmup(calmBars, cfg.detector());
        };
    }

    /** Loads the run's artifact and rejects a market/timescale mismatch. */
    private static CalibrationArtifact loadArtifact(ReplayOptions opts) {
        CalibrationArtifact artifact = CalibrationSources.load(opts.calibrationArtifact());
        if (!artifact.market().equals(opts.market()) || !artifact.timescale().equals(opts.timescale())) {
            throw new IllegalArgumentException("calibration artifact is for [" + artifact.market() + "/"
                    + artifact.timescale() + "], this run is [" + opts.market() + "/" + opts.timescale() + "]");
        }
        return artifact;
    }

    /**
     * The adaptive tuning for this run's timescale — market config, not code (the market itself is
     * validated in {@link #resolveConfig}; crypto is the only wired market today).
     */
    private static AdaptiveCalibrationConfig adaptiveConfigFor(String timescale) {
        return DAILY.equals(timescale)
                ? AdaptiveCalibrationConfig.cryptoDaily()
                : AdaptiveCalibrationConfig.cryptoIntraday();
    }

    /** Persists the run's resulting calibration when asked ({@code --save-calibration}) and calibrated. */
    private static void saveCalibration(ReplayOptions opts, PipelineEngine engine, CalibrationSource source) {
        if (opts.saveCalibration() == null) {
            return;
        }
        if (!engine.isCalibrated()) {
            LOG.warn("not saving calibration artifact: the run ended before the detector calibrated");
            return;
        }
        CalibrationArtifact artifact = source.artifact(opts.market(), opts.timescale());
        CalibrationSources.save(artifact, opts.saveCalibration());
        LOG.info("calibration artifact saved to {} (epoch {}, source {} .. {})", opts.saveCalibration(),
                artifact.epochId(), artifact.sourceFrom(), artifact.sourceTo());
    }

    /**
     * Builds the static run context echoed before the stream. {@code mode=replay} plus the calibration
     * source's provenance — which mode selected the baseline (a leading-warmup baseline is a pragmatic
     * prefix; calm-block is the rigorous walk-forward window the regression uses) and, when known, the
     * calm source window.
     */
    private static RunContext runContext(ReplayOptions opts, TimescaleConfig cfg, int calmBars, double speed,
                                         CalibrationSource calibrationSource) {
        DetectorConfig det = cfg.detector();
        return new RunContext(opts.market(), opts.timescale(), "replay", calibrationSource.provenance(),
                cfg.window(), cfg.edgeThreshold(), det.k(), det.h(), det.levelPctile(),
                det.fireArm().name(), calmBars, speed);
    }

    private static PipelineEngine buildEngine(String[] symbols, TimescaleConfig cfg, ReplayOptions opts,
                                              int calmBars, SignalSink sink, PipelineObserver observer,
                                              ObservationPolicy policy, CalibrationSource calibrationSource) {
        return PipelineEngine.builder(symbols, cfg)
                .calmBars(calmBars)
                .market(opts.market())
                .timescale(opts.timescale())
                .sink(sink)
                .observer(observer)
                .observationPolicy(policy)
                .limit(opts.limit())
                .calibrationSource(calibrationSource)
                .regime(regimeConfigFor(opts))
                .build();
    }

    /**
     * The regime timescale tuning for a regime-backbone run (H2R-2), or {@code null} for the default
     * adaptive-CUSUM fire mode. The market itself is validated in {@link #resolveConfig}; crypto is the
     * only wired market today.
     */
    private static RegimeTimescaleConfig regimeConfigFor(ReplayOptions opts) {
        return ReplayOptions.REGIME.equals(opts.fireMode()) ? RegimeTimescaleConfig.crypto() : null;
    }

    private static String[] loadUniverse(Path dataDir, ReplayOptions opts) {
        Path universePath = opts.universePath() != null
                ? opts.universePath()
                : dataDir.resolve(opts.event() + "_universe.csv");
        return UniverseCsv.read(universePath).toArray(new String[0]);
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
