package ch.tarvynanalytics.corrcalc.graphs.pipeline.cli;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.LoggingObserver;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.LoggingSink;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.NdjsonObserver;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.ObservationPolicy;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObserver;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.ReadableObserver;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SignalSink;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.ThinningObserver;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.replay.PacedReplay;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.replay.ReplayClock;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.replay.ReplayOptions;

import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;

/**
 * Command-line entry point for the pipeline. Today it has one subcommand, {@code replay}, which
 * streams stored bars through the live S1→S3 pipeline at a configurable pace and logs the signal
 * (the {@code backtest} lead-table reproduction stays a test-only driver, so it has no verb yet).
 *
 * <p>Mirrors the family's CLI recipe (graphs-algos-lib {@code ComparabilityCli}): {@link #main} is a
 * one-liner that delegates to the package-testable {@link #run(String[], PrintStream, PrintStream)},
 * which returns the exit code without calling {@link System#exit} so behaviour is unit-tested
 * directly. Exit codes: {@code 0} ran, {@code 2} usage/bad-argument, {@code 1} input-IO.</p>
 */
public final class PipelineCli {

    private PipelineCli() {
    }

    /** JVM entry point. */
    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    /**
     * Parses {@code args} and runs the requested command.
     *
     * @param args the command-line arguments
     * @param out  the standard-output stream (usage/help)
     * @param err  the error stream (diagnostics)
     * @return the process exit code ({@code 0} ran / {@code 2} usage / {@code 1} input-IO)
     */
    public static int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length == 0) {
            usage(err);
            return 2;
        }
        String command = args[0];
        if (command.equals("-h") || command.equals("--help")) {
            usage(out);
            return 0;
        }
        if (!command.equals("replay")) {
            err.println("error: unknown command [" + command + "]");
            usage(err);
            return 2;
        }
        try {
            return runReplay(args, out, err);
        } catch (UsageException e) {
            err.println("error: " + e.getMessage());
            usage(err);
            return 2;
        } catch (IllegalArgumentException e) {
            err.println("error: " + e.getMessage());
            return 2;
        } catch (UncheckedIOException e) {
            err.println("io error: " + e.getMessage());
            return 1;
        }
    }

    private static int runReplay(String[] args, PrintStream out, PrintStream err) {
        String dataDir = null;
        String event = null;
        String market = null;
        String timescale = "intraday";
        String universe = null;
        String from = null;
        String to = null;
        double speed = 60.0;
        long maxStepMs = 2000L;
        Integer calmBars = null;
        Integer limit = null;
        int heartbeatEvery = 1;
        String observe = "all";
        String style = "technical";
        boolean verbose = false;
        String calibration = "leading-warmup";
        String calibrationArtifact = null;
        String saveCalibration = null;

        for (int i = 1; i < args.length; i++) {
            String a = args[i];
            switch (a) {
                case "-h", "--help" -> {
                    usage(out);
                    return 0;
                }
                case "-v", "--verbose" -> verbose = true;
                case "--event" -> event = value(args, ++i, a);
                case "--market" -> market = value(args, ++i, a);
                case "--timescale" -> timescale = value(args, ++i, a);
                case "--speed" -> speed = parseDouble(value(args, ++i, a), a);
                case "--max-step-ms" -> maxStepMs = parseLong(value(args, ++i, a), a);
                case "--calm-bars" -> calmBars = parseInt(value(args, ++i, a), a);
                case "--limit" -> limit = parseInt(value(args, ++i, a), a);
                case "--heartbeat-every" -> heartbeatEvery = parseInt(value(args, ++i, a), a);
                case "--observe" -> observe = value(args, ++i, a);
                case "--style" -> style = value(args, ++i, a);
                case "--universe" -> universe = value(args, ++i, a);
                case "--from" -> from = value(args, ++i, a);
                case "--to" -> to = value(args, ++i, a);
                case "--calibration" -> calibration = value(args, ++i, a);
                case "--calibration-artifact" -> calibrationArtifact = value(args, ++i, a);
                case "--save-calibration" -> saveCalibration = value(args, ++i, a);
                default -> {
                    if (a.startsWith("-")) {
                        throw new UsageException("unknown option [" + a + "]");
                    }
                    if (dataDir != null) {
                        throw new UsageException("unexpected extra argument [" + a + "]");
                    }
                    dataDir = a;
                }
            }
        }

        if (dataDir == null) {
            throw new UsageException("missing <data-dir>");
        }
        if (event == null) {
            throw new UsageException("missing required --event");
        }
        if (market == null) {
            throw new UsageException("missing required --market");
        }

        if (verbose) {
            System.setProperty("cgp.log.level", "DEBUG");
        }
        if (style.equals("ndjson")) {
            // NDJSON is the machine product on stdout; push diagnostic logging to stderr so the stream
            // pipes clean. Set before any logger is touched (logback reads the target at init).
            System.setProperty("cgp.log.target", "System.err");
        }

        ReplayOptions opts = new ReplayOptions(event, market, timescale, speed, maxStepMs, calmBars,
                limit, heartbeatEvery, universe == null ? null : Path.of(universe),
                parseDate(from, "--from"), parseDate(to, "--to"),
                calibration, calibrationArtifact == null ? null : Path.of(calibrationArtifact),
                saveCalibration == null ? null : Path.of(saveCalibration));

        ObservationPolicy policy = parseObserve(observe);
        // Fires go to the product sink (loud WARN banner); the full transition series goes to the
        // observer, gated by --observe and thinned by --heartbeat-every — both the consumer's choice.
        // --style picks how each forwarded transition is rendered (terse / annotated / NDJSON).
        SignalSink sink = new LoggingSink();
        PipelineObserver base = parseStyle(style, out);
        PipelineObserver observer = heartbeatEvery > 1
                ? new ThinningObserver(base, heartbeatEvery)
                : base;
        PacedReplay.run(Path.of(dataDir), opts, sink, observer, policy, ReplayClock.of(speed, maxStepMs));
        return 0;
    }

    /**
     * Parses the {@code --observe} spec into an {@link ObservationPolicy}: {@code all}, {@code fires},
     * {@code change>=<x>} (raw weighted-change magnitude) or {@code activation>=<x>} (CUSUM threshold
     * fraction).
     */
    private static ObservationPolicy parseObserve(String spec) {
        if (spec.equals("all")) {
            return ObservationPolicy.all();
        }
        if (spec.equals("fires")) {
            return ObservationPolicy.firesOnly();
        }
        int sep = spec.indexOf(">=");
        if (sep > 0) {
            String key = spec.substring(0, sep);
            double threshold = parseDouble(spec.substring(sep + 2), "--observe");
            if (key.equals("change")) {
                return ObservationPolicy.minWeightedChange(threshold);
            }
            if (key.equals("activation")) {
                return ObservationPolicy.minActivation(threshold);
            }
        }
        throw new UsageException("--observe expects all|fires|change>=<x>|activation>=<x>, got [" + spec + "]");
    }

    /**
     * Parses the {@code --style} spec into the per-transition renderer: {@code technical} (the terse
     * {@link LoggingObserver}), {@code readable} (the annotated {@link ReadableObserver} with a legend,
     * calibration banner, σ-magnitude, severity tiers and reason codes), or {@code ndjson} (the
     * structured {@link NdjsonObserver} machine stream written to {@code out}/stdout).
     */
    private static PipelineObserver parseStyle(String spec, PrintStream out) {
        return switch (spec) {
            case "technical" -> new LoggingObserver();
            case "readable" -> new ReadableObserver();
            case "ndjson" -> new NdjsonObserver(out);
            default -> throw new UsageException("--style expects technical|readable|ndjson, got [" + spec + "]");
        };
    }

    private static String value(String[] args, int i, String option) {
        if (i >= args.length) {
            throw new UsageException("missing value for " + option);
        }
        return args[i];
    }

    private static double parseDouble(String s, String option) {
        try {
            return Double.parseDouble(s);
        } catch (NumberFormatException e) {
            throw new UsageException(option + " expects a number, got [" + s + "]");
        }
    }

    private static long parseLong(String s, String option) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            throw new UsageException(option + " expects an integer, got [" + s + "]");
        }
    }

    private static int parseInt(String s, String option) {
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            throw new UsageException(option + " expects an integer, got [" + s + "]");
        }
    }

    private static LocalDate parseDate(String s, String option) {
        if (s == null) {
            return null;
        }
        try {
            return LocalDate.parse(s);
        } catch (DateTimeParseException e) {
            throw new UsageException(option + " expects an ISO date (YYYY-MM-DD), got [" + s + "]");
        }
    }

    private static void usage(PrintStream w) {
        w.println("""
                corrcalc-graphs-pipeline — live structural-signal replay

                Usage:
                  java -jar corrcalc-graphs-pipeline-<v>-cli.jar replay <data-dir> [options]

                <data-dir>  directory of <SYMBOL>_<freq>_<event>.csv OHLCV bar files

                Required:
                  --event <name>              event id selecting the bar files + default universe
                  --market <name>             market config + output label (supported: crypto)

                Options:
                  --timescale intraday|daily  which stream to replay (default: intraday)
                  --speed <multiplier>        simulated:real time ratio; higher = faster (default: 60)
                  --max-step-ms <ms>          cap on per-bar sleep so session gaps don't stall (default: 2000)
                  --calm-bars <N>             window-points used to calibrate (default: ~40% of the series)
                  --limit <N>                 stop after N detection points (default: unlimited)
                  --heartbeat-every <N>       forward one observation in every N (default: 1)
                  --observe <spec>            which transitions to log: all|fires|change>=<x>|activation>=<x> (default: all)
                  --style technical|readable|ndjson
                                              terse metrics; an annotated stream with a legend,
                                              calibration banner, σ-magnitude, severity + reasons; or a
                                              machine-readable NDJSON stream on stdout — diagnostics go
                                              to stderr so it pipes clean (default: technical)
                  --universe <path>           symbol-list CSV (default: <data-dir>/<event>_universe.csv)
                  --from <YYYY-MM-DD>          earliest UTC bar date to keep
                  --to <YYYY-MM-DD>           latest UTC bar date to keep
                  --calibration leading-warmup|calm-block|adaptive
                                              how the detector is calibrated: the pragmatic leading
                                              prefix (default); primed from a persisted walk-forward
                                              artifact so detection starts on the first snapshot; or
                                              the adaptive online walk-forward (quietness gate +
                                              robust estimator + drift meta-monitor opening epochs)
                  --calibration-artifact <path>
                                              the artifact JSON: the calm-block baseline (required),
                                              or an adaptive run's operator-vouched prior (optional)
                  --save-calibration <path>   persist this run's resulting calibration artifact
                  -v, --verbose               DEBUG logging
                  -h, --help                  this help

                Example:
                  java -jar …-cli.jar replay ./crypto-data --event may2021_selloff \\
                    --market crypto --timescale intraday --speed 500
                """);
    }

    /** Signals a command-line usage error (exit code 2). */
    private static final class UsageException extends RuntimeException {
        UsageException(String message) {
            super(message);
        }
    }
}
