package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.engine.RunSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.stream.Collectors;

/**
 * A human-readable {@link PipelineObserver} — the {@code --style readable} counterpart to the terse
 * {@link LoggingObserver}. It exists because the raw heartbeat (bare {@code density / wD / S+ / S-})
 * is hard to interpret: this adapter prints a one-time <strong>legend + calibration banner</strong>
 * so the reader knows what the fields mean and what "normal" was, then annotates every transition with
 * its move size in calm-sigmas ({@link PipelineObservation#zScore() z}), a {@link Severity} tier
 * (WATCH/WARN/FIRE), and the bounded {@link ReasonCode} decision-trace ("why") — including, when the
 * alarm meter is hot but nothing fired, <em>which gate held it back</em>.
 *
 * <p>Pure output adapter: it programs only against the SLF4J API. FIRE-severity lines are logged at
 * WARN (red in the default {@code logback.xml}) so they stand out; everything else is INFO. Rendering
 * is factored into static, side-effect-free methods ({@link #legend()}, {@link #calibrationBanner},
 * {@link #renderLine}) so the exact text is unit-tested without capturing logs. Single-writer, like the
 * engine that drives it (the {@code headerShown} latch is not synchronised).</p>
 */
public final class ReadableObserver implements PipelineObserver {

    private static final Logger LOG = LoggerFactory.getLogger("observation");
    private static final String SEP = " · ";

    private final RunDigest digest = new RunDigest();
    private boolean headerShown;

    @Override
    public void onStart(RunContext context) {
        if (context == null) {
            throw new IllegalArgumentException("context must not be null");
        }
        LOG.info("{}", configBanner(context));
    }

    @Override
    public void onObservation(PipelineObservation observation) {
        if (observation == null) {
            throw new IllegalArgumentException("observation must not be null");
        }
        if (!headerShown) {
            LOG.info("{}", legend());
            LOG.info("{}", calibrationBanner(observation));
            headerShown = true;
        }
        digest.add(observation);
        String line = renderLine(observation);
        if (observation.severity() == Severity.FIRE) {
            LOG.warn("{}", line);
        } else {
            LOG.info("{}", line);
        }
    }

    @Override
    public void onComplete(RunSummary summary) {
        if (summary == null) {
            throw new IllegalArgumentException("summary must not be null");
        }
        LOG.info("{}", digestBlock(digest, summary));
    }

    /**
     * The one-time legend defining every field of a rendered line.
     *
     * @return the static legend block
     */
    public static String legend() {
        return """
                LEGEND  density=fraction of asset-pairs correlated (|r|>τ)   wΔ=structural move (mean|Δr|)
                        z=move size in calm-σ   act=closeness to firing max(S+,S-)/h   sev: CALM<WATCH<WARN<FIRE
                        [...]=why (an alarm needs density≥L AND act≥1.0)""";
    }

    /**
     * The one-time config banner — the static run context (config + provenance) echoed before the
     * stream. {@code calibration=leading-warmup} flags that replay's baseline is a pragmatic leading
     * warm-up, not a rigorous walk-forward calm block.
     *
     * @param c the run context
     * @return the config banner line
     */
    public static String configBanner(RunContext c) {
        return String.format(Locale.ROOT,
                "CONFIG  %s/%s  mode=%s  calibration=%s  window=%d  τ=%s  k=%s  h=%s  L=p%s  fireArm=%s  speed=%sx",
                c.market(), c.timescale(), c.mode(), c.calibration(), c.window(),
                fmt(c.edgeThreshold(), 2), fmt(c.cusumK(), 2), fmt(c.decisionInterval(), 2),
                fmt(c.levelPctile(), 0), c.fireArm(), fmt(c.speed(), 0));
    }

    /**
     * The calibration banner — what "normal" was, read off the first observation's carried calibration.
     *
     * @param o any post-calibration observation
     * @return a one-line calibration summary
     */
    public static String calibrationBanner(PipelineObservation o) {
        return String.format(Locale.ROOT,
                "CALIB   %s/%s  normal move μ=%s (σ=%s)   level gate L=%s   threshold h=%s",
                o.market(), o.timescale(),
                fmt(o.calmMu(), 4), fmt(o.calmSigma(), 4), fmt(o.levelGate(), 3), fmt(o.decisionThreshold(), 2));
    }

    /**
     * Renders one annotated transition line.
     *
     * @param o the observation to render
     * @return the human-readable line
     */
    public static String renderLine(PipelineObservation o) {
        String why = o.reasonCodes().stream().map(ReasonCode::phrase).collect(Collectors.joining(SEP));
        return String.format(Locale.ROOT,
                "%s  %-5s  density=%s  wΔ=%s (z=%s)  act=%s  S+=%s S-=%s  [%s]",
                o.asOf(),
                o.severity(),
                fmt(o.metrics().densityLevel(), 3),
                fmt(o.magnitude(), 4),
                fmtSigned(o.zScore(), 1),
                fmt(o.activation(), 2),
                fmt(o.cusumSPlus(), 2),
                fmt(o.cusumSMinus(), 2),
                why.isEmpty() ? "normal" : why);
    }

    /**
     * The end-of-run digest block — the run folded into one human line (counts by severity, peak
     * activation/σ-move, the biggest move, fires/published, fused-bar count).
     *
     * @param d the accumulated figures
     * @param s the run outcome
     * @return the digest summary line
     */
    public static String digestBlock(RunDigest d, RunSummary s) {
        String biggest = d.biggestMoveAt() == null
                ? "none"
                : fmt(d.biggestMove(), 4) + " @ " + d.biggestMoveAt();
        return String.format(Locale.ROOT,
                "DIGEST  %s/%s  %d obs (%d calm)  sev[CALM=%d WATCH=%d WARN=%d FIRE=%d]  "
                        + "fires=%d published=%d  peak act=%s  peak z=%sσ  biggest=%s  fused-bars=%d",
                nz(d.market()), nz(d.timescale()), d.observed(), s.calmBars(),
                d.count(Severity.CALM), d.count(Severity.WATCH), d.count(Severity.WARN), d.count(Severity.FIRE),
                s.fires(), s.published(),
                fmt(d.maxActivation(), 2), fmt(d.maxAbsZ(), 1), biggest, d.timeInFused());
    }

    private static String nz(String s) {
        return s == null ? "?" : s;
    }

    private static String fmt(double v, int decimals) {
        if (Double.isNaN(v)) {
            return "NaN";
        }
        return String.format(Locale.ROOT, "%." + decimals + "f", v);
    }

    private static String fmtSigned(double v, int decimals) {
        if (Double.isNaN(v)) {
            return "n/a";
        }
        return String.format(Locale.ROOT, "%+." + decimals + "fσ", v);
    }
}
