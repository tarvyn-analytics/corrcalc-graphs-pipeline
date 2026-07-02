package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.engine.RunSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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
    private final SeverityHysteresis hysteresis = new SeverityHysteresis();
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
        String line = renderLine(observation, hysteresis.step(observation));
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

    @Override
    public void onCalibrationEvent(CalibrationEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        digest.addCalibrationEvent(event);
        LOG.info("{}", calibrationEventLine(event));
    }

    /**
     * One calibration-lifecycle line — the fixed {@link CalibrationEventKind#phrase() phrase table}
     * plus the raw before/after facts, never generated text: e.g.
     * {@code CALIB-EVENT 2021-05-19T13:00:00Z  calm baseline drifted — recalibrated  epoch=2  μ 0.0100→0.0132  σ 0.0043→0.0051}.
     *
     * @param e the lifecycle event
     * @return the human-readable event line
     */
    public static String calibrationEventLine(CalibrationEvent e) {
        return String.format(Locale.ROOT, "CALIB-EVENT %s  %s  epoch=%d  μ %s→%s  σ %s→%s",
                e.asOf(), e.kind().phrase(), e.epochId(),
                fmt(e.muBefore(), 4), fmt(e.muAfter(), 4),
                fmt(e.sigmaBefore(), 4), fmt(e.sigmaAfter(), 4));
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
                        rec=recovery gauge (time-in-calm-band since the last fusion, 0→1 = healed)
                        [...]=why (an alarm needs density≥L AND act≥1.0)   who=top pairs by |Δr| (the movers)""";
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
                c.market(), c.timescale(), c.mode(), c.calibration().calibrationMode(), c.window(),
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
     * Renders one annotated transition line with the raw instantaneous {@link Severity} tier.
     *
     * @param o the observation to render
     * @return the human-readable line
     */
    public static String renderLine(PipelineObservation o) {
        return renderLine(o, o.severity());
    }

    /**
     * Renders one annotated transition line with a caller-supplied displayed tier — used by the observer
     * to show the {@link SeverityHysteresis anti-flapped} tier while every other field stays the raw
     * per-transition fact.
     *
     * @param o          the observation to render
     * @param displayTier the severity tier to show (raw or hysteresis-smoothed)
     * @return the human-readable line
     */
    public static String renderLine(PipelineObservation o, Severity displayTier) {
        String why = o.reasonCodes().stream().map(ReasonCode::phrase).collect(Collectors.joining(SEP));
        String line = String.format(Locale.ROOT,
                "%s  %-5s  density=%s  wΔ=%s (z=%s)  act=%s  S+=%s S-=%s  rec=%s  [%s]",
                o.asOf(),
                displayTier,
                fmt(o.metrics().densityLevel(), 3),
                fmt(o.magnitude(), 4),
                fmtSigned(o.zScore(), 1),
                fmt(o.activation(), 2),
                fmt(o.cusumSPlus(), 2),
                fmt(o.cusumSMinus(), 2),
                fmt(o.recoveryGauge(), 2),
                why.isEmpty() ? "normal" : why);
        // Attribution stays off CALM lines (keeps the quiet stream terse); it is the actionable add
        // exactly when something is moving (WATCH/WARN/FIRE).
        if (displayTier != Severity.CALM && !o.contributors().isEmpty()) {
            line += "  who=" + who(o.contributors());
        }
        return line;
    }

    /** Compact "who moved" rendering: the contributing pairs as {@code A–B}, comma-separated. */
    private static String who(List<PairContribution> contributors) {
        return contributors.stream()
                .map(c -> c.a() + "–" + c.b())
                .collect(Collectors.joining(","));
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
                : fmt(d.biggestMove(), 4) + " @ " + d.biggestMoveAt()
                        + (d.biggestMoveContributors().isEmpty() ? "" : " (" + who(d.biggestMoveContributors()) + ")");
        return String.format(Locale.ROOT,
                "DIGEST  %s/%s  %d obs (%d calm)  sev[CALM=%d WATCH=%d WARN=%d FIRE=%d]  "
                        + "fires=%d published=%d  peak act=%s  peak z=%sσ  biggest=%s  fused-bars=%d  "
                        + "peak recovery=%s  all-clear=%s  calib[epochs=%d recal=%d%s]",
                nz(d.market()), nz(d.timescale()), d.observed(), s.calmBars(),
                d.count(Severity.CALM), d.count(Severity.WATCH), d.count(Severity.WARN), d.count(Severity.FIRE),
                s.fires(), s.published(),
                fmt(d.maxActivation(), 2), fmt(d.maxAbsZ(), 1), biggest, d.timeInFused(),
                fmt(d.maxRecoveryGauge(), 2), allClear(d), d.epochsOpened(), d.recalibrations(),
                muJourney(d));
    }

    /** The baseline's journey over the run ({@code  μ X→Y}), or empty when no lifecycle event arrived. */
    private static String muJourney(RunDigest d) {
        if (Double.isNaN(d.muLastAfter())) {
            return "";
        }
        return " μ " + fmt(d.muFirstBefore(), 4) + "→" + fmt(d.muLastAfter(), 4);
    }

    /** Renders the de-fusion all-clear: its timestamp and the time-to-recover from the first fusion, or "none". */
    private static String allClear(RunDigest d) {
        if (d.firstAllClearAt() == null) {
            return "none";
        }
        var dur = d.timeToAllClear();
        return dur == null
                ? d.firstAllClearAt().toString()
                : d.firstAllClearAt() + " (+" + dur.toHours() + "h after fusion)";
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
