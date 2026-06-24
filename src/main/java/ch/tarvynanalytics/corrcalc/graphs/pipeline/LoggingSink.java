package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

/**
 * A {@link SignalSink} that logs every published signal via SLF4J, the simplest concrete delivery
 * adapter and the one the replay CLI wires by default. It exists so a streaming run is *visible*:
 * each signal that reaches a sink is a genuine, filter-passed structural-change fire, so it is logged
 * at <strong>WARN</strong> with a loud {@code === FUSION ===} / {@code === DEFUSION ===} banner. The
 * default {@code logback.xml} colorizes WARN red, so fires stand out against the replay engine's quiet
 * INFO calm heartbeat (which is the non-firing transition stream — never published as a signal, since
 * "no fire" is censored, never a silent zero).
 *
 * <p>This is a pure output adapter: it programs only against the SLF4J API. Stateless and
 * thread-safe.</p>
 */
public final class LoggingSink implements SignalSink {

    private static final Logger LOG = LoggerFactory.getLogger("signal");

    @Override
    public void publish(StructuralSignal signal) {
        if (signal == null) {
            throw new IllegalArgumentException("signal must not be null");
        }
        String head = signal.kind() == SignalKind.FUSION ? "=== FUSION   ===" : "=== DEFUSION ===";
        LOG.warn("{} {}/{} @ {}  wΔ={}  S+={}  S−={}  density={}  comps={}  largest={}{}",
                head,
                signal.market(),
                signal.timescale(),
                signal.asOf(),
                fmt(signal.changeMetric(), 4),
                fmt(signal.cusumSPlus(), 2),
                fmt(signal.cusumSMinus(), 2),
                fmt(signal.levelDensity(), 3),
                signal.nComponents(),
                fmt(signal.largestComponentFraction(), 2),
                leadSuffix(signal.leadVsDailyHours()));
    }

    private static String leadSuffix(Double leadHours) {
        if (leadHours == null) {
            return "";
        }
        return String.format(Locale.ROOT, "  lead=%+.1fh", leadHours);
    }

    private static String fmt(double v, int decimals) {
        if (Double.isNaN(v)) {
            return "NaN";
        }
        return String.format(Locale.ROOT, "%." + decimals + "f", v);
    }
}
