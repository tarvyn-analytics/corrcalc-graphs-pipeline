package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

/**
 * A {@link PipelineObserver} that logs each forwarded observation via SLF4J at INFO — the calm,
 * per-transition "heartbeat" the replay CLI shows. This is the visualization counterpart to
 * {@link LoggingSink}: fires are still printed loudly by the {@code LoggingSink} (WARN banner) on the
 * product seam, while this prints the quiet metric stream (density, weighted change, both CUSUM arms)
 * for whichever transitions the consumer's {@link ObservationPolicy} let through. A fired transition is
 * tagged inline so it is visible in the full series too.
 *
 * <p>Pure output adapter: programs only against the SLF4J API; stateless and thread-safe.</p>
 */
public final class LoggingObserver implements PipelineObserver {

    private static final Logger LOG = LoggerFactory.getLogger("observation");

    @Override
    public void onObservation(PipelineObservation observation) {
        if (observation == null) {
            throw new IllegalArgumentException("observation must not be null");
        }
        LOG.info("{}  density={}  wD={}  S+={}  S-={}{}",
                observation.asOf(),
                fmt(observation.metrics().densityLevel(), 3),
                fmt(observation.metrics().weightedChange(), 4),
                fmt(observation.cusumSPlus(), 2),
                fmt(observation.cusumSMinus(), 2),
                observation.fired() ? "  <== " + observation.firedKind() : "");
    }

    private static String fmt(double v, int decimals) {
        if (Double.isNaN(v)) {
            return "NaN";
        }
        return String.format(Locale.ROOT, "%." + decimals + "f", v);
    }
}
