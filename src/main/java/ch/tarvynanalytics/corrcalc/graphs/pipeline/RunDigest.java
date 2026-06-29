package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import java.time.Instant;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * A mutable accumulator that folds the per-transition observation stream into the figures an end-of-run
 * digest needs: the severity histogram, the peak activation and σ-move, the single biggest move (and
 * when), the fired count, and a time-in-fused proxy (transitions at saturated density). Fed one
 * observation at a time by an observer's {@link PipelineObserver#onObservation} and read once by its
 * {@link PipelineObserver#onComplete}.
 *
 * <p>Counts reflect the observations the observer actually <em>received</em> — i.e. after the consumer's
 * {@link ObservationPolicy} gate and any {@link ThinningObserver}; with the default {@code
 * --heartbeat-every 1} and {@code --observe all} that is the full post-calibration series. Single-writer,
 * like the engine that drives it.</p>
 */
final class RunDigest {

    private final Map<Severity, Long> bySeverity = new EnumMap<>(Severity.class);
    private String market;
    private String timescale;
    private long observed;
    private long fired;
    private long timeInFused;
    private double maxActivation;
    private double maxAbsZ;
    private double biggestMove = Double.NaN;
    private Instant biggestMoveAt;
    private List<PairContribution> biggestMoveContributors = List.of();

    /** Folds one received observation into the running figures. */
    void add(PipelineObservation o) {
        if (observed == 0) {
            market = o.market();
            timescale = o.timescale();
        }
        observed++;
        bySeverity.merge(o.severity(), 1L, Long::sum);
        if (o.fired()) {
            fired++;
        }
        double a = o.activation();
        if (Double.isFinite(a) && a > maxActivation) {
            maxActivation = a;
        }
        double z = o.zScore();
        if (Double.isFinite(z) && Math.abs(z) > maxAbsZ) {
            maxAbsZ = Math.abs(z);
        }
        double m = o.magnitude();
        if (Double.isFinite(m) && (biggestMoveAt == null || m > biggestMove)) {
            biggestMove = m;
            biggestMoveAt = o.asOf();
            biggestMoveContributors = o.contributors();   // already an immutable copy off the record
        }
        double density = o.metrics().densityLevel();
        if (Double.isFinite(density) && density >= 0.999) {
            timeInFused++;
        }
    }

    long count(Severity s) {
        return bySeverity.getOrDefault(s, 0L);
    }

    String market() {
        return market;
    }

    String timescale() {
        return timescale;
    }

    long observed() {
        return observed;
    }

    long fired() {
        return fired;
    }

    long timeInFused() {
        return timeInFused;
    }

    double maxActivation() {
        return maxActivation;
    }

    double maxAbsZ() {
        return maxAbsZ;
    }

    double biggestMove() {
        return biggestMove;
    }

    Instant biggestMoveAt() {
        return biggestMoveAt;
    }

    List<PairContribution> biggestMoveContributors() {
        return biggestMoveContributors;
    }
}
