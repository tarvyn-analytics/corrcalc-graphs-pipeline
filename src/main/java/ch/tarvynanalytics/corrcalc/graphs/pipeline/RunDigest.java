package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import java.time.Duration;
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
    private double maxRecoveryGauge;
    private Instant firstFusionAt;
    private Instant firstAllClearAt;
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
        if (o.firedKind() == SignalKind.FUSION && firstFusionAt == null) {
            firstFusionAt = o.asOf();
        }
        if (o.firedKind() == SignalKind.DEFUSION && firstAllClearAt == null) {
            firstAllClearAt = o.asOf();
        }
        double gauge = o.recoveryGauge();
        if (Double.isFinite(gauge) && gauge > maxRecoveryGauge) {
            maxRecoveryGauge = gauge;
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

    /** The peak recovery gauge seen this run (how far the structure healed back into the calm band), {@code [0,1]}. */
    double maxRecoveryGauge() {
        return maxRecoveryGauge;
    }

    /** The timestamp of the first de-fusion all-clear this run, or {@code null} if none fired. */
    Instant firstAllClearAt() {
        return firstAllClearAt;
    }

    /**
     * The time from the first fusion alarm to the first de-fusion all-clear, or {@code null} when either
     * did not occur — the run's headline "how long until the structure recovered".
     */
    Duration timeToAllClear() {
        if (firstFusionAt == null || firstAllClearAt == null || firstAllClearAt.isBefore(firstFusionAt)) {
            return null;
        }
        return Duration.between(firstFusionAt, firstAllClearAt);
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
