package ch.tarvynanalytics.corrcalc.graphs.pipeline;

/**
 * The consumer's control over <em>which</em> transitions cross the observation seam to their
 * {@link PipelineObserver}. The engine emits an observation for every scored transition; this policy —
 * supplied by whoever composes the pipeline — decides which ones are forwarded. It answers the
 * "all ticks vs only fires vs a magnitude gauge" question on the consumer side, never in the engine.
 *
 * <p>The built-in policies cover the common cases without any custom code; {@link #and(ObservationPolicy)}
 * and {@link #or(ObservationPolicy)} compose them (e.g. {@code firesOnly().or(minActivation(0.5))} to
 * watch fires plus anything past half the CUSUM threshold).</p>
 */
@FunctionalInterface
public interface ObservationPolicy {

    /**
     * Decides whether an observation is forwarded to the observer.
     *
     * @param observation the candidate observation
     * @return {@code true} to forward it, {@code false} to drop it
     */
    boolean emit(PipelineObservation observation);

    /** Forward every transition — the full series (the engine's default). */
    static ObservationPolicy all() {
        return observation -> true;
    }

    /** Forward only transitions that fired (the observation-seam echo of the product fire-stream). */
    static ObservationPolicy firesOnly() {
        return PipelineObservation::fired;
    }

    /**
     * Forward transitions whose raw structural-move magnitude {@code |mean Δr|} is at least {@code tau}.
     * A NaN-change gap never passes (it is treated as below any finite threshold).
     *
     * @param tau the minimum weighted-change magnitude, {@code >= 0}
     * @return the magnitude-gated policy
     */
    static ObservationPolicy minWeightedChange(double tau) {
        if (!(tau >= 0.0)) {
            throw new IllegalArgumentException("tau must be >= 0 [" + tau + "]");
        }
        return observation -> Math.abs(observation.magnitude()) >= tau;
    }

    /**
     * Forward transitions whose CUSUM activation {@code max(S+,S−)/h} is at least {@code frac} — i.e.
     * within that fraction of firing. {@code frac = 1.0} forwards only transitions at or past the
     * decision threshold.
     *
     * @param frac the minimum activation fraction, {@code >= 0}
     * @return the activation-gated policy
     */
    static ObservationPolicy minActivation(double frac) {
        if (!(frac >= 0.0)) {
            throw new IllegalArgumentException("frac must be >= 0 [" + frac + "]");
        }
        return observation -> observation.activation() >= frac;
    }

    /**
     * A policy that forwards only when both this and {@code other} forward.
     *
     * @param other the other policy
     * @return the conjunction
     */
    default ObservationPolicy and(ObservationPolicy other) {
        if (other == null) {
            throw new IllegalArgumentException("other policy must not be null");
        }
        return observation -> this.emit(observation) && other.emit(observation);
    }

    /**
     * A policy that forwards when either this or {@code other} forwards.
     *
     * @param other the other policy
     * @return the disjunction
     */
    default ObservationPolicy or(ObservationPolicy other) {
        if (other == null) {
            throw new IllegalArgumentException("other policy must not be null");
        }
        return observation -> this.emit(observation) || other.emit(observation);
    }
}
