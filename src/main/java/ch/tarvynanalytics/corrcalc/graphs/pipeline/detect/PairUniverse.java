package ch.tarvynanalytics.corrcalc.graphs.pipeline.detect;

import java.time.Instant;

/**
 * The pair count the density metric normalizes over when a run's variables do not all participate
 * at every instant (e.g. a symbol temporarily masked out of the panel). Polled once per emitted
 * snapshot; deliberately product-unaware — "not every variable participates at every instant" is a
 * generic streaming-correlation fact, not a product motive.
 */
@FunctionalInterface
public interface PairUniverse {

    /**
     * The participating pairs at {@code asOf}.
     *
     * @param asOf the snapshot instant
     * @return the pair count to normalize density against; {@code 0} means "every pair" (the default)
     */
    long activePairs(Instant asOf);

    /** Every pair participates always: density is never rescaled. The engine's default. */
    PairUniverse ALL = asOf -> 0L;
}
