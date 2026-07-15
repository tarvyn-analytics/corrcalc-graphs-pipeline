package ch.tarvynanalytics.corrcalc.graphs.pipeline;

/**
 * One asset pair's absolute correlation change {@code |Δr|} across a transition, by label — the
 * pipeline's label-resolved view of graphs-algos-lib's index-based {@code PairChange}. It answers the
 * "who" behind a structural move: the named pairs that contributed most to the weighted change
 * ({@code mean |Δr|}). The engine ranks these via {@code ChangeMetricsAnalyzer.topContributors} and
 * maps each index pair back to the run's symbol order; an observation carries the top-k of them.
 *
 * @param a        one asset label of the pair (the lower-index variable)
 * @param b        the other asset label of the pair (the higher-index variable)
 * @param absDelta the absolute correlation change {@code |Δr|} for this pair (finite, {@code >= 0})
 */
public record PairContribution(String a, String b, double absDelta) {
}
