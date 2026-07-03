package ch.tarvynanalytics.corrcalc.graphs.pipeline.detect;

import ch.tarvynanalytics.graphs.algos.RegimeConfig;

/**
 * The pipeline-side (market/cadence) tuning of the H2R-2 regime-state backbone: the asset-agnostic
 * GAL {@link RegimeConfig} (the two Schmitt marks + persistence run) plus the CGP density-prep
 * cadence — how the per-window density stream is aggregated and smoothed into the daily level series
 * the {@code RegimeStateDetector} consumes. The split is deliberate (design §1.2/§5.2): GAL owns the
 * cadence-agnostic detector numerics, CGP owns the aggregation/smoothing <em>policy</em> (family
 * invariant "the core is asset-agnostic; this repo holds the edges").
 *
 * <p><strong>Aggregation is per UTC calendar day</strong>, the crypto session cadence (the same day
 * key {@code SessionPolicy.INTRADAY_UTC_DAY} uses), matching the spike's daily means
 * ({@code h2r1_regime_model.py}: "aggregate 1-min density to daily means"). It is a calendar-day
 * fold rather than a fixed bar count because trading gaps make bars-per-day vary — one density per
 * UTC day is the invariant, not one per N bars (design §5.1/§8.4). {@code confirmBars} on the GAL
 * detector is therefore a <em>sample</em> count that equals a day count at this cadence: the settled
 * crypto {@code confirmBars=3} ≈ 3 days.</p>
 *
 * @param regime       the GAL Schmitt-trigger tuning (hi/lo marks + confirm run, in samples)
 * @param smoothWindow the centered-median smoothing window over the daily level series, in days
 *                     ({@code >= 1} and odd — a centered median needs a symmetric window; the spike
 *                     uses a 3-day median to kill 1-day whipsaw)
 */
public record RegimeTimescaleConfig(RegimeConfig regime, int smoothWindow) {

    /** Validates the smoothing window and the presence of the GAL tuning. */
    public RegimeTimescaleConfig {
        if (regime == null) {
            throw new IllegalArgumentException("regime config must not be null");
        }
        if (smoothWindow < 1) {
            throw new IllegalArgumentException("smoothWindow must be >= 1 [" + smoothWindow + "]");
        }
        if (smoothWindow % 2 == 0) {
            throw new IllegalArgumentException("smoothWindow must be odd (a centered median needs a "
                    + "symmetric window) [" + smoothWindow + "]");
        }
    }

    /**
     * The settled crypto regime tuning: the GAL {@link RegimeConfig#crypto()} marks
     * ({@code hi=0.85, lo=0.45, confirmBars=3}) over a <strong>3-day</strong> centered-median smooth of
     * the daily-aggregated density — the exact density prep the H2R-1 spike validated on the DATA-1
     * continuous 17-symbol tape (12 fused-regime cycles, calm FA 0.008/day).
     *
     * @return the crypto regime timescale configuration
     */
    public static RegimeTimescaleConfig crypto() {
        return new RegimeTimescaleConfig(RegimeConfig.crypto(), 3);
    }
}
