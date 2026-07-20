package ch.tarvynanalytics.corrcalc.graphs.pipeline.detect;

import java.time.Duration;

/**
 * Tuning of the optional <strong>per-symbol realized-volatility diagnostic channel</strong> — a
 * z-score series alongside the matrix-level observation stream. Every value is configuration, per
 * the family rule: a new market or cadence is wired, not coded.
 *
 * <p>The channel treats the tape as <em>continuous</em>: its only discontinuity treatment is the
 * {@code gapMask} (a longer-than-mask hole zeroes the whole return row); there is no session
 * handling, and session boundaries do not reset its windows.</p>
 *
 * @param volWindow    the rolling RMS window over per-symbol log returns, in bars ({@code >= 2});
 *                     a column stays unscored until the window has fully filled — no partial-window
 *                     volatility is ever computed
 * @param smoothWindow the trailing-median smoothing window over the per-symbol z series, in bars
 *                     ({@code >= 1}); z-rows exist only from the first full volatility window, so
 *                     no value is emitted before {@code volWindow + smoothWindow − 1} returns have
 *                     accumulated
 * @param gapMask      the largest inter-snapshot spacing treated as contiguous; a snapshot arriving
 *                     more than this after its predecessor zeroes the entire return row (every
 *                     column), so the hole never inflates the volatility read
 */
public record SymbolVolatilityConfig(int volWindow, int smoothWindow, Duration gapMask) {

    /** Validates the windows and the gap mask, bracketing the offending value. */
    public SymbolVolatilityConfig {
        if (volWindow < 2) {
            throw new IllegalArgumentException("volWindow must be >= 2 [" + volWindow + "]");
        }
        if (smoothWindow < 1) {
            throw new IllegalArgumentException("smoothWindow must be >= 1 [" + smoothWindow + "]");
        }
        if (gapMask == null || gapMask.isNegative() || gapMask.isZero()) {
            throw new IllegalArgumentException("gapMask must be positive [" + gapMask + "]");
        }
    }
}
