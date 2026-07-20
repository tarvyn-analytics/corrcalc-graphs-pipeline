package ch.tarvynanalytics.corrcalc.graphs.pipeline.calib;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * The frozen per-column baseline of the per-symbol realized-volatility diagnostic channel: a robust
 * location {@code mu[k]} and pre-scaled robust scale {@code sigma[k]} per universe column, stamped
 * with the market, timescale, epoch and source window that produced them — the same provenance
 * discipline as {@link CalibrationArtifact}. The channel scores each column as
 * {@code z = (vol − mu[k]) / sigma[k]}; a {@code NaN} or non-positive {@code sigma[k]} marks the
 * column <em>permanently unscored</em> (its z stays {@link Double#NaN} for the whole run).
 *
 * <p>Immutable value: {@code symbols} is copied and {@code mu}/{@code sigma} are defensively cloned
 * on construction (the read accessors are zero-copy, matching the engine's array contracts — do not
 * mutate what {@link #mu()} / {@link #sigma()} return).</p>
 *
 * @param market     the market the baseline was measured on (e.g. {@code "crypto"})
 * @param timescale  the timescale of the source series: {@code "intraday"} or {@code "daily"}
 *                   (never mix timescales — the baseline is as timescale-bound as the channel)
 * @param epochId    monotonic per calibration lifecycle; {@code 0} for a single-epoch offline fit
 * @param sourceFrom source-window start (inclusive) — the window's provenance
 * @param sourceTo   source-window end (exclusive)
 * @param symbols    the universe column order the per-column arrays are indexed by
 * @param mu         the per-column robust location of the volatility series
 * @param sigma      the per-column robust scale (pre-scaled); {@code NaN} or {@code <= 0} leaves
 *                   the column unscored
 */
public record SymbolVolatilityBaseline(
        String market,
        String timescale,
        long epochId,
        Instant sourceFrom,
        Instant sourceTo,
        List<String> symbols,
        double[] mu,
        double[] sigma) {

    /** Validates the provenance and per-column arrays, bracketing the offending value. */
    public SymbolVolatilityBaseline {
        if (market == null || market.isBlank()) {
            throw new IllegalArgumentException("market must be non-blank [" + market + "]");
        }
        if (!"intraday".equals(timescale) && !"daily".equals(timescale)) {
            throw new IllegalArgumentException("timescale must be intraday or daily [" + timescale + "]");
        }
        if (epochId < 0) {
            throw new IllegalArgumentException("epochId must be >= 0 [" + epochId + "]");
        }
        if (sourceFrom == null || sourceTo == null) {
            throw new IllegalArgumentException(
                    "source window must be stamped [" + sourceFrom + ", " + sourceTo + "]");
        }
        if (!sourceFrom.isBefore(sourceTo)) {
            throw new IllegalArgumentException(
                    "source window must be non-empty [" + sourceFrom + " >= " + sourceTo + "]");
        }
        if (symbols == null || symbols.isEmpty()) {
            throw new IllegalArgumentException("symbols must be non-empty [" + symbols + "]");
        }
        symbols = List.copyOf(symbols);
        if (mu == null || mu.length != symbols.size()) {
            throw new IllegalArgumentException("mu must carry one value per symbol ["
                    + (mu == null ? null : mu.length) + "], expected [" + symbols.size() + "]");
        }
        if (sigma == null || sigma.length != symbols.size()) {
            throw new IllegalArgumentException("sigma must carry one value per symbol ["
                    + (sigma == null ? null : sigma.length) + "], expected [" + symbols.size() + "]");
        }
        mu = mu.clone();
        sigma = sigma.clone();
    }

    /** Content-aware equality (the per-column arrays are compared by value, not identity). */
    @Override
    public boolean equals(Object o) {
        return o instanceof SymbolVolatilityBaseline other
                && Objects.equals(market, other.market)
                && Objects.equals(timescale, other.timescale)
                && epochId == other.epochId
                && Objects.equals(sourceFrom, other.sourceFrom)
                && Objects.equals(sourceTo, other.sourceTo)
                && Objects.equals(symbols, other.symbols)
                && Arrays.equals(mu, other.mu)
                && Arrays.equals(sigma, other.sigma);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(market, timescale, epochId, sourceFrom, sourceTo, symbols);
        result = 31 * result + Arrays.hashCode(mu);
        result = 31 * result + Arrays.hashCode(sigma);
        return result;
    }

    @Override
    public String toString() {
        return "SymbolVolatilityBaseline[market=" + market + ", timescale=" + timescale
                + ", epochId=" + epochId + ", sourceFrom=" + sourceFrom + ", sourceTo=" + sourceTo
                + ", symbols=" + symbols + ", mu=" + Arrays.toString(mu)
                + ", sigma=" + Arrays.toString(sigma) + "]";
    }
}
