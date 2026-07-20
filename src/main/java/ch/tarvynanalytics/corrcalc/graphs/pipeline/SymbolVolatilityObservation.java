package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * One bar of the optional <strong>per-symbol realized-volatility diagnostic channel</strong>: the
 * smoothed robust volatility z-score of every universe column at one timestamp, alongside the
 * matrix-level observation stream. The engine emits one of these per aligned close cross-section
 * once the channel is warm, always <em>before</em> the same bar's {@code onObservation} — a
 * consumer can therefore attach the per-symbol read to the matrix-level read of the same bar.
 *
 * <p><strong>{@link Double#NaN} means UNSCORED</strong>, never zero: a column is unscored while its
 * volatility window fills, while its smoother warms, or permanently when its baseline scale is
 * degenerate. Consumers must skip a NaN entry — never compare against it.</p>
 *
 * <p>Immutable value: {@code symbols} is copied and {@code zScores} is defensively cloned on
 * construction (the read accessor is zero-copy, matching the engine's array contracts — do not
 * mutate what {@link #zScores()} returns).</p>
 *
 * @param asOf      the timestamp of the close cross-section that produced this bar
 * @param market    the market label (e.g. {@code "crypto"})
 * @param timescale which timescale stream produced it ({@code "daily"} / {@code "intraday"})
 * @param symbols   the universe column order the z-scores are indexed by
 * @param zScores   the smoothed robust volatility z per column; {@link Double#NaN} = unscored
 */
public record SymbolVolatilityObservation(
        Instant asOf,
        String market,
        String timescale,
        List<String> symbols,
        double[] zScores) {

    /** Validates the components, copies {@code symbols} and defensively clones {@code zScores}. */
    public SymbolVolatilityObservation {
        if (asOf == null) {
            throw new IllegalArgumentException("asOf must not be null");
        }
        if (symbols == null || symbols.isEmpty()) {
            throw new IllegalArgumentException("symbols must be non-empty [" + symbols + "]");
        }
        symbols = List.copyOf(symbols);
        if (zScores == null || zScores.length != symbols.size()) {
            throw new IllegalArgumentException("zScores must carry one value per symbol ["
                    + (zScores == null ? null : zScores.length) + "], expected [" + symbols.size() + "]");
        }
        zScores = zScores.clone();
    }

    /** Content-aware equality (the {@code zScores} array is compared by value, not identity). */
    @Override
    public boolean equals(Object o) {
        return o instanceof SymbolVolatilityObservation other
                && Objects.equals(asOf, other.asOf)
                && Objects.equals(market, other.market)
                && Objects.equals(timescale, other.timescale)
                && Objects.equals(symbols, other.symbols)
                && Arrays.equals(zScores, other.zScores);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(asOf, market, timescale, symbols) + Arrays.hashCode(zScores);
    }

    @Override
    public String toString() {
        return "SymbolVolatilityObservation[asOf=" + asOf + ", market=" + market
                + ", timescale=" + timescale + ", symbols=" + symbols
                + ", zScores=" + Arrays.toString(zScores) + "]";
    }
}
