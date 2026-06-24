package ch.tarvynanalytics.corrcalc.graphs.pipeline.source;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

/**
 * One <strong>aligned cross-section</strong> of the market: a single timestamp and the close price of
 * every universe symbol at that timestamp, in the universe's column order. This is the unit a
 * {@link MarketDataSource} hands the pipeline — the inbound counterpart to the outbound
 * {@code StructuralSignal}.
 *
 * <p><strong>Alignment is the source's responsibility, not the pipeline's.</strong> A snapshot already
 * carries one close per symbol for the <em>same</em> instant; turning a real provider's per-symbol,
 * asynchronously-arriving ticks into aligned cross-sections (waiting for the slowest symbol of a bar,
 * filling/skipping gaps) is the connector's job — exactly the work an S2 connector does behind this
 * seam. Downstream, {@code ReturnBuilder} turns a stream of these into log-return vectors.</p>
 *
 * <p>Immutable value: {@code closes} is defensively copied on construction (the read accessor is
 * zero-copy, matching the engine's array contracts — do not mutate what {@link #closes()} returns).</p>
 *
 * @param timestamp the cross-section's instant (UTC)
 * @param closes    one close price per universe symbol, in column order
 */
public record MarketSnapshot(Instant timestamp, double[] closes) {

    /** Validates the components and defensively copies {@code closes}. */
    public MarketSnapshot {
        if (timestamp == null) {
            throw new IllegalArgumentException("timestamp must not be null");
        }
        if (closes == null) {
            throw new IllegalArgumentException("closes must not be null");
        }
        closes = closes.clone();
    }

    /** The number of symbols (close prices) in this cross-section. */
    public int symbolCount() {
        return closes.length;
    }

    /** Content-aware equality (the {@code closes} array is compared by value, not identity). */
    @Override
    public boolean equals(Object o) {
        return o instanceof MarketSnapshot other
                && Objects.equals(timestamp, other.timestamp)
                && Arrays.equals(closes, other.closes);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hashCode(timestamp) + Arrays.hashCode(closes);
    }

    @Override
    public String toString() {
        return "MarketSnapshot[timestamp=" + timestamp + ", closes=" + Arrays.toString(closes) + "]";
    }
}
