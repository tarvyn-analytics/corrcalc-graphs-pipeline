package ch.tarvynanalytics.corrcalc.graphs.pipeline.data;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.MarketSnapshot;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Aligns per-symbol price bars into the time-ordered {@link MarketSnapshot} cross-sections a
 * {@link ch.tarvynanalytics.corrcalc.graphs.pipeline.source.MarketDataSource} streams — the
 * common-timestamp intersection step of {@link ReturnPanels}, factored out as the <em>close</em>
 * cross-sections (before returns). Replaying stored bars then reads as
 * {@code align(...) → IterableMarketDataSource → ReturnBuilder}, which reproduces
 * {@code ReturnPanels} exactly (pinned by {@code ReturnBuilderTest}).
 *
 * <p>Only timestamps present for <em>every</em> symbol become a snapshot (the universe is time-varying,
 * so the intersection — not a fixed grid — defines the aligned series), matching
 * {@code ReturnPanels.buildIntraday/buildDaily}.</p>
 */
public final class PriceSnapshots {

    private PriceSnapshots() {
    }

    /**
     * Builds the aligned close cross-sections for {@code symbols} from their per-symbol bars.
     *
     * @param pricesBySymbol per-symbol time-ordered bars (each symbol's own series)
     * @param symbols        the ordered symbol set (column order of every snapshot)
     * @return the common-timestamp aligned snapshots, ascending by time
     */
    public static List<MarketSnapshot> align(Map<String, List<Bar>> pricesBySymbol, String[] symbols) {
        if (symbols.length == 0) {
            return List.of();
        }
        TreeSet<Instant> common = null;
        for (String symbol : symbols) {
            TreeSet<Instant> timestamps = new TreeSet<>();
            for (Bar bar : pricesBySymbol.get(symbol)) {
                timestamps.add(bar.timestamp());
            }
            if (common == null) {
                common = timestamps;
            } else {
                common.retainAll(timestamps);
            }
        }

        Map<String, Map<Instant, Double>> closeAt = new LinkedHashMap<>();
        for (String symbol : symbols) {
            Map<Instant, Double> closes = new LinkedHashMap<>();
            for (Bar bar : pricesBySymbol.get(symbol)) {
                closes.put(bar.timestamp(), bar.close());
            }
            closeAt.put(symbol, closes);
        }

        List<MarketSnapshot> out = new ArrayList<>();
        for (Instant ts : common) {
            double[] closes = new double[symbols.length];
            for (int s = 0; s < symbols.length; s++) {
                closes[s] = closeAt.get(symbols[s]).get(ts);
            }
            out.add(new MarketSnapshot(ts, closes));
        }
        return out;
    }
}
