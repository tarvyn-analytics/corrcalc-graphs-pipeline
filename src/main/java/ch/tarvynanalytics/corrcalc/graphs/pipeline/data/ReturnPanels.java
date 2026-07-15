package ch.tarvynanalytics.corrcalc.graphs.pipeline.data;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Builds aligned {@link ReturnPanel}s from per-symbol price bars — a faithful Java port of the
 * crypto spike's {@code replay_crypto_panel.build_crypto_intraday_panel} /
 * {@code build_crypto_daily_returns}. The two correctness rules it enforces:
 *
 * <ul>
 *   <li><strong>Common-timestamp intersection.</strong> Only timestamps present for <em>every</em>
 *       symbol in the set become bars; the universe is time-varying across events, so the
 *       intersection (not a fixed grid) defines the aligned panel.</li>
 *   <li><strong>No return crosses a session boundary.</strong> Intraday sessions are UTC calendar
 *       days; the first bar of each UTC day starts a new session and produces no return (the
 *       overnight gap is dropped). Daily returns are consecutive closes with no session reset.</li>
 * </ul>
 *
 * <p>The return definition is {@code log(close_t / close_{t-1})} everywhere, matching
 * {@code replay_signal.log_return}.</p>
 */
public final class ReturnPanels {

    private ReturnPanels() {
    }

    /** The single return definition used everywhere: {@code log(close / prevClose)}. */
    public static double logReturn(double prevClose, double close) {
        return Math.log(close / prevClose);
    }

    /**
     * Builds the intraday panel: common-timestamp intersection, UTC-day session reset, log returns.
     * Mirrors {@code build_crypto_intraday_panel}.
     *
     * @param pricesBySymbol per-symbol time-ordered bars (each symbol's own series)
     * @param symbols        the ordered symbol set (column order of the panel)
     * @return the aligned intraday return panel
     */
    public static ReturnPanel buildIntraday(Map<String, List<Bar>> pricesBySymbol, String[] symbols) {
        List<Instant> commonTs = commonTimestamps(pricesBySymbol, symbols);
        Map<String, Map<Instant, Double>> closeAt = closeIndex(pricesBySymbol, symbols);

        List<Instant> rts = new ArrayList<>();
        List<double[]> rows = new ArrayList<>();
        List<Integer> sessions = new ArrayList<>();

        int curSession = -1;
        LocalDate curSessionDate = null;
        Instant prevTs = null;
        for (Instant ts : commonTs) {
            LocalDate date = LocalDate.ofInstant(ts, ZoneOffset.UTC);
            if (curSessionDate == null || !date.equals(curSessionDate)) {
                curSession++;
                curSessionDate = date;
                prevTs = ts;
                continue;   // first bar of a UTC day: no within-session predecessor
            }
            rows.add(returnRow(symbols, closeAt, prevTs, ts));
            rts.add(ts);
            sessions.add(curSession);
            prevTs = ts;
        }
        return assemble(rts, symbols, rows, sessions);
    }

    /**
     * Builds the daily panel: common-date intersection, consecutive-close log returns, single
     * session. Mirrors {@code build_crypto_daily_returns}.
     *
     * @param pricesBySymbol per-symbol time-ordered daily bars
     * @param symbols        the ordered symbol set (column order of the panel)
     * @return the aligned daily return panel
     */
    public static ReturnPanel buildDaily(Map<String, List<Bar>> pricesBySymbol, String[] symbols) {
        List<Instant> commonTs = commonTimestamps(pricesBySymbol, symbols);
        Map<String, Map<Instant, Double>> closeAt = closeIndex(pricesBySymbol, symbols);

        List<Instant> rts = new ArrayList<>();
        List<double[]> rows = new ArrayList<>();
        List<Integer> sessions = new ArrayList<>();
        for (int i = 1; i < commonTs.size(); i++) {
            Instant prevTs = commonTs.get(i - 1);
            Instant ts = commonTs.get(i);
            rows.add(returnRow(symbols, closeAt, prevTs, ts));
            rts.add(ts);
            sessions.add(0);   // daily closes never gap a session
        }
        return assemble(rts, symbols, rows, sessions);
    }

    private static double[] returnRow(String[] symbols, Map<String, Map<Instant, Double>> closeAt,
                                      Instant prevTs, Instant ts) {
        double[] row = new double[symbols.length];
        for (int s = 0; s < symbols.length; s++) {
            Map<Instant, Double> closes = closeAt.get(symbols[s]);
            row[s] = logReturn(closes.get(prevTs), closes.get(ts));
        }
        return row;
    }

    private static List<Instant> commonTimestamps(Map<String, List<Bar>> pricesBySymbol, String[] symbols) {
        if (symbols.length == 0) {
            return List.of();
        }
        TreeSet<Instant> common = null;
        for (String symbol : symbols) {
            TreeSet<Instant> ts = new TreeSet<>();
            for (Bar bar : pricesBySymbol.get(symbol)) {
                ts.add(bar.timestamp());
            }
            if (common == null) {
                common = ts;
            } else {
                common.retainAll(ts);
            }
        }
        return new ArrayList<>(common);
    }

    private static Map<String, Map<Instant, Double>> closeIndex(Map<String, List<Bar>> pricesBySymbol,
                                                                String[] symbols) {
        Map<String, Map<Instant, Double>> index = new LinkedHashMap<>();
        for (String symbol : symbols) {
            Map<Instant, Double> closes = new LinkedHashMap<>();
            for (Bar bar : pricesBySymbol.get(symbol)) {
                closes.put(bar.timestamp(), bar.close());
            }
            index.put(symbol, closes);
        }
        return index;
    }

    private static ReturnPanel assemble(List<Instant> rts, String[] symbols,
                                        List<double[]> rows, List<Integer> sessions) {
        double[][] returns = rows.toArray(new double[0][]);
        int[] sessionId = new int[sessions.size()];
        for (int i = 0; i < sessionId.length; i++) {
            sessionId[i] = sessions.get(i);
        }
        return new ReturnPanel(List.copyOf(rts), symbols.clone(), returns, sessionId);
    }
}
