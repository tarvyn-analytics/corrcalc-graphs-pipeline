package ch.tarvynanalytics.pipeline.replay;

import ch.tarvynanalytics.graphs.algos.Calibration;
import ch.tarvynanalytics.graphs.algos.ChangeDetectors;
import ch.tarvynanalytics.pipeline.data.Bar;
import ch.tarvynanalytics.pipeline.data.PriceBars;
import ch.tarvynanalytics.pipeline.data.ReturnPanel;
import ch.tarvynanalytics.pipeline.data.ReturnPanels;
import ch.tarvynanalytics.pipeline.detect.DensityChangeSeries;
import ch.tarvynanalytics.pipeline.detect.SeriesBuilder;
import ch.tarvynanalytics.pipeline.detect.TimescaleConfig;

import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The full data-driven pipeline over a directory of saved Binance CSV bars: for one event and its
 * universe, load the raw {@code 1m}/{@code 1d} bars, build the aligned return panels, drive S1
 * (rolling correlation) into the density/weighted-change series, calibrate walk-forward on the calm
 * block, and score the event window into a {@link LeadTableRow}. This is the end-to-end
 * {@code CSV → S1 → S3 → alert} chain exercised by the opt-in full-replay regression (the same path
 * the spike's {@code replay_crypto_main} runs, in Java).
 *
 * <p>The raw bars are not committed (2.4 GB, gitignored); the universe symbol list is supplied by
 * the caller (the committed fixture carries it, so the era-list curation stays Python-side).</p>
 */
public final class CryptoReplay {

    private static final String INTRADAY_FREQ = "1m";
    private static final String DAILY_FREQ = "1d";

    private CryptoReplay() {
    }

    /**
     * Replays one event end-to-end into a lead-table row, using the settled crypto timescale tuning
     * (480-bar intraday / 14-bar daily, crypto detector constants).
     *
     * @param dataDir  the directory holding {@code <SYMBOL>_<freq>_<event>.csv} bar files
     * @param event    the event boundaries
     * @param universe the symbol set (column order) for this event
     * @return the lead-table row from the density-level baseline detector
     */
    public static LeadTableRow replayEvent(Path dataDir, CryptoEvent event, List<String> universe) {
        return replayEvent(dataDir, event, universe,
                TimescaleConfig.cryptoIntraday(), TimescaleConfig.cryptoDaily());
    }

    /**
     * Replays one event end-to-end with explicit timescale tuning (the window/detector constants are
     * configuration — a different market or a test fixture passes its own).
     *
     * @param dataDir     the directory holding {@code <SYMBOL>_<freq>_<event>.csv} bar files
     * @param event       the event boundaries
     * @param universe    the symbol set (column order) for this event
     * @param intradayCfg the intraday timescale tuning
     * @param dailyCfg    the daily timescale tuning
     * @return the lead-table row from the density-level baseline detector
     */
    public static LeadTableRow replayEvent(Path dataDir, CryptoEvent event, List<String> universe,
                                           TimescaleConfig intradayCfg, TimescaleConfig dailyCfg) {
        TimescaleScoring daily = scoreTimescale(dataDir, event, universe, DAILY_FREQ, dailyCfg, false);
        TimescaleScoring intraday = scoreTimescale(dataDir, event, universe, INTRADAY_FREQ, intradayCfg, true);
        return EventScorer.score(event, universe.size(), daily, intraday, intradayCfg.detector());
    }

    private static TimescaleScoring scoreTimescale(Path dataDir, CryptoEvent event, List<String> universe,
                                                   String freq, TimescaleConfig cfg, boolean intraday) {
        Map<String, List<Bar>> prices = new LinkedHashMap<>();
        for (String symbol : universe) {
            Path csv = dataDir.resolve(symbol + "_" + freq + "_" + event.name() + ".csv");
            prices.put(symbol, PriceBars.read(csv, event.calmStart(), event.end()));
        }
        String[] symbols = universe.toArray(new String[0]);
        ReturnPanel panel = intraday ? ReturnPanels.buildIntraday(prices, symbols) : ReturnPanels.buildDaily(prices, symbols);
        DensityChangeSeries series = SeriesBuilder.build(panel, cfg.window(), cfg.edgeThreshold());

        // Walk-forward: calibrate the density baseline on the calm block (density for the CUSUM and
        // the level gate alike), then carry the event-window slice for scoring.
        double[] calmDensity = sliceDensity(series, event.calmStart(), event.calmEnd());
        Calibration calibration = ChangeDetectors.calibrate(calmDensity, calmDensity, cfg.detector());
        return sliceEventWindow(series, event.start(), event.end(), calibration);
    }

    private static double[] sliceDensity(DensityChangeSeries series, LocalDate from, LocalDate to) {
        List<Double> values = new ArrayList<>();
        for (int i = 0; i < series.size(); i++) {
            if (inRange(series.timestamps().get(i), from, to)) {
                values.add(series.density()[i]);
            }
        }
        double[] out = new double[values.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = values.get(i);
        }
        return out;
    }

    private static TimescaleScoring sliceEventWindow(DensityChangeSeries series, LocalDate from, LocalDate to,
                                                     Calibration calibration) {
        List<Instant> timestamps = new ArrayList<>();
        List<Double> density = new ArrayList<>();
        List<Double> change = new ArrayList<>();
        for (int i = 0; i < series.size(); i++) {
            Instant ts = series.timestamps().get(i);
            if (inRange(ts, from, to)) {
                timestamps.add(ts);
                density.add(series.density()[i]);
                change.add(series.weightedChange()[i]);
            }
        }
        return new TimescaleScoring(calibration, List.copyOf(timestamps), toArray(density), toArray(change));
    }

    private static boolean inRange(Instant ts, LocalDate from, LocalDate to) {
        LocalDate date = LocalDate.ofInstant(ts, ZoneOffset.UTC);
        return !date.isBefore(from) && !date.isAfter(to);
    }

    private static double[] toArray(List<Double> values) {
        double[] out = new double[values.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = values.get(i);
        }
        return out;
    }
}
