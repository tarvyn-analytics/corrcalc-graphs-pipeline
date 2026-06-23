package ch.tarvynanalytics.pipeline.replay;

import ch.tarvynanalytics.graphs.algos.DetectorConfig;
import ch.tarvynanalytics.pipeline.detect.CusumLevelAlert;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Scores one event into a {@link LeadTableRow} using the density-level baseline detector — the
 * port of the spike's {@code event_alert}: run {@code AND(level, CUSUM)} over the event-window
 * density series (calibrated walk-forward on the calm block), once per timescale, and read off the
 * first alert. This is what reproduces {@code crypto_lead_table.csv}.
 *
 * <p>Walk-forward discipline lives in how {@link TimescaleScoring} was built (its calibration comes
 * from the calm block, its series from the event window); the scorer only runs the rule, so it is a
 * pure function of its inputs.</p>
 */
public final class EventScorer {

    private EventScorer() {
    }

    /**
     * Scores a single event from its two timescales' calibrated event-window slices.
     *
     * @param event        the event boundaries / labels
     * @param universeSize the symbol count after the liquidity filter
     * @param daily        the daily timescale's calibration + event-window series
     * @param intraday     the intraday timescale's calibration + event-window series
     * @param cfg          the detector tuning ({@code k}, {@code h})
     * @return the lead-table row
     */
    public static LeadTableRow score(CryptoEvent event, int universeSize,
                                     TimescaleScoring daily, TimescaleScoring intraday, DetectorConfig cfg) {
        Optional<CusumLevelAlert.Fire> dailyFire =
                CusumLevelAlert.firstFire(daily.density(), daily.density(), daily.calibration(), cfg);
        Optional<CusumLevelAlert.Fire> intradayFire =
                CusumLevelAlert.firstFire(intraday.density(), intraday.density(), intraday.calibration(), cfg);

        Instant tDaily = dailyFire.map(f -> daily.timestamps().get(f.index())).orElse(null);
        Instant tIntraday = intradayFire.map(f -> intraday.timestamps().get(f.index())).orElse(null);
        Double leadHours = (tDaily != null && tIntraday != null) ? leadHours(tDaily, tIntraday) : null;

        return new LeadTableRow(
                event.name(), universeSize, event.calmBlock(), event.eventWindow(),
                tDaily, tIntraday,
                dailyFire.map(CusumLevelAlert.Fire::fireValue).orElse(null),
                intradayFire.map(CusumLevelAlert.Fire::fireValue).orElse(null),
                daily.calibration().level(), intraday.calibration().level(),
                leadHours, dailyFire.isEmpty(), intradayFire.isEmpty(), null);
    }

    /**
     * Wall-clock hours the intraday alert led the daily one ({@code t_daily − t_intraday}), the
     * 24/7 crypto clock (elapsed wall-clock time is elapsed trading time). Positive when intraday
     * fired earlier.
     *
     * @param tDaily    the daily alert timestamp
     * @param tIntraday the intraday alert timestamp
     * @return the lead in hours
     */
    public static double leadHours(Instant tDaily, Instant tIntraday) {
        return Duration.between(tIntraday, tDaily).getSeconds() / 3600.0;
    }
}
