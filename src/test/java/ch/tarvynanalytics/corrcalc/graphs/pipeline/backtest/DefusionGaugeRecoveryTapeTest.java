package ch.tarvynanalytics.corrcalc.graphs.pipeline.backtest;

import ch.tarvynanalytics.graphs.algos.Calibration;
import ch.tarvynanalytics.graphs.algos.ChangeDetectors;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.Bar;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.PriceBars;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.ReturnPanel;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.ReturnPanels;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.DensityChangeSeries;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.SeriesBuilder;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.TimescaleConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Opt-in Phase-4 end-to-end validation of the de-fusion <strong>recovery gauge</strong> over the real
 * 60-day crypto recovery tape, at the pipeline's production <strong>1-min cadence</strong> (the spike
 * oracle {@code defusion_validate7.py} runs a 30-min stride; this confirms the cadence ports). It reuses
 * the <em>real</em> Java density path ({@link SeriesBuilder}, the pinned S1→S3 wiring) and the
 * <em>real</em> calibration ({@link ChangeDetectors#calibrate}), then runs the gauge rule
 * (trailing-{@code N_g} in-band occupancy ≥ θ with a full window, onset-armed at peak-fusion — an
 * independent re-implementation of the rule the lib's {@code RecoveryGauge} is separately pinned to by
 * Oracle A1) forward over each event's recovery.
 *
 * <p>Asserts the GO bar of the recovery-gauge validation: the genuinely-recovering events fire the all-clear, the
 * choppy non-recovery {@code may2021_selloff} is suppressed, and the gauge climbs back to ~1.0 (the
 * 0→1 recovery track). Calm false-alarm rate is 0/day by the was-fused latch (architectural — proven in
 * the oracle; not recomputed here). <strong>Skipped unless</strong>
 * {@code -Dcrypto.data.dir=/path/to/spike/crypto-data} points at the (gitignored) raw bars, so CI never
 * runs it.</p>
 *
 * <pre>{@code
 * ./mvnw test -Dtest=DefusionGaugeRecoveryTapeTest \
 *     -Dcrypto.data.dir=../corrcalc-graphs-research-scratches/spike/crypto-data
 * }</pre>
 */
@EnabledIfSystemProperty(named = "crypto.data.dir", matches = ".+")
class DefusionGaugeRecoveryTapeTest {

    private static final int GAUGE_WINDOW_SAMPLES = 2880;   // N_g = 48 h at the 1-min intraday cadence
    private static final double THETA = 0.80;
    private static final String INTRADAY_FREQ = "1m";

    @Test
    void recoveryGauge_FiresGenuineRecoveries_AndSuppressesMay2021_At1MinCadence() {
        Path dataDir = Path.of(System.getProperty("crypto.data.dir"));
        int goodFired = 0;
        int goodTotal = 0;
        boolean may2021Fired = false;

        for (CryptoEvent event : CryptoEvents.ALL) {
            EventResult r = evaluate(dataDir, event);
            if (r == null) {
                System.out.printf("%-24s SKIP (data-thin)%n", event.name());
                continue;
            }
            System.out.printf("%-24s L_band=%.3f  peak_d=%.3f  peak_gauge=%.3f  fired=%s%n",
                    event.name(), r.lBand, r.peakDensity, r.peakGauge, r.fired);
            if (event.name().equals("may2021_selloff")) {
                may2021Fired = r.fired;
            } else {
                goodTotal++;
                if (r.fired) {
                    goodFired++;
                }
                // (a) the gauge climbs back toward 1.0 as a genuine recovery heals.
                assertTrue(r.peakGauge >= THETA,
                        event.name() + " gauge should climb past theta on a recovery (peak=" + r.peakGauge + ")");
            }
        }

        // (b) every genuinely-recovering event fires the all-clear; may2021 is suppressed.
        assertEquals(goodTotal, goodFired, "all genuinely-recovering events should fire the all-clear");
        assertTrue(goodTotal >= 5, "expected the recovering-event base (got " + goodTotal + ")");
        assertEquals(false, may2021Fired, "may2021 (choppy non-recovery) must be suppressed by the theta+full-window guard");
    }

    private static EventResult evaluate(Path dataDir, CryptoEvent event) {
        List<String> universe = LeadTableFixtures.universe(event.name());
        Map<String, List<Bar>> prices = new LinkedHashMap<>();
        // Read the FULL recovery tape (calmStart .. well past the original window) so the post-event
        // recovery phase is present, not the truncated original window CryptoBacktest uses.
        LocalDate readTo = event.end().plusDays(70);
        for (String symbol : universe) {
            Path csv = dataDir.resolve(symbol + "_" + INTRADAY_FREQ + "_" + event.name() + ".csv");
            prices.put(symbol, PriceBars.read(csv, event.calmStart(), readTo));
        }
        String[] symbols = universe.toArray(new String[0]);
        ReturnPanel panel = ReturnPanels.buildIntraday(prices, symbols);
        TimescaleConfig cfg = TimescaleConfig.cryptoIntraday();
        DensityChangeSeries series = SeriesBuilder.build(panel, cfg.window(), cfg.edgeThreshold());
        if (series.size() < GAUGE_WINDOW_SAMPLES + 10) {
            return null;
        }

        // Calibrate mu_D / sigma_D on the calm block (the real walk-forward calibration); L_band = mu_D + bandC*sigma_D.
        double[] calmDensity = sliceCalmDensity(series, event);
        double[] calmChange = sliceCalmChange(series, event);
        if (calmDensity.length < 5) {
            return null;
        }
        Calibration cal = ChangeDetectors.calibrate(calmChange, calmDensity, cfg.detector());
        double lBand = cal.muDensity() + cfg.detector().defusion().bandC() * cal.sigmaDensity();

        // Onset-arm at the in-event peak-fusion sample (the ground-truth latch, as the spike oracle does),
        // then run the trailing-occupancy gauge forward over the recovery.
        double[] d = series.density();
        List<Instant> ts = series.timestamps();
        int peak = peakFusionIndex(d, ts, event);
        if (peak < 0) {
            return null;
        }
        return runGauge(d, peak, lBand);
    }

    /** The gauge rule: trailing N_g in-band occupancy, fire on a full window with occupancy >= theta and the current sample in-band. */
    private static EventResult runGauge(double[] d, int peak, double lBand) {
        boolean fired = false;
        double peakGauge = 0.0;
        int inBandInWindow = 0;
        for (int i = peak + 1; i < d.length; i++) {
            int windowStart = Math.max(peak + 1, i - GAUGE_WINDOW_SAMPLES + 1);
            if (i - peak > GAUGE_WINDOW_SAMPLES) {
                // the sample leaving the trailing window
                if (d[i - GAUGE_WINDOW_SAMPLES] <= lBand) {
                    inBandInWindow--;
                }
            }
            boolean inBand = d[i] <= lBand;
            if (inBand) {
                inBandInWindow++;
            }
            int windowSize = i - windowStart + 1;
            double gauge = (double) inBandInWindow / windowSize;
            peakGauge = Math.max(peakGauge, gauge);
            boolean windowFull = (i - peak) >= GAUGE_WINDOW_SAMPLES;
            if (!fired && windowFull && gauge >= THETA && inBand) {
                fired = true;
            }
        }
        return new EventResult(fired, peakGauge, lBand, d[peak]);
    }

    private static int peakFusionIndex(double[] d, List<Instant> ts, CryptoEvent event) {
        int peak = -1;
        double best = -1.0;
        for (int i = 0; i < d.length; i++) {
            LocalDate date = LocalDate.ofInstant(ts.get(i), ZoneOffset.UTC);
            if (!date.isBefore(event.start()) && !date.isAfter(event.end()) && d[i] > best) {
                best = d[i];
                peak = i;
            }
        }
        return peak;
    }

    private static double[] sliceCalmDensity(DensityChangeSeries series, CryptoEvent event) {
        return slice(series, event, series.density());
    }

    private static double[] sliceCalmChange(DensityChangeSeries series, CryptoEvent event) {
        return slice(series, event, series.weightedChange());
    }

    private static double[] slice(DensityChangeSeries series, CryptoEvent event, double[] values) {
        int n = 0;
        for (int i = 0; i < series.size(); i++) {
            if (inCalm(series.timestamps().get(i), event)) {
                n++;
            }
        }
        double[] out = new double[n];
        int j = 0;
        for (int i = 0; i < series.size(); i++) {
            if (inCalm(series.timestamps().get(i), event)) {
                out[j++] = values[i];
            }
        }
        return out;
    }

    private static boolean inCalm(Instant ts, CryptoEvent event) {
        LocalDate date = LocalDate.ofInstant(ts, ZoneOffset.UTC);
        return !date.isBefore(event.calmStart()) && !date.isAfter(event.calmEnd());
    }

    private record EventResult(boolean fired, double peakGauge, double lBand, double peakDensity) {
    }
}
