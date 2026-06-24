package ch.tarvynanalytics.corrcalc.graphs.pipeline.detect;

import ch.tarvynanalytics.graphs.algos.Calibration;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;

import java.util.Optional;

/**
 * The scalar {@code AND(level-gate, two-sided CUSUM)} alert rule — a faithful Java port of the
 * spike's {@code replay_alert.alerts}. This is the <strong>density-level baseline</strong> detector
 * that produced {@code crypto_lead_table.csv}; the pipeline keeps it (rather than reusing the S3
 * {@link ch.tarvynanalytics.graphs.algos.ChangeDetector}) precisely because the baseline runs the
 * CUSUM on the <em>density</em> series, whereas the S3 detector runs it on the weighted-change
 * series. The rule is generic over two parallel series so it serves both:
 *
 * <ul>
 *   <li><strong>Density baseline</strong> (reproduces the lead table): {@code fireValues} =
 *       {@code levelValues} = density — one series for both gates, exactly as the spike's
 *       {@code value_key="density"}.</li>
 *   <li><strong>Change detector</strong> (the S3 structure): {@code fireValues} = weighted change,
 *       {@code levelValues} = density — the same split the S3 {@code CusumChangeDetector} makes.</li>
 * </ul>
 *
 * <p>Rule (spike §1.2): {@code z_t = (fire_t − μ)/σ}; {@code S+_t = max(0, S+_{t−1} + z_t − k)};
 * {@code S−_t = max(0, S−_{t−1} − z_t − k)}; gate A is {@code level_t ≥ L}; gate B is
 * {@code S+_t > h}; fire on the first {@code A ∧ B}, then debounce (reset {@code S+}). A NaN fire
 * value is a gap: the accumulators carry unchanged and no alert opens (matching the S3 detector).</p>
 */
public final class CusumLevelAlert {

    private CusumLevelAlert() {
    }

    /**
     * The first alert in a single window: scans the whole series as one debounce window (no per-row
     * reset), exactly as the spike's {@code event_alert} scores one event window with {@code S+}
     * starting at zero.
     *
     * @param fireValues  the series the CUSUM runs on (density for the baseline; change for S3)
     * @param levelValues the series the level gate tests (density in both cases); same length as {@code fireValues}
     * @param cal         the calm-window calibration {@code (μ, σ, L)}
     * @param cfg         the detector tuning supplying {@code k} and {@code h}
     * @return the first fire, or empty if the window never fired (a censored miss)
     */
    public static Optional<Fire> firstFire(double[] fireValues, double[] levelValues,
                                           Calibration cal, DetectorConfig cfg) {
        requireParallel(fireValues, levelValues);
        Cusum state = new Cusum();
        for (int t = 0; t < fireValues.length; t++) {
            Fire fire = state.step(t, fireValues[t], levelValues[t], cal, cfg);
            if (fire != null) {
                return Optional.of(fire);
            }
        }
        return Optional.empty();
    }

    /**
     * Counts the fires over a series partitioned into debounce windows by {@code resetIds} (e.g. a
     * per-calm-session id) — the spike's {@code calm_fa_rate} / {@code false_alarm_rate} mechanic,
     * where each session gets an independent chance to fire. Crossing into a new id re-arms the
     * detector (debounce flag cleared, {@code S+} reset to zero).
     *
     * @param fireValues  the CUSUM series
     * @param levelValues the level-gate series (same length)
     * @param resetIds    the window id per row (same length); a change re-arms the detector
     * @param cal         the calm-window calibration
     * @param cfg         the detector tuning
     * @return the number of fires across all windows
     */
    public static int countFires(double[] fireValues, double[] levelValues, int[] resetIds,
                                 Calibration cal, DetectorConfig cfg) {
        requireParallel(fireValues, levelValues);
        if (resetIds.length != fireValues.length) {
            throw new IllegalArgumentException(
                    "resetIds length [" + resetIds.length + "] must equal series length [" + fireValues.length + "]");
        }
        Cusum state = new Cusum();
        int fires = 0;
        Integer current = null;
        for (int t = 0; t < fireValues.length; t++) {
            if (current == null || resetIds[t] != current) {
                current = resetIds[t];
                state.reArm();
            }
            if (state.step(t, fireValues[t], levelValues[t], cal, cfg) != null) {
                fires++;
            }
        }
        return fires;
    }

    private static void requireParallel(double[] fireValues, double[] levelValues) {
        if (fireValues.length != levelValues.length) {
            throw new IllegalArgumentException(
                    "fire/level series lengths differ: [" + fireValues.length + "] vs [" + levelValues.length + "]");
        }
    }

    /** Mutable two-sided CUSUM with one-fire-per-window debounce. */
    private static final class Cusum {
        private double sPlus;
        private double sMinus;
        private boolean alreadyFired;

        void reArm() {
            alreadyFired = false;
            sPlus = 0.0;
        }

        Fire step(int index, double fireValue, double levelValue, Calibration cal, DetectorConfig cfg) {
            if (!Double.isNaN(fireValue)) {   // NaN fire value is a gap: carry accumulators, never fire
                double z = (fireValue - cal.mu()) / cal.sigma();
                sPlus = Math.max(0.0, sPlus + z - cfg.k());
                sMinus = Math.max(0.0, sMinus - z - cfg.k());
            }
            boolean gateLevel = levelValue >= cal.level();   // NaN level => false
            boolean gateCusum = sPlus > cfg.h();
            boolean fire = !alreadyFired && !Double.isNaN(fireValue) && gateLevel && gateCusum;
            if (fire) {
                Fire result = new Fire(index, fireValue, levelValue, sPlus, sMinus);
                alreadyFired = true;
                sPlus = 0.0;   // debounce: reset the upper arm after firing
                return result;
            }
            return null;
        }
    }

    /**
     * A fired alert.
     *
     * @param index      the row index in the scored series at which the alert fired
     * @param fireValue  the CUSUM series value at the fire (the density for the baseline detector —
     *                   this is the {@code *_density_at_alert} column of the lead table)
     * @param levelValue the level-gate series value at the fire
     * @param sPlus      the upper-arm CUSUM at the fire
     * @param sMinus     the lower-arm CUSUM at the fire
     */
    public record Fire(int index, double fireValue, double levelValue, double sPlus, double sMinus) {
    }
}
