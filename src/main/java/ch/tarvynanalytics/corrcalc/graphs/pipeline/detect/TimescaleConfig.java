package ch.tarvynanalytics.corrcalc.graphs.pipeline.detect;

import ch.tarvynanalytics.graphs.algos.DefusionConfig;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;

/**
 * The per-timescale tuning of one stream: the S1 rolling-window width {@code W} (owned by the
 * correlation engine) plus the S3 {@link DetectorConfig} (the alert constants {@code k}, {@code h},
 * level percentile, edge threshold {@code τ}, sigma floor). Every value is configuration — a new
 * market or timescale is wired, not coded (build-design §6).
 *
 * <p>The settled crypto constants (crypto-n8-verdict): intraday window {@code 480} (8 h of 1-min
 * bars), daily window {@code 14}; both timescales share the crypto detector constants
 * ({@code k=1.5, h=8, p99, τ=0.5}) — only the window differs, exactly as the spike's
 * {@code replay_crypto_main} does.</p>
 *
 * @param window   the rolling-window width in bars ({@code >= 2})
 * @param detector the S3 detector tuning (alert constants + edge threshold)
 * @param rearm    the continuous-stream re-arm cadence (H2 Q4); {@link RearmConfig#disabled()} keeps
 *                 the one-fire-per-run behaviour
 */
public record TimescaleConfig(int window, DetectorConfig detector, RearmConfig rearm) {

    /** A single-window configuration with the re-arm cadence disabled (the pre-H2 behaviour). */
    public TimescaleConfig(int window, DetectorConfig detector) {
        this(window, detector, RearmConfig.disabled());
    }

    /**
     * The crypto intraday timescale: 480-bar (8 h) window, crypto detector constants, with the
     * de-fusion <strong>all-clear ENABLED</strong> (gauge spec §8, Phase-4 validated on the 60-day
     * recovery tape: 6/6 recovering events fire, may2021 suppressed, calm FA 0.000/day). The gauge
     * window is {@code 2880} samples — {@code W_g = 48 h} at the intraday 1-min cadence — with the
     * settled {@code θ=0.80}, {@code bandC=0.75}. The cadence-specific {@code N_g} lives here (the
     * pipeline edge owns the market/cadence wiring); the library stays cadence-agnostic.
     */
    public static TimescaleConfig cryptoIntraday() {
        DetectorConfig base = DetectorConfig.crypto();
        DetectorConfig withDefusion = new DetectorConfig(base.k(), base.h(), base.levelPctile(),
                base.edgeThreshold(), base.epsilonSigma(), base.fireArm(),
                new DefusionConfig(0.75, 0.80, 2880, true));
        // Re-arm on the all-clear + 48 h cool-down (2880 one-minute bars); the calendar backstop is
        // off — de-fusion drives the cadence on this timescale (numerics spec Q4.1).
        return new TimescaleConfig(480, withDefusion, new RearmConfig(true, 2880, 0.25, 5, 0));
    }

    /**
     * The crypto daily comparator: 14-bar window, the same crypto detector constants. De-fusion firing
     * stays <strong>disabled</strong> on the daily stream: a 48 h recovery-gauge window is only ~2 daily
     * samples, too coarse to be a meaningful all-clear — the recovery track is an intraday product.
     */
    public static TimescaleConfig cryptoDaily() {
        // No all-clear on daily (de-fusion off) — re-arm on S⁺-relaxation with the gate closed
        // (5 sustained bars below 0.25·h), with the 21-day calendar backstop (numerics spec Q4.2).
        return new TimescaleConfig(14, DetectorConfig.crypto(), new RearmConfig(true, 2, 0.25, 5, 21));
    }

    /** The edge threshold {@code τ} (an edge exists where {@code |r| > τ}). */
    public double edgeThreshold() {
        return detector.edgeThreshold();
    }
}
