package ch.tarvynanalytics.pipeline.detect;

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
 */
public record TimescaleConfig(int window, DetectorConfig detector) {

    /** The crypto intraday timescale: 480-bar (8 h) window, crypto detector constants. */
    public static TimescaleConfig cryptoIntraday() {
        return new TimescaleConfig(480, DetectorConfig.crypto());
    }

    /** The crypto daily comparator: 14-bar window, the same crypto detector constants. */
    public static TimescaleConfig cryptoDaily() {
        return new TimescaleConfig(14, DetectorConfig.crypto());
    }

    /** The edge threshold {@code τ} (an edge exists where {@code |r| > τ}). */
    public double edgeThreshold() {
        return detector.edgeThreshold();
    }
}
