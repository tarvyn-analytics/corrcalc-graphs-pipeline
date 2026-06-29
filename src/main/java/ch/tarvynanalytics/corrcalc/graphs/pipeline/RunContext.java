package ch.tarvynanalytics.corrcalc.graphs.pipeline;

/**
 * The static run context echoed before the observation stream — the configuration known up front
 * (window, edge threshold, CUSUM constants, level-gate percentile, firing arm) plus the run's
 * provenance ({@code mode} = replay/live and how the baseline was {@code calibration}-d). The
 * <em>learned</em> calibration (μ, σ, L) is deliberately not here — it is discovered from the calm block
 * and arrives with the first observation. Built by the driver (e.g. {@code PacedReplay}) and delivered
 * once through {@link PipelineObserver#onStart}.
 *
 * @param market           market label
 * @param timescale        timescale label (daily/intraday)
 * @param mode             how the data is sourced: {@code "replay"} (a future live feed is {@code "live"})
 * @param calibration      how the baseline was selected: {@code "leading-warmup"} (replay's pragmatic
 *                         leading prefix) vs {@code "calm-block"} (a rigorous walk-forward calm window)
 * @param window           the S1 rolling-window width {@code W} (bars)
 * @param edgeThreshold    the edge threshold {@code τ} (an edge exists where {@code |r| > τ})
 * @param cusumK           the CUSUM reference value {@code k} (in calm-σ units)
 * @param decisionInterval the CUSUM decision interval {@code h} (in calm-σ units)
 * @param levelPctile      the calm-density percentile defining the absolute level gate {@code L}
 * @param fireArm          which CUSUM arm opens an alert (e.g. {@code "UPPER"})
 * @param calmBars         window-points consumed to calibrate
 * @param speed            the replay pace (simulated:real time ratio)
 */
public record RunContext(
        String market,
        String timescale,
        String mode,
        String calibration,
        int window,
        double edgeThreshold,
        double cusumK,
        double decisionInterval,
        double levelPctile,
        String fireArm,
        int calmBars,
        double speed) {
}
