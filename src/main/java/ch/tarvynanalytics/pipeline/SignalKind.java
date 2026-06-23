package ch.tarvynanalytics.pipeline;

/**
 * The direction of a structural-change signal. The S3 detector is two-sided (build-design §3.3):
 * the upper CUSUM arm fires a {@link #FUSION} (the correlation structure tightened — an
 * exit/risk-on-correlation alert), and the lower arm a {@link #DEFUSION} (the structure loosened —
 * the symmetric re-entry mirror). v1 opens alerts on {@link #FUSION}; {@link #DEFUSION} is carried
 * from day one so re-entry detection is a later config flip, not a redesign.
 */
public enum SignalKind {
    /** The correlation structure tightened (upper CUSUM arm). */
    FUSION,
    /** The correlation structure loosened (lower CUSUM arm). */
    DEFUSION
}
