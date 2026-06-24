package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import java.time.Instant;
import java.util.List;

/**
 * The pipeline's product: a published structural-change signal/event (build-design §5.1) — the
 * B2B "signal-as-a-product", not a UI. A dashboard becomes one consumer of this stream. Emitted by
 * the wiring when the S3 detector fires (and, optionally, on every transition for downstream
 * consumers that want the full series); passed through a {@link SignalFilter} and fanned out to the
 * configured {@link SignalSink}s.
 *
 * <p>This is an immutable value: {@code universe} is defensively copied. All metric fields carry the
 * detector's raw output for the firing transition; {@code validity} is populated by the filter stage
 * (no-op by default), and {@code leadVsDailyHours} is filled in only when a lead can be measured
 * (both timescales fired).</p>
 *
 * @param asOf                     the timestamp of the matrix that produced this signal
 * @param market                   the market label (e.g. {@code "crypto"})
 * @param timescale                which timescale stream produced it ({@code "daily"} / {@code "intraday"})
 * @param universe                 the variable labels of the correlation matrix, in column order
 * @param kind                     fusion (upper arm) or de-fusion (lower arm)
 * @param changeMetric             the primary weighted edge-change {@code mean |Δr|} for this transition
 * @param cusumSPlus               the upper-arm CUSUM statistic after this transition
 * @param cusumSMinus              the lower-arm CUSUM statistic after this transition
 * @param levelDensity             the secondary absolute density {@code mean[|r|>τ]} (the context gate)
 * @param edgeXorFraction          the secondary discrete edge-flip fraction {@code |E_t △ E_{t-1}|/|V|}
 * @param nComponents              union-find component count of the thresholded graph at this transition
 * @param largestComponentFraction the largest connected component as a fraction of the universe size
 * @param validity                 the filter verdict (no-op default ACCEPT, score {@code null})
 * @param leadVsDailyHours         wall-clock hours this fire led the daily detector, or {@code null} if not measurable
 */
public record StructuralSignal(
        Instant asOf,
        String market,
        String timescale,
        List<String> universe,
        SignalKind kind,
        double changeMetric,
        double cusumSPlus,
        double cusumSMinus,
        double levelDensity,
        double edgeXorFraction,
        int nComponents,
        double largestComponentFraction,
        Validity validity,
        Double leadVsDailyHours) {

    /** Defensive copy of the universe so the published signal cannot be mutated by the producer. */
    public StructuralSignal {
        universe = universe == null ? List.of() : List.copyOf(universe);
    }

    /**
     * Returns a copy of this signal with the filter verdict replaced — used by the publisher to
     * attach the {@link SignalFilter}'s assessment to a freshly detected signal.
     *
     * @param newValidity the verdict to attach
     * @return a copy carrying {@code newValidity}
     */
    public StructuralSignal withValidity(Validity newValidity) {
        return new StructuralSignal(asOf, market, timescale, universe, kind, changeMetric,
                cusumSPlus, cusumSMinus, levelDensity, edgeXorFraction, nComponents,
                largestComponentFraction, newValidity, leadVsDailyHours);
    }

    /**
     * The validity verdict from the {@link SignalFilter} stage (build-design §5.3, feature #1a).
     *
     * @param filtered {@code true} if the filter suppressed this signal as likely invalid
     * @param score    an optional confidence score in {@code [0,1]}, or {@code null} when the filter
     *                 does not produce one (the no-op default)
     */
    public record Validity(boolean filtered, Double score) {
        /** The default verdict of the no-op filter: accepted, no score. */
        public static Validity accepted() {
            return new Validity(false, null);
        }
    }
}
