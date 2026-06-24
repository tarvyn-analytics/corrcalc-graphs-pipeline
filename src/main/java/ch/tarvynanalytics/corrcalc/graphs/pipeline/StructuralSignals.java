package ch.tarvynanalytics.corrcalc.graphs.pipeline;

import ch.tarvynanalytics.graphs.algos.model.ChangeMetrics;
import ch.tarvynanalytics.graphs.algos.model.ChangeSignal;

import java.time.Instant;
import java.util.List;

/**
 * Bridges the S3 detector's {@link ChangeSignal} (graphs-algos-lib) into the pipeline's published
 * {@link StructuralSignal}. This is the only place the S3 model crosses into the pipeline's event
 * schema; the validity block starts {@link StructuralSignal.Validity#accepted() accepted} and is
 * (re)assigned by the {@link SignalFilter} stage, and the lead is filled in only when measurable.
 */
public final class StructuralSignals {

    private StructuralSignals() {
    }

    /**
     * Maps a fired S3 {@link ChangeSignal} to a {@link StructuralSignal}.
     *
     * @param signal           the S3 detector signal for the firing transition
     * @param asOf             the timestamp of the matrix that produced it
     * @param market           the market label (e.g. {@code "crypto"})
     * @param timescale        the timescale label ({@code "daily"} / {@code "intraday"})
     * @param universe         the variable labels in column order
     * @param kind             fusion (upper arm) or de-fusion (lower arm)
     * @param leadVsDailyHours the lead over the daily detector in hours, or {@code null} if not measurable
     * @return the published structural-change signal (validity = accepted; the filter may override)
     */
    public static StructuralSignal fromChangeSignal(ChangeSignal signal, Instant asOf, String market,
                                                    String timescale, List<String> universe,
                                                    SignalKind kind, Double leadVsDailyHours) {
        ChangeMetrics m = signal.metrics();
        return new StructuralSignal(
                asOf, market, timescale, universe, kind,
                m.weightedChange(), signal.sPlus(), signal.sMinus(),
                m.densityLevel(), m.edgeXor(), m.nComponents(), m.largestComponentFraction(),
                StructuralSignal.Validity.accepted(), leadVsDailyHours);
    }
}
