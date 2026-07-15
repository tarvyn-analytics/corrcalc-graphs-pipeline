package ch.tarvynanalytics.corrcalc.graphs.pipeline.engine;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.ReturnBuilder;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.MarketDataSource;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.MarketSnapshot;

import java.time.Instant;
import java.util.Optional;

/**
 * The single run loop that connects the inbound seam to the engine: it pulls {@link MarketSnapshot}s
 * from a {@link MarketDataSource}, turns each into a return bar with a {@link ReturnBuilder}, and feeds
 * the bars into a {@link PipelineEngine} — pacing the detection phase via a {@link Pace} hook. This is
 * the one place the three pieces meet; a replay and a live run differ only in the source they pass and
 * the {@code Pace} they choose.
 *
 * <p>Pacing matches the engine's calibration boundary: only post-calibration bars are paced (the warm-up
 * and calm prefix run as fast as the source yields them), so a replay's first impression starts
 * immediately and then unfolds at the scaled wall-clock rate. The loop stops at end-of-stream, when the
 * engine requests a stop (a detection-point limit), or on interruption.</p>
 */
public final class PipelineDriver {

    private PipelineDriver() {
    }

    /**
     * Drives {@code source} through {@code builder} into {@code engine}, paced by {@code pace}, until the
     * source is exhausted, the engine requests a stop, or the thread is interrupted.
     *
     * @param source  the inbound market-data source
     * @param builder the prices→returns stage (its session policy must match the source's frequency)
     * @param engine  the pipeline engine to feed
     * @param pace    the pacing hook ({@code ReplayClock} for a replay, {@link Pace#none()} for live)
     * @return the engine's run summary
     */
    public static RunSummary run(MarketDataSource source, ReturnBuilder builder, PipelineEngine engine,
                                 Pace pace) {
        if (source == null || builder == null || engine == null || pace == null) {
            throw new IllegalArgumentException("source, builder, engine and pace must be non-null");
        }
        Instant prevBarTs = null;
        try {
            Optional<MarketSnapshot> next;
            while ((next = source.poll()).isPresent()) {
                Optional<ReturnBuilder.ReturnBar> bar = builder.accept(next.get());
                if (bar.isPresent()) {
                    Instant ts = bar.get().asOf();
                    if (engine.isCalibrated()) {
                        pace.between(prevBarTs, ts);   // pace only the detection phase
                    }
                    engine.onReturns(ts, bar.get().returns());
                    prevBarTs = ts;
                }
                if (engine.stopRequested() || Thread.currentThread().isInterrupted()) {
                    break;
                }
            }
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();   // stop promptly; the summary so far is returned
        }
        engine.finish();   // end-of-stream: flush the regime aggregator + report an open-at-EOF regime
        return engine.summary();
    }
}
