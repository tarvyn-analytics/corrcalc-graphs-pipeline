package ch.tarvynanalytics.corrcalc.graphs.pipeline.source;

import java.util.Optional;

/**
 * The pipeline's inbound seam: a pull-based source of {@link MarketSnapshot}s over one fixed universe
 * at one sampling frequency. This is the SPI a data provider implements so the pipeline can consume it
 * — the {@code PipelineDriver} repeatedly {@link #poll()}s and feeds each snapshot through a
 * {@code ReturnBuilder} into the {@code PipelineEngine}, knowing nothing about where the data came from.
 *
 * <p><strong>Pull, single-writer, one frequency.</strong> Exactly one driver thread calls
 * {@link #poll()}; the implementation blocks until the next aligned cross-section is available (a live
 * feed drains an internal queue) and returns {@link Optional#empty()} when the stream has ended (a
 * replay exhausts its stored bars). Mixing frequencies or universes in one source is not supported —
 * construct one source per stream, exactly as one {@code RollingCorrelationEngine}/{@code ChangeDetector}
 * runs per timescale.</p>
 *
 * <p><strong>Scope.</strong> {@link IterableMarketDataSource} (in-memory / stored bars) is the in-process
 * implementation shipped here. Network connectors (websocket/REST to a broker or exchange) implement
 * this same interface and are the S2 deliverable; the contract above is all they must satisfy.</p>
 */
public interface MarketDataSource extends AutoCloseable {

    /**
     * The universe this source streams, in the stable column order every {@link MarketSnapshot#closes()}
     * uses (and the order the downstream engine's correlation matrix is built in).
     *
     * @return the symbol labels (column order)
     */
    String[] universe();

    /**
     * Blocks until the next aligned cross-section is available and returns it, or {@link Optional#empty()}
     * when the stream has ended.
     *
     * @return the next snapshot, or empty at end-of-stream
     * @throws InterruptedException if the thread is interrupted while waiting for the next snapshot
     */
    Optional<MarketSnapshot> poll() throws InterruptedException;

    /** Releases any resources held by the source (sockets, threads). The default is a no-op. */
    @Override
    default void close() {
        // no resources to release by default
    }
}
