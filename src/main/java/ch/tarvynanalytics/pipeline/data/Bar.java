package ch.tarvynanalytics.pipeline.data;

import java.time.Instant;

/**
 * One price bar: the bar's close timestamp and its close price. The pipeline only needs the close
 * (returns are close-to-close); open/high/low/volume from the source CSV are dropped on read.
 *
 * @param timestamp the bar's timestamp (UTC instant)
 * @param close     the close price
 */
public record Bar(Instant timestamp, double close) {
}
