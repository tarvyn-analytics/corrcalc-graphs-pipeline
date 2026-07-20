package ch.tarvynanalytics.corrcalc.graphs.pipeline.engine;

import ch.tarvynanalytics.corrcalc.graphs.pipeline.CalibrationEvent;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.CollectingSink;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObservation;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.PipelineObserver;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.RegimeEvent;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.SymbolVolatilityObservation;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.calib.SymbolVolatilityBaseline;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.ReturnBuilder;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.data.SessionPolicy;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.SymbolVolatilityConfig;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.detect.TimescaleConfig;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.IterableMarketDataSource;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.MarketDataSource;
import ch.tarvynanalytics.corrcalc.graphs.pipeline.source.MarketSnapshot;
import ch.tarvynanalytics.graphs.algos.DetectorConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The per-symbol volatility channel at the engine/driver seam: builder validation, the strict
 * no-op contract when unconfigured (the engine stays byte-identical), the driver forwarding, and
 * the pinned ordering — a warm channel's {@code onSymbolVolatility} always precedes the same bar's
 * {@code onObservation}.
 */
class PipelineEngineSymbolVolatilityTest {

    private static final TimescaleConfig CFG = new TimescaleConfig(12, DetectorConfig.crypto());
    private static final SymbolVolatilityConfig VOL_CFG =
            new SymbolVolatilityConfig(4, 3, Duration.ofDays(2));

    /** One typed entry of the interleaved observer stream, for ordering assertions. */
    private record Event(String type, Instant asOf) {
    }

    @Test
    void symbolVolatility_NullConfigOrBaseline_Rejected() {
        PipelineEngine.Builder builder = PipelineEngine.builder(syms(4), CFG);

        assertThrows(IllegalArgumentException.class,
                () -> builder.symbolVolatility(null, baseline(syms(4))));
        assertThrows(IllegalArgumentException.class,
                () -> builder.symbolVolatility(VOL_CFG, null));
    }

    @Test
    void build_BaselineSymbolsDifferFromEngineSymbols_RejectedWithBracketedValues() {
        String[] shuffled = {"S1", "S0", "S2", "S3"};   // same set, wrong order
        PipelineEngine.Builder builder = PipelineEngine.builder(syms(4), CFG).calmBars(20)
                .sink(new CollectingSink()).symbolVolatility(VOL_CFG, baseline(shuffled));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, builder::build);
        assertTrue(ex.getMessage().contains("[") && ex.getMessage().contains("S1"), ex.getMessage());
    }

    @Test
    void onCloses_Unconfigured_IsStrictNoOpAndKeepsEngineByteIdentical() {
        List<Object> withCloses = new ArrayList<>();
        List<Object> withoutCloses = new ArrayList<>();
        List<SymbolVolatilityObservation> vol = new ArrayList<>();
        PipelineEngine fed = allStreamEngine(withCloses, vol);
        PipelineEngine twin = allStreamEngine(withoutCloses, vol);

        Instant t0 = Instant.parse("2024-01-01T00:00:00Z");
        Random rng = new Random(7L);
        for (int i = 0; i < 80; i++) {
            Instant asOf = t0.plus(i, ChronoUnit.DAYS);
            double[] closes = new double[4];
            double[] returns = new double[4];
            for (int s = 0; s < 4; s++) {
                closes[s] = 100.0 + rng.nextDouble();
                returns[s] = 0.01 * rng.nextGaussian();
            }
            fed.onCloses(asOf, closes);   // strict no-op: not configured
            fed.onReturns(asOf, returns);
            twin.onReturns(asOf, returns);
        }
        // Even a wrong-length cross-section is ignored — the channel is absent, not validating.
        assertDoesNotThrow(() -> fed.onCloses(t0, new double[]{1.0}));

        assertTrue(vol.isEmpty(), "an unconfigured engine never emits the volatility channel");
        assertFalse(withCloses.isEmpty(), "the run scored transitions");
        assertEquals(withoutCloses, withCloses,
                "onCloses must leave every observer stream byte-identical");
        assertEquals(twin.summary(), fed.summary());
    }

    @Test
    void run_ConfiguredChannel_EmitsEveryWarmBarBeforeTheSameBarsObservation() {
        List<Event> events = new ArrayList<>();
        List<SymbolVolatilityObservation> vol = new ArrayList<>();
        PipelineObserver observer = new PipelineObserver() {
            @Override
            public void onObservation(PipelineObservation observation) {
                events.add(new Event("observation", observation.asOf()));
            }

            @Override
            public void onSymbolVolatility(SymbolVolatilityObservation observation) {
                events.add(new Event("volatility", observation.asOf()));
                vol.add(observation);
            }
        };
        PipelineEngine engine = PipelineEngine.builder(syms(4), CFG).calmBars(20).market("crypto")
                .timescale("daily").sink(new CollectingSink()).observer(observer)
                .symbolVolatility(VOL_CFG, baseline(syms(4))).build();

        RunSummary summary = PipelineDriver.run(source(120, 4, 11L),
                new ReturnBuilder(syms(4), SessionPolicy.DAILY_SINGLE), engine, Pace.none());

        assertTrue(summary.detectionPoints() > 0, "the run scored transitions");
        assertFalse(vol.isEmpty(), "the driver forwarded closes into the channel");
        assertEquals("crypto", vol.get(0).market());
        assertEquals("daily", vol.get(0).timescale());
        assertEquals(List.of(syms(4)), vol.get(0).symbols());
        assertEquals(4, vol.get(0).zScores().length);

        // The pinned ordering: wherever both streams carry the same bar, the volatility bar
        // arrived first (the driver feeds onCloses before the same snapshot's return bar).
        Map<Instant, Integer> firstVolatility = firstIndexOf(events, "volatility");
        Map<Instant, Integer> firstObservation = firstIndexOf(events, "observation");
        assertFalse(firstObservation.isEmpty());
        for (Map.Entry<Instant, Integer> obs : firstObservation.entrySet()) {
            Integer volIndex = firstVolatility.get(obs.getKey());
            if (volIndex != null) {
                assertTrue(volIndex < obs.getValue(),
                        "volatility must precede the observation at [" + obs.getKey() + "]");
            }
        }
        // The channel warms long before calibration ends here, so every scored bar has both.
        assertEquals(summary.detectionPoints(),
                firstObservation.keySet().stream().filter(firstVolatility::containsKey).count());
    }

    @Test
    void onSessionBoundary_DoesNotResetTheVolatilityChannel() {
        List<SymbolVolatilityObservation> interrupted = new ArrayList<>();
        List<SymbolVolatilityObservation> straight = new ArrayList<>();
        PipelineEngine a = engine(new ArrayList<>(), interrupted, baseline(syms(4)));
        PipelineEngine b = engine(new ArrayList<>(), straight, baseline(syms(4)));

        Instant t0 = Instant.parse("2024-01-01T00:00:00Z");
        Random rng = new Random(3L);
        for (int i = 0; i < 10; i++) {
            double[] closes = new double[4];
            for (int s = 0; s < 4; s++) {
                closes[s] = 100.0 * Math.exp(0.01 * rng.nextGaussian());
            }
            if (i == 5) {
                a.onSessionBoundary();   // must slide straight through: no window/predecessor reset
            }
            Instant asOf = t0.plus(i, ChronoUnit.DAYS);
            a.onCloses(asOf, closes);
            b.onCloses(asOf, closes);
        }

        assertFalse(straight.isEmpty(), "the channel warmed and emitted");
        assertEquals(straight, interrupted, "a session boundary never resets the channel");
    }

    @Test
    void noOpObserver_IgnoresTheVolatilityChannel() {
        SymbolVolatilityObservation bar = new SymbolVolatilityObservation(Instant.EPOCH, "crypto",
                "daily", List.of("A"), new double[]{1.0});
        assertDoesNotThrow(() -> PipelineObserver.noOp().onSymbolVolatility(bar));
    }

    // ---------------------------------------------------------------- helpers

    /**
     * An unconfigured engine recording <em>every</em> observer stream (observations, calibration
     * events, regime events, density levels) into one ordered list — the byte-identity twin proof
     * compares whole streams, not just observations.
     */
    private static PipelineEngine allStreamEngine(List<Object> stream,
                                                  List<SymbolVolatilityObservation> vol) {
        PipelineObserver observer = new PipelineObserver() {
            @Override
            public void onObservation(PipelineObservation observation) {
                stream.add(observation);
            }

            @Override
            public void onCalibrationEvent(CalibrationEvent event) {
                stream.add(event);
            }

            @Override
            public void onRegimeEvent(RegimeEvent event) {
                stream.add(event);
            }

            @Override
            public void onDensityLevel(Instant asOf, double smoothedLevel) {
                stream.add(List.of(asOf, smoothedLevel));
            }

            @Override
            public void onSymbolVolatility(SymbolVolatilityObservation observation) {
                vol.add(observation);
            }
        };
        return PipelineEngine.builder(syms(4), CFG).calmBars(20).market("crypto").timescale("daily")
                .sink(new CollectingSink()).observer(observer).build();
    }

    /** An engine collecting both observer streams; the channel is present iff {@code base} is. */
    private static PipelineEngine engine(List<PipelineObservation> observations,
                                         List<SymbolVolatilityObservation> vol,
                                         SymbolVolatilityBaseline base) {
        PipelineObserver observer = new PipelineObserver() {
            @Override
            public void onObservation(PipelineObservation observation) {
                observations.add(observation);
            }

            @Override
            public void onSymbolVolatility(SymbolVolatilityObservation observation) {
                vol.add(observation);
            }
        };
        PipelineEngine.Builder builder = PipelineEngine.builder(syms(4), CFG).calmBars(20)
                .market("crypto").timescale("daily").sink(new CollectingSink()).observer(observer);
        if (base != null) {
            builder.symbolVolatility(VOL_CFG, base);
        }
        return builder.build();
    }

    private static SymbolVolatilityBaseline baseline(String[] symbols) {
        double[] mu = new double[symbols.length];
        double[] sigma = new double[symbols.length];
        java.util.Arrays.fill(mu, 0.008);
        java.util.Arrays.fill(sigma, 0.004);
        return new SymbolVolatilityBaseline("crypto", "daily", 0L,
                Instant.parse("2023-01-01T00:00:00Z"), Instant.parse("2023-02-01T00:00:00Z"),
                List.of(symbols), mu, sigma);
    }

    /** The first stream index of each {@code asOf} for one event type. */
    private static Map<Instant, Integer> firstIndexOf(List<Event> events, String type) {
        Map<Instant, Integer> out = new HashMap<>();
        for (int i = 0; i < events.size(); i++) {
            if (events.get(i).type().equals(type)) {
                out.putIfAbsent(events.get(i).asOf(), i);
            }
        }
        return out;
    }

    /** Calm random-walk closes, daily-spaced (mirrors {@code PipelineDriverTest}). */
    private static MarketDataSource source(int rows, int symbols, long seed) {
        Random rng = new Random(seed);
        Instant t0 = Instant.parse("2024-01-01T00:00:00Z");
        double[] price = new double[symbols];
        java.util.Arrays.fill(price, 100.0);
        List<MarketSnapshot> snapshots = new ArrayList<>();
        for (int t = 0; t < rows; t++) {
            double[] closes = new double[symbols];
            for (int s = 0; s < symbols; s++) {
                price[s] *= Math.exp(0.01 * rng.nextGaussian());
                closes[s] = price[s];
            }
            snapshots.add(new MarketSnapshot(t0.plus(t, ChronoUnit.DAYS), closes));
        }
        return new IterableMarketDataSource(syms(symbols), snapshots);
    }

    private static String[] syms(int n) {
        String[] out = new String[n];
        for (int s = 0; s < n; s++) {
            out[s] = "S" + s;
        }
        return out;
    }
}
