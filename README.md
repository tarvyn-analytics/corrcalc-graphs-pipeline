# corrcalc-graphs-pipeline

> Feature-complete and stable; maintained as-is.

A **streaming structural-change detector for market correlation structure**. It
watches a universe of assets bar by bar, maintains the rolling correlation
matrix incrementally, scores how much the *structure* of that matrix changes
over time, and publishes a signal when the market fuses into one tightly
correlated block (**FUSION**) or relaxes back out of it (**DEFUSION**) — the
regime shifts that matter for diversification and risk, detected as they
happen rather than in hindsight.

It is the integration layer over two sibling libraries:

- [**corrcalc-lib**](https://github.com/tarvyn-analytics/corrcalc-lib)
  (`ch.tarvynanalytics.corrcalc:corrcalc-lib-core`) — the online rolling-Pearson
  engine (`stream` package): feed returns, get N×N correlation-matrix snapshots
  on a cadence, `O(N²)` per bar with zero allocation.
- [**graphs-algos-lib**](https://github.com/tarvyn-analytics/graphs-algos-lib)
  (`ch.tarvynanalytics.graphs:graphs-algos-lib`) — the temporal change detector
  (`ChangeDetector`): feed consecutive matrices, get the weighted-Δr change
  metric plus a two-sided CUSUM change-point signal.

Both libraries stay asset-agnostic — they never import a connector, calendar,
sink or filter. Only this pipeline knows about all of them at once:

```
                         ┌──────────────────── PipelineEngine (the hub) ───────────────────────┐
 MarketDataSource ─poll→ ReturnBuilder ─returns→ rolling corr ─matrices→ temporal change ───────┤
   (a source:            (log returns,           (corrcalc-lib:          (graphs-algos-lib:     │
    stored bars / live)   drop session-first)     RollingCorrelations)    ChangeDetector)       │
                                                                                                │
   PipelineDriver pulls the source, builds returns, paces, and feeds the engine ───────────────┘
                                                          │
                            fire? ┌─────────────────────────────────────────────┐ every transition
                       StructuralSignal → SignalFilter → SignalSink   │   PipelineObservation → ObservationPolicy → PipelineObserver
                          (the censored product fire-stream)          │        (the full series; the consumer's policy decides)
```

The product is a **published structural-change signal** (`StructuralSignal`),
not a UI — a dashboard would be just one more consumer of the stream.

## Build

```bash
./mvnw clean verify    # tests + JaCoCo 80/70 gate
```

**Requires JDK 25.** The two upstream libraries resolve from GitHub Packages
(which needs a GitHub token with `read:packages`); for tokenless local work,
install each to `~/.m2` once from a sibling checkout:

```bash
( cd ../corrcalc-lib && ./mvnw -DskipTests install )
( cd ../graphs-algos-lib && ./mvnw -DskipTests install )
```

## The CLI — live replay

The pipeline ships a CLI that **replays stored bars through the real pipeline
at a configurable pace** and logs the signal as it evolves — the quickest way
to *see* what the engine produces. Build the runnable jar with the `cli`
profile (a shaded uber-jar; the published thin jar is unaffected):

```bash
./mvnw -Pcli -DskipTests package
java -jar target/corrcalc-graphs-pipeline-*-cli.jar replay <data-dir> --event <name> --market crypto \
  --timescale intraday --speed 500
```

`<data-dir>` holds per-symbol `<SYMBOL>_<freq>_<event>.csv` OHLCV bars; the
universe defaults to `<data-dir>/<event>_universe.csv`. Each window-end
transition logs a **calm heartbeat** (density, weighted-change, both CUSUM
arms); every genuine **FUSION/DEFUSION** fire is published to the sink and
printed as a highlighted banner so it stands out from the calm stream:

```
INFO  density=0.000  wD=0.068  S+=0.00  S-=0.00
INFO  density=0.000  wD=0.104  S+=0.37  S-=0.00
INFO  density=1.000  wD=0.733  S+=28.19 S-=0.00
WARN  === FUSION   === crypto/daily @ 2021-01-25T00:00:00Z  wΔ=0.7331  S+=28.19  density=1.000  comps=1
INFO  density=1.000  wD=0.312  S+=9.46  S-=0.00
```

| Argument | Default | Meaning |
|---|---|---|
| `<data-dir>` | — | directory of `<SYMBOL>_<freq>_<event>.csv` bar files (positional) |
| `--event <name>` | *(required)* | event id selecting the bar files + the default universe |
| `--market <name>` | *(required)* | market config + the published signal's label (supported: `crypto`) |
| `--timescale intraday\|daily` | `intraday` | which stream to replay (`1m` / `1d`; picks the window + detector tuning) |
| `--speed <multiplier>` | `60` | simulated : real time ratio — higher replays faster |
| `--max-step-ms <ms>` | `2000` | cap on the per-bar sleep so session gaps don't stall the view |
| `--calm-bars <N>` | ~40% of the series | window-points used to calibrate the detector (a leading warm-up) |
| `--limit <N>` | unlimited | stop after N detection points |
| `--heartbeat-every <N>` | `1` | forward one observation in every N (thin a noisy stream) |
| `--observe <spec>` | `all` | which transitions reach the heartbeat: `all` \| `fires` \| `change>=<x>` \| `activation>=<x>` \| `density` (per-day smoothed density record; regime fire mode + `--style ndjson`) |
| `--style technical\|readable\|ndjson` | `technical` | terse metrics; an annotated stream with legend/banner/severity; or machine-readable NDJSON on stdout (diagnostics to stderr, pipes clean) |
| `--calibration leading-warmup\|calm-block\|adaptive` | `leading-warmup` | how the detector is calibrated: leading prefix; primed from a persisted walk-forward artifact; or the adaptive online walk-forward estimator |
| `--calibration-artifact <path>` | — | the artifact JSON (required for `calm-block`; optional operator-vouched prior for `adaptive`) |
| `--save-calibration <path>` | — | persist this run's resulting calibration artifact |
| `--fire-mode cusum\|regime` | `cusum` | which detector drives the fire-stream: the adaptive-CUSUM detector or the regime backbone on daily-smoothed density (continuous-tape fire) |
| `--universe <path>` | `<data-dir>/<event>_universe.csv` | explicit symbol-list CSV |
| `--from` / `--to <YYYY-MM-DD>` | — | optional UTC date filter on bars |
| `-v, --verbose` | off | DEBUG logging |

Default calibration is a leading warm-up of the replayed series (a pragmatic
choice for a first visual impression), not the rigorous walk-forward calm block
the regression tests use — pick `--calibration calm-block|adaptive` for the
honest modes. Exit codes: `0` ran, `2` usage/bad-argument, `1` input-IO.
`--help` documents every flag.

## The math — from bars to a fire

1. **Log returns, one timescale per stream.** Prices become per-bar log
   returns (`log(close / prevClose)`); no return ever crosses a session
   boundary (UTC-day reset for crypto). Macro (daily) and micro (intraday) run
   as separate engines end to end — the only sanctioned cross-timescale
   combination is a matrix blend, never interleaved bars.
2. **Rolling correlation matrix.** corrcalc-lib's streaming engine maintains
   the Pearson matrix over a sliding window `W` (rank-one slide per bar,
   allocation-free) and emits snapshots on a cadence.
3. **Two structure summaries per snapshot.**
   *Edge density* — the fraction of pairs with `|r|` above a threshold τ: a
   direct reading of how much of the market is glued together.
   *Weighted-Δr* — graphs-algos-lib's change metric between consecutive
   matrices, weighting each pair's correlation change so large coordinated
   shifts dominate idiosyncratic noise.
4. **Detection, two fire modes.**
   `cusum` — an `AND(level-gate, two-sided CUSUM)` detector on the change
   metric: the CUSUM arms (`S+`, `S−` with slack `k`, threshold `h`)
   accumulate standardized drift, the level gate suppresses fires while
   absolute density is unremarkable.
   `regime` — a state machine on daily-smoothed density with hysteresis
   (enter above a high mark, exit below a low mark, confirmed for a minimum
   number of days): the continuous-tape mode that fires on regime entry/exit
   rather than transient spikes.
5. **Calibration.** Detector baselines `(μ, σ, level)` come from a calm
   window: a leading warm-up (CLI default), a persisted walk-forward
   **calibration artifact** (JSON, reusable across runs), or an online
   adaptive estimator that re-baselines as the tape evolves. All thresholds
   are configuration, not code — a new market is wired, not coded.

## Validation — a pinned historical-event regression

The detector suite is validated against **eight named crypto market events**
(2020–2023: the COVID crash, the May-2021 and China-ban selloffs, the
November-2021 top, LUNA, FTX, SVB and the yen-carry shock) over a 17-symbol
universe of 1-minute bars, as a bit-for-bit pinned regression:

- **CI-deterministic (committed fixtures).** The multi-GB raw bar dataset is
  *not* committed. Instead a compact fixture per event/timescale — the
  calibration `(μ, σ, level)` plus the event-window
  `(timestamp, density, weighted_change)` series — lives under
  `src/test/resources/` and fully determines every detection lead-time column
  (the CUSUM resets at the event-window start). CI reproduces the whole lead
  table on every build, no raw data needed.
- **Opt-in full reproduction (local).** Point the driver at a local copy of
  the raw bars to run the *whole* pipeline (panel building → correlation →
  change detection → full lead + false-alarm tables):

  ```bash
  ./mvnw test -Dtest=CryptoFullBacktestDriverTest -Dcrypto.data.dir=/path/to/crypto-data
  ```

  This test is skipped when `crypto.data.dir` is unset (the CI default).

Headline result the regression pins: on intraday bars the detector catches the
COVID, May-2021, LUNA and FTX events with a lead of hours-to-days over the
daily timescale (which sees only May-2021, ~47 h later), and the change-metric
detector additionally recovers saturated-regime events (China ban, Nov-2021,
SVB, yen) that an absolute-density gate structurally cannot see because calm
density is already ≈ 1.0.

## Performance

The replay path streams bars from disk instead of materializing the tape, so
**heap stays constant regardless of input size** — multi-year, multi-GB
1-minute datasets replay in a steady few hundred MB. The per-bar cost is
dominated by the upstream rolling-correlation slide, which is allocation-free
`O(N²)` per bar (measured at ~2.9M bars/s for N=16, ~140K bars/s for N=100 on
commodity hardware — see the
[corrcalc-lib streaming benchmarks](https://github.com/tarvyn-analytics/corrcalc-lib#streaming-engine)).
The graph-side change metric adds one `O(N²)` pass per emitted snapshot.

## Two output seams — fires vs. observations

The pipeline emits on **two distinct seams**, and *what crosses each is the
consumer's choice*:

- **The product fire-stream** (`StructuralSignal` → `SignalFilter` →
  `SignalSink`) carries only genuine, filter-passed fires — the censored
  signal. No fire is ever silently dropped.
- **The observation seam** (`PipelineObservation` → `ObservationPolicy` →
  `PipelineObserver`) carries *every* scored transition — fired or not. The
  engine never decides verbosity; the consumer installs an
  `ObservationPolicy` (`all` / `firesOnly` / `minWeightedChange(τ)` /
  `minActivation(frac)`, freely composed) that decides which observations
  reach their observer. `--observe` selects this policy and
  `--heartbeat-every` thins it.

So "push every tick, only fires, or just the big moves" is a one-line policy
on whoever composes the pipeline — not a property baked into the engine.

## Consuming a live market-data stream

The pipeline pulls its input through one inbound SPI, `source.MarketDataSource`:
a provider plugs in by implementing a single blocking `poll()` that yields
**aligned cross-sections** (`MarketSnapshot` = one timestamp + a close per
universe symbol). `PipelineDriver` does the rest — turning prices into log
returns (`ReturnBuilder`), feeding the engine, and pacing:

```java
MarketDataSource feed = new MyBrokerWebSocketSource(universe);     // your connector: poll() blocks for the next bar
ReturnBuilder    builder = new ReturnBuilder(universe, SessionPolicy.INTRADAY_UTC_DAY);
PipelineEngine   engine  = PipelineEngine.builder(universe, TimescaleConfig.cryptoIntraday())
        .calmBars(480).market("crypto").timescale("intraday")
        .sink(new LoggingSink())                                   // product fires
        .observer(new LoggingObserver())                          // full series
        .observationPolicy(ObservationPolicy.minActivation(0.5))  // …consumer's choice
        .build();

PipelineDriver.run(feed, builder, engine, Pace.none());           // live: no artificial pacing
```

A replay is the same composition with an `IterableMarketDataSource` over stored
bars and a `ReplayClock` as the `Pace` (exactly what the CLI wires). **Bar
alignment** — waiting for the slowest symbol of a bar, gap handling — is the
connector's job behind `poll()`; the seam, the in-memory source, the
`ReturnBuilder` and the driver all ship here.

## Layout

```
ch.tarvynanalytics.corrcalc.graphs.pipeline
├── StructuralSignal / SignalKind        # the published fire event (the product)
├── SignalFilter / SignalSink            # the fire-stream SPIs (filter + multi-sink delivery seam)
├── PipelineObservation / PipelineObserver / ObservationPolicy  # the observation seam (every transition)
├── source/                              # MarketDataSource SPI + MarketSnapshot + IterableMarketDataSource (inbound seam)
├── engine/                              # PipelineEngine (hub) + PipelineDriver + Pace
├── data/                                # bars → aligned snapshots → log returns (ReturnBuilder)
├── detect/                              # the detectors: density-level baseline, CUSUM wiring, regime backbone
├── backtest/                            # per-event scoring + the event lead-table regression driver
├── replay/                              # the wall-clock-paced replay driver over the source seam (the CLI's core)
└── cli/                                 # PipelineCli — the `java -jar` entry point (`replay` verb)
```

The Java package is `ch.tarvynanalytics.corrcalc.graphs.pipeline` (the graph
analysis extends corrcalc's correlation output); the Maven coordinates are
`ch.tarvynanalytics.corrcalc.graphs:corrcalc-graphs-pipeline`.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
