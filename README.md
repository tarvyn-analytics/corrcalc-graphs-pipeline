# corrcalc-graphs-pipeline

The **Initiative-S S4** streaming structural-change pipeline service — the integration
that wires the two Initiative-S primitives into one product:

```
                         ┌──────────────────── PipelineEngine (the hub) ───────────────────────┐
 MarketDataSource ─poll→ ReturnBuilder ─returns→  S1 online corr ─matrices→ S3 temporal change ─┤
   (a source:            (log returns,            (corrcalc-lib:            (graphs-algos-lib:   │
    stored bars / live)   drop session-first)      RollingCorrelations)      ChangeDetector)     │
                                                                                                 │
   PipelineDriver pulls the source, builds returns, paces, and feeds the engine ───────────────┘
                                                          │
                            fire? ┌─────────────────────────────────────────────┐ every transition
                       StructuralSignal → SignalFilter → SignalSink   │   PipelineObservation → ObservationPolicy → PipelineObserver
                          (the censored product fire-stream)          │        (the full series; the consumer's policy decides)
```

Its product is a **published structural-change signal** (`StructuralSignal`), not a UI — a
B2B *signal-as-a-product*. A dashboard (S5) becomes one consumer of this stream. Every box above is
real: `engine.PipelineEngine` is the source-agnostic orchestrator, `PipelineDriver` connects a
`MarketDataSource` (inbound seam) to it, and the two output seams (fires vs. observations) are
described under *Two output seams* below.

It depends on two private libraries from this org:

- **S1** `ch.tarvynanalytics.corrcalc:corrcalc-lib-core` — the online rolling-Pearson engine
  (`stream` package): feed returns, get N×N correlation-matrix snapshots on a cadence.
- **S3** `ch.tarvynanalytics.graphs:graphs-algos-lib` — the temporal change detector
  (`ChangeDetector`): feed consecutive matrices, get the weighted-Δr change metric + two-sided
  CUSUM change-point signal.

The asset-agnostic core (S1 + S3) never imports a connector, calendar, sink or filter. Only
this pipeline knows about all of them at once.

## What it does — the n=8 crypto regression

The first milestone reproduces the spike's **n=8 crypto detection** end-to-end, as a pinned
regression. Two detectors run over the same S1 matrix stream:

1. **Density-level baseline** — `AND(level-gate, CUSUM-on-density)`, a faithful port of the
   spike's `replay_alert`. It reproduces `crypto_lead_table.csv` bit-for-bit (intraday detects
   covid / may2021 / luna / ftx; daily detects may2021 only, ~47 h later).
2. **S3 change detector** — `AND(level-gate, CUSUM-on-weighted-change)`, the *better* primitive.
   It recovers the four saturated-regime both-misses (china / nov2021 / svb / yen) that the
   absolute-level gate structurally cannot see (calm density already ≈ 1.0).

### Two regression layers

- **CI-deterministic (committed fixtures).** The 2.4 GB of raw 1-minute crypto bars are *not*
  committed. Instead a compact fixture per event/timescale — the calibration `(μ, σ, level)`
  plus the **event-window** `(timestamp, density, weighted_change)` series — is committed under
  `src/test/resources/`. Because the CUSUM resets to zero at the event-window start, those two
  pieces fully determine every `crypto_lead_table.csv` column, so CI reproduces the table without
  the raw data. The fixtures are exported by
  `corrcalc-graphs-research-scratches/spike/export_pipeline_fixture.py`.
- **Opt-in full reproduction (local).** Point the driver at a local copy of the raw bars to run
  the *whole* pipeline (panel building → S1 → S3 → full lead table + false-alarm table):

  ```bash
  ./mvnw test -Dtest=CryptoFullBacktestDriverTest -Dcrypto.data.dir=/path/to/spike/crypto-data
  ```

  This test is skipped when `crypto.data.dir` is unset (the CI default).

## Build

```bash
./mvnw clean verify    # tests + JaCoCo 80/70 gate
```

**Requires JDK 25.** The two upstream libraries are **private GitHub-Packages** artifacts. For
local work, install each to `~/.m2` once from a sibling checkout (no token needed):

```bash
( cd ../../corrcalc/corrcalc-lib && ./mvnw -DskipTests install )
( cd ../../graphs/graphs-algos-lib && ./mvnw -DskipTests install )
```

CI resolves them from GitHub Packages via a `PACKAGES_TOKEN` secret (read:packages on both
libraries; write:packages on this repo for the publish job) — see `.github/workflows/`.

## Run — live replay CLI

The pipeline ships a CLI that **replays stored bars through the real S1→S3 pipeline at a configurable
pace** and logs the signal as it evolves — the quickest way to *see* what the engine produces. Build
the runnable jar with the `cli` profile (a shaded uber-jar; the normal/published thin jar is
unaffected):

```bash
./mvnw -Pcli -DskipTests package
java -jar target/corrcalc-graphs-pipeline-*-cli.jar replay <data-dir> --event <name> --market crypto \
  --timescale intraday --speed 500
```

`<data-dir>` holds the spike's per-symbol `<SYMBOL>_<freq>_<event>.csv` OHLCV bars; the universe
defaults to `<data-dir>/<event>_universe.csv`. Each window-end transition logs a **calm heartbeat**
(density, weighted-change, both CUSUM arms); every genuine **FUSION/DEFUSION** fire is published to
the `LoggingSink` and printed as a highlighted `=== FUSION ===` banner (WARN, colorized) so it stands
out from the calm stream:

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
| `--observe <spec>` | `all` | which transitions reach the heartbeat: `all` \| `fires` \| `change>=<x>` \| `activation>=<x>` |
| `--universe <path>` | `<data-dir>/<event>_universe.csv` | explicit symbol-list CSV |
| `--from` / `--to <YYYY-MM-DD>` | — | optional UTC date filter on bars |
| `-v, --verbose` | off | DEBUG logging |

Calibration here is a leading warm-up of the replayed series (a pragmatic choice for a first visual
impression), not the rigorous walk-forward calm block the `backtest` regression uses. Exit codes:
`0` ran, `2` usage/bad-argument, `1` input-IO.

### Two output seams — fires vs. observations

The pipeline emits on **two distinct seams**, and *what crosses each is the consumer's choice*:

- **The product fire-stream** (`StructuralSignal` → `SignalFilter` → `SignalSink`) carries only
  genuine, filter-passed fires — the censored B2B signal. The CLI prints these as the loud WARN
  `=== FUSION ===` banner via `LoggingSink`. Unchanged contract: no fire is ever silently dropped.
- **The observation seam** (`PipelineObservation` → `ObservationPolicy` → `PipelineObserver`) carries
  *every* scored transition — fired or not. The engine never decides verbosity; the consumer installs
  an `ObservationPolicy` (`all` / `firesOnly` / `minWeightedChange(τ)` / `minActivation(frac)`, freely
  composed) that decides which observations reach their observer. `--observe` selects this policy and
  `--heartbeat-every` thins it; the CLI's `LoggingObserver` prints the quiet INFO heartbeat.

So "push every tick, only fires, or just the big moves" is a one-line policy on whoever composes the
pipeline — not a property baked into the engine.

### Consuming a live market-data stream

The pipeline pulls its input through one inbound SPI, `source.MarketDataSource`, so a provider plugs in
by implementing a single blocking `poll()` that yields **aligned cross-sections** (`MarketSnapshot` =
one timestamp + a close per universe symbol). `PipelineDriver` does the rest — turning prices into log
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

A replay is the same composition with an `IterableMarketDataSource` over stored bars and a `ReplayClock`
as the `Pace` (exactly what `PacedReplay` wires for the CLI). **Bar alignment** — waiting for the slowest
symbol of a bar, gap handling — is the connector's job behind `poll()`; real websocket/REST connectors
are the S2 deliverable. The seam, the in-memory source, the `ReturnBuilder` and the driver ship here.

## Layout

```
ch.tarvynanalytics.corrcalc.graphs.pipeline
├── StructuralSignal / SignalKind        # the published fire event (the product)
├── SignalFilter / SignalSink            # the fire-stream SPIs (filter + multi-sink delivery seam)
├── LoggingSink / FanOutSink / CollectingSink  # concrete sinks (LoggingSink highlights fires)
├── PipelineObservation / PipelineObserver / ObservationPolicy  # the observation seam (every transition)
├── LoggingObserver / ThinningObserver   # the heartbeat observer + a thinning decorator
├── source/                              # MarketDataSource SPI + MarketSnapshot + IterableMarketDataSource (inbound seam)
├── engine/                              # PipelineEngine (hub) + PipelineDriver + Pace
├── data/                                # bars → aligned snapshots (PriceSnapshots) → log returns (ReturnBuilder / ReturnPanels)
├── detect/                              # the density-level baseline alert + S3 wiring
├── backtest/                            # per-event scoring + the n=8 lead-table regression driver
├── replay/                              # the wall-clock-paced replay driver over the source seam (the CLI's core)
└── cli/                                 # PipelineCli — the `java -jar` entry point (`replay` verb)
```

The Java package is `ch.tarvynanalytics.corrcalc.graphs.pipeline` (the graph analysis extends
corrcalc's correlation output); the Maven coordinates are
`ch.tarvynanalytics.corrcalc.graphs:corrcalc-graphs-pipeline`.
