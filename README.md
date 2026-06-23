# corrcalc-graphs-pipeline

The **Initiative-S S4** streaming structural-change pipeline service — the integration
that wires the two Initiative-S primitives into one product:

```
 MarketDataSource → ReturnBuilder → S1 online correlation → S3 temporal change → SignalFilter → SignalSink
   (saved CSVs)      (log returns,    (corrcalc-lib:          (graphs-algos-lib:    (#1a no-op       (multi-sink
                      UTC-day reset)   RollingCorrelations)    ChangeDetector)       default)         fan-out)
```

Its product is a **published structural-change signal** (`StructuralSignal`), not a UI — a
B2B *signal-as-a-product*. A dashboard (S5) becomes one consumer of this stream.

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
  ./mvnw test -Dtest=CryptoFullReplayDriverTest -Dcrypto.data.dir=/path/to/spike/crypto-data
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

## Layout

```
ch.tarvynanalytics.pipeline
├── StructuralSignal / SignalKind        # the published event (the product)
├── SignalFilter / SignalSink            # the output SPIs (filter + multi-sink delivery seam)
├── data/                                # CSV bars → aligned log-return panels (UTC-day sessions)
├── detect/                              # the density-level baseline alert + S3 wiring
└── replay/                              # per-event orchestration + the n=8 regression driver
```
