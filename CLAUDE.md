# CLAUDE.md

Guidance for Claude Code when working in this repository. Read `README.md` first — it owns
the overview, the architecture diagram, the regression design and the build/run commands.
Family conventions: `../corrcalc-graphs-meta/README.md`. This is **Initiative-S S4** (Jira
`CGP`), the *consumer* of the two libraries — it adds the dependencies and orchestration the
libraries are forbidden from carrying. (The original build design lives in the meta repo's
git history; the shipped behavior is documented here and in the README.)

## Commands

```bash
./mvnw clean verify                       # tests + JaCoCo 80/70 — run before claiming done
./mvnw test -Dtest=ClassName              # single test class
./mvnw -Ppublish -DskipTests javadoc:jar  # CI's javadoc gate — plain verify never runs it
# install the private upstream libs to ~/.m2 once (no token needed):
( cd ../../corrcalc/corrcalc-lib && ./mvnw -DskipTests install )
( cd ../../graphs/graphs-algos-lib && ./mvnw -DskipTests install )
```

**Requires JDK 25** (`--release 25`; the upstream libs' `--release 21` bytecode resolves fine).

## Invariants — keep these

1. **The core is asset-agnostic; this repo holds the edges.** S1 (corrcalc-lib) and S3
   (graphs-algos-lib) never import a connector, calendar, sink or filter — pipeline concerns
   (CSV formats, calendars, sinks) stay here.
2. **Never mix timescales in one return vector.** Macro (daily) and micro (intraday) run as
   separate S1 engines and separate S3 detectors. No return crosses a session boundary
   (UTC-day reset for crypto). The only sanctioned cross-timescale combination is the matrix
   blend `M_final = w·M_long + (1−w)·M_today`, never concatenation.
3. **Correlate log returns (`Math.log(close/prevClose)`), never raw prices** — the same
   definition as the Python spike's `replay_signal.log_return`.
4. **Thresholds are config, not code.** Every per-asset constant (window `W`, CUSUM `k`/`h`,
   level percentile, edge threshold `τ`, calm-block geometry, regime hi/lo/confirm marks) is
   configuration — a new market is *wired*, not coded.
5. **Two fire modes, two purposes — don't conflate them.** The density-level baseline exists
   to reproduce `crypto_lead_table.csv` (the n=8 regression); the regime backbone
   (`RegimeStateDetector` on smoothed density) is the continuous-tape fire; the change-CUSUM
   is demoted to a leading annotation. The regression pins the first; the product ships the
   regime backbone.
6. **The reference oracle is the Python spike** (`corrcalc-graphs-research-scratches/spike/`:
   `replay_crypto_main.py`, `replay_alert.py`, `replay_crypto_panel.py`, `crypto_universe.py`,
   `replay_crypto_vec.py`, `replay_signal.py`). The Java must match bar-for-bar; divergence is
   a bug until proven otherwise. This repo pins the wiring + alert layer + orchestration.
7. **SPI implementations are package-private where they can be.** Public surface = the seams:
   inbound `source.MarketDataSource` (pull-based `poll`, single-writer, one frequency per
   source; bar alignment is the connector's job), the fire-stream (`SignalFilter`,
   `SignalSink` — the censored product) vs the observation seam (`PipelineObserver`,
   `ObservationPolicy` — every transition, the *consumer* decides forwarding), the
   event/observation records, the orchestrator + drivers (`engine.PipelineEngine`,
   `PipelineDriver`/`Pace`, `data.ReturnBuilder` — reproduces `ReturnPanels` bar-for-bar,
   drops the cross-session return but never resets the window), the replay CLI, and the
   config records. Validation throws with offending values in `[brackets]`.
8. **Coverage gates 80% line / 70% branch** enforced by `verify`; tests in the same commit.

## Testing conventions

Family rules: `method_Scenario_Expectation`; test packages mirror main 1:1; assertions
`(expected, actual)`; numerics proven against an independent naive implementation on seeded
data or the committed spike fixture (exported by `spike/export_pipeline_fixture.py`) — never
against the code's own output. The opt-in full-data drivers run with
`-Dcrypto.data.dir=../corrcalc-graphs-research-scratches/spike/crypto-data` and are skipped
in CI.

## Delivery

Jira **CGP**. GitFlow: **PR-only** squash into `develop`, branch `feature/CGP-<n>-eb-<desc>`;
releases `develop`→`main` as a true merge commit. Conventional commits with issue key;
`git config --local commit.gpgsign false` + `--no-gpg-sign` under WSL. CI:
`-Ppublish clean verify sonar:sonar` on PRs + SonarCloud gate; GitHub Packages SNAPSHOT on
push; needs the `PACKAGES_TOKEN` secret (PAT with `read:packages` on both libs +
`write:packages` here) and `SONAR_TOKEN`.
