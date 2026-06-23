# CLAUDE.md

Guidance for Claude Code when working in this repository.

Read `README.md` first — it owns the overview, the architecture diagram, the n=8 regression
design and the build/run commands. This file tells you how to work on the code.

This is **Initiative-S S4** (Jira `CGP`), a cross-cluster project under `corrcalc-graphs/`. The
authoritative design is `corrcalc-graphs/corrcalc-graphs-meta/design/initiative-s-build-design.md`
§5; the family router is `corrcalc-graphs/CLAUDE.md`. It is the *consumer* of two libraries — it
adds normal dependencies and orchestration that the libraries are forbidden from carrying.

## Commands

```bash
./mvnw clean verify           # tests + JaCoCo 80/70 — run before claiming done
./mvnw test                   # tests only
./mvnw test -Dtest=ClassName  # single test class
# install the private upstream libs to ~/.m2 once (no token needed):
( cd ../../corrcalc/corrcalc-lib && ./mvnw -DskipTests install )
( cd ../../graphs/graphs-algos-lib && ./mvnw -DskipTests install )
```

**Requires JDK 25** (bytecode target `--release 25` — the family build JDK; corrcalc-lib already
requires JDK 25 to build its incubator Vector kernels). The upstream libs publish `--release 21`
bytecode, which a release-25 consumer resolves without issue.

## Invariants — keep these

1. **The core is asset-agnostic; this repo holds the edges.** S1 (corrcalc-lib) and S3
   (graphs-algos-lib) never import a connector, calendar, sink or filter. This pipeline is the
   only place that wires them to market-specific edges. Do not push pipeline concerns
   (CSV formats, calendars, sinks) into the libraries.
2. **Never mix timescales in one return vector.** Macro (daily) and micro (intraday) run as
   *separate* S1 engine instances and *separate* S3 detectors, each fed a single-frequency return
   vector. No return crosses a session boundary (UTC-day reset for crypto). The only sanctioned
   cross-timescale combination is the matrix blend `M_final = w·M_long + (1−w)·M_today`, never
   concatenation. (build-design §1.)
3. **Correlate returns, never raw prices.** Log returns (`Math.log(close/prevClose)`) are the
   input to S1 everywhere — the same definition the Python spike's `replay_signal.log_return` uses.
4. **Thresholds are config, not code.** Every per-asset constant (window `W`, CUSUM `k`/`h`, level
   percentile, edge threshold `τ`, calm-block geometry) is configuration. A new market is *wired*,
   not coded. The crypto constants live in `DetectorConfig.crypto()` (graphs-algos-lib) and the
   pipeline's own config records — never as literals in a detector body.
5. **Two detectors, two purposes — don't conflate them.** The **density-level baseline** (a port
   of the spike's `replay_alert.alerts` over the *density* series) exists only to reproduce
   `crypto_lead_table.csv`. The **S3 `ChangeDetector`** (CUSUM over the *weighted-change* series)
   is the product's real detector and recovers the saturated-regime misses. They are different
   detectors; the regression pins the first, the product ships the second.
6. **The reference oracle is the Python spike.** `corrcalc-graphs-research-scratches/spike/`
   (`replay_crypto_main.py`, `replay_alert.py`, `replay_crypto_panel.py`, `crypto_universe.py`,
   `replay_crypto_vec.py`, `replay_signal.py`) is the cross-language oracle the Java must match
   bar-for-bar. Reproduce it; do not "improve" the numerics — divergence is a bug until proven
   otherwise. S1's matrix path is already pinned to it in corrcalc-lib (Oracle B); S3's metric in
   graphs-algos-lib. This repo pins the *wiring + alert layer + orchestration*.
7. **SPI implementations are package-private where they can be.** Public surface is the SPIs
   (`SignalFilter`, `SignalSink`), the event records (`StructuralSignal`, `SignalKind`), the
   public entry points (the orchestrator + driver), and the config records. Validation throws
   with the offending value(s) in brackets.
8. **Coverage gates 80% line / 70% branch** are enforced by `verify`. New code arrives with tests
   in the same commit.

## Testing conventions

Same as the rest of the family: naming `method_Scenario_Expectation`; test packages mirror main
1:1; assertion arguments are `(expected, actual)`; numerical correctness is proven against an
independent naive implementation on seeded data or against the committed spike fixture — never
against the code's own output. The big regression fixtures (committed derived series) are exported
by `corrcalc-graphs-research-scratches/spike/export_pipeline_fixture.py`; the opt-in full-data
driver (`-Dcrypto.data.dir=...`) reproduces the entire table from local raw bars and is skipped in
CI.

## Delivery: Jira, Git, PRs, CI

- **Jira** (project `CGP`, *corrcalc-graphs Pipeline*): every deliverable hangs off an issue; epic
  for S4, task per shippable unit. Transition `In Progress` when starting, `Done` with a
  PR/commit reference when finished. (Scratch/exploration stays in the meta ROADMAP, not Jira.)
- **GitFlow**: `main` ← `develop` ← `feature/*`. Branch naming
  `feature/CGP-<n>-eb-<short-description>`. PRs target `develop`, squash-merged; release merges go
  `develop` → `main` as a true merge commit.
- **PR-only delivery.** Never push straight to `develop`/`main`. Branch → commit → push branch →
  open PR → wait for `validate-on-pull-request` CI green → merge (admin bypass once green, since
  the author can't self-approve).
- **Commit style**: conventional commits with scope and issue key, e.g.
  `feat(pipeline): [CGP-n]: wire S1 stream into the change detector`. GPG signing fails under WSL
  — `git config --local commit.gpgsign false` and use `git commit --no-gpg-sign`.
- **GitHub** (`tarvyn-analytics/corrcalc-graphs-pipeline`, private): use the `gh` CLI.
- **CI** (`.github/workflows/`): `validate-on-pull-request.yml` runs
  `./mvnw -Ppublish clean verify sonar:sonar` and enforces the SonarCloud gate;
  `build-on-push.yml` publishes the SNAPSHOT to GitHub Packages on push. **CI prerequisites
  (owner-provisioned):** a `PACKAGES_TOKEN` secret (a PAT with `read:packages` on both
  `corrcalc-lib` and `graphs-algos-lib`, plus `write:packages` here), a SonarCloud project
  `tarvyn-analytics_corrcalc-graphs-pipeline`, and a `SONAR_TOKEN` secret.
```
