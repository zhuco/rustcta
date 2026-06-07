# Backtest App Migration Plan

Status date: 2026-06-07

This is a directory and runtime-boundary plan for moving backtest and research
runners toward `apps/backtest`. It does not add exchange API behavior.

Implementation status: the app skeleton now exists as `apps/backtest` with the
operator binary `rustcta-backtest`. `crates/rustcta-backtest` now owns the
app-facing dependency boundary, contains the first extracted implementation
modules (`factors`, `scoring`, `schema`, `data`, `indicators`, `replay`,
`offline_runtime`, `matching::book`, `matching::ledger`, and
`matching::engine`), and keeps the legacy `rustcta::backtest::runtime` API
behind the temporary `legacy-runtime` feature while most reusable backtest
implementation still lives under `src/backtest/*`. The extracted `data` slice
includes `data::exchange_metadata`. The new crate also owns root-free symbol
normalization helpers and small runtime-support helpers for venue constraints,
rounding, latency duration, market type parsing, side inversion, and quote
asset extraction.
The schema extraction uses legacy-compatible `BacktestKline`, `BacktestTrade`,
`BacktestFee`, `BacktestOrderSide`, and `BacktestMarketType` DTOs in
`rustcta-types` so historical replay JSON keeps the same field names and
PascalCase enum values.

Current migration decision: backtest is no longer an active expansion target in
the fast industrial migration batch. Keep the extracted slices above compiling
and keep the compatibility binaries available, but do not make further
`src/backtest/*` extraction a blocker for control/API/tools/supervisor
migration. Operationally, teams can skip running `rustcta-backtest`, `backtest`,
and `short_ladder_mtf_grid` while the production workspace migration is being
closed.

Do not treat `crates/rustcta-backtest` extraction as a trivial move. The
current `src/backtest/*` tree is about 15k lines and still references legacy
root `core` types, mean-reversion config/model code, short-ladder strategy
code, websocket connection helpers, and the old Binance dataset acquisition
path. Full extraction should happen only after those dependencies are split or
wrapped behind offline-data/research contracts, so it does not collide with the
parallel exchange gateway migration.

## Current Boundary

`src/bin/backtest.rs` is already a thin process entry:

- it parses `rustcta::backtest::runtime::BacktestCli`;
- it calls `rustcta::backtest::runtime::execute_cli`;
- it delegates stable one-line operator output to
  `rustcta::backtest::runtime::render_command_output`.

`apps/backtest/src/main.rs` now mirrors that same composition path and exposes
the new workspace binary `rustcta-backtest`; the root `backtest` binary remains
available for compatibility.

Most backtest behavior already lives under `src/backtest/*`, with the largest
runtime surface currently concentrated in `src/backtest/runtime/mod.rs`.
Integration tests import this path directly, especially
`rustcta::backtest::runtime::{BacktestCli, BacktestCommand, execute_cli,
BacktestRuntime, BacktestRuntimeConfig}`.

`src/bin/short_ladder_mtf_grid.rs` is different: it is a large
auto-discovered legacy binary. It owns CLI parsing, grid spec parsing,
configuration mutation, dataset filtering, nested run loops, report file
creation, CSV rendering, Markdown rendering, filename sanitation, and final
stdout. Its Clap command name is `short-ladder-mtf-grid`, while the current
Cargo auto-discovered binary name is `short_ladder_mtf_grid`.

Current visible references:

| Source | Current reference | Migration impact |
| --- | --- | --- |
| `Cargo.toml` | explicit `[[bin]] name = "backtest"` -> `src/bin/backtest.rs` | Keep until compatibility wrappers are intentionally retired. |
| `README.md` | lists `backtest` under operational CLIs | Update only after `rustcta-backtest` exists; keep legacy example during transition. |
| `docs/industrial_directory_migration_plan.md` | assigns `backtest` and `short_ladder_mtf_grid` to future `apps/backtest` | This plan expands that row into an implementation checklist. |
| `tests/backtest_cli.rs` | parses argv starting with `backtest` and matches all `BacktestCommand` variants | Do not rename or hide `BacktestCli`/`BacktestCommand` during the first migration. |
| `tests/backtest_runtime.rs` and related tests | exercise `execute_cli`, `BacktestRuntime`, data/replay/strategy modules | Preserve public module paths or add compatibility re-exports before extraction. |
| `scripts/ordi_grid_backtest.py` | standalone Python grid backtester | Not a Rust backtest command dependency. Do not fold it into `apps/backtest` in this slice. |

Historical docs under `docs/superpowers/*` in `HEAD` include examples such as
`cargo run --bin backtest -- fetch-klines` and references to
`short_ladder_mtf_grid`. If those docs are restored or kept as active runbooks,
they should be updated after the app command exists, while preserving a legacy
compatibility section.

## Target Names

Use one primary app package and one primary app binary:

| Boundary | Name | Notes |
| --- | --- | --- |
| App package | `rustcta-backtest-app` | Lives at `apps/backtest`. Matches the existing `*-app` pattern used by service app packages where a library crate name may be needed later. |
| Primary binary | `rustcta-backtest` | New operator-facing command for backtest and research workflows. |
| Library crate | `rustcta-backtest` | Exists as a narrow facade; later extraction target for reusable backtest behavior currently under `src/backtest/*`. |
| Legacy root binary | `backtest` | Keep `cargo run --bin backtest -- ...` during the transition. |
| Legacy root grid binary | `short_ladder_mtf_grid` | Keep `cargo run --bin short_ladder_mtf_grid -- ...` until the grid command is available under `rustcta-backtest` and docs are switched. |

Do not add a second workspace binary named `backtest` inside `apps/backtest`
while the root package still provides `backtest`; duplicate bin names make
workspace-level `cargo run --bin backtest` ambiguous. The new app should use
only `rustcta-backtest` until the legacy root bin is removed or deliberately
converted.

## Command Compatibility

The new command surface should keep the existing subcommand names for the main
backtest CLI:

| Legacy command | New app command | Compatibility rule |
| --- | --- | --- |
| `cargo run --bin backtest -- fetch-klines ...` | `cargo run -p rustcta-backtest-app --bin rustcta-backtest -- fetch-klines ...` | Same flags, same manifest side effects, same summary stdout. |
| `cargo run --bin backtest -- capture-depth ...` | `cargo run -p rustcta-backtest-app --bin rustcta-backtest -- capture-depth ...` | Same flags and output; no exchange API changes in this migration. |
| `cargo run --bin backtest -- capture-trades ...` | `cargo run -p rustcta-backtest-app --bin rustcta-backtest -- capture-trades ...` | Same flags and output; no exchange API changes in this migration. |
| `cargo run --bin backtest -- run-mean-reversion ...` | same subcommand under `rustcta-backtest` | Preserve JSON summary behavior and stdout wording. |
| `cargo run --bin backtest -- scan-mean-reversion ...` | same subcommand under `rustcta-backtest` | Preserve worker flag semantics and report schema. |
| `cargo run --bin backtest -- walk-forward-mean-reversion ...` | same subcommand under `rustcta-backtest` | Preserve report schema and ranking semantics. |
| `cargo run --bin backtest -- analyze-mean-reversion ...` | same subcommand under `rustcta-backtest` | Preserve `--top-runs` default and report schema. |
| `cargo run --bin backtest -- scan-trend ...` | same subcommand under `rustcta-backtest` | Preserve scan config schema and report schema. |
| `cargo run --bin backtest -- scan-trend-factor ...` | same subcommand under `rustcta-backtest` | Preserve scan config schema and scoring output. |
| `cargo run --bin backtest -- scan-mtf-trend-factor ...` | same subcommand under `rustcta-backtest` | Preserve MTF config schema and report schema. |
| `cargo run --bin backtest -- run-short-ladder ...` | same subcommand under `rustcta-backtest` | Preserve mode parsing and report output. |
| `cargo run --bin backtest -- run-short-ladder-mtf-execution ...` | same subcommand under `rustcta-backtest` | Preserve execution interval defaults and report output. |
| `cargo run --bin short_ladder_mtf_grid -- ...` | `cargo run -p rustcta-backtest-app --bin rustcta-backtest -- short-ladder-mtf-grid ...` | Same flags, same CSV columns, same Markdown report structure, same final stdout. |

The app can expose `short-ladder-mtf-grid` as a subcommand because that matches
the current Clap command display name. The legacy Cargo bin name with
underscores remains available through the root package until compatibility is
retired.

## Module Ownership

Keep `apps/backtest/src/main.rs` thin. It should own only process composition:

- parse command line arguments;
- initialize environment/tracing if needed;
- call a library executor;
- render the returned output or delegate rendering to a library helper;
- return process errors.

Move or keep reusable behavior in crate/module code, not in app `main.rs`:

| Logic | Target module/crate ownership | App main ownership |
| --- | --- | --- |
| `BacktestCli`, subcommand args, and compatibility aliases | Library module such as `backtest::cli` or current `backtest::runtime` until split | Call `parse()`. |
| `BacktestCommandOutput` and stdout summary wording | Library helper such as `backtest::output::render_command_output` | Print returned lines. |
| `execute_cli` dispatch | Library runtime module | Await/call executor. |
| `BacktestRuntimeConfig`, symbol/time parsing, runtime construction | Library runtime module | None. |
| Kline/depth/trade dataset read/write, raw capture import, exchange metadata, and replay | `rustcta-backtest` contains root-free dataset read/write, Binance futures depth/trade raw capture import, exchange metadata snapshots, and replay; legacy `backtest::data` keeps compatibility includes for extracted modules while live capture/network acquisition stays legacy | None. |
| Indicators, matching book, matching ledger, factors, and scoring | `rustcta-backtest` contains root-free indicators, order book, ledger, factor expansion, and scoring; legacy `backtest::*` paths keep compatibility includes | None. |
| Matching engine | `rustcta-backtest` contains root-free matching engine state, market/limit order processing, queue handling, and ledger integration; legacy `backtest::matching::engine` keeps a compatibility include | None. |
| Symbol and runtime support helpers | `rustcta-backtest` contains root-free symbol normalization, Binance futures symbol conversion, venue constraint filtering, price/quantity rounding, latency duration, market type parsing, side inversion, and quote asset extraction; legacy runtime imports the compatibility modules | None. |
| Strategy scan/run logic | Existing `backtest::strategy`; move after strategy/runtime dependencies are split | None. |
| Network dataset acquisition used by `fetch-klines`, `capture-depth`, `capture-trades` | Library dataset acquisition functions; do not change exchange API contracts in this directory migration | None beyond invoking command. |
| `short_ladder_mtf_grid` args and spec structs | New library module such as `backtest::research::short_ladder_mtf_grid` | Parse through shared args or delegate to a subcommand. |
| Grid spec parsing and validation | Same grid library module, with focused unit tests | None. |
| Grid run loops and per-run report path generation | Same grid library module | None. |
| Grid CSV/Markdown rendering | Same grid library module or `backtest::research::reports` | None. |

The first implementation does not need to perfectly split the large
`src/backtest/runtime/mod.rs`. A low-risk path is:

1. add `apps/backtest` with `rustcta-backtest` delegating to the existing
   `rustcta::backtest::runtime` API; done
2. extract stdout rendering from `src/bin/backtest.rs` into a shared module so
   both root and app binaries print identical messages; done
3. extract `short_ladder_mtf_grid` into a shared module and make both the old
   root bin and the new app subcommand call it; done
4. introduce `crates/rustcta-backtest` as a narrow facade so the app no longer
   depends directly on the legacy root crate; done
5. incrementally move `src/backtest/*` implementation modules into
   `crates/rustcta-backtest`, keeping root compatibility re-exports.
   First slices done: `factors`, `scoring`, `schema`, `data`,
   `indicators`, `replay`, `symbol`, `runtime_support`,
   `offline_runtime`, `matching::book`, `matching::ledger`, and
   `matching::engine` now live in
   `crates/rustcta-backtest`, and
   `src/backtest/factors/mod.rs`, `src/backtest/scoring/mod.rs`,
   `src/backtest/schema/mod.rs`, `src/backtest/indicators/mod.rs`,
   `src/backtest/replay/mod.rs`, `src/backtest/matching/book.rs`,
   `src/backtest/matching/ledger.rs`, and `src/backtest/matching/engine.rs`
   are compatibility includes for the old `rustcta::backtest::*` paths. The
   offline runtime extraction covers local replay loading and kline partition
   planning without importing root `core` types; the legacy runtime facade
   delegates those read-only paths through the compatibility include. The
   data/replay extraction includes root-free Binance futures dataset readers
   and writers, depth/trade raw capture import, plus exchange metadata
   snapshots in `rustcta-backtest`; legacy data modules keep compatibility
   includes for extracted modules, and live capture/network acquisition stays
   legacy until that boundary is split. The matching ledger extraction keeps
   cash, fee, funding, position,
   and PnL accounting root-free. The matching engine extraction keeps offline
   market/limit order execution root-free while strategy runtimes remain
   legacy.

The temporary root-package dependency is intentionally allowlisted only for
`crates/rustcta-backtest/Cargo.toml` in
`scripts/check_industrial_boundaries.sh`. Adding that dependency to any app or
other industrial crate remains a boundary violation.

The root `rustcta` crate should keep compatibility paths such as
`rustcta::backtest::runtime::BacktestCli` until tests and downstream imports
are intentionally migrated.

## Test and `cargo run` Compatibility

Preserve these contracts during the first app migration:

- `cargo run --bin backtest -- --help` continues to work.
- `cargo run --bin backtest -- fetch-klines ...` keeps the same flags and
  stdout wording.
- `cargo run --bin short_ladder_mtf_grid -- --help` continues to work.
- `tests/backtest_cli.rs` continues to parse argv beginning with `backtest`.
- Public test imports from `rustcta::backtest::*` continue to compile.
- Report schemas and filenames are unchanged unless a later schema migration is
  explicitly planned.

Avoid changing `#[command(name = "backtest")]` on the existing `BacktestCli`
until help-output compatibility is separately tested. The new app binary can
still parse the same subcommands; if distinct help branding is required, add it
through an app-level wrapper rather than by breaking the root CLI contract.

## Parallel Implementation Slices

These slices can be implemented independently after this planning step. Each
slice should re-check the worktree and avoid unrelated source changes.

| Slice | Scope | Acceptance commands |
| --- | --- | --- |
| 1. Reference inventory | Re-run command references in active docs, scripts, tests, and `Cargo.toml`; decide which docs become legacy examples. | `rg -n "cargo run --bin backtest|short_ladder_mtf_grid|fetch-klines|run-short-ladder" docs scripts README.md Cargo.toml tests` |
| 2. App skeleton | Done: add `apps/backtest` package with bin `rustcta-backtest` delegating to existing backtest runtime. Keep root bins untouched. | `cargo check -p rustcta-backtest-app --bin rustcta-backtest`; `cargo check --bin backtest`; `cargo run -p rustcta-backtest-app --bin rustcta-backtest -- --help` |
| 3. Shared output renderer | Done: move the `BacktestCommandOutput` stdout match from `src/bin/backtest.rs` into a shared function, then use it from both app and root bin. | `cargo test --test backtest_cli`; `cargo check --bin backtest`; `cargo check -p rustcta-backtest-app --bin rustcta-backtest` |
| 4. Grid module extraction | Done: move `short_ladder_mtf_grid` parsing, run loop, CSV, and Markdown logic into `rustcta::backtest::runtime::short_ladder_mtf_grid` without changing old flags or output. | `cargo check --bin short_ladder_mtf_grid`; focused unit tests for grid spec parsing if added |
| 5. Grid app subcommand | Done: add `rustcta-backtest short-ladder-mtf-grid ...` and delegate to the same grid module. | `cargo run -p rustcta-backtest-app --bin rustcta-backtest -- short-ladder-mtf-grid --help`; `cargo check --bin short_ladder_mtf_grid` |
| 6. Docs and runbooks | Update active examples to prefer `rustcta-backtest`; keep a compatibility section for old Cargo bin names. | `rg -n "cargo run --bin backtest|short_ladder_mtf_grid" docs README.md scripts` |
| 7. Compatibility retirement decision | After scripts and runbooks are updated, decide whether root bins remain wrappers, emit deprecation text, or are removed in a later milestone. | `cargo run --bin backtest -- --help`; `cargo run --bin short_ladder_mtf_grid -- --help`; workspace bin-name ambiguity check before removal |

Do not use networked fetch/capture commands as acceptance for directory moves.
Use parse tests, `--help`, `cargo check`, and tiny fixture tests unless a later
task explicitly requests runtime validation.

## Risks and Open Coordination

- The current app migration should not develop or refactor exchange adapters.
  `fetch-klines`, `capture-depth`, and `capture-trades` remain dataset
  acquisition commands that call existing library behavior.
- `src/backtest/runtime/mod.rs` is large. Splitting it is useful, but app
  creation should not wait for a full backtest crate extraction.
- Avoid duplicate workspace binary names. The app bin should be
  `rustcta-backtest`; legacy `backtest` stays in the root package until a
  removal milestone.
- `short_ladder_mtf_grid` should be treated as backtest/research scope for this
  migration. Any later boundary-check inventory should classify it consistently
  with `docs/industrial_directory_migration_plan.md`.
- As `crates/rustcta-backtest` grows beyond the current facade, keep root
  re-exports long enough for existing tests and downstream imports to move
  deliberately.
