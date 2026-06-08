# Industrial Directory Migration Plan

Status date: 2026-06-07

This document inventories the current runtime entry boundaries under
`retired root bin directory`, `apps/`, and `crates/`, then assigns each retired binary to a target
application category.

## Scope

The target directory structure is not the first milestone. It is part of the
complete industrial migration target.

The first milestone should freeze ownership, preserve compatibility binary
names, and keep behavior stable while app crates and platform crates become the
new runtime boundaries. Moving every file into its final directory before the
runtime contracts are extracted would mix directory work with behavior changes.

This round is directory and architecture planning only:

- Do not modify Rust source in this round.
- Do not develop exchange API details in this round.
- Do not add or change venue signing, REST, websocket, order placement, symbol
  rule, or adapter behavior.
- Do not remove legacy binaries until compatibility shims and operator command
  names are intentionally planned.

## Current Runtime Boundary Inventory

### `retired root bin directory`

`retired root bin directory` is still owned by the root `rustcta` package. Most files are Cargo
auto-discovered legacy binaries, while `backtest` is also explicitly declared
in the root `Cargo.toml`.

Observed characteristics:

- Most binaries import `legacy root crate path *` directly and compose legacy modules,
  concrete exchange adapters, strategy runtimes, execution paths, or web
  models in the binary itself.
- `retired root bin directory/control_api.rs` is a large standalone legacy HTTP runtime and is not
  equivalent to the current scaffold in `apps/control-api`.
- `retired root bin directory/cross_arb_live.rs` owns the nested module
  `retired root bin directory/cross_arb_server/ws.rs`, so moving it requires handling that local
  module boundary.
- Several binaries are one-off diagnostics, canaries, reporters, or dry-run
  service skeletons. These are good candidates for a future `apps/tools`
  package, but their existing names should remain temporarily available.

The root `src/main.rs` is also a runtime entry, although it is not under
`retired root bin directory`. It remains the legacy `rustcta` strategy launcher and should be
treated as compatibility until supervised strategy app boundaries are ready.

### `apps/`

`apps/` already contains the first target app crate scaffolds:

- `apps/gateway` -> `rustcta-gateway`
- `apps/supervisor` -> `rustcta-supervisor`
- `apps/control-api` -> `rustcta-control-api`
- `apps/cli` -> `rustcta-industrial`
- `apps/backtest` -> `retired-backtest`

These app crates should remain thin process composition layers. Long-term
behavior belongs in `crates/*` or strategy crates, not in large `main.rs`
files.

The current tool group is `tools/ops` for canaries, probes, diagnostics,
reporters, and migration utilities that are not long-running platform
services.

### `crates/`

`crates/` contains library boundaries, not runtime entry points. The current
runtime-relevant roles are:

- `rustcta-types`: stable serializable boundary types.
- `rustcta-exchange-api`: exchange protocol contracts.
- `rustcta-exchange-gateway`: gateway protocol, gateway client/server support,
  and crate-private gateway adapters.
- `rustcta-execution-api`: execution command/event protocol.
- `rustcta-execution-router`: typed execution routing and ledger append path.
- `rustcta-event-ledger`: append-only event/order/fill/account/audit contracts.
- `rustcta-strategy-sdk`: adapter-free strategy runtime contract.
- `rustcta-supervisor`: process registry and lifecycle implementation.
- `rustcta-control-api`: generic multi-strategy control API routes/models.
- `retired-core-compat`: compatibility re-export bridge.

`crates/*` should own reusable behavior. `apps/*` should own process startup,
argument parsing, environment loading, and dependency wiring.

## Apps/Bin Migration Matrix

| Category | Target app/bin | Current boundary | Recommendation | Compatibility decision |
| --- | --- | --- | --- | --- |
| gateway | `apps/gateway` / `rustcta-gateway` | Target app already exists and starts `rustcta-exchange-gateway` with the paper adapter. No current `retired root bin directory/*.rs` should be moved into the gateway daemon directly. | Keep gateway as the exchange boundary process. Future work may expand app startup configuration, but exchange API details stay out of this round. | Keep current app. Do not repurpose canaries or probes as gateway runtime code. |
| supervisor | `apps/supervisor` / `rustcta-supervisor` | Target app already exists. Legacy strategy runners are processes to be supervised, not supervisor implementation. | Keep supervisor thin and move process lifecycle behavior into `crates/rustcta-supervisor`. Treat live strategy binaries as supervised app/process candidates. | Keep `src/main.rs`, `retired root bin directory/cross_arb_live.rs`, and `retired root bin directory/funding_arb_live.rs` as compatibility until strategy process specs and app wrappers are ready. |
| control-api | `apps/control-api` / `rustcta-control-api` | `apps/control-api` is a generic scaffold. `retired root bin directory/control_api.rs` remains the legacy full HTTP/runtime entry. | Split legacy control behavior into `crates/rustcta-control-api` modules before replacing the retired binary. | Keep `retired root bin directory/control_api.rs` compatibility for now because it owns legacy routes, static UI serving, command queues, and legacy read models. |
| cli | `apps/cli` / `rustcta-industrial` | `apps/cli` now has `doctor` plus read-only migration inventory commands that reuse the `rustcta-tools-ops` legacy bin matrix. Several legacy one-shot operator commands still live in `retired root bin directory`. | Convert additional read-only and operator commands into subcommands after their service code is extracted or wrapped. | Keep legacy command names until the new CLI has equivalent output and flags. |
| tools | `tools/ops` / `rustcta-tools-ops` | Canaries, probes, symbol utilities, reporters, and dry-run service skeletons are scattered through `retired root bin directory`. | Use the tools workspace area for operational utilities that are not daemons in the platform control plane. | Keep old binary names as wrappers or aliases during migration, especially for live-order canaries. |
| backtest | `apps/backtest` / `retired-backtest` | `apps/backtest` now provides the workspace backtest app, while `retired root bin directory/backtest.rs` and `retired root bin directory/retired_short_ladder_grid.rs` remain compatibility entries. | Keep moving root-heavy backtest library slices behind `crates/retired-backtest` only when they do not destabilize the fast migration batch. | Keep root `backtest` and `retired_short_ladder_grid` names until scripts and operator docs fully switch to the new app name. |

## Legacy Binary Disposition

| Legacy entry | Category | Target | Proposed disposition |
| --- | --- | --- | --- |
| `src/main.rs` | supervisor / cli compatibility | Supervised strategy app boundary plus `apps/cli` operator surface | Temporarily retain as the root `rustcta` compatibility launcher. Do not move in the first directory milestone. |
| `retired root bin directory/control_api.rs` | control-api | `apps/control-api` plus `crates/rustcta-control-api` modules | Migrate later. Keep compatibility until routes, state, static serving, command queue, and read models are split into crate modules. |
| `retired root bin directory/cross_arb_live.rs` | supervisor-managed runtime | Future strategy/process app launched by `apps/supervisor` | Keep compatibility. It still composes exchange registry, execution, strategy runtime, private sync, and local WS module in one binary. |
| `retired root bin directory/cross_arb_server/ws.rs` | support module | Crate module or future strategy app module | Not a standalone bin. Move only together with, or before, `cross_arb_live` extraction. |
| `retired root bin directory/funding_arb_live.rs` | supervisor-managed runtime | Future funding strategy process app | Keep compatibility until funding strategy runtime and supervisor process specs are ready. |
| `retired root bin directory/backtest.rs` | backtest | `apps/backtest` / `retired-backtest` | Workspace app now exists; keep this as the root compatibility bin during transition. |
| `retired root bin directory/retired_short_ladder_grid.rs` | backtest | `apps/backtest` subcommand or secondary bin | Workspace app now exposes the migrated command surface; keep this compatibility bin while scripts move. |
| `retired root bin directory/cross_arb_preflight.rs` | cli | `apps/cli` subcommand such as `rustcta-industrial cross-arb preflight` | Migrate after preserving current output semantics. Keep legacy name during transition. |
| `retired root bin directory/cross_arb_observe.rs` | cli | `apps/cli` subcommand such as `cross-arb observe` | Migrate after read-only service extraction or wrapper introduction. |
| `retired root bin directory/funding_arb_observe.rs` | cli | `apps/cli` subcommand such as `funding observe` | Migrate after read-only service extraction or wrapper introduction. |
| `retired root bin directory/cross_arb_account_audit.rs` | cli | `apps/cli` audit subcommand | Migrate later. It touches private account reads and should keep compatibility until output and credential boundaries are stable. |
| `retired root bin directory/cross_arb_fee_audit.rs` | cli | `apps/cli` audit subcommand | Migrate later with account/fill audit commands. |
| `retired root bin directory/cross_arb_order_admin.rs` | cli | `apps/cli` admin subcommand, later routed through control/execution boundaries | Migrate later. Keep compatibility and confirmation flags because it can query, cancel, or close private orders. |
| `retired root bin directory/bitget_order_canary.rs` | tools | `tools/ops` live-order canary | Migrate later with strict compatibility. Do not change exchange API details in directory migration work. |
| `retired root bin directory/bitget_spot_order_canary.rs` | tools | `tools/ops` spot live-order canary | Migrate later with strict compatibility. |
| `retired root bin directory/exchange_order_canary.rs` | tools | `tools/ops` generic order canary | Migrate later with strict compatibility. |
| `retired root bin directory/hyperliquid_self_test.rs` | tools | `tools/ops` self-test command | Migrate as a tool once the tools package exists. No adapter behavior changes. |
| `retired root bin directory/gateio_bitget_spot_symbols.rs` | tools | `tools/ops symbols gateio-bitget-spot` | Removed after the tools command covered the utility path. |
| `retired root bin directory/ws_proxy_probe.rs` | tools | `tools/ops ws-proxy-probe` | Removed after the tools command covered the diagnostic path. |
| `retired root bin directory/account_position_reporter.rs` | tools / supervised job | `tools/ops` reporter, later launchable by supervisor spec | Migrate later. It is an operational reporting job, not core supervisor implementation. |
| `retired root bin directory/trend_report.rs` | tools / supervised job | `tools/ops reporter trend` | Removed after the supervisor spec was switched to the tools command. |
| `retired root bin directory/smart_money_binance_collector.rs` | tools | `tools/ops smart-money binance-collector` | Removed after the tools command covered the dry-run utility path. |
| `retired root bin directory/smart_money_hyperliquid_wallet_ingestion.rs` | tools | `tools/ops smart-money hyperliquid-wallet-ingestion` | Removed after the tools command covered the dry-run utility path. |
| `retired root bin directory/smart_money_portfolio_service.rs` | tools | `tools/ops smart-money portfolio-service` | Removed after the tools command covered the dry-run utility path. |

## Compatibility Policy

Until a category is fully migrated:

- Preserve existing binary names used by scripts, docs, and operators.
- Prefer thin compatibility wrappers over duplicate behavior.
- Do not make `apps/*/src/main.rs` large. Move reusable logic into `crates/*`
  or strategy crates first.
- Avoid mixing directory movement with exchange API development.
- For any binary that can place, cancel, or close orders, preserve existing
  confirmation flags and output fields before changing its app owner.

## Parallel Codex Follow-up Slices

These slices can be assigned independently after this planning document. Each
slice should re-check the dirty worktree and respect the active file-scope
instructions for its own task.

| Slice | Scope | Suggested output | Independence notes |
| --- | --- | --- | --- |
| B: Cargo bin inventory | Confirm every Cargo-discovered root binary and decide whether root `Cargo.toml` should explicitly list compatibility bins. | A doc or manifest table covering package owner, bin name, target app, and compatibility name. | Read-only or docs-only. Can run in parallel with app planning. |
| C: Backtest app plan | Design `apps/backtest` package naming, command names, and compatibility path for `backtest` and `retired_short_ladder_grid`. | Backtest-specific migration checklist and acceptance criteria. | Does not depend on exchange API work. |
| D: Tools app plan | Design `tools/ops` package grouping for probes, canaries, reporters, symbol utilities, and smart-money dry-run utilities. | Tool taxonomy, command naming, and first-wave migration list. | Can run in parallel with CLI work if command names are coordinated. |
| E: Control API split | Plan extraction of `retired root bin directory/control_api.rs` into `crates/rustcta-control-api` modules before app replacement. | Route/service/model module map, risk list, and compatibility route checklist. | Should not edit exchange API details. |
| F: CLI command surface | First bridge landed: `rustcta-industrial migration verify-retired-src` and `migration verify-retired-src` expose the legacy bin matrix through the industrial CLI. Remaining work maps `cross_arb_*` and `funding_arb_*` one-shot commands only after their root-heavy service code is split. | CLI command tree, flag compatibility table, and output compatibility requirements. | Coordinate with tools only on commands that could fit both groups. |
| G: Supervisor process specs | First bridge landed: `rustcta-supervisor --print-legacy-spec <template>` can generate JSON specs for legacy long-running runtimes and reporters. | Keep `docs/supervisor_process_specs.md` current; later add checked-in deployment specs once config names are final. | Does not require moving strategy logic first. |
| H: Compatibility and deprecation | Define when old root binary names can become wrappers, aliases, warnings, or removals. | Compatibility policy with staged dates or milestones, plus docs/runbook update list. | Depends on category plans, but can start with inventory. |
| I: Boundary checks | Extend boundary checks to flag new unclassified `retired root bin directory` entries and prevent app crates from importing gateway-private modules. | Script/check design and expected failure examples. | Can run after the migration matrix is accepted. |

## Suggested First Implementation Order

1. Add inventory and compatibility policy before moving files.
2. Create target app plans for `apps/backtest` and `tools/ops`, because they
   are lower risk than live strategy and control API migration.
3. Split `retired root bin directory/control_api.rs` into library modules before replacing the
   legacy control API binary.
4. Use the supervisor legacy-spec generator for long-running legacy runtimes
   before moving those runtimes.
5. Move CLI/admin commands only after flag and output compatibility has an
   explicit checklist.
