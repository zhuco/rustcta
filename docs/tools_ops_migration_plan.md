# Tools Ops Migration Plan

Status date: 2026-06-07

Archived status: this plan preserves old tools/ops migration context. Do not use
retired-root binary names here as current run commands unless a current
workspace app/tool command also documents them.

This is the first practical migration plan for `tools/ops`. It follows
`docs/industrial_directory_migration_plan.md`: directory ownership first,
behavior compatibility first, and no exchange API development in this slice.

This document tracks the tools/ops migration. The first low-risk smart-money
dry-run commands now have new `rustcta-tools-ops` subcommands and legacy root
binary wrappers.

## Current Boundary

`tools/ops` already exists as the workspace package `rustcta-tools-ops`. Its
current command surface includes migration inventory plus the first migrated
dry-run smart-money commands, public/report rendering commands, and the first
public connectivity probe:

```text
rustcta-tools-ops verify-retired-src
rustcta-tools-ops verify-retired-src
rustcta-tools-ops smart-money binance-collector
rustcta-tools-ops smart-money hyperliquid-wallet-ingestion
rustcta-tools-ops smart-money portfolio-service
rustcta-tools-ops probe ws-proxy
rustcta-tools-ops symbols gateio-bitget-spot
rustcta-tools-ops ws-proxy-probe
rustcta-tools-ops reporter trend
rustcta-tools-ops reporter account-position render
```

`tools/ops/src/lib.rs` already classifies many legacy `retired root bin directory/*.rs` files as
`ToolOps`. The first implementation batch should turn that ownership metadata
into real operator commands without changing exchange adapters, REST signing,
websocket behavior, order placement semantics, or Cargo package ownership.

## Recommended Command Tree

Use one binary, `rustcta-tools-ops`, with grouped subcommands. Historical
binary names should stay available during migration as wrappers or aliases.

```text
rustcta-tools-ops
  verify-retired-src
  smart-money
    binance-collector
    hyperliquid-wallet-ingestion
    portfolio-service
  probe
    ws-proxy
    hyperliquid-self-test
  symbols
    gateio-bitget-spot
  canary
    exchange-order
    bitget-perp-order
    bitget-spot-order
  audit
    cross-arb-account
    cross-arb-fee
  admin
    cross-arb-order
  reporter
    account-position
    trend
```

Suggested legacy aliases while runbooks move:

| Legacy binary | New command |
| --- | --- |
| `smart_money_binance_collector` | `rustcta-tools-ops smart-money binance-collector` |
| `smart_money_hyperliquid_wallet_ingestion` | `rustcta-tools-ops smart-money hyperliquid-wallet-ingestion` |
| `smart_money_portfolio_service` | `rustcta-tools-ops smart-money portfolio-service` |
| `ws_proxy_probe` | `rustcta-tools-ops probe ws-proxy` |
| `gateio_bitget_spot_symbols` | `rustcta-tools-ops symbols gateio-bitget-spot` |
| `hyperliquid_self_test` | `rustcta-tools-ops probe hyperliquid-self-test` |
| `exchange_order_canary` | `rustcta-tools-ops canary exchange-order` |
| `bitget_order_canary` | `rustcta-tools-ops canary bitget-perp-order` |
| `bitget_spot_order_canary` | `rustcta-tools-ops canary bitget-spot-order` |
| `cross_arb_account_audit` | `rustcta-tools-ops audit cross-arb-account` |
| `cross_arb_fee_audit` | `rustcta-tools-ops audit cross-arb-fee` |
| `cross_arb_order_admin` | `rustcta-tools-ops admin cross-arb-order` |
| `account_position_reporter` | `rustcta-tools-ops reporter account-position` |
| `trend_report` | `rustcta-tools-ops reporter trend` |

## Dependency Boundary Guard

`tools/ops` must not depend on the legacy root `rustcta` package while the
legacy root package depends on `rustcta-tools-ops` for compatibility wrappers.
That direct edge creates a Cargo cycle:

```text
rustcta -> rustcta-tools-ops -> rustcta
```

Therefore a retired binary can move into `tools/ops` only if its implementation
does not import `legacy root crate path *`, or after the required behavior has first been
extracted into a separate non-root crate. `trend_report` now follows that route
through `crates/rustcta-reporting`. The root-dependent
`account_position_reporter` still stays as a retired binary, but its reusable
configuration, exposure aggregation, balance helpers, Markdown report
formatting, account snapshot provider trait, ticker-pricing logic, and WeCom
markdown webhook helper now live in `crates/rustcta-reporting`.
`rustcta-tools-ops reporter account-position render` now exposes the safe
render-only slice by consuming a local JSON `AccountPositionReportInput`
snapshot and printing Markdown, without opening exchange connections, sending
webhooks, or depending on legacy `rustcta`. The remaining blocker for the live
reporter is wiring a non-root private account provider implementation that can
run outside the legacy root.

`account_position_reporter` has started that extraction path. The pure
configuration/read-model/markdown pieces now live in
`crates/rustcta-reporting::account_position_report`:

- `AccountPositionReporterConfig` plus default config parsing helpers.
- `AccountPositionInput`, `AccountBalanceInput`, `AccountPositionReportInput`,
  `AccountPositionReport`, and `SymbolExposure`.
- `AccountPositionProvider` for root-free live balance/position/ticker reads.
- `collect_account_position_report`, `estimate_balances_usdt_value`,
  `resolve_asset_price_usdt`, `build_account_position_report`,
  `sum_usdt_balance_value`, `is_stable_valued_asset`, and deterministic
  markdown rendering helpers.
- WeCom markdown webhook payload construction plus response validation/sending
  through `send_account_position_markdown_webhook`.

The retired binary now maps `legacy root crate path core::types::{Balance, Position}` into
those root-free input models behind a `LegacyExchangeAccountPositionProvider`
adapter. It still owns `AccountManager` bootstrap, root `Exchange` acquisition,
logger setup, HTTP client construction, and the reporting loop. Do not move it
into `tools/ops` until the root adapter is replaced by a non-root account
provider or kept outside the tools crate boundary.

## First Batch Order

### 1. `retired root bin directory/smart_money_binance_collector.rs` (removed)

Target command: `rustcta-tools-ops smart-money binance-collector`.

Status: migrated. The root binary wrapper has been removed; use
`rustcta-tools-ops smart-money binance-collector`.

Why low risk:

- Short file, currently about config loading plus formatted dry-run output.
- Uses `SmartMoneyServiceConfig::load_yaml` and prints counts/settings.
- Explicitly says no Binance network connections are opened.
- No private credentials, no order path, no long-running loop.

Acceptance commands for the implementation slice:

```bash
cargo run -p rustcta-tools-ops -- smart-money binance-collector --help
cargo run -p rustcta-tools-ops -- smart-money binance-collector --config config/smart_money.yml
```

Expected acceptance: the tools command exits successfully and prints the
dry-run summary fields.

### 2. `retired root bin directory/smart_money_hyperliquid_wallet_ingestion.rs` (removed)

Target command: `rustcta-tools-ops smart-money hyperliquid-wallet-ingestion`.

Status: migrated. The root binary wrapper has been removed; use
`rustcta-tools-ops smart-money hyperliquid-wallet-ingestion`.

Why low risk:

- Short file with the same dry-run skeleton pattern.
- Loads `SmartMoneyServiceConfig`, counts enabled wallets, and prints ingestion
  settings.
- Explicitly says no Hyperliquid network connections are opened.
- No private REST calls and no order behavior.

Acceptance commands:

```bash
cargo run -p rustcta-tools-ops -- smart-money hyperliquid-wallet-ingestion --help
cargo run -p rustcta-tools-ops -- smart-money hyperliquid-wallet-ingestion --config config/smart_money.yml
```

Expected acceptance: the tools command exits successfully and prints the
dry-run summary fields.

### 3. `retired root bin directory/smart_money_portfolio_service.rs` (removed)

Target command: `rustcta-tools-ops smart-money portfolio-service`.

Status: migrated. The root binary wrapper has been removed; use
`rustcta-tools-ops smart-money portfolio-service`.

Why low risk:

- Short file with dry-run output only.
- Loads portfolio constraints from `SmartMoneyServiceConfig`.
- Explicitly says no portfolio orders or network connections are created.
- Although the name says `service`, the current behavior is not a daemon.

Acceptance commands:

```bash
cargo run -p rustcta-tools-ops -- smart-money portfolio-service --help
cargo run -p rustcta-tools-ops -- smart-money portfolio-service --config config/smart_money.yml
```

Expected acceptance: the tools command exits successfully and prints the
dry-run summary fields.

### 4. `retired root bin directory/ws_proxy_probe.rs` (removed)

Target command: `rustcta-tools-ops probe ws-proxy`.

Status: migrated. The grouped command and the historical top-level
`rustcta-tools-ops ws-proxy-probe` alias both call
`rustcta_tools_ops::run_ws_proxy_probe`; the root binary wrapper has been
removed.

Why low risk:

- Public websocket connectivity probe using the proxy-aware connector.
- No private credentials and no order path.
- Operator-facing diagnostic, clearly not a gateway daemon.
- Slightly higher risk than smart-money dry-runs because it performs external
  network I/O and has a larger case table.

Acceptance commands:

```bash
cargo run -p rustcta-tools-ops -- ws-proxy-probe --help
cargo run -p rustcta-tools-ops -- probe ws-proxy --help
cargo run -p rustcta-tools-ops -- ws-proxy-probe --exchange binance-spot --frames 1 --timeout-ms 12000
cargo run -p rustcta-tools-ops -- probe ws-proxy --exchange binance-spot --frames 1 --timeout-ms 12000
```

Expected acceptance: help output is available from grouped and top-level alias
paths; the one-case probe either succeeds or fails with a connectivity error.

### 5. `retired root bin directory/gateio_bitget_spot_symbols.rs` (removed)

Target command: `rustcta-tools-ops symbols gateio-bitget-spot`.

Status: migrated. The root binary wrapper has been removed; use
`rustcta-tools-ops symbols gateio-bitget-spot`.

Why low risk:

- Public symbol/rule discovery utility.
- Does not read private credentials and does not place orders.
- Useful as a tool command because it produces operator config input.
- Higher risk than `ws_proxy_probe` only because it uses multiple public REST
  calls and manual argument parsing that should be preserved exactly.

Acceptance commands:

```bash
cargo run -p rustcta-tools-ops -- symbols gateio-bitget-spot --help
cargo run -p rustcta-tools-ops -- symbols gateio-bitget-spot --limit 5
cargo run -p rustcta-tools-ops -- symbols gateio-bitget-spot --limit 5 --yaml
```

Expected acceptance: `--limit 5` output uses the expected symbol format;
`--yaml` still prints one `  - SYMBOL` line per symbol.

### 6. `retired root bin directory/trend_report.rs` (removed)

Target command: `rustcta-tools-ops reporter trend`.

Status: migrated after helper extraction. The trend reporter implementation now
lives in the non-root `crates/rustcta-reporting` crate, `rustcta-tools-ops
reporter trend` owns the operator command, and the supervisor spec launches
that tools command directly.

Additional migration guardrails:

- Keep `rustcta-reporting` free of `legacy root crate path *` dependencies.
- Keep runbooks and supervisor specs pointed at `rustcta-tools-ops reporter trend`.
- Keep routine validation limited to the legacy `--help` path unless a test
  webhook/config is intentionally provided, because the command can enter the
  infinite reporting loop and send notifications.

Acceptance commands:

```bash
cargo check -p rustcta-reporting
cargo check -p rustcta-tools-ops
cargo run -p rustcta-tools-ops -- reporter trend --help
```

### 6a. `retired root bin directory/account_position_reporter.rs`

Target command: `rustcta-tools-ops reporter account-position`.

Status: partially migrated. The render-only command
`rustcta-tools-ops reporter account-position render --config <yaml> --input
<json>` is now owned by `tools/ops`; it reads a local
`AccountPositionReportInput` JSON snapshot, builds the report with
`rustcta-reporting`, and prints Markdown. The live reporter still remains a
retired binary because it directly imports `legacy root crate path *` for `Exchange`,
`AccountManager`, market types, logger setup, and HTTP client construction.
The pure formatting/report-model slice and the root-free live read/provider
trait have moved into `crates/rustcta-reporting` so that future work can migrate
behavior without creating the Cargo cycle `rustcta -> rustcta-tools-ops ->
rustcta`.

Extracted root-free helpers:

- Config defaults and YAML loading via `AccountPositionReporterConfig`.
- Balance and position input DTOs independent of legacy `Position`/`Balance`.
- `AccountPositionProvider`, plus `collect_account_position_report` for
  balance/position collection through a non-root trait.
- Exposure aggregation, dust filtering, stablecoin checks, USDT balance
  summarization, ticker candidate pricing, and markdown rendering.
- WeCom markdown webhook payload construction plus response validation/sending
  helper.

Remaining blockers before tools ownership:

- Replace the legacy `LegacyExchangeAccountPositionProvider` adapter with a
  provider implementation that does not require `dyn legacy root crate path Exchange`, or keep
  that adapter outside `tools/ops`.
- Keep WeCom delivery out of the render-only tools command; the shared helper
  is available, but sending still has external side effects.

Acceptance commands for the current slice:

```bash
cargo test -p rustcta-reporting
cargo test -p rustcta-tools-ops
cargo run -q -p rustcta-tools-ops -- reporter account-position render --help
cargo check --bin account_position_reporter
scripts/check_industrial_boundaries.sh
```

### 7. `retired root bin directory/hyperliquid_self_test.rs`

Target command: `rustcta-tools-ops probe hyperliquid-self-test`.

Status: deferred from the low-risk Task 4 implementation slice. The legacy
binary still directly imports `legacy root crate path exchanges::hyperliquid::HyperliquidExchange`
plus root `legacy root crate path core` types. Moving that implementation into `tools/ops`
would require the forbidden dependency edge `rustcta-tools-ops -> rustcta`.
It also performs private credential reads and private account reads before the
order-safe `HYPERLIQUID_RUN_ORDERS=false` gate. Keep it as a retired binary
until a non-root Hyperliquid account/probe adapter is extracted or the command
is split into a public/read-only root-free probe.

Why it is still first-wave but last in the batch:

- It is an ops self-test rather than a platform service.
- Default `HYPERLIQUID_RUN_ORDERS` is false, so order/cancel testing is opt-in.
- It does require Hyperliquid credentials and performs private read calls, so it
  should move only after the read-only/public tools above are stable.

Acceptance commands:

```bash
cargo run -p rustcta-tools-ops -- probe hyperliquid-self-test --help
HYPERLIQUID_RUN_ORDERS=false cargo run -p rustcta-tools-ops -- probe hyperliquid-self-test
HYPERLIQUID_RUN_ORDERS=false cargo run --bin hyperliquid_self_test
```

Expected acceptance after the non-root adapter work exists: both paths keep
`HYPERLIQUID_RUN_ORDERS=false` as the default/order-safe mode. The credentialed
command may fail in an environment without keys, but it should fail at the same
setup boundary from both paths and must not place or cancel orders unless
`HYPERLIQUID_RUN_ORDERS=true`. The retired binary currently has no CLI parser,
so do not pass `--help` to the legacy path because it would still enter the
environment/credential flow.

## Live-Order Canary Compatibility

Live-order canaries should not be moved in the same implementation PR as the
dry-run/public first batch. Plan them as a second guarded slice:

- `exchange_order_canary` -> `rustcta-tools-ops canary exchange-order`
- `bitget_order_canary` -> `rustcta-tools-ops canary bitget-perp-order`
- `bitget_spot_order_canary` -> `rustcta-tools-ops canary bitget-spot-order`

Compatibility requirements:

- Keep retired binary names available until runbooks and scripts have moved.
- Preserve every existing flag name, default value, output JSON field, and
  error message that acts as an operator safety gate.
- Keep `--execute` and `--confirm-live-order` as separate required gates.
- Preserve config dry-run checks. For perp canaries, `config.execution.dry_run`
  must continue to block unconfirmed live execution. For spot canary,
  `bitget_config.dry_run = !args.execute` must remain behaviorally equivalent.
- Preserve small-notional caps and estimated-notional caps.
- Preserve preflight refusal when open orders or nonzero positions already
  exist.
- Do not add venue support, change signing, change REST endpoints, alter symbol
  rules, or change adapter behavior during directory migration.
- Prefer a wrapper/adapter function inside `tools/ops` over copied logic. The
  old binary can call the same function so old and new paths cannot drift.

Canary slice acceptance commands:

```bash
cargo run -p rustcta-tools-ops -- canary exchange-order --help
cargo run --bin exchange_order_canary -- --help
cargo run -p rustcta-tools-ops -- canary bitget-perp-order --help
cargo run --bin bitget_order_canary -- --help
cargo run -p rustcta-tools-ops -- canary bitget-spot-order --help
cargo run --bin bitget_spot_order_canary -- --help
```

For non-live validation, use configs that remain dry-run and verify the same
blocking behavior through both names. Do not include a real `--execute
--confirm-live-order` command as routine migration acceptance; that belongs to
a deliberate operator canary runbook.

Current migration slice status: `rustcta-tools-ops` now owns non-mutating safety
plan commands for these canary, audit, and admin entries. The new commands
preserve legacy flag names/defaults, validate confirmation gates and bounded
dry-run inputs locally, and render JSON safety plans with preserved legacy
output fields. They do not open private exchange connections, place orders,
cancel orders, close positions, or replace the legacy live binaries.

Risk note: `exchange_order_canary` can open and close USDT perpetual positions,
`bitget_order_canary` can open and close a Bitget perpetual position, and
`bitget_spot_order_canary` can submit a Bitget spot IOC order. Treat them as
production-impacting even when the expected notional is small.

## Entries Not To Move Yet

These should stay under their current compatibility paths until their own
split plans are accepted:

| Entry | Reason to defer |
| --- | --- |
| `src/main.rs` | Legacy strategy launcher; should move only with supervisor/process specs. |
| `retired root bin directory/control_api.rs` | Large legacy HTTP runtime with routes, static UI, command queue, snapshots, and restart behavior. |
| `retired root bin directory/cross_arb_live.rs` | Long-running live strategy process with exchange registry, execution, private sync, and local WS module. |
| `retired root bin directory/cross_arb_server/ws.rs` | Local support module for `cross_arb_live`, not a standalone ops tool. |
| `retired root bin directory/funding_arb_live.rs` | Live order strategy runtime; requires supervisor/process-spec planning. |
| `retired root bin directory/backtest.rs` | Explicit root `Cargo.toml` bin; belongs to the backtest app plan, not tools/ops. |
| `retired root bin directory/retired_short_ladder_grid.rs` | Research/backtest/strategy scope, not ops diagnostics. |
| `retired root bin directory/cross_arb_preflight.rs` | Safer as `apps/cli` command because it is a live-readiness gate rather than an ad-hoc tool. |
| `retired root bin directory/cross_arb_observe.rs` | Read-only public observation runtime, but strategy-observe semantics fit CLI/strategy ownership better than first tools batch. |
| `retired root bin directory/funding_arb_observe.rs` | Read-only observation plus notification behavior; coordinate with CLI and funding strategy ownership. |
| `retired root bin directory/cross_arb_account_audit.rs` | Private account read across venues; move after credential/output compatibility checklist. |
| `retired root bin directory/cross_arb_fee_audit.rs` | Private fill reads and rebate assumptions; move with audit command compatibility. |
| `retired root bin directory/cross_arb_order_admin.rs` | Can query, cancel, or close positions; requires strict admin safety compatibility. |
| `retired root bin directory/account_position_reporter.rs` | Long-running reporter with private account/exchange reads and Webhook side effects. Pure config, exposure aggregation, balance helpers, and Markdown formatting are now in `rustcta-reporting`; extract or wrap the live account read boundary before moving the command. |

## Implementation Shape For Each Slice

Use small, reversible slices:

1. Extract the retired binary's `Args` and `main` body into a callable command
   function under `tools/ops`.
2. Wire the new `rustcta-tools-ops` subcommand to that function.
3. Replace the legacy `retired root bin directory/<name>.rs` with a thin compatibility wrapper
   only after output/flags are preserved.
4. Keep the old binary name until a separate deprecation milestone says it can
   be removed.

Do not combine this with exchange API changes, Cargo manifest rewrites, or
runtime behavior changes.

## Batch Acceptance Summary

For every migrated file:

```bash
cargo check -p rustcta-tools-ops
cargo test -p rustcta-tools-ops
cargo run -p rustcta-tools-ops -- verify-retired-src
cargo run -p rustcta-tools-ops -- <new-command> --help
```

The checked-in `rustcta-tools-ops` CLI smoke tests cover the bounded local
acceptance surface for migrated commands:

```bash
cargo run -p rustcta-tools-ops -- verify-retired-src --target tools
cargo run -p rustcta-tools-ops -- verify-retired-src
cargo run -p rustcta-tools-ops -- reporter trend --help
cargo run -p rustcta-tools-ops -- reporter account-position render --help
cargo run -p rustcta-tools-ops -- ws-proxy-probe --help
cargo run -p rustcta-tools-ops -- probe ws-proxy --help
cargo run -p rustcta-tools-ops -- smart-money binance-collector --config config/smart_money.yml
cargo run -p rustcta-tools-ops -- smart-money hyperliquid-wallet-ingestion --config config/smart_money.yml
cargo run -p rustcta-tools-ops -- smart-money portfolio-service --config config/smart_money.yml
```

These smoke checks intentionally avoid public websocket/REST connections,
private account reads, order placement, cancellations, admin actions, and
canary execution. Public symbol/probe live cases remain manual acceptance
commands, not default test cases.

Task 4 completion note: the low-risk command slice is complete for commands
that are already root-free and non-mutating. The tests now also assert
smart-money dry-run summaries, migrated help surfaces, and shared legacy wrapper
forwarding. `hyperliquid_self_test` remains classified as ToolOps but is not
migrated in this slice because it is still root-dependent and credentialed.

For legacy binaries that already use `clap`, also run:

```bash
cargo run --bin <legacy-bin> -- --help
```

For legacy binaries with manual or environment-only argument handling, validate
the smallest bounded compatibility command instead of assuming `--help` exists.

For dry-run smart-money commands, also run the new and legacy command against
`config/smart_money.yml` and compare summary fields.

For public probes/symbol utilities, run one bounded public-network case from
both paths and compare output shape, allowing external connectivity failures to
be environment-dependent.

For live-order canaries/admin commands, routine migration acceptance is limited
to help output, flag compatibility, dry-run refusal, and confirmation-gate
checks. Real order/cancel/close commands require a separate operator-approved
canary runbook.
