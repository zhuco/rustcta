# Industrial Workspace Migration Status

Status date: 2026-06-07

This file tracks implementation progress against
`docs/industrial_cta_platform_architecture_assessment.md`.
Directory/runtime migration details are now split into:

- `docs/industrial_directory_migration_plan.md`
- `docs/control_web_directory_migration_plan.md`
- `docs/backtest_app_migration_plan.md`
- `docs/tools_ops_migration_plan.md`

The architecture document is the full industrial refactor target, not a first
milestone document. The current program name is:

```text
industrial-workspace-migration
```

The migration is complete only when the target workspace/process architecture
has replaced the monolithic runtime boundaries. This status file records what
has landed so far and what remains.

## Current Result

The repository now has the target workspace shape and the first industrial
contracts are compiling and tested. The old `rustcta` package is still present
for compatibility and still owns most concrete runtime behavior.

Latest cleanup note:

- `docs/industrial_workspace_update_2026-06-07_v0.3.9.md`
- workspace/root package version: `0.3.9`
- local validation in this cleanup pass: `cargo fmt --all --check`,
  `cargo check --workspace`, `scripts/check_industrial_boundaries.sh`,
  `cargo clippy --workspace --all-targets --all-features`, and
  `cargo test --workspace --all-features`

Full workspace tests passed earlier in this migration:

```bash
cargo test --workspace --all-features
```

Earlier full-workspace result:

- passed across the legacy crate, new industrial crates, app crates, strategy
  wrapper crates, integration tests, and doc tests
- ignored tests remain the existing live/read-only or live websocket tests that
  are intentionally gated

Current active scope for this document update includes the non-gateway
directory/runtime migration slice. Web control panel and strategy migration
work can proceed in parallel, but shared API boundary files should be treated
as owned interfaces and edited with explicit coordination.

## Implemented

- Root `Cargo.toml` defines a Cargo workspace while preserving the existing
  `rustcta` package.
- Foundation crates:
  - `crates/rustcta-types`
  - `crates/rustcta-exchange-api`
  - `crates/rustcta-execution-api`
  - `crates/rustcta-strategy-sdk`
- Platform crates:
  - `crates/rustcta-exchange-gateway`
  - `crates/rustcta-execution-router`
  - `crates/rustcta-event-ledger`
  - `crates/rustcta-supervisor`
  - `crates/rustcta-control-api`
  - `crates/rustcta-core-compat`
- App crates:
  - `apps/gateway`
  - `apps/supervisor`
  - `apps/control-api`
  - `apps/cli`
  - `apps/backtest`
- Tool crates:
  - `tools/ops`
- Strategy wrapper crates:
  - `strategies/avellaneda-stoikov`
  - `strategies/spot-spot-arbitrage`
  - `strategies/cross-exchange-arbitrage`
  - `strategies/funding-arbitrage`
  - `strategies/hedged-grid`
  - `strategies/mean-reversion`
  - `strategies/poisson-market-maker`
  - `strategies/range-grid`
  - `strategies/short-ladder-live`
  - `strategies/trend`
- Directory-level AI ownership files were added under `crates/`,
  `strategies/`, `apps/`, and `tools/`.
- Active directory structure docs now identify `apps/`, `crates/`,
  `strategies/`, `tools/`, `web-ui/`, and the legacy compatibility `src/`
  root as the current workspace layout. The latest cleanup pass removed only
  ignored generated output, local runtime scratch files, and empty legacy
  directories.
- `rustcta-exchange-gateway` exposes a typed local gateway protocol,
  mock/in-memory gateway, secret-like payload rejection, and Axum health,
  status, and request routes.
- `rustcta-exchange-gateway` now has a crate-private adapter module and an
  `AdapterBackedGateway` registry that routes typed gateway operations through
  registered `ExchangeClient` implementations.
- `rustcta-exchange-gateway` local protocol now covers readback, mutation,
  batch place/cancel, cancel-all, capability discovery, public order-book
  subscriptions, and private account/order/fill subscription requests.
- `rustcta-exchange-gateway` is split into focused protocol, client, mock,
  stream, security, status, HTTP, and adapter modules. The exchange API crate
  is also split into focused account, market, order, stream, capability, error,
  protocol, context, and client modules.
- `rustcta-exchange-gateway` now includes the first extracted adapter slice:
  `paper`, implemented against the new `rustcta-types` and
  `rustcta-exchange-api` contracts instead of the old monolith types.
- `rustcta-exchange-gateway` paper adapter now implements the full local paper
  mutation surface used by the typed gateway protocol: single place/cancel,
  batch place, batch cancel, cancel-all, query, open-order readback, balances,
  books, fees, fills, and public/private subscription acknowledgements.
- `rustcta-exchange-gateway` now includes migrated concrete public/private
  readback adapter modules for Binance, Bitget, CoinEx, Gate.io, KuCoin, MEXC,
  and OKX under its crate-private `adapters/` tree. Private mutation and
  private stream operations remain explicit unsupported paths unless an
  adapter implements them.
- `rustcta-exchange-gateway` now exposes a `GatewayClient` abstraction and an
  in-process client for typed gateway requests, including typed helper methods
  for every current gateway operation.
- `apps/gateway` starts the adapter-backed gateway on a local bind address and
  registers the paper adapter by default.
- `apps/gateway` now has app-local configuration parsing and adapter wiring
  tests for bind address, adapter list, and per-adapter public REST base URL
  overrides. The process entrypoint remains a thin startup wrapper.
- `apps/gateway` now also loads exchange private REST credentials from local
  process environment into adapter configs, keeps debug output redacted, and
  verifies that private REST capabilities are exposed only through the typed
  gateway client once the gateway process owns those credentials.
- `rustcta-execution-router` contains the initial dry-run execution boundary,
  validates execution identity before routing, and can route typed
  `OrderCommand`, `CancelCommand`, and `CancelAllCommand` through a gateway
  client into a typed gateway implementation.
- `rustcta-execution-router` can attach an event ledger writer and now appends
  order, cancel, and cancel-all command/ack events around the typed gateway
  mutation path.
- `rustcta-execution-router` now also appends order, cancel, and cancel-all
  command plus rejected ack events for typed `DryRun`/`LiveDryRun` routes. The
  gateway mutation stays blocked, but the execution decision is still replayable
  through the event ledger.
- `rustcta-event-ledger` provides append-only event/order/fill/account/audit
  ledger contracts, an in-memory implementation, schema-versioned persisted
  records, replay ordering, and secret-field rejection.
- `rustcta-event-ledger` now also provides a JSONL durable ledger
  implementation with append, replay, sequence allocation, decode errors, file
  sync, and secret-field rejection before write. This is the first durable
  event/order/fill/account/audit ledger baseline; database storage can build on
  the same `LedgerWriter`/`LedgerReader`/`LedgerStore` traits later.
- `rustcta-supervisor` contains schema-versioned process registry records,
  lifecycle command records, heartbeat update models, supervisor snapshots, and
  the first real operating-system child process lifecycle implementation:
  start, stop, restart, pid tracking, restart count, restart backoff, stale
  heartbeat marking, and optional stdout/stderr log redirection.
- `rustcta-supervisor` now includes a JSON file process registry store that can
  load/save `SupervisorSnapshot` records and lets app startup preserve process
  registry state across invocations.
- `apps/supervisor` can load a JSON `StrategyProcessSpec`, start it, emit the
  started process record, persist registry state through `--registry-path`, and
  stop it after a bounded run-once window.
- `apps/supervisor` also has a read-only HTTP service mode:
  `rustcta-supervisor --serve --bind <addr> --registry-path <path>`. It exposes
  `/api/health`, `/api/snapshot`, `/api/processes`, and
  `/api/processes/:id` from the JSON registry without accepting remote
  lifecycle mutations.
- `apps/supervisor` also has a local persisted-registry maintenance command:
  `rustcta-supervisor --registry-path <path> --mark-stale-heartbeats-ms <ms>`.
  It applies the stale-heartbeat policy, saves the updated registry snapshot,
  and prints the processes marked failed without exposing a remote mutation API.
- `rustcta-supervisor` now includes root-free legacy process spec templates for
  `cross_arb_live`, `funding_arb_live`, `spot_spot_live_dry_run`,
  `trend_report`, and `account_position_reporter`. `apps/supervisor` can print
  those specs through `rustcta-supervisor --print-legacy-spec <template>`
  without starting the process, opening exchange connections, or changing
  strategy code.
- `config/supervisor/*.spec.json` now includes checked-in supervisor specs for
  the small closeout runtime set, including the reporter flows and
  cross/funding arbitrage runtimes, and `rustcta-supervisor` tests verify the
  examples do not drift from the root-free template builder.
- `apps/supervisor` can validate a checked-in `StrategyProcessSpec` through
  `rustcta-supervisor --validate-spec <path>` without starting legacy
  processes, opening exchange connections, or touching credentials.
- Fast-close strategy entry acceptance is pinned for the small runtime set:
  `trend_report` and `account_position_reporter` are the lowest-risk first
  runs because they are reporter/operator flows; `cross_arb_live` is the first
  candidate live runtime once config and credentials are selected; and
  `funding_arb_live` stays a secondary candidate. `spot_spot_live_dry_run` is
  now part of the checked-in supervisor spec set and points at the existing
  VSN-only live-dry-run config. The readiness summary must report the positive
  gates `live_dry_run.enabled=true`, `live_dry_run.build_order_requests=true`,
  `live_dry_run.submit_orders=false`, `small_live_gate.explicit_live_confirmation=true`,
  `live_preflight.max_live_notional_per_trade=3.6`,
  `live_preflight.max_total_live_notional=50`, `monitoring.http_enabled=false`,
  `monitoring.expose_publicly=false`, `spot_symbol_control.require_write_auth=true`,
  and `live_preflight.require_withdraw_permission_absent=true`. Keep it behind
  the reporter and cross-venue smoke sequence because the same config still runs
  the live runtime path with `dry_run=false`, `live_trading_enabled=true`,
  `kill_switch.allow_live_orders=true`,
  `live_preflight.require_api_key_trade_permission=true`,
  `inventory_rebalance.allow_market_rebalance=true`, and
  `inventory_rebalance.allow_lossy_rebalance_when_blocked=true`. The checked-in
  specs for all five validate, and app-level CLI contract tests also assert their
  referenced config files exist, reject inline credential/home-directory path
  drift in the spot/spot config, and pin those live/readiness flags explicitly.
- `apps/supervisor` can run read-only registry validation through
  `rustcta-supervisor --registry-path <path> --validate-registry`, returning a
  status-count summary and invalid-process list without starting or mutating
  processes. Missing registries are treated as valid empty registries for CI
  bootstrap checks.
- `apps/supervisor` now has app-level contract tests for offline spec printing,
  spec validation, missing-registry validation, and read-only HTTP health/detail
  behavior. These tests do not start legacy processes, open network sockets, or
  touch credentials.
- `rustcta-control-api` exposes generic multi-strategy routes for strategy
  list/detail/snapshot/command, process list, gateway status, and events.
- `rustcta-control-api` now exposes generic workspace, agents, commands, and
  credential-status routes, and its public gateway status shape is a local DTO
  instead of a direct dependency on `rustcta-exchange-gateway`.
- `rustcta-control-api` now also exposes read-only legacy-compatible control
  aliases `/api/status`, `/api/config`, and `/api/config/summary`. These reuse
  the new workspace/config summary DTOs, remain secret-free, and do not revive
  legacy raw exchange key management routes.
- `rustcta-control-api` now exposes detail routes for generic control-plane
  entities: `/api/agents/:agent_id`, `/api/processes/:process_id`, and
  `/api/commands/:command_id`.
- `rustcta-control-api` has been split from one large `lib.rs` into
  `models.rs`, `state.rs`, `routes.rs`, and `router.rs`, with `lib.rs`
  re-exporting the public API.
- `rustcta-control-api` can attach an event ledger store and now writes
  accepted operator lifecycle commands as secret-free `OperatorCommandEvent`
  records before returning command acceptance.
- `rustcta-control-api` now replays the attached event ledger through
  `/api/events` and exposes `/api/audit` for audit/operator-command events,
  while preserving empty-array responses when no ledger is configured. Both
  routes accept `?from_sequence=<u64>` for incremental ledger replay; the
  `/api/events` query filters `ledger_events` only because the legacy in-memory
  `commands` list has no sequence cursor.
- `rustcta-control-api` now also exposes `/api/control/audit` as a read-only
  compatibility alias to the same audit read model, including
  `?from_sequence=<u64>`. It does not add mutation behavior or revive legacy
  secret/key routes.
- `rustcta-control-api` now exposes generic, secret-free `/api/risk`,
  `/api/risk/events`, `/api/fees`, and `/api/logs` read models. The crate also
  includes a JSON-value legacy dashboard snapshot adapter for risk/fee/log
  summaries that avoids importing legacy `src/web` or exchange adapter types.
  The legacy dashboard risk-event slice is pinned by a route-level regression:
  `/api/risk/events` reads the same snapshot-backed source as the risk summary,
  returns the public `RiskEventView` shape, and stays read-only.
- `rustcta-control-api` now exposes read-only `/api/inventory` from the legacy
  dashboard snapshot as a schema-versioned JSON rows view. Empty local state
  returns a stable empty `rows` array, while legacy snapshot rows are copied
  through a secret-field filter and pinned by route-level regression coverage.
- `rustcta-control-api` now exposes read-only `/api/books` from the legacy
  dashboard snapshot with the same schema-versioned JSON rows view and
  recursive secret-field filtering used by `/api/inventory`.
- `rustcta-control-api` now exposes read-only `/api/exchanges` from the legacy
  dashboard snapshot with the same schema-versioned JSON rows view and
  recursive secret-field filtering used by `/api/inventory` and `/api/books`.
- `rustcta-control-api` now exposes read-only `/api/trades/recent` from the
  legacy dashboard snapshot `trades` array as `recent_trades`, keeping it
  separate from future trade-ledger DTOs while reusing the same
  schema-versioned JSON rows view and recursive secret-field filtering.
- `rustcta-control-api` now exposes read-only `/api/opportunities/recent` from
  the legacy dashboard snapshot `opportunities` array as
  `recent_opportunities`, keeping it separate from the legacy aggregate
  `/api/opportunities` shape while reusing the same schema-versioned JSON rows
  view and recursive secret-field filtering.
- `rustcta-control-api` now exposes read-only aggregate `/api/opportunities`
  with the legacy dashboard shape `{ recent, arbitrage, statistics }`, sourcing
  `recent` from `opportunities`, `arbitrage` from `arbitrage_opportunities`, and
  `statistics` from `arbitrage_statistics`. The aggregate is a public
  schema-versioned view with recursive secret-field filtering, while
  `/api/opportunities/recent` remains available as the independent rows-only
  slice for low-conflict consumers.
- `rustcta-control-api` symbols migration is now pinned by the industrial
  boundary script for the next read-only `/api/symbols` slice. Once any symbols
  model/adapter/router/handler/test evidence appears, the script requires a
  public symbols view field, extraction from `spot_symbol_rules`, `spot_control`,
  `five_exchange_scanner/symbol_coverage`, and
  `five_exchange_scanner/recommendations`, route wiring to `routes::symbols`, a
  handler returning `state.snapshot().await.symbols`, and route-level assertions
  for `symbol_rules`, `spot_control`, `scanner`, `symbol_coverage`, and
  `recommendations`.
- Basic legacy dashboard snapshot route inventory for this low-conflict control
  API batch:
  - migrated and pinned: `/api/inventory`, `/api/books`, `/api/exchanges`, and
    `/api/trades/recent` now read legacy snapshot arrays through `JsonRowsView`,
    stay read-only, and keep recursive secret-field filtering. The industrial
    boundary script pins route evidence for these four slices through a shared
    `require_snapshot_route_evidence` helper. `/api/trades/recent` is pinned
    with separate route-path and legacy snapshot-key evidence because the public
    route reads the legacy `trades` array as `recent_trades`.
  - migrated with partial-migration guard:
    `/api/opportunities/recent` now reads the legacy `opportunities` array as
    `recent_opportunities`, stays read-only, and avoids changing the aggregate
    `/api/opportunities` response. The industrial boundary script requires its
    complete `JsonRowsView` evidence set once any route/model/adapter/test
    evidence appears.
  - migrated aggregate slice: `/api/opportunities` now mirrors the legacy
    `{ recent, arbitrage, statistics }` read shape as a public
    `OpportunitiesView`, while keeping `/api/opportunities/recent` independent.
    Because this response is not a `JsonRowsView`, the boundary script uses a
    dedicated partial guard: once any aggregate evidence appears, it requires a
    public aggregate view field, legacy `recent`/`arbitrage`/`statistics`
    adapter evidence from `opportunities`/`arbitrage_opportunities`/
    `arbitrage_statistics`, router and handler wiring, and route-level
    assertions for all three response sections.
  - migrated and pinned: `/api/symbols` now mirrors the legacy read-only shape
    through `SymbolsView`, sourcing `symbol_rules` from `spot_symbol_rules`,
    `spot_control` from the legacy control snapshot, and
    `scanner.symbol_coverage`/`scanner.recommendations` from
    `five_exchange_scanner`. The boundary script now requires the full model,
    legacy snapshot adapter, router, handler, and regression-test evidence set
    if any symbols migration evidence appears.
  - frozen out of this batch: raw credential write/delete routes remain
    legacy-only; the new public control API keeps credential handling
    status-only until a gateway/agent-owned credential service exists.
  - paused to finish migration faster: backtest extension work remains disabled
    for this batch except for compile fixes required by shared crate moves.
- `rustcta-control-api` now exposes read-only `/api/strategy-logs` as a
  schema-versioned strategy log tail view. It is path-configured by the app
  boundary, limits tail bytes/lines, redacts sensitive marker lines, and does
  not return host file paths.
- `rustcta-control-api` now exposes read-only `/api/processes/:id/logs` and
  `/api/strategies/:id/logs` from supervisor process `log_path` data. Process
  and strategy list/detail responses are sanitized through `StrategyProcessView`
  and expose only `log_configured`, not local file paths.
- `apps/control-api` can wire that audit path to a durable JSONL ledger through
  `RUSTCTA_CONTROL_API_AUDIT_LEDGER_PATH`.
- `apps/control-api` can follow a supervisor registry snapshot through
  `RUSTCTA_CONTROL_API_SUPERVISOR_REGISTRY_PATH`, composing process state with
  the legacy dashboard snapshot instead of letting legacy risk/fee/log data hide
  supervisor processes.
- `apps/control-api` can publish a local agent summary through
  `RUSTCTA_CONTROL_API_AGENT_ID`, `RUSTCTA_CONTROL_API_TENANT_ID`, and
  `RUSTCTA_CONTROL_API_AGENT_CAPABILITIES`, so the generic workspace/agent
  routes have a concrete local deployment source while the remote agent protocol
  remains future work.
- `apps/control-api` can follow the latest sanitized legacy dashboard snapshot
  on each request through `RUSTCTA_CONTROL_API_LEGACY_SNAPSHOT_PATH`, giving the
  new control API a non-empty migration path for risk, fee, and log panels
  without moving legacy exchange or credential logic into the control API crate.
- `apps/control-api` can wire the strategy log tail bridge through
  `RUSTCTA_CONTROL_API_STRATEGY_LOG_PATH`,
  `RUSTCTA_CONTROL_API_STRATEGY_LOG_TAIL_LINES`, and
  `RUSTCTA_CONTROL_API_STRATEGY_LOG_TAIL_BYTES`.
- `apps/control-api` can optionally serve the built Dioxus web panel through
  `RUSTCTA_CONTROL_API_STATIC_DIR`, with SPA fallback to `index.html`. This
  moves web static hosting into the app boundary while keeping
  `rustcta-control-api` focused on API contracts.
- `apps/control-api` now has a tested app-local configuration and wiring
  module. Environment parsing, state construction, audit ledger wiring,
  legacy snapshot wiring, supervisor registry wiring, strategy log tail
  limits, and optional static file serving are owned by the app boundary while
  `main.rs` remains a thin process entrypoint.
- Fast closeout for running a small strategy set: `apps/control-api` now has an
  app-level contract test that builds the configured router from both a
  supervisor registry snapshot and a legacy dashboard snapshot, then verifies
  `/api/strategies`, `/api/processes/:id`, and `/api/symbols`. This proves the
  control API can present checked-in supervisor specs such as
  `cross_arb_live`, `funding_arb_live`, `spot_spot_live_dry_run`,
  `trend_report`, and `account_position_reporter` without requiring a full
  strategy or backtest expansion.
- `rustcta-control-api` now keeps credential handling status-only in the new
  public API surface. Raw exchange key store management routes and request
  shapes such as `/api/exchange-api-keys` are not exposed from the new control
  API crate; legacy key-management behavior remains frozen outside this
  industrial crate boundary until a gateway/agent-owned credential service is
  introduced.
- Strategy crates depend on `rustcta-strategy-sdk` and expose
  `StrategyRuntime` implementations, `StrategySpec`, config schemas, snapshot
  schemas, secret-free snapshots, and the first adapter-free core slices.
- `rustcta-strategy-avellaneda-stoikov` now contains the first real migrated
  adapter-free Avellaneda-Stoikov core slice beyond the legacy module: full
  strategy/config DTOs, market precision/rule helpers, state/order DTOs,
  reservation-price and optimal-spread formulas, adaptive volatility spread
  multiplier, EWMA annualized volatility calculation, quote generation with
  inventory skew and liquidity multiplier, order quantity sizing with exchange
  steps/min-notional/max-size checks, fill accounting, unrealized PnL/drawdown
  refresh, local risk snapshot, terminal order state detection, and symbol
  matching. This is still a partial core migration; legacy controller runtime,
  exchange IO, websocket handling, external risk-evaluator wiring, order
  lifecycle reconciliation, and runtime orchestration remain in
  `src/strategies/avellaneda_stoikov` until SDK market-data/execution contracts
  replace those root-runtime paths.
- `rustcta-strategy-mean-reversion` now contains the first real migrated
  strategy-core slice beyond the thin wrapper: adapter-free Kline/order-side
  DTOs, precision/quantity utilities, indicator calculations, range-condition
  scoring, liquidity checks, and deterministic order-plan generation. This is
  still a partial core migration; legacy async data acquisition, exchange
  access, order submission, and task orchestration remain in
  `src/strategies/mean_reversion` until SDK market-data/execution contracts
  replace those root-runtime paths.
- `rustcta-strategy-hedged-grid` now contains the first real migrated
  adapter-free grid core slice beyond the thin wrapper: strategy core config
  validation, follow/fee config DTOs, precision resolution and quantization
  helpers, grid price levels, order quantity quantization, order intent/ledger
  DTOs, full in-memory grid side book and order-ledger operations, fill/action
  DTOs, risk-state evaluation, deterministic initial grid order planning, and
  the first adapter-free `GridEngine` state-machine slice for grid rebuild,
  open-fill rolling, partial-fill waiting, post-only reprice/cancel, and
  kill-switch cancel intent generation. The strategy crate now also includes
  adapter-free follow and reconcile inventory logic for inventory-shortage
  follow moves, close-coverage trimming, open/close limit enforcement, slot
  refill, and duplicate price-level normalization. The latest slice also moves
  adapter-free near-gap repair and far-order trimming into the crate so complete
  fills can immediately repair open-ladder shape before the periodic reconcile
  pass. The crate now also covers legacy underwater close behavior, strict
  pairing close-price behavior, same-price repeated fill rolling, and pending
  open notional budget pressure with adapter-free regression tests. This is
  still a partial core migration; the remaining legacy controller, multi-symbol
  runtime, exchange access, and order submission remain in
  `src/strategies/hedged_grid` until the strategy runtime is moved onto SDK
  market-data/execution contracts.
- `rustcta-strategy-funding-arbitrage` now contains the first real migrated
  adapter-free funding-arbitrage core slice beyond the thin wrapper:
  secret-free observe/live configuration validation, funding symbol/snapshot/
  instrument DTOs, candidate qualification, settlement-window and snapshot-age
  filtering, orderability quantity checks, per-exchange selection, and startup
  markdown rendering. It also contains an adapter-free live planning slice for
  per-exchange execution windows, skipped candidates, live result summaries,
  client order id construction, and scan scheduling. The legacy
  `funding_rate_arbitrage/live.rs` runtime now delegates client-order-id
  construction and scheduler scan-time calculation to that migrated live-plan
  core while retaining exchange IO and task orchestration. This is still a partial
  core migration; legacy async market adapter scanning, live scheduler,
  execution adapter wiring, order submission, fill readback, and webhook sending
  remain in
  `src/strategies/funding_rate_arbitrage` until SDK market-data/execution/app
  contracts replace those root-runtime paths.
- `rustcta-strategy-spot-spot-arbitrage` now contains the first real migrated
  adapter-free Spot-to-Spot arbitrage core slice beyond the thin wrapper:
  venue/rejection/book-source DTOs, local order-book levels and cached-book
  helpers, spread/cost/net-edge estimation, depth-notional consumption, and
  summary report aggregation for opportunities and simulated trades, plus
  adapter-free risk state for cooldowns, daily/trade loss limits, consecutive
  rejection limits, per-symbol/total notional limits, and exchange-health
  blacklisting. This is still a partial core migration; legacy config loading,
  inventory ownership, fee model integration, paper/live
  execution, lifecycle, replay, websocket market data, and control-plane
  integration remain in
  `src/strategies/spot_spot_taker_arbitrage` until SDK market-data/execution
  contracts replace those root-runtime paths. The legacy
  `src/strategies/spot_spot_taker_arbitrage/risk.rs` module is now a
  compatibility adapter around the migrated `SpotRiskState` core instead of a
  duplicate risk-state implementation.
- `rustcta-strategy-cross-exchange-arbitrage` now contains the first real
  migrated adapter-free cross-exchange arbitrage domain slice beyond the thin
  wrapper: exchange/symbol/domain state DTOs, route and simulated bundle state,
  maker/taker fee modeling with negative maker support, funding direction
  modeling, and funding settlement ledger records. This is still a partial core
  migration; legacy config loading, runtime orchestration, tasks, execution
  planner/router wiring, market data, private sync, storage, dashboard, risk,
  position, and replay behavior remain in
  `src/strategies/cross_exchange_arbitrage` until SDK market-data/execution/app
  contracts replace those root-runtime paths.
- `rustcta-strategy-range-grid` now contains the first real migrated
  adapter-free range-grid core slice beyond the legacy module: range-grid
  config DTOs, indicator snapshots, `MarketRegime` and `RegimeDecision` DTOs,
  the range/trend classifier, pair runtime state, precision helpers, and
  deterministic grid order planning with min-notional and max-position budget
  handling. It now also absorbs the useful core semantics from the non-exported
  legacy `src/strategies/oscillation_grid.rs`: fixed-notional arithmetic and
  geometric grid planning, OSC-tagged client intents, local oscillation-grid
  state DTOs, and buy/sell fill transitions that replenish same-side orders
  while preserving the legacy simplified profit accounting. The legacy
  `range_grid/domain/classifier.rs` and
  `range_grid/infrastructure/planner.rs` modules are now compatibility adapters
  around the migrated `rustcta-strategy-range-grid` classifier and planner
  cores instead of duplicate range/trend and grid-plan implementations. This is still a partial core migration;
  legacy controller, symbol tasks, notifications, risk
  application, precision service, indicator service, websocket market data, and
  runtime execution remain in `src/strategies/range_grid` until SDK
  market-data/execution/app contracts replace those root-runtime paths.
- `rustcta-strategy-short-ladder-live` now contains the first real migrated
  adapter-free short-ladder-live core slice beyond the legacy module: live
  strategy/config DTOs, ladder/take-profit defaults, symbol/runtime state DTOs,
  adopted-short progress mapping, cumulative layer notional helpers, short
  ladder last-price inference, add-layer gating, max-notional capping, held-bar
  calculation, precision formatting helpers, short trailing take-profit state,
  maker price/quantity sizing helpers, dual-position params, pending initial
  order matching, short-ladder exit-order filtering, deterministic client order
  id construction, and layer-notional reconstruction. This is still a partial
  core migration; legacy market signal building, execution adapter calls,
  websocket exit loops, logging, task orchestration, and runtime state wiring
  remain in `src/strategies/short_ladder_live` until SDK market-data/execution
  contracts replace those root-runtime paths.
- `rustcta-strategy-poisson-market-maker` now contains the first real migrated
  adapter-free Poisson queue market-making core slice beyond the legacy module:
  strategy/account/trading/model/risk config DTOs, order-flow events, Poisson
  parameter initialization and EMA updates, order-slot state with client and
  exchange id indexes, precision helpers, refresh decisions, market activity
  factor, dynamic spread calculation, deterministic quote/order planning,
  counterpart cancel planning after fills, local risk limits and risk snapshot
  DTOs, stop-loss/inventory/daily-loss action classification, post-only price
  adjustment, legacy error classifiers, and stream-name construction. This is
  still a partial core migration; legacy controller runtime, WebSocket loops,
  account/position sync, real order submission/cancel, external risk evaluator
  wiring, webhook notifications, trade collection, and task orchestration
  remain in `src/strategies/poisson_market_maker` until SDK market-data,
  execution, and app contracts replace those root-runtime paths.
- `rustcta-strategy-trend` now contains the first real migrated adapter-free
  trend core slice beyond the legacy module: strategy/risk/indicator/signal/
  position/stop config DTOs, legacy-compatible config validation, trend signal
  DTOs, breakout and pullback trade-signal generation, signal quality gates,
  entry allocation and entry order planning, adapter-free position book state,
  dual-side position keys, position sizing, pyramid rules, PnL update/close
  transitions, adapter-free candle interval/event/snapshot DTOs, local
  CandleBook cache state for replace/append/trim/seed semantics, Bollinger/EMA/
  slope/Fibonacci/OFI/orderbook-tilt/regime helper functions, minimum-history
  and data-quality gates, adapter-free trend indicator output composition,
  adapter-free trend snapshot composition, and indicator event decision logic
  for symbol filtering, non-final timeframe filtering, snapshot presence,
  history gating, output composition, and publish timestamp selection,
  adapter-free monitoring performance metrics, trade-record accounting,
  drawdown tracking, Sharpe/recovery calculations, and performance report
  assembly, execution order planning DTOs, symbol precision helpers,
  maker/taker price selection, amount/price quantization, slippage calculation,
  and entry client-order-id construction,
  stop-manager DTOs, initial stop calculation, profit-lock, losing time-stop,
  breakeven, PnL/trailing-stop updates, partial take-profit checks, stop
  reports, risk-layer DTOs, four-layer risk evaluation, and trade approval
  checks. This is still a partial core migration; legacy strategy runtime,
  shared data ingestion, indicator service async subscription/cache-read/
  shared-data publish bridge, market feed WebSocket/bootstrap/broadcast runtime,
  execution engine exchange IO/retry/timeout/cancel/report wiring, order
  tracker, stop manager runtime, position sync, user stream, monitoring async
  loop/log writer/alert integration, webhook/status reports, and
  account-manager wiring remain in `src/strategies/trend` until SDK market-data,
  execution, and app contracts
  replace those root-runtime paths.
- `tools/ops` establishes the operator/tooling workspace area and records a
  tested migration matrix for current legacy `src/bin/*.rs` entrypoints. This
  keeps directory/runtime restructuring moving without expanding exchange API
  surface area in this batch.
- `apps/backtest` establishes the offline backtest/research app boundary with
  the `rustcta-backtest` binary. The legacy root `backtest` binary remains as a
  compatibility path.
- `crates/rustcta-backtest` now owns the app-facing backtest dependency
  boundary and the first extracted implementation modules:
  `rustcta_backtest::factors`, `rustcta_backtest::scoring`,
  `rustcta_backtest::schema`, `rustcta_backtest::data`,
  `rustcta_backtest::indicators`, `rustcta_backtest::replay`,
  `rustcta_backtest::symbol`, `rustcta_backtest::runtime_support`, and
  `rustcta_backtest::offline_runtime`, plus the root-free
  `rustcta_backtest::matching::book` order book state and
  `rustcta_backtest::matching::ledger` position/cash ledger and
  `rustcta_backtest::matching::engine` offline matching engine. The data/replay
  slice includes root-free Binance futures kline/depth/trade dataset readers
  and writers, depth/trade raw capture import, plus exchange metadata
  trading-pair snapshot readers and writers used by the new crate's
  replay/loading boundary. The offline runtime slice now owns local replay
  loading and kline partition planning without importing root `core` types; the
  legacy runtime facade delegates those read-only paths through compatibility
  includes. The symbol/runtime-support slice now owns
  root-free symbol normalization, Binance futures symbol conversion, venue
  constraint filtering, price/quantity rounding, latency duration, market type
  parsing, side inversion, and quote asset extraction used by the legacy
  runtime compatibility path. The indicators slice now
  calculates trend/volatility/volume series against `BacktestKline` without
  depending on root `core` types. The matching ledger slice now tracks fills,
  funding, realized/unrealized PnL, cash, and equity against backtest DTOs
  without importing root `core` types. The matching engine slice now handles
  offline market/limit order execution, queue priority, book matching, and
  ledger integration in the new crate while strategy runtimes remain legacy.
  The legacy `src/backtest/data` dataset, exchange metadata, indicator,
  matching ledger, and matching engine modules are preserved through
  compatibility includes, while live capture/network acquisition remains in the
  legacy data tree. The legacy
  `rustcta::backtest::factors`, `rustcta::backtest::scoring`,
  `rustcta::backtest::schema`, `rustcta::backtest::indicators`,
  `rustcta::backtest::replay`, `rustcta::backtest::matching::book`,
  `rustcta::backtest::matching::ledger`, and
  `rustcta::backtest::matching::engine` paths are preserved through
  compatibility includes, and the legacy
  `rustcta::backtest::runtime` facade is gated behind the temporary
  `legacy-runtime` feature. Backtest schema now uses legacy-compatible
  `BacktestKline`, `BacktestTrade`, `BacktestFee`, `BacktestOrderSide`, and
  `BacktestMarketType` DTOs from `rustcta-types` rather than depending on root
  `core` types in the new crate.
- `src/bin/backtest.rs` and `apps/backtest/src/main.rs` now share
  `rustcta::backtest::runtime::render_command_output`, so old and new backtest
  entrypoints cannot drift in operator summary text.
- `src/bin/short_ladder_mtf_grid.rs` is now a thin compatibility wrapper around
  `rustcta::backtest::runtime::short_ladder_mtf_grid`, and
  `rustcta-backtest short-ladder-mtf-grid` exposes the same grid command under
  the backtest app boundary without changing flags, CSV/Markdown output, or the
  final summary line.
- Fast-migration decision for the current batch: backtest is paused as an
  expansion target. Keep the extracted `rustcta-backtest` slices compiling and
  keep legacy compatibility binaries available, but do not make additional
  `src/backtest/*` extraction a blocker for the control/API/tools/supervisor
  migration. Operators can skip running backtest binaries while closing the
  production workspace migration.
- `rustcta-tools-ops` now owns the first migrated dry-run operator commands:
  `smart-money binance-collector`, `smart-money hyperliquid-wallet-ingestion`,
  and `smart-money portfolio-service`. The legacy root binaries remain thin
  wrappers around the same functions.
- `rustcta-tools-ops` now owns the public `ws-proxy-probe` connectivity probe
  without depending on the legacy root crate. The legacy `src/bin/ws_proxy_probe.rs`
  binary remains a thin compatibility wrapper around the same tool function.
- `rustcta-tools-ops` now owns the public/read-only
  `symbols gateio-bitget-spot` discovery command. The legacy
  `src/bin/gateio_bitget_spot_symbols.rs` binary remains a thin compatibility
  wrapper around the same implementation.
- `crates/rustcta-reporting` now owns the first non-root reporting helper:
  `trend_report`. `rustcta-tools-ops reporter trend` owns the operator command,
  and the legacy `src/bin/trend_report.rs` wrapper now forwards through the
  same `legacy_tools_ops_shim` pattern used by the other migrated tools,
  avoiding duplicate reporter wiring in the root binary while preserving the
  old command name.
- `rustcta-tools-ops reporter account-position render` now owns the safe
  account-position render-only slice. It reads a local
  `AccountPositionReportInput` JSON snapshot, uses `rustcta-reporting` to build
  the exposure report, and prints Markdown without exchange access, webhook
  sends, or a legacy root dependency.
- `tools/ops` migration now documents and preserves the Cargo dependency
  boundary that prevents a direct `rustcta-tools-ops -> rustcta` dependency
  while the legacy root package still depends on `rustcta-tools-ops` for thin
  compatibility wrappers. Root-dependent long-running/reporting binaries such
  as `account_position_reporter` remain legacy entries until their reusable
  helpers are extracted into non-root crates.
- `account_position_reporter` now has a root-free helper/provider slice in
  `crates/rustcta-reporting::account_position_report`: config defaults/YAML
  loading, balance and position DTOs, exposure aggregation, stablecoin/USDT
  helpers, ticker candidate pricing, `AccountPositionProvider`, and markdown
  rendering. The WeCom markdown payload/response validation and send helper now
  also live in `rustcta-reporting`, while the legacy binary still adapts
  `dyn rustcta::Exchange` into the provider boundary and owns `AccountManager`
  bootstrap, logger setup, HTTP client construction, and the reporting loop, so
  it is intentionally not wired into `tools/ops` yet.
- `rustcta-tools-ops` now exposes `verify-legacy-bins`, which dynamically scans
  `src/bin/*.rs` and fails if any legacy binary is unclassified or if the
  migration matrix contains stale entries.
- `rustcta-tools-ops legacy-bin-plan --target <target>` can print filtered
  migration slices for tools, backtest app, CLI, control API, strategy runtime, or
  legacy compatibility work.
- `rustcta-tools-ops` now exposes the public websocket probe through the grouped
  `probe ws-proxy` command while preserving the top-level `ws-proxy-probe`
  compatibility alias and legacy root wrapper.
- `rustcta-tools-ops` now has CLI smoke coverage for migration inventory,
  legacy-bin verification, migrated reporter help, and both websocket probe
  aliases. The smoke tests use the built `rustcta-tools-ops` binary and avoid
  public-network, private-account, canary, admin, or order paths.
- `apps/cli` now exposes the same migration inventory as the industrial CLI
  entrypoint: `rustcta-industrial migration legacy-bin-plan --target <target>`
  and `rustcta-industrial migration verify-legacy-bins --src-bin-dir src/bin`.
  This keeps the operator-facing industrial command root useful without
  depending on the legacy root crate.
- `apps/cli` now exposes the first legacy one-shot operator command bridge:
  `rustcta-industrial cross-arb preflight`. It preserves the legacy
  `cross_arb_preflight` flags (`--config`, `--private`/`--private-readonly`,
  timeout and sample controls) by delegating to the existing compatibility
  binary through a subprocess, so the CLI app does not gain a dependency on the
  legacy root crate.
- `apps/cli` now exposes offline supervisor spec helpers through
  `rustcta-industrial supervisor print-legacy-spec --template <template>` and
  `rustcta-industrial supervisor validate-spec --path <spec.json>`, plus
  read-only registry validation through
  `rustcta-industrial supervisor validate-registry --path <registry.json>`.
  These reuse `rustcta-supervisor` spec/registry builders and validators
  without starting processes, opening exchange connections, or touching
  credentials.
- `apps/cli` has the required acceptance shell around the next read-only
  supervisor closeout command:
  `rustcta-industrial supervisor readiness --spec-dir config/supervisor`. The
  CLI smoke now requires the command to emit JSON proving the five checked-in
  specs are reported in first-run order, are all `valid`, have
  `config_exists=true`, and keep `spot_spot_live_dry_run` marked
  `operator_gated=true` and `first_run=false`. This is an acceptance guard for
  the command implementation; it does not start processes, open exchange
  connections, touch credentials, or change strategy code.
- `apps/cli` now exposes low-risk ops aggregation through
  `rustcta-industrial ops smart-money ...`,
  `rustcta-industrial ops reporter account-position render`, and
  `rustcta-industrial ops symbols gateio-bitget-spot`. This reuses migrated
  `rustcta-tools-ops` helpers for dry-run summaries, local report rendering,
  and public symbol command wiring, while intentionally not exposing canary,
  admin, private-audit, order, cancel, or close commands.
- `apps/cli` also exposes read-only event-ledger inspection through
  `rustcta-industrial ledger validate --path <jsonl>` and
  `rustcta-industrial ledger summary --path <jsonl> --from-sequence <n>`.
  `validate` now returns a validation-shaped report with `valid=true` only
  after replay succeeds, while `summary` keeps the replay statistics and
  per-kind counts. Both commands use `rustcta-event-ledger::JsonlLedger`
  replay, accept missing files as empty ledgers, and rely on the ledger reader's
  secret-field rejection before reporting `secret_free=true`.
- `.github/workflows/non-gateway-industrial.yml` adds a focused CI gate for the
  current non-gateway industrial slice: migrated-file rustfmt checks,
  event-ledger/control-api/supervisor/tools/industrial-cli/backtest tests and
  checks, supervisor app contract tests, backtest and tools command help
  smokes including both `ws-proxy-probe` and `probe ws-proxy`, safe
  `rustcta-industrial` smokes for `doctor`, ledger validate, smart-money
  dry-run, account-position render help, and supervisor registry validation,
  the control API JSONL smoke, legacy bin matrix verification, and
  `scripts/check_industrial_boundaries.sh`. The control API smoke now starts
  `apps/control-api` against a temporary supervisor registry and legacy
  dashboard snapshot, then performs HTTP assertions for `/api/workspace`,
  `/api/strategies`, `/api/processes`, `/api/processes/:id`, `/api/symbols`,
  `/api/risk`, and `/api/strategy-logs`.
- `scripts/check_industrial_boundaries.sh` enforces the first dependency
  boundaries:
  - the focused non-gateway workflow must keep the migrated control-api,
    supervisor app, tools ops, and industrial CLI contract tests/smokes, plus
    the boundary check itself, so CI coverage cannot be silently weakened
  - strategy wrappers cannot depend on `rustcta-exchange-gateway`
  - strategy wrapper source cannot import old concrete exchange internals
  - strategy wrappers, `rustcta-control-api`, and `web-ui` cannot reference
    old concrete adapter paths such as `rustcta::exchanges::{mexc, coinex,
    gateio, bitget, binance, kucoin, okx, paper}` or
    `rustcta::exchanges::trading_adapters`
  - strategy wrappers, `rustcta-control-api`, and `web-ui` cannot reference
    gateway private adapter paths such as
    `rustcta_exchange_gateway::adapters`
  - control API cannot expose obvious secret fields by shape
  - raw exchange credential write/delete route helpers such as
    `/api/exchange-api-keys`, `exchange_api_key_store`, and env-store
    read/write helpers cannot be promoted into `apps/control-api` or the
    focused non-gateway workflow; legacy write/delete behavior stays frozen in
    `src/bin/control_api.rs` until a gateway/agent-owned credential
    administration service exists
  - strategy/execution crates do not read `.env` directly
  - tools cannot import concrete exchange adapter internals or become
    long-running service/runtime owners
  - legacy `src/bin/*.rs` entrypoints must remain classified in the
    `rustcta-tools-ops` migration matrix
  - the only allowed temporary legacy-root dependency under the industrial
    workspace is `crates/rustcta-backtest -> rustcta::backtest::runtime`, to be
    retired as backtest implementation modules move into the dedicated crate
- Legacy test/config drift found during the workspace migration was fixed:
  - cross-exchange arbitrage tests were updated to current live-small config
    semantics
  - spot-spot taker arbitrage tests were updated to current min-notional and
    depth semantics
  - live preflight CLI tests now use a valid two-exchange strategy pair
  - `config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml` now matches the
    manual VSN-only live flow by disabling automatic initial entry

## Verified

Commands already verified:

```bash
cargo test --workspace --all-features
cargo test -p rustcta-tools-ops
cargo run -q -p rustcta-tools-ops -- verify-legacy-bins --src-bin-dir src/bin
cargo run -q -p rustcta-tools-ops -- legacy-bin-plan --target tools
cargo run -q -p rustcta-tools-ops -- ws-proxy-probe --help
cargo run -q -p rustcta-tools-ops -- probe ws-proxy --help
cargo check -p rustcta-industrial-cli
scripts/control_api_smoke_test.sh
cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- migration verify-legacy-bins --src-bin-dir src/bin
cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- migration legacy-bin-plan --target tool-ops
cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- cross-arb preflight --help
cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- supervisor print-legacy-spec --template trend-report --strategy-id trend-report-cli --run-id run-cli --tenant-id local --config config/trend_report.yml --working-dir . --log-dir logs/supervisor --restart-backoff-ms 3000
cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- supervisor validate-spec --path config/supervisor/trend_report.spec.json
cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- supervisor validate-registry --path /tmp/rustcta-missing-registry.json
cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- supervisor readiness --spec-dir config/supervisor
cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- ledger validate --path /tmp/rustcta-missing-events.jsonl
cargo run -q -p rustcta-industrial-cli --bin rustcta-industrial -- ledger summary --path /tmp/rustcta-missing-events.jsonl --from-sequence 1
cargo check -p rustcta-control-api
cargo test -p rustcta-control-api
cargo check -p rustcta-control-api-app
RUSTCTA_CONTROL_API_BIND=127.0.0.1:0 RUSTCTA_CONTROL_API_AGENT_ID=local-agent RUSTCTA_CONTROL_API_TENANT_ID=local RUSTCTA_CONTROL_API_AUDIT_LEDGER_PATH=<tmp>/audit.jsonl RUSTCTA_CONTROL_API_SUPERVISOR_REGISTRY_PATH=<tmp>/registry.json RUSTCTA_CONTROL_API_STRATEGY_LOG_PATH=<tmp>/strategy.log RUSTCTA_CONTROL_API_STATIC_DIR=<tmp>/dist timeout 2s cargo run -q -p rustcta-control-api-app --bin rustcta-control-api
cargo test -p rustcta-event-ledger
cargo test -p rustcta-execution-router
cargo test -p rustcta-supervisor
cargo check -p rustcta-supervisor-app
cargo run -q -p rustcta-supervisor-app --bin rustcta-supervisor -- --print-legacy-spec cross-arb-live --strategy-id cross-live-main --run-id run-local --tenant-id local --config config/cross_exchange_arbitrage_usdt.yml --working-dir . --log-dir logs/supervisor --restart-backoff-ms 5000
cargo run -q -p rustcta-supervisor-app --bin rustcta-supervisor -- --validate-spec config/supervisor/cross_arb_live.spec.json
cargo run -q -p rustcta-supervisor-app --bin rustcta-supervisor -- --registry-path /tmp/rustcta-missing-registry.json --validate-registry
cargo run -q -p rustcta-supervisor-app --bin rustcta-supervisor -- --registry-path <tmp>/registry.json
cargo run -q -p rustcta-supervisor-app --bin rustcta-supervisor -- --registry-path <tmp>/registry.json --mark-stale-heartbeats-ms 1
timeout 2s cargo run -q -p rustcta-supervisor-app --bin rustcta-supervisor -- --serve --bind 127.0.0.1:0 --registry-path <tmp>/registry.json
```

Current directory-boundary slice validation can still use focused non-gateway
checks while the rest of the workspace migration continues in parallel:

```bash
rustfmt --edition 2021 apps/backtest/src/main.rs src/bin/backtest.rs src/bin/short_ladder_mtf_grid.rs src/backtest/runtime/mod.rs src/backtest/runtime/short_ladder_mtf_grid.rs tools/ops/src/lib.rs tools/ops/src/main.rs tools/ops/src/ws_proxy_probe.rs src/bin/smart_money_binance_collector.rs src/bin/smart_money_hyperliquid_wallet_ingestion.rs src/bin/smart_money_portfolio_service.rs src/bin/ws_proxy_probe.rs tests/backtest_cli.rs
cargo check -p rustcta-tools-ops
cargo test -p rustcta-tools-ops
cargo check -p rustcta-backtest
cargo check -p rustcta-backtest-app --bin rustcta-backtest
cargo check --bin backtest
cargo check --bin short_ladder_mtf_grid
cargo check --bin ws_proxy_probe
cargo check --bin gateio_bitget_spot_symbols
cargo run -q -p rustcta-backtest-app --bin rustcta-backtest -- --help
cargo run -q -p rustcta-backtest-app --bin rustcta-backtest -- short-ladder-mtf-grid --help
cargo run -q --bin short_ladder_mtf_grid -- --help
cargo test --test backtest_cli
cargo check -p rustcta-control-api
cargo test -p rustcta-control-api
cargo check -p rustcta-control-api-app
RUSTCTA_CONTROL_API_BIND=127.0.0.1:0 RUSTCTA_CONTROL_API_AGENT_ID=local-agent RUSTCTA_CONTROL_API_TENANT_ID=local RUSTCTA_CONTROL_API_LEGACY_SNAPSHOT_PATH=<tmp>/dashboard_snapshot.json RUSTCTA_CONTROL_API_AUDIT_LEDGER_PATH=<tmp>/audit.jsonl RUSTCTA_CONTROL_API_SUPERVISOR_REGISTRY_PATH=<tmp>/registry.json RUSTCTA_CONTROL_API_STRATEGY_LOG_PATH=<tmp>/strategy.log RUSTCTA_CONTROL_API_STATIC_DIR=<tmp>/dist timeout 2s cargo run -q -p rustcta-control-api-app --bin rustcta-control-api
cargo run -q -p rustcta-tools-ops -- verify-legacy-bins --src-bin-dir src/bin
cargo run -q -p rustcta-tools-ops -- legacy-bin-plan --target tools
cargo run -q -p rustcta-tools-ops -- smart-money binance-collector --config config/smart_money.yml
cargo run -q -p rustcta-tools-ops -- smart-money hyperliquid-wallet-ingestion --config config/smart_money.yml
cargo run -q -p rustcta-tools-ops -- smart-money portfolio-service --config config/smart_money.yml
cargo run -q -p rustcta-tools-ops -- symbols gateio-bitget-spot --help
cargo run -q -p rustcta-tools-ops -- ws-proxy-probe --help
cargo run -q --bin smart_money_binance_collector -- --config config/smart_money.yml
cargo run -q --bin smart_money_hyperliquid_wallet_ingestion -- --config config/smart_money.yml
cargo run -q --bin smart_money_portfolio_service -- --config config/smart_money.yml
cargo run -q --bin gateio_bitget_spot_symbols -- --help
cargo run -q --bin ws_proxy_probe -- --help
scripts/check_industrial_boundaries.sh
```

Latest focused result for this non-gateway slice: passed.

Latest local validation for the current non-gateway boundary slice:

```bash
rustfmt --edition 2021 --check crates/rustcta-backtest/src/offline_runtime.rs crates/rustcta-backtest/src/lib.rs src/backtest/offline_runtime.rs src/backtest/mod.rs src/backtest/runtime/mod.rs
cargo test -p rustcta-control-api
cargo test -p rustcta-execution-router
cargo test -p rustcta-backtest
cargo check -p rustcta-backtest --features legacy-runtime
cargo test --test backtest_runtime --test backtest_cli
cargo test -p rustcta-reporting
cargo test -p rustcta-tools-ops
cargo check -p rustcta-supervisor-app
cargo run -q -p rustcta-supervisor-app --bin rustcta-supervisor -- --registry-path <tmp>/registry.json --mark-stale-heartbeats-ms 1
cargo check --bin account_position_reporter
scripts/check_industrial_boundaries.sh
```

These checks confirm that `rustcta-control-api` remains status-only for
credentials, `rustcta-backtest` owns the current root-free backtest slices
including offline replay loading, and the current reporting/tools extraction
still compiles without a `rustcta-tools-ops -> rustcta` dependency cycle.

`cargo fmt --all --check`, `cargo check --workspace`, and
`cargo test --workspace --all-features` should be re-run after the remaining
parallel non-gateway slices settle, because legacy root/app boundaries are
still changing outside this gateway workstream.

Gateway extraction verification:

```bash
cargo fmt -p rustcta-exchange-api -p rustcta-exchange-gateway -p rustcta-execution-router -p rustcta-gateway --check
cargo test -p rustcta-exchange-api -p rustcta-exchange-gateway -p rustcta-execution-router -p rustcta-gateway
cargo test -p rustcta-gateway
cargo test -p rustcta-exchange-gateway
cargo check -p rustcta-gateway
cargo test -p rustcta-execution-router
```

Latest focused local gateway verification:

```bash
cargo fmt -p rustcta-exchange-api -p rustcta-exchange-gateway -p rustcta-gateway --check
cargo check -p rustcta-gateway
cargo test -p rustcta-exchange-gateway
cargo test -p rustcta-gateway
```

These checks passed after the default paper gateway path was extended to route
batch place, batch cancel, and cancel-all through `AdapterBackedGateway`.

Focused verification also passed for the new architecture crates and strategy
wrappers:

```bash
cargo test -p rustcta-types -p rustcta-exchange-api -p rustcta-execution-api -p rustcta-strategy-sdk -p rustcta-exchange-gateway -p rustcta-execution-router -p rustcta-event-ledger -p rustcta-supervisor -p rustcta-control-api -p rustcta-core-compat -p rustcta-strategy-avellaneda-stoikov -p rustcta-strategy-spot-spot-arbitrage -p rustcta-strategy-cross-exchange-arbitrage -p rustcta-strategy-funding-arbitrage -p rustcta-strategy-hedged-grid -p rustcta-strategy-mean-reversion -p rustcta-strategy-poisson-market-maker -p rustcta-strategy-range-grid -p rustcta-strategy-short-ladder-live -p rustcta-strategy-trend
```

## Remaining Work

The full industrial migration is not complete yet. The remaining work is the
rest of the target-state architecture, not optional polish.

Required next implementation batches:

- Continue directory/runtime boundary migration by moving legacy `src/bin/*.rs`
  entrypoints into `apps/`, `tools/`, or independent strategy runtime crates
  according to the `rustcta-tools-ops` migration matrix.
- Continue `tools/ops` migration only with bounded non-root tools. The
  low-risk public/read-only and dry-run first wave is already migrated:
  smart-money dry-run summaries, `ws_proxy_probe`,
  `gateio_bitget_spot_symbols`, `trend_report`, and account-position
  render-only reporting. Defer root-dependent live reporters, live
  canary/admin commands, and private audit commands until their non-root helper
  crates or safety gates are migrated explicitly.
- Pause further backtest extraction for the fast migration batch. The current
  `crates/rustcta-backtest` slices are tested and should stay green, but
  remaining root-heavy backtest areas such as live dataset capture, strategy
  runtimes, and networked acquisition paths are no longer blockers for closing
  the production workspace migration.
- Move existing execution router, fee model, live-dry-run, reservations,
  idempotency, and reconciliation into `rustcta-execution-router`.
- Extend event ledger writes from the new typed execution path to every
  remaining legacy/runtime mutation path, and choose the production durable
  backend after the JSONL baseline has covered local replay/audit workflows.
- Migrate real strategies into independent crates using `rustcta-strategy-sdk`,
  not only wrapper shells.
  - Current status: ten strategy crates (`mean-reversion`, `hedged-grid`,
    `funding-arbitrage`, `spot-spot-arbitrage`, `cross-exchange-arbitrage`,
    `range-grid`, `short-ladder-live`, `poisson-market-maker`, and
    `trend`, plus `avellaneda-stoikov`) now have
    adapter-free planning/core or domain slices, and the strict wrapper audit no
    longer finds wrapper-only strategy crates.
    `funding-arbitrage` additionally has an adapter-free live-plan slice,
    `hedged-grid` now has adapter-free synchronous `GridEngine`
    rebuild/fill-roll/follow/reconcile/near-gap repair slices plus underwater
    close, strict pairing, same-price repeated fill, and pending-open budget
    pressure coverage, and `range-grid` has adapter-free config/classifier/grid
    planner coverage plus absorbed legacy oscillation-grid fixed/geometric plan
    and fill-state coverage. `short-ladder-live` now has adapter-free ladder model,
    precision, sizing, pending-order matching, exit-order filtering, and client
    order id coverage. `avellaneda-stoikov` now has adapter-free config,
    market-rule, quote-formula, order-sizing, fill-accounting, and risk-snapshot
    coverage. `poisson-market-maker` now has adapter-free config, order-flow,
    Poisson parameter, order-slot state, spread, order-plan, risk-snapshot, and
    classifier coverage. `spot-spot-arbitrage` now also has adapter-free
    risk-state/cooldown/blacklist coverage. `trend` now has adapter-free config validation,
    signal-generation, entry-allocation/order-plan, position-book/sizing,
    pyramid, PnL, candle interval/event/snapshot DTOs, CandleBook cache
    replace/append/trim/seed semantics, Bollinger/EMA/slope/Fibonacci/OFI/
    tilt/regime helpers, minimum-history and data-quality gates, trend
    indicator-output and trend-snapshot composition, indicator event decision
    coverage, monitoring performance metrics/reporting core, execution order
    planning/precision/slippage/client-id coverage, stop-state, partial
    take-profit, risk-layer, and trade-approval coverage. The legacy
    `src/strategies/oscillation_grid.rs` file is not an exported runtime target;
    its useful fixed-grid/geometric-grid core has been absorbed into
    `rustcta-strategy-range-grid` instead of creating a separate strategy crate.
    These crates are still partial core migrations; live
    runtime orchestration, execution, market data, storage, and integration
    paths remain rooted in the legacy `src/strategies/*` modules and must not
    be treated as fully migrated strategy implementations.
  - `RUSTCTA_STRICT_STRATEGY_MIGRATION=1 scripts/check_industrial_boundaries.sh`
    enables the strict audit gate that fails while wrapper-only strategy crates
    remain. It now passes for the current strategy crate set, but it is only a
    wrapper-shell audit; the default boundary check remains a
    dependency/ownership guard for mixed migration batches.
- Extend the supervisor process lifecycle beyond the current start/stop/restart
  foundation: heartbeat collection from strategy runtimes, runtime snapshot
  collection, crash recovery, deployment-specific spec promotion, and
  authenticated lifecycle mutation APIs. Control API can already expose
  supervisor-registry-backed process and strategy views, tail configured
  process logs from registry data, and read legacy snapshot-backed symbols.
  `apps/supervisor` has a read-only service mode, and legacy process specs can
  be printed or loaded locally, but recovery policies and runtime snapshot
  collection still need deployment-level decisions.
- Split the existing large `src/bin/control_api.rs` into
  `apps/control-api` process/static-host wiring and
  `rustcta-control-api` route/service/model modules. Current audit:
  `apps/control-api` already owns env-based app wiring, optional static SPA
  hosting, legacy dashboard snapshot following, supervisor registry following,
  audit JSONL wiring, and strategy log tail wiring. `rustcta-control-api`
  already owns the generic, secret-free workspace/agents/strategies/processes,
  status/config, risk/fees/logs, events/audit, credential-status, and
  supervisor-backed read models. The legacy binary still owns the large
  web-console contract surface: legacy dashboard routes for exchanges, symbols,
  books, opportunities, trades, inventory, balance history, disabled state,
  dry-run plans, recorder/preflight/reconciliation/kill-switch summaries,
  spot/cross-arb dashboards, scanner, hedge-policy, runtime-publisher,
  symbol-control read models, strategy config editing/restart-script plumbing,
  static hosting defaults, SSE/event polling, and legacy control mutation
  aliases. It also still contains raw exchange API key env-store write/delete
  paths; those are frozen legacy behavior and should not be promoted into the
  new public control API surface.
- Convert the Dioxus UI to a generic multi-strategy operator workspace.
- Promote the event/order/fill/account/audit ledger from the current JSONL
  durable baseline to the selected production store when deployment
  requirements are fixed.
- Retire legacy control/UI key write paths and route production secret
  administration through gateway-owned local paths. The gateway app now owns
  adapter construction, local env-based private credential loading, redacted
  config debug output, `GatewayOnly` status, and a complete in-memory paper
  mutation surface for local end-to-end runs; remaining key-management work is
  outside the gateway crate boundary.
- Add CI gates for workspace check, tests, clippy, boundary checks, and web
  build. A focused non-gateway CI gate now exists; the full workspace/clippy/web
  gate still needs to be enabled after the remaining parallel migration slices
  settle.
- Run and either pass or explicitly document:
  - `cargo clippy --workspace --all-targets --all-features`
  - web UI release build
  - local end-to-end paper run through gateway, execution router, supervisor,
    control API, and web UI
  - venue-specific gateway live/read-only validation for migrated concrete
    adapters when deployment credentials and network access are available

## Next Batch

Continue the same full migration program with:

```text
industrial-workspace-migration-directory-boundaries
```

Scope:

- Can parallelize now:
  - add CLI smoke/contract coverage for the industrial CLI command tree
    (`migration`, `ledger`, `supervisor`, `ops`, and the existing
    `cross-arb preflight` bridge) without running public-network or private
    account paths
  - split the next control-api work into these bounded sub-tasks:
    route/model extraction for secret-free legacy dashboard read routes into
    `crates/rustcta-control-api`; static hosting and app wiring parity in
    `apps/control-api`; legacy compatibility alias tests for status/config,
    events/audit, logs, and selected dashboard read routes; secret-free
    regression tests that reject raw key/secret/passphrase fields in new API
    responses; and a control panel coordination task to freeze route names and
    DTO shapes with the web UI agent before moving panel-facing endpoints
  - extend boundary/CI checks around classified legacy bins and app/tool
    command help without touching exchange gateway crates
- Requires coordination:
  - web UI multi-strategy work, because another agent is actively changing the
    control panel and it shares `rustcta-control-api` route contracts
  - control panel endpoint migration from legacy `src/bin/control_api.rs`,
    because static hosting and panel-facing DTOs must stay compatible with the
    web UI agent's current work; coordinate especially before moving scanner,
    hedge-policy, symbol-control, runtime-publisher, and spot/cross-arb
    dashboard routes
  - strategy runtime migration, because another agent is moving strategy logic
    and live orchestration still depends on legacy `src/strategies/*` modules
  - production event-ledger backend selection, because deployment requirements
    and mutation-path ownership are not final
- Paused for this fast batch:
  - further root-heavy backtest extraction, except compile fixes needed to keep
    the already migrated compatibility paths green
- Do not assign in this non-gateway batch:
  - exchange gateway feature work or gateway-private adapter changes
  - raw credential key write/delete path promotion, including
    `/api/exchange-api-keys` POST/DELETE behavior; keep the new control API
    status-only until a gateway/agent-owned credential administration path is
    designed
  - strategy runtime migration, except interface contracts explicitly owned by
    the strategy migration agent
  - root-heavy backtest extraction beyond compile fixes for the existing
    compatibility slices
  - live-order canary/admin/private-audit migrations unless their helper crate,
    dry-run refusal, confirmation gates, and operator-approved validation plan
    are split first
