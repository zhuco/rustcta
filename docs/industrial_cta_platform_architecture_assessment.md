# Industrial CTA Platform Architecture Assessment

Assessment date: 2026-06-06

This document evaluates the current RustCTA architecture from a professional
software engineering and CTA trading-system engineering perspective. It is a
target-state and migration guide for turning the current single-repository CTA
system into an industrial multi-strategy, multi-process, AI-parallelized
platform.

## Executive Summary

The current architecture is a usable foundation. It already has the hard pieces
that many early CTA systems lack:

- multi-exchange adapters and a unified exchange contract
- execution dry-run and live-dry-run gates
- preflight, kill switch, disabled-symbol controls, and reconciliation concepts
- separated browser control panel and sanitized runtime snapshots
- strategy modules, scanner modules, and operational CLIs

The architecture should not be rewritten from scratch. It should be migrated
into a workspace and process-oriented platform with strict domain boundaries.

The main missing industrial-grade pieces are:

- a stable public API surface for open-source users
- a standalone exchange gateway that is the only component allowed to hold
  exchange credentials
- a standalone execution API and router that owns order semantics, risk gates,
  reservations, and reconciliation
- a strategy SDK so independently developed strategies cannot reach into
  exchange internals
- a supervisor that manages multiple strategy processes and run lifecycles
- a multi-strategy control API and web panel
- a durable event, order, fill, account, and audit ledger
- explicit tenant, account, run, and strategy identity in every command/event
- AI development ownership boundaries and contract tests

The recommended migration is a one-time workspace migration with compatibility
shims only where they reduce risk. The target shape should be created in one
pass, while feature migration can be validated crate by crate.

## Current Architecture Assessment

### Strengths

The current repository already separates several critical concerns:

- `retired exchange tree/` contains venue-specific exchange code.
- `retired exchange tree/unified.rs` contains the unified Spot-oriented
  `ExchangeClient` contract.
- `src/execution/` contains order commands, execution routing, fee model,
  reconciliation, and live-dry-run planning.
- `src/control/spot_control/` contains runtime safety controls, lifecycle
  state, audit, and snapshots.
- `src/web/` contains sanitized read models.
- `web-ui/dioxus/` is already separated from the strategy runtime.
- `retired root bin directory/control_api.rs` already serves the web panel and reads sanitized
  snapshots.

These are useful boundaries and should be preserved.

### Main Weaknesses

The current structure is still a monolith from an industrial platform
perspective:

- `src/lib.rs` exposes broad internal modules and re-exports, making it hard to
  define a stable open-source SDK boundary.
- `ExchangeClient` and `TradingAdapter` both represent exchange-facing
  behavior, but their responsibilities are not yet formally separated.
- `control_api` mixes read-only dashboard APIs, local API key management,
  strategy YAML editing, process restart, history persistence, and command
  queue behavior in one large binary.
- Strategy code lives inside one crate and can still depend on too much
  internal implementation detail.
- API keys are moving into local control paths, but there is not yet a hard
  process boundary that makes the exchange gateway the only credential owner.
- Multi-strategy and multi-process behavior is not yet a first-class platform
  model.
- Persistent execution state is not yet treated as an append-only source of
  truth.
- There is no formal AI contributor ownership model at crate/directory level.

These issues do not invalidate the current system. They identify the next
architecture boundary that must be established before the project becomes an
industrial platform.

## Target Architecture

The target architecture should separate the system into four planes:

1. Data plane: market data, exchange connectivity, account reads, websocket
   streams, rate limits, normalized exchange events.
2. Execution plane: order intent, risk gates, reservations, routing,
   reconciliation, execution events.
3. Strategy plane: strategy logic, config schema, snapshots, commands, and
   process lifecycle.
4. Control plane: supervisor, dashboard, audit, logs, operator commands, and
   SaaS/local agent integration.

Recommended repository layout:

```text
crates/
  rustcta-types/
  rustcta-exchange-api/
  rustcta-exchange-gateway/
  rustcta-execution-api/
  rustcta-execution-router/
  rustcta-strategy-sdk/
  rustcta-supervisor/
  rustcta-control-api/
  retired-core-compat/

strategies/
  spot-spot-arbitrage/
  cross-exchange-arbitrage/
  funding-arbitrage/
  hedged-grid/
  mean-reversion/

apps/
  gateway/
  supervisor/
  control-api/
  cli/

web-ui/
  dioxus/
```

The old `src/` tree may remain temporarily through `retired-core-compat`, but it
should stop being the architectural center.

## Required Domain Boundaries

### rustcta-types

Purpose: stable, dependency-light domain types.

Must contain:

- exchange id, account id, tenant id, strategy id, run id
- canonical symbol and exchange symbol
- market type, order side, order type, time-in-force, position side
- order status, fill status, liquidity role
- balances, positions, fees, order book snapshot, fills
- error classes and event envelopes
- capability models

Rules:

- no dependency on adapters, strategies, web, or supervisor
- schema-version all externally persisted or networked types
- every external command/event should include `schema_version`

### rustcta-exchange-api

Purpose: stable exchange SDK contract.

Must define:

- `ExchangeClient`
- market data requests/responses
- account requests/responses
- order mutation requests/responses
- user stream events
- exchange error classification
- capability declarations

Rules:

- no strategy dependency
- no web dependency
- no supervisor dependency
- no direct process management

### rustcta-exchange-gateway

Purpose: only credential-owning process and only low-level exchange connector.

Must own:

- exchange API keys and secret loading
- signing
- REST clients
- websocket clients
- venue-specific rate limits
- exchange reconnect logic
- normalized market data and private events
- account and symbol rule cache
- exchange-specific request validation

Must expose:

- local gateway server
- typed gateway client
- health and capability APIs
- market data subscriptions
- private account/order/fill stream
- order mutation API

Rules:

- strategies cannot import venue adapter modules
- control API cannot import venue adapter modules
- web UI cannot access credentials
- cloud SaaS should not hold user exchange credentials in the first production
  stage

### rustcta-execution-api

Purpose: stable execution command and event protocol.

Every order command must include:

- `tenant_id`
- `account_id`
- `strategy_id`
- `run_id`
- `client_order_id`
- `idempotency_key`
- `risk_profile_id`
- `requested_at`

Must define:

- `TradingAdapter`
- `OrderCommand`
- `OrderAck`
- `CancelCommand`
- `CancelAck`
- `ExecutionEvent`
- `FillEvent`
- `ReconciliationEvent`
- `RiskDecision`

Rules:

- the API is strategy-facing and gateway-facing
- it should not contain venue-specific signing or REST details
- all command types must be serializable for audit and replay

### rustcta-execution-router

Purpose: industrial order path and safety boundary.

Must own:

- dry-run and live-dry-run behavior
- kill switch checks
- preflight checks
- symbol disable checks
- account reservation checks
- max notional and max order-rate checks
- client order id generation and validation policy
- fee model lookup
- order reconciliation
- stale book rejection
- idempotency cache

Rules:

- router talks to the gateway through a gateway/execution client
- router does not directly hold API keys
- router persists order lifecycle events before and after gateway calls
- router can replay events into a deterministic state view

### rustcta-strategy-sdk

Purpose: safe strategy development interface.

Must define:

```rust
trait StrategyRuntime {
    fn spec(&self) -> StrategySpec;
    async fn start(&mut self, ctx: StrategyContext) -> anyhow::Result<()>;
    async fn stop(&mut self) -> anyhow::Result<()>;
    async fn handle_event(&mut self, event: StrategyEvent) -> anyhow::Result<()>;
    async fn snapshot(&self) -> anyhow::Result<StrategySnapshot>;
}
```

Must also define:

- `StrategySpec`
- config schema
- snapshot schema
- supported command schema
- risk capability declaration
- market data subscription declaration
- required account permissions

Rules:

- strategy crates cannot read `.env`
- strategy crates cannot import exchange adapters
- strategy crates cannot call REST endpoints directly
- strategy crates submit intent through `StrategyContext`
- all strategy snapshots must be secret-free

### rustcta-supervisor

Purpose: multi-strategy, multi-process lifecycle control.

Must own:

- strategy instance registry
- process start, stop, restart
- run id creation
- process health checks
- log tailing
- snapshot collection
- command routing
- crash recovery
- restart backoff
- lock files and pid files

Each strategy instance should have:

- `strategy_id`
- `strategy_kind`
- `run_id`
- `tenant_id`
- `config_path`
- `status`
- `process_id`
- `started_at`
- `last_heartbeat_at`
- `last_snapshot_at`

Rules:

- supervisor does not hold exchange credentials
- supervisor may start local gateway and strategy processes
- supervisor is the source of truth for process lifecycle state

### rustcta-control-api

Purpose: operator and web API.

Must own:

- authenticated read APIs
- authenticated operator command APIs
- strategy list and detail views
- gateway status view
- process status view
- event stream
- audit log API
- config schema API
- local-only admin routes when needed

Should expose:

```text
GET  /api/strategies
GET  /api/strategies/:id
GET  /api/strategies/:id/snapshot
GET  /api/strategies/:id/config-schema
POST /api/strategies/:id/start
POST /api/strategies/:id/stop
POST /api/strategies/:id/restart
POST /api/strategies/:id/command
GET  /api/processes
GET  /api/gateway/status
GET  /api/events
GET  /api/audit
```

Rules:

- no direct exchange API calls
- no direct venue adapter imports
- secret-store routes must be local-only or gateway-admin-only
- writes require auth, idempotency keys, audit records, and expected versions

### web-ui/dioxus

Purpose: pure multi-strategy operator UI.

Must show:

- strategy instances
- strategy detail
- strategy config from schema
- process health
- gateway status
- account and inventory read models
- risk and kill switch state
- event stream and logs

Rules:

- no hard dependency on one strategy as the navigation center
- strategy-specific advanced pages are extensions, not the platform baseline
- no secret display except masked configured/not-configured status
- no raw private exchange payload rendering

## Additional Engineering Changes Required

### 1. Durable Event Ledger

An industrial CTA system needs an append-only event ledger.

Minimum event families:

- `StrategyLifecycleEvent`
- `StrategySnapshotEvent`
- `MarketDataHealthEvent`
- `OrderCommandEvent`
- `OrderAckEvent`
- `CancelCommandEvent`
- `FillEvent`
- `BalanceSnapshotEvent`
- `PositionSnapshotEvent`
- `RiskDecisionEvent`
- `ReconciliationEvent`
- `OperatorCommandEvent`
- `AuditEvent`

Recommended storage split:

- SQLite or Postgres for operational commands, audit, orders, fills, accounts,
  and process registry.
- ClickHouse for high-volume market data, opportunities, scanner outputs, and
  analytics.
- JSONL only for local fallback and emergency debugging, not as the long-term
  system of record.

### 2. Account and Capital Model

The current system should be extended with a first-class account model:

- tenant
- user
- exchange
- exchange account id
- internal account id
- subaccount
- permission profile
- capital allocation
- strategy allocation
- reserved balance
- unmanaged inventory
- manual inventory

Without this model, multi-strategy execution can double-spend balances or
liquidate inventory owned by another strategy.

### 3. Idempotency and Order Identity

All mutation paths need formal idempotency.

Required ids:

- `command_id`
- `idempotency_key`
- `client_order_id`
- `exchange_order_id`
- `strategy_id`
- `run_id`
- `account_id`
- `correlation_id`

Rules:

- retrying the same command must not create a second unintended order
- client order ids must encode enough source context for reconciliation
- idempotency decisions must be persisted before gateway mutation

### 4. Risk Engine Boundary

Risk should be a formal engine, not scattered checks.

Required risk layers:

- global kill switch
- tenant/account kill switch
- strategy kill switch
- symbol and exchange disable registry
- stale book rejection
- max notional per order
- max notional per symbol
- max notional per account
- max open orders
- max order rate
- max drawdown
- max inventory concentration
- unsupported capability rejection
- unmanaged inventory protection

Each risk decision should be persisted with:

- decision id
- input summary
- pass/fail
- blockers
- timestamp
- source strategy and run id

### 5. Gateway Protocol

The gateway should initially use a simple local protocol before introducing
more infrastructure.

Recommended first protocol:

- HTTP JSON for request/response operations
- SSE or WebSocket for events
- versioned request and response models
- bearer token or local socket auth
- strict localhost binding by default

gRPC can be considered later if latency, schema governance, or language
interop require it.

### 6. Observability

Industrial operation requires standard telemetry.

Required metrics:

- websocket connection state
- reconnect count
- REST latency by exchange and endpoint
- rate-limit usage
- book age
- order submit latency
- order ack latency
- cancel latency
- fill lag
- reconciliation lag
- risk rejection count
- strategy heartbeat age
- process restart count

Required logs:

- structured JSON logs
- no secrets
- `tenant_id`, `strategy_id`, `run_id`, `account_id`, `exchange`, `symbol`
  context where applicable

### 7. Schema Versioning

Every persisted or networked model needs versioning:

- event envelopes
- strategy config
- strategy snapshot
- gateway request/response
- execution command/event
- control API responses

This allows strategies, gateway, and web UI to evolve independently.

### 8. Testing Matrix

Minimum industrial testing matrix:

- unit tests for pure domain models
- request-spec tests for every exchange private REST endpoint
- parser fixture tests for every websocket private/public payload
- dry-run route tests proving no adapter side effects
- risk rejection tests
- idempotency tests
- reconciliation replay tests
- supervisor process lifecycle tests
- control API route tests
- web build test
- end-to-end local paper run
- live read-only validation harness
- small-live canary before real trading expansion

Every exchange adapter change must include:

- signed request mock coverage
- error classification coverage
- capability update if behavior changes
- parser fixture if websocket behavior changes

### 9. AI Parallel Development Governance

The target architecture must make AI contributors safe by construction.

Recommended ownership:

- Exchange AI: only `rustcta-exchange-gateway/src/adapters/<exchange>/` and
  related tests/fixtures.
- Strategy AI: only `strategies/<strategy>/` and strategy-specific docs/tests.
- Web AI: only `web-ui/dioxus/` and control API view models.
- Platform AI: `rustcta-types`, `rustcta-execution-api`,
  `rustcta-strategy-sdk`, supervisor, and compatibility shims.
- Integration AI: workspace build, docs, CI, migration cleanup.

Each major directory should have an `AGENTS.md` defining:

- allowed edit scope
- forbidden dependencies
- required tests
- public contracts that cannot be changed without coordination
- examples of accepted patterns

## Migration Strategy

The migration should be done as a single target-state restructuring, but with
clear validation stages.

### Stage 1: Workspace Foundation

Create workspace crates and move stable types first:

- `rustcta-types`
- `rustcta-exchange-api`
- `rustcta-execution-api`
- `rustcta-strategy-sdk`

Keep compatibility exports so the old code compiles during migration.

### Stage 2: Gateway Extraction

Move exchange adapters into `rustcta-exchange-gateway`.

Deliverables:

- gateway library
- gateway binary
- typed local client
- adapter tests migrated and passing
- credential loading only in gateway/admin paths

### Stage 3: Execution Extraction

Move router, dry-run, preflight integration, fee model, reservations, and
reconciliation into `rustcta-execution-router`.

Deliverables:

- execution router crate
- event ledger write path
- idempotency support
- gateway-client-backed execution path

### Stage 4: Strategy SDK and Strategy Migration

Migrate strategies into independent crates under `strategies/`.

First migration candidates:

- `spot-spot-arbitrage`
- `cross-exchange-arbitrage`
- `funding-arbitrage`

Each migrated strategy must:

- expose `StrategySpec`
- expose config schema
- expose snapshot schema
- use `StrategyContext`
- avoid direct exchange adapter imports

### Stage 5: Supervisor and Multi-Process Runtime

Add `rustcta-supervisor`.

Deliverables:

- process registry
- lifecycle commands
- heartbeat and snapshot collection
- log tailing
- crash recovery
- restart backoff

### Stage 6: Control API and Web Multi-Strategy UI

Split current `control_api` into route/service/model modules under
`rustcta-control-api`.

Then update the Dioxus UI to use generic strategy APIs as the baseline.

### Stage 7: Compatibility Cleanup

Remove or downgrade legacy monolith entrypoints only after:

- workspace check passes
- migrated strategies run
- control API can list multiple strategies
- gateway is the only credential owner
- old docs are updated or superseded

## Acceptance Criteria

The industrial migration is not complete until all conditions below are true:

- `cargo fmt` passes.
- `cargo check --workspace` passes.
- `cargo test --workspace --all-features` passes.
- `cargo clippy --workspace --all-targets --all-features` passes or all
  remaining warnings are explicitly documented.
- Web UI release build passes.
- Gateway can run as a standalone process.
- Supervisor can start, stop, and restart at least one strategy process.
- Control API can display multiple strategy instances.
- A strategy process can submit dry-run orders through execution router and
  gateway client.
- No strategy crate imports concrete exchange adapter modules.
- Web UI and control API do not access raw exchange secrets.
- All order commands include strategy, run, account, and idempotency identity.
- All execution mutations produce durable audit/order events.
- Reconciliation can rebuild order state from persisted events plus exchange
  readback.

## Priority Matrix

### P0: Must Fix During Industrial Migration

- Workspace crate boundaries.
- Credential boundary: gateway is the only exchange-secret owner.
- Stable `types`, `exchange-api`, `execution-api`, and `strategy-sdk`.
- Multi-strategy identity fields in all execution commands.
- Persistent order/fill/audit/event ledger.
- Control API split from the current large binary.
- Supervisor for multi-process lifecycle.
- Strategy crates forbidden from direct exchange adapter access.

### P1: Should Fix Before Public Launch

- Schema versioning for all persisted and networked payloads.
- Directory-level `AGENTS.md` files for AI development.
- Structured telemetry and metrics.
- Config schema driven web forms.
- Local-only gateway admin for API keys.
- Live read-only validation reports per exchange.
- CI workflow for workspace check, test, clippy, and web build.

### P2: Can Follow After Platform Stabilizes

- SaaS cloud control plane.
- Remote agent registration.
- Hosted marketplace for strategy templates.
- gRPC protocol if HTTP/SSE becomes limiting.
- Plugin sandboxing beyond process isolation.
- Paid strategy distribution and licensing.

## Final Recommendation

The project is ready for a full industrial architecture migration. This
document is the complete target-state refactor plan, not a first-milestone-only
plan.

The correct overall program name is:

```text
industrial-workspace-migration
```

The program is complete only when the monolithic architecture has been replaced
by the target workspace and process model:

- stable shared type crate
- exchange API crate
- standalone exchange gateway and gateway app
- execution API crate
- execution router crate
- strategy SDK crate
- independent strategy crates
- supervisor crate and supervisor app
- split control API crate and app
- multi-strategy Dioxus web UI
- durable event/order/fill/account/audit ledger
- gateway-only credential ownership
- no direct strategy dependency on concrete exchange adapters

The first execution batch should still be the foundation batch:

```text
industrial-workspace-migration-foundation
```

It creates the workspace, extracts `rustcta-types`,
`rustcta-exchange-api`, `rustcta-execution-api`, and
`rustcta-strategy-sdk`, and adds compatibility shims. This is an implementation
sequence detail, not the architectural endpoint.

After the foundation batch lands, the remaining migration work should continue
as part of the same industrial migration program: gateway extraction, execution
router extraction, strategy crate migration, supervisor implementation, control
API split, web UI multi-strategy migration, event ledger hardening, and legacy
compatibility cleanup.
