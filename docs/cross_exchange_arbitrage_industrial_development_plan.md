# Cross Exchange Arbitrage Industrial Development Plan

## 1. Purpose

This document is the implementation plan for building an industrial-grade
cross-exchange USDT perpetual arbitrage system in RustCTA.

It extends `docs/cross_exchange_arbitrage_usdt_perp.md` from strategy design
into engineering execution. The target system must support:

- Pluggable exchange market data adapters.
- Unified output for ticker, order book, trade, funding, mark price, and
  instrument metadata.
- Maker + Taker arbitrage simulation and later small-capital live execution.
- Independent execution engine with signal-to-order state machines.
- Automatic position reconciliation, recovery, and alerting.
- WebSocket stability layer with reconnect, heartbeat, sequence checks, and
  order book recovery.
- Historical storage, replay, metrics, dashboard, and fault injection tests.
- Parallel implementation by several AI coding agents without stepping on the
  same files.

The first production milestone is not "full live trading". The first milestone
is a reliable industrial simulation and shadow-trading system that can run for
at least 7 days with traceable market data, opportunity records, simulated
fills, funding accounting, and health metrics.

## 2. Engineering Principles

### 2.1 Safety First

Cross-exchange arbitrage is not risk-free. The main live risk is not signal
calculation; it is maker fill followed by failed taker hedge. Therefore:

- No new live entry is allowed unless both market data and order routes are
  healthy.
- Any exchange in `degraded`, `close_only`, or `offline` state must affect
  opportunity eligibility immediately.
- Position reconciliation must be treated as a first-class system.
- Every state transition must be idempotent and recoverable after process
  restart.
- Taker hedges must use executable order book VWAP and route health, not last
  price.

### 2.2 Incremental Refactor

The current `Exchange` trait is broad and already used by existing strategies.
Do not delete or break it in the first phase. Add smaller traits and adapter
wrappers while keeping the legacy trait as a compatibility facade.

### 2.3 Compile-Time Plugins First

In Rust, dynamic plugins add operational and safety complexity. The first
implementation should use compile-time plugins:

- Each exchange implements a set of traits.
- An `ExchangeRegistry` registers enabled adapters from config.
- Capabilities are declared by each adapter.

Dynamic loading can be revisited only after the system is stable.

### 2.4 Hot Path Stays Simple

The real-time calculation is mostly:

- For each canonical symbol, compare valid quotes across exchanges.
- Compute executable VWAP on 5 order book levels.
- Select max and min executable prices after fees, funding, and buffers.

For 4-10 exchanges and 100-300 symbols, hand-written Rust in memory is enough.
The bottleneck will be WebSocket quality, JSON parsing, exchange latency, and
order-route reliability, not max/min calculation.

Use ClickHouse/Polars/DataFusion for offline analytics and replay, not for the
live decision hot path.

## 3. Target Runtime Modes

The strategy must support these modes:

```text
observe:
    Subscribe, normalize, validate, record, and display data only.

simulation:
    Simulate maker fills, taker hedges, funding settlement, and closes.

shadow:
    Generate real execution plans and client order ids, but do not send orders.
    Compare planned execution with market replay.

live_small:
    White-listed symbols only, small notional, strict risk limits, auto recovery.

live_scaled:
    Only after stable observe/simulation/shadow/live_small results.
```

Mode must be config-driven. The default mode for new code is `simulation`.

## 4. High-Level Architecture

```text
Exchange WebSocket / REST
        |
        v
Exchange Adapter Plugin
        |
        v
WsSupervisor + RestRouteManager
        |
        v
Market Event Normalizer
        |
        v
Market Quality Gate
        |
        v
OrderBookCache / FundingCache / InstrumentMetaCache
        |
        v
Opportunity Engine
        |
        v
Signal Engine
        |
        +------------------------+
        |                        |
        v                        v
Simulation Engine          Execution Engine
        |                        |
        v                        v
Arbitrage Bundle Ledger    Exchange Trading Adapter
        |                        |
        +-----------+------------+
                    v
          Position Reconciler
                    |
                    v
      Dashboard / Alerts / ClickHouse / Replay
```

The critical contract is that strategy code must not consume raw exchange
messages. It consumes normalized, validated market state only.

## 5. Proposed Module Layout

### 5.1 New Core Modules

```text
src/market/
├── mod.rs
├── adapter.rs
├── event.rs
├── funding.rs
├── health.rs
├── instrument.rs
├── orderbook.rs
├── registry.rs
├── routing.rs
├── symbol.rs
├── trade.rs
├── vwap.rs
└── ws_supervisor.rs

src/execution/
├── mod.rs
├── bundle.rs
├── command.rs
├── engine.rs
├── hedge.rs
├── idempotency.rs
├── ledger.rs
├── reconciler.rs
├── recovery.rs
├── router.rs
└── state_machine.rs

src/strategies/cross_exchange_arbitrage/
├── mod.rs
├── config.rs
├── dashboard.rs
├── fees.rs
├── funding.rs
├── opportunity.rs
├── risk.rs
├── signal.rs
├── simulation.rs
├── state.rs
├── symbols.rs
└── tasks.rs
```

### 5.2 Exchange Adapter Modules

Existing exchange files can remain under `src/exchanges/`. Add adapter
implementations incrementally:

```text
src/exchanges/
├── binance.rs
├── okx.rs
├── bitget.rs        # new
├── gate.rs          # new
└── adapters/
    ├── mod.rs
    ├── binance_market.rs
    ├── binance_trading.rs
    ├── okx_market.rs
    ├── okx_trading.rs
    ├── bitget_market.rs
    ├── bitget_trading.rs
    ├── gate_market.rs
    └── gate_trading.rs
```

This split keeps protocol code exchange-specific while preserving unified
market/execution contracts.

### 5.3 Config and Persistence

```text
config/cross_exchange_arbitrage_usdt.yml
sql/cross_exchange_arbitrage.sql
docs/cross_exchange_arbitrage_industrial_development_plan.md
```

The first dashboard can extend the existing monitor server or use a separate
`cross_arb_server`. Prefer a separate server until the API stabilizes.

## 6. Market Data Layer

### 6.1 Responsibilities

The market layer owns:

- WebSocket connection lifecycle.
- Subscription batching and resubscription.
- Raw message parsing.
- Normalization into canonical events.
- Order book sequence validation.
- Snapshot/delta recovery.
- Stale quote detection.
- Bad book filtering.
- Funding and mark price refresh.
- Instrument metadata refresh.
- Per-exchange and per-route health state.

The market layer does not own:

- Trading decisions.
- Order placement.
- Position reconciliation.
- PnL accounting.

### 6.2 Core Traits

Conceptual API:

```rust
#[async_trait::async_trait]
pub trait MarketDataAdapter: Send + Sync {
    fn exchange(&self) -> ExchangeId;
    fn capabilities(&self) -> MarketCapabilities;
    async fn load_instruments(&self) -> anyhow::Result<Vec<InstrumentMeta>>;
    async fn load_funding(&self, symbols: &[CanonicalSymbol]) -> anyhow::Result<Vec<FundingSnapshot>>;
    fn build_public_ws_subscriptions(&self, symbols: &[ExchangeSymbol]) -> Vec<WsSubscription>;
    fn parse_public_ws_message(&self, raw: &str, recv_ts: DateTime<Utc>) -> anyhow::Result<Vec<MarketEvent>>;
    async fn fetch_orderbook_snapshot(&self, symbol: &ExchangeSymbol, depth: u16) -> anyhow::Result<OrderBookSnapshot>;
}
```

The exact types should be implemented in `src/market/`.

### 6.3 Unified Market Events

```text
MarketEvent
├── Ticker(TickerEvent)
├── OrderBookSnapshot(OrderBookSnapshot)
├── OrderBookDelta(OrderBookDelta)
├── Trade(TradeEvent)
├── Funding(FundingSnapshot)
├── MarkPrice(MarkPriceSnapshot)
├── InstrumentStatus(InstrumentStatusEvent)
└── Heartbeat(RouteHeartbeat)
```

Required event fields:

```text
exchange
canonical_symbol
exchange_symbol
market_type
exchange_ts
recv_ts
latency_ms
sequence
is_snapshot
source_route
quality_flags
raw_ref
```

`raw_ref` can be an optional short id pointing to stored raw logs. Do not store
large raw JSON in hot-path structs.

### 6.4 Canonical Symbol Rules

Canonical format:

```text
BTC/USDT
PEPE/USDT
WIF/USDT
```

Exchange formats:

```text
Binance: BTCUSDT
OKX:     BTC-USDT-SWAP
Bitget:  BTCUSDT
Gate:    BTC_USDT
```

Symbol mapping must be loaded from exchange metadata, not hardcoded. Every
mapping must include:

```text
canonical_symbol
exchange
exchange_symbol
base
quote
settle_asset
contract_type
contract_size
price_tick
quantity_step
min_qty
min_notional
price_precision
quantity_precision
status
supports_hedge_mode
supports_reduce_only
supports_post_only
supports_ioc
```

### 6.5 Order Book Cache

The first version uses top 5 levels:

```text
depth_levels = 5
```

Cache key:

```text
(exchange, canonical_symbol)
```

Cached value:

```text
OrderBook5 {
    exchange,
    canonical_symbol,
    exchange_symbol,
    bids: [BookLevel; 5],
    asks: [BookLevel; 5],
    exchange_ts,
    recv_ts,
    sequence,
    source_route,
    quality,
}
```

Validation rules:

- bids and asks must be non-empty.
- all prices and quantities must be positive.
- best bid must be lower than best ask.
- book age must be below `stale_quote_ms`.
- sequence must be monotonic where the exchange provides sequence ids.
- snapshot recovery is required after sequence gap.

Invalid books remain visible in diagnostics but cannot produce open signals.

### 6.6 VWAP Calculator

The VWAP function is a core safety primitive.

Inputs:

```text
side: buy or sell
target_notional_usdt
orderbook_5
max_levels
```

Outputs:

```text
TakerVwap {
    filled_qty
    filled_notional
    vwap_price
    levels_used
    depth_enough
    slippage_pct
    best_price
}
```

Rules:

- Buy consumes asks from lowest to highest.
- Sell consumes bids from highest to lowest.
- If 5-level depth is insufficient, return `depth_enough=false`.
- Do not pretend full fill when depth is insufficient.
- Unit tests must cover buy/sell, exact depth, insufficient depth, empty book,
  crossed book, and zero values.

### 6.7 WebSocket Supervisor

`WsSupervisor` is a reusable stability layer below exchange adapters.

Responsibilities:

- Connect to primary endpoint.
- Send heartbeat according to exchange protocol.
- Detect no-message timeout.
- Detect heartbeat timeout.
- Reconnect with exponential backoff and jitter.
- Restore subscriptions after reconnect.
- Detect subscription failure and split batch if needed.
- Track sequence gaps.
- Trigger snapshot recovery.
- Switch to backup route when route health degrades.
- Emit route health events and metrics.

Route health states:

```text
healthy:
    usable for new entries and closes

degraded:
    no new entries; closes allowed if order route is healthy

close_only:
    no new entries; only cancel, reduce, and close

offline:
    no dependency allowed for new opportunities
```

Each route tracks:

```text
endpoint
route_type
status
last_ok_at
last_error_at
last_message_at
latency_ms_p50
latency_ms_p95
consecutive_errors
reconnect_count
sequence_gap_count
rate_limit_hits
active_subscription_count
```

## 7. Opportunity and Signal Layer

### 7.1 Opportunity Engine

The opportunity engine recalculates only the symbol affected by a market data
update. With 4-10 exchanges, a direct loop over valid exchange books is enough.

For each canonical symbol:

1. Collect valid books across enabled exchanges.
2. Skip exchanges with stale, bad, degraded, close-only, or offline market data.
3. For every ordered pair `(long_exchange, short_exchange)`, compute both:
   - high-side maker short + low-side taker long.
   - low-side maker long + high-side taker short.
4. Compute raw spread, executable VWAP, fees, funding expectation, slippage
   buffer, maker non-fill penalty, and safety buffer.
5. Emit ranked opportunities and reject reasons.

### 7.2 Opportunity Fields

```text
opportunity_id
canonical_symbol
long_exchange
short_exchange
maker_exchange
taker_exchange
maker_side
taker_side
maker_price
taker_vwap
target_notional
raw_open_spread
maker_taker_net_edge
open_fee_est
close_fee_est
expected_funding
slippage_pct
depth_notional
book_age_ms
route_status
can_open
reject_reason
created_at
expires_at
```

### 7.3 Signal Contract

The strategy emits signals, not direct orders:

```text
ArbSignal {
    signal_id
    mode
    opportunity_id
    action: Open | Close | EmergencyClose | Cancel | Noop
    confidence
    expected_edge
    max_notional
    risk_flags
    generated_at
}
```

Signals are consumed by simulation or execution depending on runtime mode.

### 7.4 Net Edge Formula

Entry decision must use executable net edge:

```text
maker_taker_net_edge =
    raw_open_spread
  - maker_entry_fee_rate
  - taker_hedge_fee_rate
  - expected_maker_close_fee_rate
  - expected_taker_close_fee_rate
  + expected_funding_until_close
  - taker_slippage_buffer
  - maker_non_fill_penalty
  - safety_buffer
```

Displayed raw spread is not an open signal.

## 8. Simulation Engine

### 8.1 Responsibilities

Simulation must be realistic enough to decide whether live trading is worth
attempting. It must simulate:

- Maker order placement.
- Maker queue/wait and fill inference.
- Taker hedge VWAP after maker fill.
- Partial hedge and orphan-leg risk.
- Funding settlement.
- Maker + Taker normal close.
- Dual Taker lock-profit close.
- Emergency close.
- Fees and PnL attribution.

### 8.2 Maker Fill Model

Inputs:

```text
maker_side
maker_price
best_bid
best_ask
trade_updates
book_updates
max_wait_ms
```

Initial conservative model:

```text
Sell maker fills when:
    later best_ask <= maker_price
    or trade price >= maker_price

Buy maker fills when:
    later best_bid >= maker_price
    or trade price <= maker_price
```

If trade data is unavailable and book changes infer fill, mark it as:

```text
fill_inference_type = book_inferred_fill
```

Never report inferred fills as real exchange fills.

### 8.3 Simulation Bundle

Every simulated or live position is represented as an `ArbitrageBundle`.

```text
bundle_id
mode
canonical_symbol
status
open_time
close_time
long_exchange
short_exchange
maker_exchange
taker_exchange
long_entry_vwap
short_entry_vwap
long_qty
short_qty
target_notional
entry_spread
current_spread
open_fee
close_fee
funding_pnl
gross_spread_pnl
net_pnl
max_adverse_spread
max_favorable_spread
orphan_loss
close_reason
last_reconcile_at
created_from_signal_id
```

Bundle statuses:

```text
observing
maker_pending
maker_timeout
maker_filled
hedging
open_simulated
closing_simulated
closed
expired
orphan_leg
risk_stopped
depth_insufficient
reconcile_required
```

## 9. Execution Engine

### 9.1 Responsibilities

The execution engine owns all order side effects:

- Convert signals into order plans.
- Place maker order with `post_only`.
- Track maker order fill, partial fill, timeout, and cancel.
- Place taker hedge using IOC or equivalent protected order.
- Recover from partial hedge.
- Close bundle with Maker + Taker or dual Taker.
- Maintain idempotency across retries and restarts.
- Emit execution events to ledger and dashboard.

Strategy code must never call exchange `create_order` directly.

### 9.2 Trading Adapter Trait

Conceptual API:

```rust
#[async_trait::async_trait]
pub trait TradingAdapter: Send + Sync {
    fn exchange(&self) -> ExchangeId;
    fn capabilities(&self) -> TradingCapabilities;
    async fn place_order(&self, command: OrderCommand) -> anyhow::Result<OrderAck>;
    async fn cancel_order(&self, command: CancelCommand) -> anyhow::Result<CancelAck>;
    async fn get_order(&self, query: OrderQuery) -> anyhow::Result<OrderState>;
    async fn get_open_orders(&self, symbol: Option<&ExchangeSymbol>) -> anyhow::Result<Vec<OrderState>>;
    async fn get_positions(&self, symbol: Option<&ExchangeSymbol>) -> anyhow::Result<Vec<ExchangePosition>>;
    async fn get_balances(&self) -> anyhow::Result<Vec<ExchangeBalance>>;
}
```

### 9.3 Order Command Model

```text
OrderCommand {
    command_id
    bundle_id
    exchange
    canonical_symbol
    exchange_symbol
    intent
    side
    position_side
    order_type
    quantity
    price
    time_in_force
    post_only
    reduce_only
    client_order_id
    max_slippage_pct
    created_at
}
```

Order intents:

```text
OpenLongMaker
OpenShortMaker
HedgeLongTaker
HedgeShortTaker
CloseLongMaker
CloseShortMaker
CloseLongTaker
CloseShortTaker
EmergencyCloseLongTaker
EmergencyCloseShortTaker
CancelMaker
```

### 9.4 Idempotency

Every order command must have a deterministic `client_order_id`:

```text
crossarb-{mode}-{bundle_short_id}-{leg}-{attempt}
```

Before placing a retry, the engine must check whether an order with the same
client id already exists or filled. This avoids duplicate hedges after process
restart or network timeout.

### 9.5 Orphan Leg Recovery

When maker fills but taker hedge fails:

1. Enter `orphan_leg`.
2. Disable new entries globally or at least for the affected exchange pair.
3. Retry hedge once with a wider but bounded slippage guard.
4. If hedge still fails, reverse the maker leg with taker order if route allows.
5. Record orphan loss, extra fee, and max unhedged duration.
6. Alert immediately.

This flow must be testable without real exchange access.

## 10. Position Reconciliation System

### 10.1 Responsibilities

The reconciler compares:

- Local bundle ledger.
- Local order ledger.
- Exchange open orders.
- Exchange positions.
- Exchange trade fills.
- Exchange balances.
- Funding income records where available.

It must produce:

```text
ReconcileReport {
    exchange
    canonical_symbol
    severity
    local_position
    exchange_position
    local_open_orders
    exchange_open_orders
    detected_drift
    recommended_action
    auto_action_taken
    alert_required
    checked_at
}
```

### 10.2 Reconciliation Severity

```text
ok:
    local and exchange state match within tolerance

minor_drift:
    small rounding/fill difference; auto adjust local ledger

order_drift:
    unknown open order or missing local order; cancel or import

position_drift:
    position mismatch; block new entries and recover

orphan_exposure:
    one leg exists without hedge; emergency workflow

unknown_critical:
    cannot determine state; close_only and alert
```

### 10.3 Cadence

Suggested intervals:

```text
open orders:
    every 3-5 seconds during active execution

positions:
    every 5-10 seconds in live modes

balances:
    every 30-60 seconds

funding income:
    after funding settlement windows and every 5 minutes

full audit:
    every 1-5 minutes
```

During `orphan_leg` or `reconcile_required`, cadence increases immediately.

### 10.4 Auto Recovery Rules

Auto recovery must be conservative:

- Unknown local order found on exchange: import if client id matches; otherwise
  cancel if not reduce-only and no related bundle exists.
- Missing local order that exchange says filled: import fill and update bundle.
- Position quantity mismatch within rounding tolerance: adjust local ledger and
  log.
- Single-leg exposure: stop new entries and run orphan recovery.
- Ambiguous exchange state: set exchange to `close_only` and alert.

## 11. Risk Management

### 11.1 Risk Gates

Risk gates are checked before signal emission and before order placement:

```text
global_enabled
mode_allows_live_orders
symbol_whitelisted
exchange_pair_allowed
both_market_routes_healthy
both_order_routes_healthy
books_not_stale
depth_enough
funding_not_dangerous
target_notional_within_limits
no_recent_orphan_cooldown
max_open_bundles_not_exceeded
max_symbol_exposure_not_exceeded
max_exchange_exposure_not_exceeded
```

### 11.2 Circuit Breakers

Required breakers:

- Global kill switch.
- Exchange kill switch.
- Symbol kill switch.
- Exchange-pair kill switch.
- New-entry pause.
- Close-only mode.
- Funding-risk pause.
- Route-health pause.
- Orphan-loss pause.
- Consecutive-error pause.

### 11.3 Live Small Defaults

Live mode must default to strict values:

```text
max_bundle_notional: 20-100 USDT
max_open_bundles: 1-3
symbol_whitelist_required: true
new_symbols_cooldown_mins: 60
max_orphan_count_per_hour: 0
max_order_error_rate: very low
```

These are defaults, not strategy targets. Scaling requires evidence.

## 12. Persistence and Analytics

### 12.1 Hot State

In-memory state:

- Latest order books.
- Latest funding snapshots.
- Latest route health.
- Active opportunities.
- Active bundles.
- Active orders.

Use `DashMap`, `RwLock`, or channel ownership depending on access pattern. Keep
the hot path simple and measurable.

### 12.2 Durable Ledger

Required durable records:

```text
market_book_snapshots_sampled
funding_snapshots
opportunities
signals
execution_commands
order_events
bundle_events
reconcile_reports
route_health_events
risk_events
alerts
```

ClickHouse is suitable for high-volume event and time-series data. Local JSONL
can be used during early development, but the storage interface should allow
ClickHouse behind it.

### 12.3 Offline Computation

Use tools by purpose:

```text
Live hot path:
    Rust in-memory loops over valid exchange books.

Historical storage:
    ClickHouse.

Research, parameter sweep, reports:
    Polars or Python notebooks reading ClickHouse/Parquet.

Embedded SQL/replay in Rust:
    DataFusion if needed later.

Columnar exchange format:
    Arrow/Parquet.
```

Do not add GPU or distributed processing in the first implementation. It is not
the current bottleneck.

## 13. Dashboard and APIs

### 13.1 API Endpoints

Suggested endpoints:

```text
GET /api/cross-arb/status
GET /api/cross-arb/opportunities
GET /api/cross-arb/bundles/open
GET /api/cross-arb/bundles/history
GET /api/cross-arb/exchanges
GET /api/cross-arb/routes
GET /api/cross-arb/reconcile
GET /api/cross-arb/risk-events
GET /api/cross-arb/config-summary
POST /api/cross-arb/control/pause-new-entries
POST /api/cross-arb/control/resume-new-entries
POST /api/cross-arb/control/close-only
POST /api/cross-arb/control/kill-switch
```

### 13.2 Required Views

The dashboard must show:

- Current runtime mode.
- Whether new entries are allowed.
- Exchange health matrix.
- Route health matrix.
- Latest valid opportunities.
- Reject reasons.
- Active simulated/live bundles.
- Funding contribution.
- Fee contribution.
- Net PnL by bundle.
- Orphan and recovery events.
- Reconciliation reports.
- Last message age per exchange.
- Sequence gap count.
- Reconnect count.

## 14. Configuration

The config should extend the existing example in
`cross_exchange_arbitrage_usdt_perp.md`.

Required top-level sections:

```text
strategy
market
thresholds
sizing
fees
funding
exchanges
routing
execution
reconciliation
risk
persistence
dashboard
alerts
universe
```

Config loading must validate:

- mode is valid.
- enabled exchanges have required routes.
- live mode has whitelist.
- fees exist for all enabled exchanges.
- risk limits are positive.
- thresholds are internally consistent.
- exchange pair is tradable.

Invalid config must fail at startup, not during trading.

## 15. Testing Strategy

### 15.1 Unit Tests

Required unit test areas:

- Symbol normalization.
- Instrument metadata parsing.
- Order book validation.
- VWAP buy/sell.
- Fee model.
- Funding direction.
- Net edge calculation.
- Opportunity pair generation.
- Maker fill inference.
- Bundle state transitions.
- Client order id generation.
- Reconcile severity classification.
- Risk gate decisions.

### 15.2 Integration Tests

Required integration tests:

- Feed recorded book events and verify opportunities.
- Simulate maker fill and taker hedge.
- Simulate partial taker hedge.
- Simulate sequence gap and snapshot recovery.
- Simulate stale quote and opportunity rejection.
- Simulate exchange degraded state and new-entry block.
- Simulate position drift and recovery action.
- Simulate funding settlement.
- Simulate dual Taker lock-profit close.

### 15.3 Fault Injection

Fault scenarios:

- WebSocket disconnect.
- Heartbeat timeout.
- Sequence gap.
- Snapshot fetch failure.
- REST private route timeout.
- Order placement returns unknown status.
- Cancel returns unknown order.
- Maker fills after cancel attempt.
- Taker IOC partially fills.
- One exchange enters close-only.
- Exchange metadata changes min quantity or tick size.

### 15.4 Acceptance Criteria Before Live Small

Observe/simulation acceptance:

- Runs for 7 continuous days.
- Records reconnect count, sequence gaps, bad books, and stale quote count.
- Every simulated fill can be traced to a book/trade event.
- 5-level depth insufficiency never produces fake full fills.
- Funding and fees are visible by bundle.
- Maker fill probability, taker hedge success rate, and orphan loss are
  reported.
- All exchange state transitions affect entry eligibility.

Live-small acceptance:

- Symbol whitelist enforced.
- Kill switch tested.
- Close-only tested.
- Reconciler tested with fake drift.
- IOC/reduce-only/post-only semantics tested per exchange with tiny orders.
- Client order id idempotency tested.
- No direct order placement from strategy modules.

## 16. Multi-AI Parallel Development Plan

### 16.1 Coordination Rules

Several AI agents can work in parallel only if contracts and ownership are
clear.

Rules:

- AI-0 owns shared contracts and integration.
- Other agents do not edit shared contract files without AI-0 coordination.
- Each agent owns a bounded module directory.
- Each PR/change must include tests or a clear reason tests are deferred.
- All agents run `cargo fmt` before handoff.
- Shared interfaces are stabilized before adapter implementation starts.
- No agent should rewrite existing exchange files broadly unless assigned.
- No agent should change live-order behavior outside `src/execution/`.

Suggested branch names:

```text
crossarb/contracts
crossarb/market-core
crossarb/exchange-adapters
crossarb/execution
crossarb/simulation
crossarb/dashboard-persistence
crossarb/qa-replay
```

### 16.2 AI Roles

#### AI-0: Architect and Integrator

Ownership:

```text
src/market/mod.rs
src/execution/mod.rs
src/strategies/cross_exchange_arbitrage/mod.rs
docs/
config/cross_exchange_arbitrage_usdt.yml
```

Tasks:

- Freeze initial data contracts.
- Create module skeletons.
- Keep legacy `Exchange` compatibility.
- Review cross-module dependencies.
- Merge work from other agents.
- Resolve compile errors.
- Maintain development checklist.

Deliverables:

- Compiling skeleton.
- Contract tests.
- Integration build passing.

#### AI-1: Market Core and WebSocket Stability

Ownership:

```text
src/market/event.rs
src/market/orderbook.rs
src/market/vwap.rs
src/market/health.rs
src/market/routing.rs
src/market/ws_supervisor.rs
```

Tasks:

- Implement normalized market events.
- Implement order book validation.
- Implement 5-level VWAP.
- Implement route health model.
- Implement supervisor state machine skeleton.
- Add tests for stale books, crossed books, sequence gaps, reconnect decisions.

Deliverables:

- Market core unit tests passing.
- No exchange-specific logic in market core.

#### AI-2: Exchange Adapter Plugins

Ownership:

```text
src/market/adapter.rs
src/market/instrument.rs
src/market/symbol.rs
src/market/registry.rs
src/exchanges/adapters/
```

Tasks:

- Define market adapter and trading adapter wrappers.
- Implement Binance market adapter first.
- Implement OKX market adapter second.
- Add Bitget and Gate skeletons.
- Normalize instrument metadata.
- Add exchange capability declarations.

Deliverables:

- Binance and OKX market metadata loading.
- Binance and OKX order book message parsing tests.
- Bitget and Gate compile-time skeletons.

#### AI-3: Execution Engine and Reconciler

Ownership:

```text
src/execution/
```

Tasks:

- Implement bundle state machine.
- Implement order command model.
- Implement client order id/idempotency.
- Implement execution planner for Maker + Taker.
- Implement orphan recovery state machine.
- Implement reconciliation report model and severity classifier.

Deliverables:

- State transition tests.
- Idempotency tests.
- Reconciliation classifier tests.
- No direct dependency on strategy internals.

#### AI-4: Strategy, Opportunity, and Simulation

Ownership:

```text
src/strategies/cross_exchange_arbitrage/
```

Tasks:

- Implement config model.
- Implement universe/long-tail symbol selection.
- Implement opportunity scanning.
- Implement fee and funding model.
- Implement signal generation.
- Implement simulation bundle lifecycle.
- Implement dual Taker lock-profit simulation.

Deliverables:

- Opportunity tests from synthetic order books.
- Funding direction tests.
- Simulation open/close tests.
- Reject reason coverage.

#### AI-5: Persistence, Dashboard, and Alerts

Ownership:

```text
src/strategies/cross_exchange_arbitrage/dashboard.rs
src/bin/cross_arb_server.rs
sql/cross_exchange_arbitrage.sql
```

Tasks:

- Define read models for dashboard.
- Implement API endpoints.
- Add storage trait.
- Add JSONL storage first, ClickHouse implementation second.
- Add alert event model.

Deliverables:

- API returns status/opportunities/bundles/routes/reconcile.
- Storage tests with temp files where applicable.
- SQL schema draft.

#### AI-6: QA, Replay, and Fault Injection

Ownership:

```text
tests/cross_exchange_arbitrage/
scripts/
```

Tasks:

- Build recorded event replay fixtures.
- Build fault injection scenarios.
- Add integration tests.
- Add scripts to run focused cross-arb tests.
- Add simulated 7-day run checklist.

Deliverables:

- Replay test harness.
- Fault injection tests.
- Validation script.

### 16.3 Dependency Order

```text
Step 0:
    AI-0 creates module skeleton and contracts.

Step 1:
    AI-1 builds market core.
    AI-3 builds execution state models.
    AI-4 builds config and pure calculation models.

Step 2:
    AI-2 implements Binance/OKX adapters against market contracts.
    AI-4 wires opportunity scanner to market cache.
    AI-5 builds dashboard read models from mocked state.

Step 3:
    AI-3 wires execution engine to trading adapter traits.
    AI-6 builds replay/fault tests.

Step 4:
    AI-0 integrates end-to-end observe and simulation mode.

Step 5:
    AI-2 adds Bitget/Gate.
    AI-5 adds ClickHouse persistence.
    AI-6 expands integration tests.

Step 6:
    Shadow mode and live-small readiness work.
```

### 16.4 Parallel Work Boundaries

Do not let multiple agents edit the same file during the same phase.

Shared files requiring AI-0 coordination:

```text
Cargo.toml
src/lib.rs
src/core/mod.rs
src/strategies/mod.rs
src/market/mod.rs
src/execution/mod.rs
```

Agents should prefer adding new files in owned directories instead of editing
legacy files. Compatibility exports can be added by AI-0 at integration time.

## 17. Milestone Plan

### Phase 0: Contracts and Skeleton

Goal:

Create compile-safe module structure and shared types.

Deliverables:

- `src/market/` skeleton.
- `src/execution/` skeleton.
- `src/strategies/cross_exchange_arbitrage/` skeleton.
- Config skeleton.
- Contract tests.

Acceptance:

- `cargo check` passes.
- Existing strategies still compile.

### Phase 1: Market Data Foundation

Goal:

Reliable normalized market state for order book, ticker, trade, funding, and
instrument metadata.

Deliverables:

- Market events.
- Symbol mapping.
- OrderBook5 cache.
- VWAP calculator.
- Market quality gates.
- WsSupervisor skeleton.

Acceptance:

- Synthetic book tests pass.
- Stale/crossed/invalid books rejected.
- Reconnect/sequence-gap logic testable.

### Phase 2: Binance and OKX Observe Mode

Goal:

Run observe mode on Binance and OKX USDT perpetuals.

Deliverables:

- Binance market adapter.
- OKX market adapter.
- Batch subscription handling.
- Funding refresh.
- Instrument metadata refresh.
- Basic dashboard/status endpoint.

Acceptance:

- Continuous observe run without strategy execution.
- Message age, reconnect, bad book, and sequence metrics visible.

### Phase 3: Opportunity and Simulation

Goal:

End-to-end simulation with realistic Maker + Taker mechanics.

Deliverables:

- Opportunity scanner.
- Fee model.
- Funding model.
- Maker fill inference.
- Taker hedge VWAP.
- Bundle lifecycle.
- Normal close and dual Taker close simulation.

Acceptance:

- Simulated opportunities and bundles visible.
- PnL split into spread, fees, funding, orphan loss.
- Reject reasons visible.

### Phase 4: Execution and Reconciliation Shadow Mode

Goal:

Produce live-grade order plans without sending live orders.

Deliverables:

- Execution engine.
- Order command ledger.
- Bundle state machine.
- Client order id/idempotency.
- Reconciler model.
- Recovery decisions.

Acceptance:

- Shadow mode emits realistic commands.
- Reconciler detects synthetic drift.
- No exchange order side effects.

### Phase 5: Bitget/Gate and Full Exchange Matrix

Goal:

Support target exchange set: Binance, OKX, Bitget, Gate.

Deliverables:

- Bitget market adapter.
- Gate market adapter.
- Trading adapter skeletons.
- Instrument metadata compatibility tests.

Acceptance:

- At least market observe mode works for all four.
- Common symbols can be discovered and filtered.

### Phase 6: Live Small Preparation

Goal:

Prepare tiny notional live mode behind strict controls.

Deliverables:

- Trading adapters for enabled exchanges.
- Post-only maker order support.
- IOC taker support.
- Reduce-only close support.
- Live reconciler.
- Kill switch.
- Alerting.

Acceptance:

- Tiny order tests pass per exchange.
- Close-only and kill switch tested.
- Live config requires whitelist.
- Operator can pause new entries.

## 18. Implementation Checklist

### Must Have Before Any Live Order

- [ ] Symbol whitelist required in live mode.
- [ ] Exchange route health blocks new entries.
- [ ] Order book stale checks block opportunities.
- [ ] 5-level VWAP used for taker calculations.
- [ ] Maker fill does not imply taker success.
- [ ] Orphan recovery implemented.
- [ ] Reconciler implemented and running.
- [ ] Client order id idempotency implemented.
- [ ] Kill switch implemented.
- [ ] Post-only, IOC, reduce-only verified per exchange.
- [ ] Fees and funding included in PnL.
- [ ] Funding danger window blocks entries when configured.
- [ ] Dashboard shows exchange and route health.
- [ ] Alerts work for orphan, route offline, and reconcile critical.

### Must Not Do

- [ ] Do not calculate opportunities from last price.
- [ ] Do not assume 5-level depth is enough when it is not.
- [ ] Do not trade on stale books.
- [ ] Do not keep opening when a trading route is degraded.
- [ ] Do not let strategy modules place orders directly.
- [ ] Do not live trade non-whitelisted symbols.
- [ ] Do not ignore funding direction.
- [ ] Do not hide fee/funding/orphan loss inside total PnL.
- [ ] Do not scale notional before observe/simulation evidence exists.

## 19. Recommended First Task Batch

The first implementation batch should be small and contract-oriented:

1. Add module skeletons:
   - `src/market/`
   - `src/execution/`
   - `src/strategies/cross_exchange_arbitrage/`

2. Implement pure market primitives:
   - `CanonicalSymbol`
   - `ExchangeId`
   - `OrderBook5`
   - `BookQuality`
   - `TakerVwap`
   - `calculate_taker_vwap`

3. Implement opportunity pure functions:
   - fee model
   - funding direction model
   - raw spread calculation
   - net edge calculation

4. Add unit tests before any live WebSocket work.

This gives all AI agents stable contracts and avoids building adapters on top
of unclear types.

## 20. Final Target

The project should become a system where adding a new exchange, strategy, or
monitored pair does not require rewriting execution safety logic.

Expected final separation:

```text
New exchange:
    implement MarketDataAdapter + TradingAdapter + InstrumentMeta mapping.

New arbitrage strategy:
    consume market cache and emit ArbSignal.

New risk rule:
    add a risk gate used by both simulation and live execution.

New dashboard field:
    read from event ledger or runtime state, not from raw exchange internals.
```

This is the path from a strategy prototype to an industrial cross-exchange
arbitrage tool.
