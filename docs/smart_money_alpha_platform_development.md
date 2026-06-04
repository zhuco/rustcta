# Smart Money Alpha Platform Development Document

Status: draft for implementation planning

Repository: `rustcta`

Current package version: `0.3.7`

Primary objective: evolve the current multi-exchange CTA framework into a Smart Money Adaptive Alpha Platform that consumes wallet intelligence, ranks traders dynamically, aggregates alpha opinions, constructs target portfolios, and backtests execution against historical order book liquidity.

## 1. Executive Summary

The current project is a single Rust crate with a broad set of trading-system components:

- Core types, exchange abstractions, request management, WebSocket helpers, monitoring, and basic risk management under `src/core/`.
- Account orchestration under `src/cta/`.
- Market-data contracts, instrument metadata, symbol normalization, precision, order book, funding, route health, and WebSocket supervision under `src/market/`.
- Exchange adapters for Binance, OKX, Bitmart, Hyperliquid, and a unified private perpetual gateway path for Bitget, Gate, Bybit, MEXC, and HTX under `src/exchanges/`.
- Execution contracts for order commands, idempotency, routing, reconciliation, recovery, hedging, user streams, and state machines under `src/execution/`.
- Multiple live strategy families under `src/strategies/`.
- Backtesting data capture, replay, matching, factor scans, and strategy hooks under `src/backtest/`.
- Operational binaries under `src/bin/`.

This is a strong base for exchange connectivity, low-level execution, strategy runtime, and backtest infrastructure. It is not yet a smart-money research platform. The current architecture is strategy-action centric: strategies tend to consume market/account events and produce direct trading actions. The target architecture must become portfolio-allocation centric: wallets produce opinions, opinions aggregate into alpha pressure, alpha pressure becomes target exposure, and only then does risk/execution convert target state into executable orders.

The recommended path is incremental. Keep the current crate operational, add smart-money modules behind new binaries and storage adapters, then split stable domains into workspace crates once interfaces settle.

## 2. Current Architecture Analysis

### 2.1 Current Top-Level Layout

```text
src/
  core/           shared trading types, errors, config, risk, request/ws utilities
  cta/            account manager and runtime account model
  market/         market contracts, symbols, instruments, books, funding, route health
  exchanges/      exchange adapters and unified private perpetual gateway
  execution/      order commands, router, state machine, idempotency, reconciliation
  strategies/     live strategies and strategy-specific state/risk/execution
  backtest/       replay, datasets, matching, strategy scans
  analysis/       analysis entry point
  utils/          indicators, symbol conversion, signatures, logging, webhook
  bin/            operational CLIs and daemons
tests/            integration and regression tests
docs/             design notes, runbooks, release notes, strategy specs
config/           strategy and account configuration templates
sql/              SQL seeds and schema snippets
```

### 2.2 Runtime Model

The current runtime is centered around strategy binaries and strategy modules:

1. Load configuration.
2. Initialize account and exchange adapters through `AccountManager`.
3. Subscribe to market data or account/user streams.
4. Strategy receives market/account events.
5. Strategy applies local signal/risk/execution logic.
6. Orders are routed through exchange-specific or gateway adapters.
7. Strategy-specific state and logs track outcomes.

This model works for independent CTA/grid/arbitrage strategies. It is less suitable for cross-wallet portfolio research because wallet profiling, scoring, alpha aggregation, portfolio construction, and execution simulation must be shared platform services rather than embedded inside one strategy.

### 2.3 Current Module Responsibilities

| Module | Current Responsibility | Development Assessment |
|---|---|---|
| `core` | Shared types, errors, config, request handling, WebSocket utilities, basic risk manager | Useful but partly legacy. Keep core types while avoiding further growth of global state. |
| `cta` | Account configuration and exchange instance lifecycle | Reusable for live trading accounts; not enough for trader/wallet metadata. |
| `market` | Pure market contracts: exchange IDs, canonical symbols, instruments, order book, funding, routing health | Strong reusable platform layer. |
| `exchanges` | REST/WS adapters and private perpetual gateway protocols | Strong reusable adapter layer; Hyperliquid wallet intelligence needs new read-side APIs. |
| `execution` | Order command contracts, idempotency, router, reconciliation, recovery, bundle state machine | Strong reusable execution foundation; should be generalized from arbitrage bundles to portfolio transitions. |
| `strategies` | Many live strategies: trend, grids, funding arb, cross-exchange arb, market making, copy trading | Keep existing strategies operational; do not build smart-money platform inside legacy strategy modules. |
| `backtest` | Dataset capture, replay, matching, scans, strategy interface | Good base; must be extended to historical order book replay and point-in-time wallet scoring. |
| `utils` | Symbol conversion, signatures, indicators, logging, webhook | Reuse selectively; avoid adding platform business logic here. |
| `bin` | Strategy/ops entry points | Add new smart-money service binaries here initially. |

### 2.4 Existing Strengths

- Exchange coverage is broad and practical.
- Market and execution contracts are already partially side-effect free.
- The private perpetual gateway isolates exchange-specific protocol encoding from strategy code.
- Cross-exchange arbitrage has mature operational ideas: risk modes, stale private stream detection, reconciliation drift, orphan exposure, close-only, kill switch, precision-aware sizing.
- Backtest infrastructure already includes replay and matching concepts.
- There are many tests around backtest and exchange fixtures.
- Hyperliquid support exists for perpetual trading and signing.

### 2.5 Existing Weaknesses

- The project is still a single package, which encourages cross-module coupling.
- Strategies own too much signal/risk/execution behavior locally.
- Strategy output is action/order oriented rather than portfolio-target oriented.
- `copy_trading` is conceptually opposite to the desired platform: it mirrors a source instead of treating wallets as noisy alpha factors.
- Current global risk is simple threshold logic and is not portfolio/risk-budget/regime aware.
- There is no persistent trader metadata model, wallet ranking history, score history, cluster history, or portfolio state history.
- ClickHouse is present, but Postgres/SQLx, Redis, NATS, `tracing`, and `config` are not yet wired as platform dependencies.
- Backtests can use datasets and replay, but the target requirement needs Binance Futures order book replay with sequence correctness, taker sweep simulation, fees, funding, latency, and partial fills.

## 3. Target Product Definition

Build a Smart Money Adaptive Alpha Platform.

The platform must not:

- Copy one trader.
- Mirror wallet trades blindly.
- Emit simple buy/sell signals as the final product.

The platform must:

- Monitor thousands of wallets.
- Persist all wallet facts and derived metrics.
- Classify traders into multi-label behavioral clusters.
- Rank wallets dynamically.
- Detect market regimes.
- Convert wallet behavior into alpha opinions.
- Aggregate alpha opinions into target exposures.
- Construct risk-adjusted target portfolios.
- Simulate execution using historical order book liquidity.
- Enforce portfolio and capital constraints.

Final output:

```text
TargetPortfolio {
  as_of,
  nav,
  gross_notional,
  leverage,
  weights: {
    "BTCUSDT": +0.30,
    "ETHUSDT": +0.15,
    "SOLUSDT": -0.10,
    "HYPEUSDT": +0.05,
    "USDT": +0.60
  }
}
```

## 4. Capital and Trading Constraints

Initial portfolio assumptions:

| Constraint | Value |
|---|---:|
| Initial capital | `2000 USDT` |
| Standard per-signal entry notional | `1000 USDT` |
| Maximum leverage | `10x` |
| Maximum gross notional exposure | `20000 USDT` |
| Market | Perpetual futures |
| Direction | Long and short allowed |
| Margin simulation | Cross margin |
| Execution simulation | Taker market orders only |
| Taker fee | `0.04%` per fill |

Hard invariants:

```text
gross_notional <= 20000 USDT
gross_notional / nav <= 10
standard_new_entry_notional <= 1000 USDT unless explicitly scaled down/up by risk config
fees charged on every entry, exit, scale, and reverse fill
funding applied at historical funding timestamps
```

## 5. Target Architecture

### 5.1 Architecture Principles

- Event-driven: ingestion, profiling, scoring, alpha aggregation, portfolio construction, and execution communicate through durable event streams.
- Domain-driven: wallet, trader, score, regime, alpha, portfolio, order, and backtest are explicit domains.
- Hexagonal: domain logic is independent from Postgres, ClickHouse, Redis, NATS, and exchange APIs.
- CQRS: writes are append/fact oriented; query models are materialized for dashboards and services.
- Async-first: all IO adapters use Tokio and backpressure-aware workers.
- Portfolio-first: the final platform output is target allocation, not raw order intent.

### 5.2 Target System Diagram

```text
                 +-------------------------+
                 | Hyperliquid Wallet APIs |
                 +-----------+-------------+
                             |
                             v
                    Wallet Ingestion
                             |
                             v
+----------------+     NATS wallet.*      +------------------+
| Binance Futures| ---------------------> | ClickHouse Facts |
| WS/REST        |     NATS market.*      +------------------+
+-------+--------+                         +------------------+
        |                                  | Postgres State   |
        v                                  +------------------+
 Market Collector                                  |
        |                                          v
        +------------------------------> Feature/Profile Worker
                                                   |
                                                   v
                                            Clustering Worker
                                                   |
                                                   v
                                            Scoring Worker <----- Regime Worker
                                                   |
                                                   v
                                           Alpha Aggregator
                                                   |
                                                   v
                                        Portfolio Constructor
                                                   |
                                                   v
                                              Risk Engine
                                                   |
                          +------------------------+------------------------+
                          v                                                 v
                 Execution Simulator                              Live Execution Engine
                          |                                                 |
                          v                                                 v
                  Backtest Results                                 Reconciled Positions
```

### 5.3 Service Boundaries

| Service | Input | Output | Storage |
|---|---|---|---|
| `market-collector` | Binance depth/trade/mark/funding streams | `market.depth`, `market.trade`, `market.mark`, `market.funding` events | ClickHouse raw market facts |
| `wallet-ingestor` | Hyperliquid wallet/account endpoints and WS | `wallet.position`, `wallet.trade`, `wallet.funding`, `wallet.equity` events | ClickHouse wallet facts, Postgres wallet metadata |
| `profile-worker` | Wallet facts | Wallet profile snapshots | Postgres profile history, Redis latest profile |
| `cluster-worker` | Profiles and behavior features | Multi-label cluster history | Postgres cluster history |
| `regime-worker` | Market facts | Regime snapshots | Postgres/ClickHouse regime history |
| `scoring-worker` | Profiles, clusters, regimes, recent PnL | Wallet score matrix | Postgres score history, Redis latest scores |
| `alpha-aggregator` | Wallet positions and scores | Symbol alpha pressure | Postgres/ClickHouse alpha history |
| `portfolio-engine` | Alpha, risk budget, vol/corr/liquidity | Target portfolio | Postgres portfolio targets |
| `risk-engine` | Target portfolio and current state | Approved/scaled/rejected target | Postgres risk decisions |
| `execution-sim` | Approved target, historical book state | Simulated fills, NAV evolution | ClickHouse executions, Postgres run summary |
| `execution-live` | Approved target, live account state | Orders and reconciled positions | Postgres orders, ClickHouse fills |

## 6. Rust Implementation Structure

### 6.1 Near-Term Structure Inside Current Repository

Add a new smart-money tree without breaking existing strategy modules:

```text
src/smart_money/
  mod.rs
  domain/
    wallet.rs
    trader.rs
    profile.rs
    cluster.rs
    regime.rs
    score.rs
    alpha.rs
    portfolio.rs
    risk.rs
    execution.rs
  application/
    wallet_ingestion.rs
    profile_worker.rs
    cluster_worker.rs
    regime_worker.rs
    scoring_worker.rs
    alpha_aggregator.rs
    portfolio_engine.rs
    risk_engine.rs
    execution_simulator.rs
  adapters/
    postgres.rs
    clickhouse.rs
    redis.rs
    nats.rs
    hyperliquid_wallet.rs
    binance_replay.rs
  config.rs
  errors.rs
```

Add binaries:

```text
src/bin/smart_money_market_collector.rs
src/bin/smart_money_wallet_ingestor.rs
src/bin/smart_money_scoring_worker.rs
src/bin/smart_money_portfolio_engine.rs
src/bin/smart_money_backtest.rs
```

### 6.2 Longer-Term Workspace Split

Once interfaces stabilize, split into workspace crates:

```text
crates/
  smartmoney-domain/
  smartmoney-application/
  smartmoney-market-data/
  smartmoney-wallet-ingestion/
  smartmoney-profile/
  smartmoney-clustering/
  smartmoney-regime/
  smartmoney-scoring/
  smartmoney-alpha/
  smartmoney-portfolio/
  smartmoney-risk/
  smartmoney-execution-sim/
  smartmoney-backtest/
  adapters-postgres/
  adapters-clickhouse/
  adapters-redis/
  adapters-nats/
```

### 6.3 Dependency Additions

Current dependencies already include:

- `tokio`
- `reqwest`
- `serde`
- `serde_json`
- `thiserror`
- `anyhow`
- `chrono`
- `clickhouse`
- `rust_decimal`
- `tokio-tungstenite`
- `clap`
- `axum`

Add:

```toml
sqlx = { version = "0.8", features = ["runtime-tokio-rustls", "postgres", "uuid", "chrono", "rust_decimal", "json"] }
redis = { version = "0.27", features = ["tokio-comp"] }
async-nats = "0.38"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
config = "0.14"
```

Version numbers should be checked during implementation against the current Rust toolchain and dependency compatibility.

## 7. Domain Models

### 7.1 Wallet and Trader

```rust
pub struct TraderId(pub uuid::Uuid);
pub struct WalletId(pub uuid::Uuid);

pub struct Trader {
    pub trader_id: TraderId,
    pub first_seen: DateTime<Utc>,
    pub last_active: Option<DateTime<Utc>>,
    pub status: TraderStatus,
}

pub struct Wallet {
    pub wallet_id: WalletId,
    pub trader_id: TraderId,
    pub address: String,
    pub venue: Venue,
    pub first_seen: DateTime<Utc>,
    pub last_seen: Option<DateTime<Utc>>,
    pub active: bool,
}
```

### 7.2 Wallet Facts

```rust
pub enum Direction {
    Long,
    Short,
    Flat,
}

pub struct WalletPositionSnapshot {
    pub wallet_id: WalletId,
    pub symbol: String,
    pub direction: Direction,
    pub quantity: Decimal,
    pub entry_price: Decimal,
    pub mark_price: Decimal,
    pub notional_usdt: Decimal,
    pub equity_usdt: Decimal,
    pub margin_used_usdt: Decimal,
    pub leverage: Decimal,
    pub unrealized_pnl_usdt: Decimal,
    pub observed_at: DateTime<Utc>,
}

pub struct WalletTrade {
    pub wallet_id: WalletId,
    pub symbol: String,
    pub direction: Direction,
    pub price: Decimal,
    pub quantity: Decimal,
    pub notional_usdt: Decimal,
    pub fee_usdt: Decimal,
    pub executed_at: DateTime<Utc>,
    pub external_id: String,
}
```

### 7.3 Profile

```rust
pub struct WalletProfile {
    pub wallet_id: WalletId,
    pub as_of: DateTime<Utc>,
    pub total_return: Decimal,
    pub return_30d: Decimal,
    pub return_90d: Decimal,
    pub return_180d: Decimal,
    pub sharpe: Option<Decimal>,
    pub sortino: Option<Decimal>,
    pub calmar: Option<Decimal>,
    pub max_drawdown: Decimal,
    pub win_rate: Decimal,
    pub profit_factor: Decimal,
    pub average_holding_secs: i64,
    pub average_leverage: Decimal,
    pub maximum_leverage: Decimal,
    pub trade_frequency_daily: Decimal,
    pub position_concentration: Decimal,
    pub risk_per_trade: Decimal,
    pub signal_reproducibility: Decimal,
    pub capital_efficiency: Decimal,
    pub behavior_stability: Decimal,
    pub recent_performance: Decimal,
}
```

### 7.4 Score Matrix

```rust
pub struct WalletScoreMatrix {
    pub wallet_id: WalletId,
    pub as_of: DateTime<Utc>,
    pub trend_score: Decimal,
    pub range_score: Decimal,
    pub high_volatility_score: Decimal,
    pub low_volatility_score: Decimal,
    pub risk_on_score: Decimal,
    pub risk_off_score: Decimal,
    pub current_regime_score: Decimal,
    pub recent_performance_score: Decimal,
    pub consistency_score: Decimal,
    pub signal_quality_score: Decimal,
    pub execution_quality_score: Decimal,
    pub final_score: Decimal,
}
```

### 7.5 Alpha Opinion

```rust
pub struct WalletOpinion {
    pub wallet_id: WalletId,
    pub symbol: String,
    pub direction: Direction,
    pub confidence: Decimal,
    pub conviction: Decimal,
    pub dynamic_score: Decimal,
    pub observed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

pub struct AggregatedAlpha {
    pub symbol: String,
    pub as_of: DateTime<Utc>,
    pub long_pressure: Decimal,
    pub short_pressure: Decimal,
    pub consensus_strength: Decimal,
    pub capital_weighted_consensus: Decimal,
    pub cluster_weighted_consensus: Decimal,
    pub net_alpha_score: Decimal,
    pub alpha_confidence_score: Decimal,
}
```

### 7.6 Portfolio

```rust
pub struct TargetPortfolio {
    pub portfolio_id: uuid::Uuid,
    pub as_of: DateTime<Utc>,
    pub nav_usdt: Decimal,
    pub max_gross_notional_usdt: Decimal,
    pub max_leverage: Decimal,
    pub target_weights: BTreeMap<String, Decimal>,
    pub target_notional: BTreeMap<String, Decimal>,
    pub cash_weight: Decimal,
}
```

## 8. Data Storage Design

### 8.1 Storage Split

Use Postgres for canonical entities and slowly changing state:

- Traders.
- Wallet registry.
- Trader metadata.
- Profile history.
- Score history.
- Ranking history.
- Cluster history.
- Regime snapshots.
- Portfolio targets.
- Risk decisions.
- Orders and order state.
- Backtest run metadata.

Use ClickHouse for high-volume facts:

- Binance order book updates.
- Binance trades.
- Mark price and funding history.
- Wallet trades.
- Wallet positions.
- Wallet equity snapshots.
- Alpha pressure snapshots.
- Execution fills.
- NAV snapshots.

Use Redis for hot state:

- Latest wallet score.
- Latest wallet profile.
- Latest target portfolio.
- Idempotency keys.
- Worker leases.
- Stream lag checkpoints.

Use NATS for event distribution:

- `market.binance.depth`
- `market.binance.trade`
- `market.binance.mark`
- `market.binance.funding`
- `wallet.hyperliquid.position`
- `wallet.hyperliquid.trade`
- `wallet.hyperliquid.equity`
- `wallet.profile.updated`
- `wallet.score.updated`
- `alpha.symbol.updated`
- `portfolio.target.created`
- `risk.decision.created`
- `execution.fill.created`

### 8.2 Postgres Schema

```sql
create table traders (
    trader_id uuid primary key,
    canonical_name text,
    first_seen timestamptz not null,
    last_active timestamptz,
    status text not null,
    metadata jsonb not null default '{}'
);

create table wallets (
    wallet_id uuid primary key,
    trader_id uuid not null references traders(trader_id),
    address text not null unique,
    venue text not null,
    first_seen timestamptz not null,
    last_seen timestamptz,
    active boolean not null default true,
    metadata jsonb not null default '{}'
);

create table wallet_profiles (
    wallet_id uuid not null references wallets(wallet_id),
    as_of timestamptz not null,
    total_return numeric not null,
    return_30d numeric not null,
    return_90d numeric not null,
    return_180d numeric not null,
    sharpe numeric,
    sortino numeric,
    calmar numeric,
    max_drawdown numeric not null,
    win_rate numeric not null,
    profit_factor numeric not null,
    average_holding_secs bigint not null,
    average_leverage numeric not null,
    maximum_leverage numeric not null,
    trade_frequency_daily numeric not null,
    position_concentration numeric not null,
    risk_per_trade numeric not null,
    signal_reproducibility numeric not null,
    capital_efficiency numeric not null,
    behavior_stability numeric not null,
    recent_performance numeric not null,
    inputs jsonb not null default '{}',
    primary key (wallet_id, as_of)
);

create table trader_cluster_history (
    trader_id uuid not null references traders(trader_id),
    wallet_id uuid not null references wallets(wallet_id),
    as_of timestamptz not null,
    cluster text not null,
    confidence numeric not null,
    features jsonb not null,
    primary key (trader_id, wallet_id, as_of, cluster)
);

create table wallet_score_history (
    wallet_id uuid not null references wallets(wallet_id),
    as_of timestamptz not null,
    trend_score numeric not null,
    range_score numeric not null,
    high_volatility_score numeric not null,
    low_volatility_score numeric not null,
    risk_on_score numeric not null,
    risk_off_score numeric not null,
    current_regime_score numeric not null,
    recent_performance_score numeric not null,
    consistency_score numeric not null,
    signal_quality_score numeric not null,
    execution_quality_score numeric not null,
    final_score numeric not null,
    inputs jsonb not null default '{}',
    primary key (wallet_id, as_of)
);

create table wallet_ranking_history (
    wallet_id uuid not null references wallets(wallet_id),
    as_of timestamptz not null,
    universe text not null,
    rank integer not null,
    percentile numeric not null,
    score numeric not null,
    primary key (wallet_id, as_of, universe)
);

create table market_regime_history (
    as_of timestamptz primary key,
    bull_trend numeric not null,
    bear_trend numeric not null,
    range_score numeric not null,
    volatility_expansion numeric not null,
    volatility_compression numeric not null,
    risk_on numeric not null,
    risk_off numeric not null,
    funding_extreme numeric not null,
    liquidity_crisis numeric not null,
    trend_acceleration numeric not null,
    trend_exhaustion numeric not null,
    inputs jsonb not null default '{}'
);

create table target_portfolios (
    portfolio_id uuid not null,
    as_of timestamptz not null,
    nav_usdt numeric not null,
    gross_notional_usdt numeric not null,
    leverage numeric not null,
    target_weights jsonb not null,
    target_notional jsonb not null,
    source_alpha_snapshot timestamptz not null,
    status text not null,
    primary key (portfolio_id, as_of)
);

create table risk_decisions (
    decision_id uuid primary key,
    portfolio_id uuid not null,
    target_as_of timestamptz not null,
    decided_at timestamptz not null,
    decision text not null,
    scaled_weights jsonb,
    reject_reasons jsonb not null default '[]',
    risk_metrics jsonb not null default '{}'
);
```

### 8.3 ClickHouse Schema

```sql
create table binance_depth_events (
    ts DateTime64(3),
    recv_ts DateTime64(3),
    symbol LowCardinality(String),
    first_update_id UInt64,
    final_update_id UInt64,
    prev_final_update_id UInt64,
    bids Array(Tuple(Decimal(38, 18), Decimal(38, 18))),
    asks Array(Tuple(Decimal(38, 18), Decimal(38, 18)))
) engine = MergeTree
partition by toYYYYMMDD(ts)
order by (symbol, ts, final_update_id);

create table binance_trades (
    ts DateTime64(3),
    recv_ts DateTime64(3),
    symbol LowCardinality(String),
    trade_id UInt64,
    side LowCardinality(String),
    price Decimal(38, 18),
    quantity Decimal(38, 18),
    notional_usdt Decimal(38, 8)
) engine = MergeTree
partition by toYYYYMMDD(ts)
order by (symbol, ts, trade_id);

create table market_funding (
    ts DateTime64(3),
    symbol LowCardinality(String),
    exchange LowCardinality(String),
    funding_rate Decimal(18, 10),
    mark_price Decimal(38, 18),
    next_funding_ts DateTime64(3)
) engine = MergeTree
partition by toYYYYMM(ts)
order by (exchange, symbol, ts);

create table wallet_position_snapshots (
    ts DateTime64(3),
    wallet_id UUID,
    address String,
    venue LowCardinality(String),
    symbol LowCardinality(String),
    direction Int8,
    quantity Decimal(38, 18),
    entry_price Decimal(38, 18),
    mark_price Decimal(38, 18),
    notional_usdt Decimal(38, 8),
    equity_usdt Decimal(38, 8),
    margin_used_usdt Decimal(38, 8),
    leverage Decimal(18, 8),
    unrealized_pnl_usdt Decimal(38, 8)
) engine = MergeTree
partition by toYYYYMM(ts)
order by (wallet_id, symbol, ts);

create table wallet_trades (
    ts DateTime64(3),
    wallet_id UUID,
    address String,
    venue LowCardinality(String),
    symbol LowCardinality(String),
    side LowCardinality(String),
    price Decimal(38, 18),
    quantity Decimal(38, 18),
    notional_usdt Decimal(38, 8),
    fee_usdt Decimal(38, 8),
    external_id String
) engine = MergeTree
partition by toYYYYMM(ts)
order by (wallet_id, symbol, ts);

create table alpha_snapshots (
    ts DateTime64(3),
    symbol LowCardinality(String),
    long_pressure Decimal(38, 18),
    short_pressure Decimal(38, 18),
    consensus_strength Decimal(18, 8),
    capital_weighted_consensus Decimal(38, 18),
    cluster_weighted_consensus Decimal(38, 18),
    net_alpha_score Decimal(18, 8),
    alpha_confidence_score Decimal(18, 8)
) engine = MergeTree
partition by toYYYYMM(ts)
order by (symbol, ts);

create table execution_fills (
    ts DateTime64(3),
    run_id UUID,
    portfolio_id UUID,
    symbol LowCardinality(String),
    side LowCardinality(String),
    requested_notional_usdt Decimal(38, 8),
    filled_notional_usdt Decimal(38, 8),
    quantity Decimal(38, 18),
    avg_price Decimal(38, 18),
    fee_usdt Decimal(38, 8),
    slippage_bps Decimal(18, 8),
    liquidity_levels_consumed UInt32,
    partial_fill UInt8
) engine = MergeTree
partition by toYYYYMM(ts)
order by (run_id, symbol, ts);
```

## 9. Noise Filtering Rules

The platform must aggressively remove low-quality wallet signals before profile/scoring.

Default rules:

- Ignore trades below configurable `min_trade_notional_usdt`, default `100 USDT`.
- Ignore trades below `0.10%` of wallet equity.
- Ignore positions below configurable `min_position_notional_usdt`, default `250 USDT`.
- Ignore positions below `0.50%` of wallet equity.
- Ignore wallets with equity below `5000 USDT`.
- Ignore wallets with fewer than `50` closed trades.
- Ignore wallets with less than `45` calendar days of history.
- Ignore wallets active on fewer than `15` distinct days.
- Ignore wallets with max leverage above `15x`, unless explicitly classified as high-risk and capped to very low influence.
- Ignore wallets with max drawdown above `40%`.
- Ignore wallets with profit factor below `1.10`.
- Ignore stale wallet state older than `2 * ingestion_interval`.
- Downweight wallets whose PnL is dominated by one trade: largest trade PnL above `50%` of total realized PnL.
- Downweight wallets whose current position is likely a hedge: low notional, opposite recent dominant exposure, short expected holding time, or near-zero net exposure across related assets.

Filtering output should be persisted:

```rust
pub struct WalletFilterDecision {
    pub wallet_id: WalletId,
    pub as_of: DateTime<Utc>,
    pub accepted: bool,
    pub reasons: Vec<String>,
    pub input_snapshot_id: String,
}
```

## 10. Profiling, Clustering, and Scoring

### 10.1 Wallet Profile Metrics

Persist every profile snapshot. Do not calculate only in memory.

Metrics:

- Total return.
- 30D, 90D, 180D return.
- Sharpe, Sortino, Calmar.
- Maximum drawdown.
- Win rate.
- Profit factor.
- Average holding time.
- Average leverage.
- Maximum leverage.
- Trade frequency.
- Position concentration.
- Risk per trade.
- Signal reproducibility.
- Capital efficiency.
- Behavior stability.
- Recent performance.

### 10.2 Multi-Label Trader Clusters

Supported labels:

- Trend.
- Swing.
- Intraday.
- Scalper.
- Momentum.
- Mean reversion.
- Whale.
- Institutional style.
- Consistent alpha.
- Gambler.
- New alpha.

Initial rule-based implementation:

| Cluster | Rule Sketch |
|---|---|
| Trend | Longer holding periods, entries aligned with realized trend, positive convexity during breakouts |
| Swing | Holding period from hours to days, moderate turnover, moderate leverage |
| Intraday | Most trades closed within one day, high activity, low overnight exposure |
| Scalper | Median holding time below 30 minutes, high trade frequency, small average PnL per trade |
| Momentum | Adds into winners, positive return autocorrelation, strong relative-strength exposure |
| Mean reversion | Buys weakness/sells strength, profits in range regimes, negative entry momentum |
| Whale | Top percentile equity or position notional |
| Institutional style | Lower leverage, diversified symbols, controlled drawdown, stable sizing |
| Consistent alpha | Stable Sharpe/Sortino, low drawdown, repeatable edge |
| Gambler | High leverage, high drawdown, erratic sizing, poor loss control |
| New alpha | Shorter history, strong recent score, capped confidence |

Later implementation can use HDBSCAN, Gaussian mixture models, or online clustering on normalized behavior features.

### 10.3 Dynamic Score Formula

Default formula:

```text
final_score =
  0.22 * current_regime_score
+ 0.18 * recent_performance_score
+ 0.15 * consistency_score
+ 0.15 * signal_quality_score
+ 0.10 * execution_quality_score
+ 0.10 * capital_efficiency
+ 0.10 * risk_adjusted_return_score
- penalties
```

Decay old data:

```text
decay_weight(age_days) = exp(-lambda * age_days)
```

Penalties:

- Recent drawdown acceleration.
- Leverage spike.
- Unstable trade frequency.
- Cluster drift.
- PnL dominated by one event.
- Large position in low-liquidity asset.
- Deteriorating execution quality.

## 11. Market Regime Engine

Persist regime history.

Regime labels:

- Bull trend.
- Bear trend.
- Range.
- Volatility expansion.
- Volatility compression.
- Risk-on.
- Risk-off.
- Funding extreme.
- Liquidity crisis.
- Trend acceleration.
- Trend exhaustion.

Inputs:

- BTC/ETH/HYPE trend slopes.
- Moving-average distance.
- ADX or trend strength.
- Realized volatility percentile.
- Order book spread/depth.
- Funding z-score.
- Open interest and liquidation proxies where available.
- Cross-asset breadth.

Output:

```rust
pub struct MarketRegimeSnapshot {
    pub as_of: DateTime<Utc>,
    pub bull_trend: Decimal,
    pub bear_trend: Decimal,
    pub range: Decimal,
    pub volatility_expansion: Decimal,
    pub volatility_compression: Decimal,
    pub risk_on: Decimal,
    pub risk_off: Decimal,
    pub funding_extreme: Decimal,
    pub liquidity_crisis: Decimal,
    pub trend_acceleration: Decimal,
    pub trend_exhaustion: Decimal,
}
```

## 12. Alpha Aggregation

Wallets produce opinions, not orders.

Opinion weight:

```text
opinion_weight =
  dynamic_score
* confidence
* conviction
* freshness_decay
* cluster_diversification_weight
* liquidity_cap
```

Per-symbol aggregation:

```text
long_pressure = sum(weight where direction = long)
short_pressure = sum(weight where direction = short)
net_alpha = (long_pressure - short_pressure) / (long_pressure + short_pressure + epsilon)
consensus_strength = abs(net_alpha)
capital_weighted_consensus = sum(wallet_equity * weight * direction)
cluster_weighted_consensus = sum(cluster_weight * cluster_alpha)
```

Reject or downweight aggregated alpha when:

- One wallet contributes more than `20%` of total pressure.
- One cluster contributes more than `40%` of total pressure.
- Input wallet states are stale.
- Signal conflicts with liquidity crisis regime.
- Alpha direction is overcrowded by funding extremes.
- Confidence is high but source diversity is low.

## 13. Portfolio Construction

The portfolio engine consumes aggregated alpha and outputs target allocations.

MVP algorithm:

```text
raw_notional_i = standard_entry_notional * sign(alpha_i) * confidence_scale
vol_adjusted_i = raw_notional_i / max(realized_vol_i, vol_floor)
liquidity_capped_i = min(abs(vol_adjusted_i), liquidity_cap_i) * sign(vol_adjusted_i)
asset_capped_i = clamp(liquidity_capped_i, -asset_cap_i, asset_cap_i)
gross_scaled = scale_to_gross_limit(asset_capped, 20000 USDT)
weights_i = gross_scaled_i / nav
```

Hard checks:

```text
sum(abs(target_notional_i)) <= 20000
sum(abs(target_notional_i)) / nav <= 10
new position default notional <= 1000 before optimizer scaling
cash_weight = 1 - sum(abs(target_weights_i)) where cash can be negative only if leverage is used
```

Institutional optimizer target:

```text
maximize:
    alpha' w
  - lambda_risk * w' Sigma w
  - lambda_turnover * abs(w - current_w)
  - lambda_concentration * concentration_penalty(w)

subject to:
  gross_notional <= 20000
  leverage <= 10
  abs(asset_notional_i) <= asset_cap_i
  liquidity_participation_i <= participation_cap_i
  expected_drawdown <= drawdown_budget
  margin_used <= cross_margin_capacity
```

## 14. Binance Order Book Replay

Backtests must not use candle-only fills.

Required Binance Futures data:

- Diff depth stream.
- Periodic REST depth snapshots.
- Trade stream.
- Mark price stream.
- Funding stream/history.

Replay algorithm:

1. Load the nearest valid REST depth snapshot before the replay start.
2. Apply buffered diff events after snapshot sequence.
3. Require valid sequence continuity.
4. Maintain bids and asks in price-ordered maps.
5. Advance replay clock by event timestamp.
6. Publish book state to the execution simulator.
7. Resync or fail the run on sequence gaps depending on strictness mode.

Execution simulator must query book state at `decision_ts + latency_ms`, not at the decision timestamp.

## 15. Execution Simulation

Simulation mode: taker-only market orders.

Buy order:

- Sweep asks from best ask upward.
- Consume visible depth until quantity or notional target is filled.
- Calculate VWAP from consumed levels.
- Record levels consumed.
- If insufficient depth, record partial fill and residual.

Sell order:

- Sweep bids from best bid downward.

Fees:

```text
fee_usdt = filled_notional_usdt * 0.0004
```

Slippage:

```text
buy_slippage_bps = (vwap / arrival_mid - 1) * 10000
sell_slippage_bps = (1 - vwap / arrival_mid) * 10000
```

Latency model:

```rust
pub struct LatencyModel {
    pub decision_to_submit_ms: u64,
    pub exchange_ack_ms: u64,
    pub jitter_ms: u64,
}
```

Partial fill policy:

- Fill available depth up to participation cap.
- Cancel residual or retry after configured delay.
- Recompute book state for every retry.
- Charge fees only on filled notional.

## 16. Position and NAV Management

Position state:

```rust
pub struct PositionState {
    pub symbol: String,
    pub quantity: Decimal,
    pub average_entry_price: Decimal,
    pub notional_usdt: Decimal,
    pub realized_pnl_usdt: Decimal,
    pub unrealized_pnl_usdt: Decimal,
    pub funding_pnl_usdt: Decimal,
    pub fee_paid_usdt: Decimal,
    pub margin_used_usdt: Decimal,
}
```

Supported transitions:

- Open.
- Scale in.
- Scale out.
- Reduce.
- Close.
- Reverse.

NAV:

```text
nav = initial_capital
    + realized_pnl
    + unrealized_pnl
    + funding_pnl
    - fees
```

Cross-margin leverage:

```text
gross_notional = sum(abs(position_notional))
leverage = gross_notional / nav
margin_used = gross_notional / max_leverage
```

## 17. Risk Management

Risk must override alpha and portfolio construction.

Hard limits:

- Maximum gross notional: `20000 USDT`.
- Maximum leverage: `10x`.
- Maximum standard entry size: `1000 USDT`.
- No trade if market data is stale.
- No trade if wallet score inputs are stale.
- No trade if order book replay/live book is invalid.

Recommended soft limits:

- Daily loss reduce risk at `-2% NAV`.
- Daily loss close-only at `-3% NAV`.
- Daily loss kill switch at `-5% NAV`.
- Drawdown risk scale at `8%`.
- Drawdown close-only at `12%`.
- Drawdown kill switch at `15%`.
- Single asset cap: BTC/ETH up to `35%` gross, majors `20%`, alts `10%`, illiquid assets `5%`.
- Cluster alpha cap: no single trader cluster above `40%` of aggregate pressure.
- Wallet alpha cap: no single wallet above `20%` of pressure.
- Funding extreme: reduce exposure on crowded paying side.
- Liquidity crisis: reduce gross target leverage.

Risk decision:

```rust
pub enum RiskDecisionKind {
    Approved,
    Scaled,
    Rejected,
    CloseOnly,
    KillSwitch,
}
```

## 18. Execution State Machine

Portfolio transition flow:

```text
TargetCreated
  -> RiskChecked
  -> Approved
  -> TransitionPlanned
  -> OrderPlanned
  -> Submitted
  -> PartiallyFilled
  -> Filled
  -> PositionVerified
  -> Reconciled
```

Failure flows:

```text
Submitted -> Rejected -> Replanned
Submitted -> Timeout -> RetryPending -> Submitted
PartiallyFilled -> ResidualCancelled -> ResidualReplanned
Any -> ExchangeDisconnected -> RecoveryMode -> ReconcileRequired
Any -> ReconcileMismatch -> CloseOnly
Any -> KillSwitch -> EmergencyFlatten
```

Idempotency:

```text
client_order_id = hash(run_id, portfolio_version, symbol, action, attempt)
```

This can reuse the existing `execution::idempotency`, `execution::command`, `execution::router`, and `execution::reconciler` concepts.

## 19. Backtesting Framework

Backtest rules:

- No lookahead bias.
- Wallet profiles use only facts available at that timestamp.
- Wallet scores update daily.
- Wallet rankings update weekly.
- Market regimes update from historical market data only.
- Portfolio rebalances use only available score/regime/alpha snapshots.
- Execution uses historical order book state at latency-adjusted arrival time.
- Fees are charged on every fill.
- Funding is charged/credited at historical funding timestamps.
- Partial fills, residuals, and slippage are recorded.

Backtest output metrics:

- Total return.
- Annualized return.
- Sharpe.
- Sortino.
- Calmar.
- Maximum drawdown.
- Drawdown duration.
- Turnover.
- Gross PnL.
- Fee cost.
- Funding PnL.
- Slippage cost.
- Net PnL.
- Fill ratio.
- Partial fill ratio.
- Alpha contribution by wallet cluster.
- Alpha contribution by regime.
- Benchmark comparison against BTC, ETH, HYPE, passive basket, and cash.

## 20. Development Roadmap

### Phase 0: Architecture Stabilization

- Add `smart_money` module namespace.
- Add missing dependencies.
- Add config loader for smart-money services.
- Add `tracing` setup without disrupting existing `log` usage.
- Define domain models and repository traits.
- Add Postgres migrations and ClickHouse schemas.

Deliverable:

- Compile-only smart-money domain and adapter skeleton.

### Phase 1: Binance Market Data Foundation

- Implement Binance Futures depth/trade/mark/funding collector.
- Persist raw events to ClickHouse.
- Implement snapshot + diff sequence validation.
- Build order book replay engine.
- Add tests for sequence gaps, snapshot recovery, and top-of-book correctness.

Deliverable:

- Replayable Binance order book dataset.

### Phase 2: Hyperliquid Wallet Ingestion

- Implement wallet registry.
- Implement Hyperliquid account state fetch.
- Ingest wallet trades, positions, funding, equity, and liquidation history when available.
- Persist wallet facts.
- Add ingestion checkpoints and stale-wallet detection.
- Initial monitored address batch: [smart_money_hyperliquid_initial_monitoring_batch.md](smart_money_hyperliquid_initial_monitoring_batch.md).

Deliverable:

- Durable wallet fact store for thousands of wallets.

### Phase 3: Profiles, Clusters, Scores

- Implement filtering rules.
- Implement profile metrics.
- Implement rule-based clustering.
- Implement market regime snapshots.
- Implement score matrix and ranking history.

Deliverable:

- Historical wallet ranking and score series.

### Phase 4: Alpha and Portfolio

- Convert wallet positions into opinions.
- Aggregate opinions into symbol alpha.
- Build target portfolio constructor under capital constraints.
- Add risk veto/scaling.
- Persist target portfolios and risk decisions.

Deliverable:

- Portfolio target output generated from wallet intelligence.

### Phase 5: Reality-Based Backtesting

- Integrate order book replay with portfolio transitions.
- Implement taker sweep execution simulation.
- Apply fees, funding, latency, slippage, and partial fills.
- Produce NAV and performance reports.

Deliverable:

- End-to-end point-in-time backtest.

### Phase 6: Shadow and Live Execution

- Run smart-money portfolio engine in shadow mode.
- Compare target vs simulated fill vs real market book.
- Integrate approved portfolio transitions with existing execution router.
- Add reconciliation, close-only, and emergency flatten flows.

Deliverable:

- Controlled live execution path.

## 21. Testing Strategy

Unit tests:

- Filtering decisions.
- Profile calculations.
- Cluster classification.
- Score decay.
- Alpha aggregation.
- Portfolio constraints.
- Risk decisions.
- Order book sweep fill math.
- Fee and funding accounting.

Integration tests:

- Binance snapshot + diff replay.
- ClickHouse write/read path.
- Postgres migrations.
- NATS event publishing/consumption.
- End-to-end backtest over a small fixture.

Regression tests:

- Known wallet profile fixtures.
- Known order book fill fixtures.
- Known portfolio transition fixtures.
- No-lookahead tests where future wallet PnL must not affect current ranking.

## 22. Production Deployment Plan

Recommended deployment units:

- `smart-money-market-collector.service`
- `smart-money-wallet-ingestor.service`
- `smart-money-profile-worker.service`
- `smart-money-scoring-worker.service`
- `smart-money-portfolio-engine.service`
- `smart-money-backtest-worker.service`
- `smart-money-execution-daemon.service`

Infrastructure:

- Postgres for canonical state.
- ClickHouse for facts.
- Redis for hot cache and idempotency.
- NATS for event bus.
- Prometheus/Grafana or equivalent for metrics.
- JSON `tracing` logs with run ID, wallet ID, portfolio ID, and order ID.

Operational controls:

- Manual pause.
- Close-only.
- Kill switch.
- Per-service lag monitoring.
- Ingestion freshness checks.
- Reconciliation drift alerts.
- Order book sequence gap alerts.

## 23. Migration Notes From Current Codebase

Keep:

- `src/market` contracts.
- `src/execution` command/router/reconciliation ideas.
- `src/exchanges` adapter patterns.
- `src/backtest` replay/matching foundations.
- Cross-exchange arbitrage risk-state patterns.

Refactor:

- Move common execution sizing/precision helpers out of strategy-specific modules when stable.
- Convert execution state from arbitrage bundle-specific to generic portfolio transition-specific.
- Move durable storage out of strategy-local JSONL sinks for smart-money services.
- Replace log-only operational telemetry with structured `tracing`.

Avoid:

- Building smart-money logic inside `copy_trading`.
- Adding trader/wallet metadata into generic `core::types`.
- Adding persistence side effects into pure domain modules.
- Running live execution before replay and shadow-mode validation.

## 24. Main Failure Modes

- Binance depth sequence gaps corrupt replay.
- Wallet ingestion lag creates stale alpha.
- Hyperliquid schema/API changes.
- Wallet identity fragmentation across addresses.
- Hidden off-chain hedges make wallet behavior misleading.
- One whale or cluster dominates alpha pressure.
- Score model overfits recent lucky wallets.
- Funding and fees erase gross alpha.
- Liquidity model is too optimistic.
- Cross-margin liquidation model is too lenient.
- Partial fills produce residual exposure.
- Live exchange state diverges from internal state.
- Redis/Postgres/ClickHouse inconsistency after partial outages.

## 25. Future Research Directions

- Entity clustering across related wallets.
- Bayesian trader skill estimation.
- Regime-conditioned score weights.
- Online learning for alpha half-life.
- Wallet adversarial behavior detection.
- Causal impact modeling: wallet action before price movement.
- Capacity estimation per symbol and cluster.
- CVaR or expected shortfall optimizer.
- Execution model calibration from real fills.
- Shadow portfolios for promoted, downgraded, and newly discovered wallets.

## 26. Immediate Implementation Checklist

1. Add dependencies and `src/smart_money/mod.rs`.
2. Add domain model files.
3. Add Postgres migration directory for smart-money schema.
4. Add ClickHouse schema files for Binance and wallet facts.
5. Implement Binance market collector.
6. Implement order book replay engine.
7. Implement wallet registry and Hyperliquid wallet ingestion adapter.
8. Implement wallet filter and profile worker.
9. Implement score matrix and rank history.
10. Implement alpha aggregation.
11. Implement portfolio constructor with `2000 USDT / 10x / 20000 USDT` hard constraints.
12. Implement taker execution simulator with `0.04%` fee.
13. Add point-in-time backtest runner.
14. Add shadow-mode report before live execution.
