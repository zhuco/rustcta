-- Smart Money Alpha Platform high-volume ClickHouse schema.
--
-- Scope:
-- - Binance Futures depth/trades/mark/funding facts.
-- - Hyperliquid wallet position/trade/equity/funding facts.
-- - Alpha snapshots, execution fills, NAV snapshots, and backtest time-series results.
-- - ReplacingMergeTree latest tables populated by materialized views for service reads.

CREATE DATABASE IF NOT EXISTS rustcta;

CREATE TABLE IF NOT EXISTS rustcta.smart_money_binance_depth_events
(
    ts DateTime64(3, 'UTC'),
    recv_ts DateTime64(3, 'UTC'),
    symbol LowCardinality(String),
    first_update_id UInt64,
    final_update_id UInt64,
    prev_final_update_id UInt64,
    bids Array(Tuple(price Decimal(38, 18), quantity Decimal(38, 18))),
    asks Array(Tuple(price Decimal(38, 18), quantity Decimal(38, 18))),
    book_depth UInt16,
    sequence_gap Bool DEFAULT false,
    source LowCardinality(String) DEFAULT 'binance_futures_ws',
    raw_event String DEFAULT ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (symbol, ts, final_update_id)
TTL ts + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS rustcta.smart_money_binance_trades
(
    ts DateTime64(3, 'UTC'),
    recv_ts DateTime64(3, 'UTC'),
    symbol LowCardinality(String),
    trade_id UInt64,
    side LowCardinality(String),
    price Decimal(38, 18),
    quantity Decimal(38, 18),
    notional_usdt Decimal(38, 8),
    is_buyer_maker Bool,
    source LowCardinality(String) DEFAULT 'binance_futures_ws'
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (symbol, ts, trade_id)
TTL ts + INTERVAL 365 DAY;

CREATE TABLE IF NOT EXISTS rustcta.smart_money_binance_mark_prices
(
    ts DateTime64(3, 'UTC'),
    recv_ts DateTime64(3, 'UTC'),
    symbol LowCardinality(String),
    mark_price Decimal(38, 18),
    index_price Decimal(38, 18),
    estimated_settle_price Nullable(Decimal(38, 18)),
    funding_rate Decimal(18, 10),
    next_funding_ts DateTime64(3, 'UTC'),
    source LowCardinality(String) DEFAULT 'binance_futures_mark'
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (symbol, ts)
TTL ts + INTERVAL 365 DAY;

CREATE TABLE IF NOT EXISTS rustcta.smart_money_market_funding
(
    ts DateTime64(3, 'UTC'),
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    funding_rate Decimal(18, 10),
    mark_price Decimal(38, 18),
    index_price Nullable(Decimal(38, 18)),
    funding_interval_hours UInt8 DEFAULT 8,
    next_funding_ts Nullable(DateTime64(3, 'UTC')),
    source LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (exchange, symbol, ts)
TTL ts + INTERVAL 1095 DAY;

CREATE TABLE IF NOT EXISTS rustcta.smart_money_wallet_position_snapshots
(
    ts DateTime64(3, 'UTC'),
    recv_ts DateTime64(3, 'UTC'),
    wallet_id UUID,
    address String,
    trader_id UUID,
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
    unrealized_pnl_usdt Decimal(38, 8),
    liquidation_price Nullable(Decimal(38, 18)),
    raw_snapshot String DEFAULT ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (wallet_id, symbol, ts)
TTL ts + INTERVAL 1095 DAY;

CREATE TABLE IF NOT EXISTS rustcta.smart_money_wallet_trades
(
    ts DateTime64(3, 'UTC'),
    recv_ts DateTime64(3, 'UTC'),
    wallet_id UUID,
    address String,
    trader_id UUID,
    venue LowCardinality(String),
    symbol LowCardinality(String),
    side LowCardinality(String),
    direction_after Int8,
    price Decimal(38, 18),
    quantity Decimal(38, 18),
    notional_usdt Decimal(38, 8),
    fee_usdt Decimal(38, 8),
    realized_pnl_usdt Nullable(Decimal(38, 8)),
    external_id String,
    raw_trade String DEFAULT ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (wallet_id, symbol, ts, external_id)
TTL ts + INTERVAL 1095 DAY;

CREATE TABLE IF NOT EXISTS rustcta.smart_money_wallet_equity_snapshots
(
    ts DateTime64(3, 'UTC'),
    recv_ts DateTime64(3, 'UTC'),
    wallet_id UUID,
    address String,
    trader_id UUID,
    venue LowCardinality(String),
    equity_usdt Decimal(38, 8),
    available_margin_usdt Nullable(Decimal(38, 8)),
    total_position_notional_usdt Decimal(38, 8),
    unrealized_pnl_usdt Decimal(38, 8),
    raw_snapshot String DEFAULT ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (wallet_id, ts)
TTL ts + INTERVAL 1095 DAY;

CREATE TABLE IF NOT EXISTS rustcta.smart_money_wallet_funding_events
(
    ts DateTime64(3, 'UTC'),
    recv_ts DateTime64(3, 'UTC'),
    wallet_id UUID,
    address String,
    trader_id UUID,
    venue LowCardinality(String),
    symbol LowCardinality(String),
    position_side LowCardinality(String),
    notional_usdt Decimal(38, 8),
    funding_rate Decimal(18, 10),
    funding_usdt Decimal(38, 8),
    external_id String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (wallet_id, symbol, ts, external_id)
TTL ts + INTERVAL 1095 DAY;

CREATE TABLE IF NOT EXISTS rustcta.smart_money_alpha_snapshots
(
    ts DateTime64(3, 'UTC'),
    run_id Nullable(UUID),
    symbol LowCardinality(String),
    long_pressure Decimal(38, 18),
    short_pressure Decimal(38, 18),
    consensus_strength Decimal(18, 8),
    capital_weighted_consensus Decimal(38, 18),
    cluster_weighted_consensus Decimal(38, 18),
    net_alpha_score Decimal(18, 8),
    alpha_confidence_score Decimal(18, 8),
    contributing_wallets UInt32,
    dominant_wallet_share Decimal(18, 8),
    dominant_cluster_share Decimal(18, 8),
    inputs String DEFAULT ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (symbol, ts)
TTL ts + INTERVAL 1095 DAY;

CREATE TABLE IF NOT EXISTS rustcta.smart_money_execution_fills
(
    ts DateTime64(3, 'UTC'),
    run_id UUID,
    portfolio_id UUID,
    portfolio_as_of DateTime64(3, 'UTC'),
    fill_id String,
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    side LowCardinality(String),
    requested_notional_usdt Decimal(38, 8),
    filled_notional_usdt Decimal(38, 8),
    quantity Decimal(38, 18),
    avg_price Decimal(38, 18),
    fee_usdt Decimal(38, 8),
    funding_usdt Decimal(38, 8) DEFAULT 0,
    slippage_bps Decimal(18, 8),
    liquidity_levels_consumed UInt32,
    partial_fill Bool,
    simulated Bool DEFAULT true,
    client_order_id String DEFAULT '',
    exchange_order_id String DEFAULT '',
    raw_fill String DEFAULT ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (run_id, portfolio_id, symbol, ts, fill_id)
TTL ts + INTERVAL 1095 DAY;

CREATE TABLE IF NOT EXISTS rustcta.smart_money_nav_snapshots
(
    ts DateTime64(3, 'UTC'),
    run_id UUID,
    portfolio_id Nullable(UUID),
    nav_usdt Decimal(38, 8),
    cash_usdt Decimal(38, 8),
    gross_notional_usdt Decimal(38, 8),
    net_notional_usdt Decimal(38, 8),
    leverage Decimal(18, 8),
    realized_pnl_usdt Decimal(38, 8),
    unrealized_pnl_usdt Decimal(38, 8),
    fees_usdt Decimal(38, 8),
    funding_usdt Decimal(38, 8),
    drawdown Decimal(18, 8),
    positions String DEFAULT ''
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (run_id, ts)
TTL ts + INTERVAL 1095 DAY;

CREATE TABLE IF NOT EXISTS rustcta.smart_money_backtest_results
(
    completed_at DateTime64(3, 'UTC'),
    run_id UUID,
    run_name String,
    dataset_start DateTime64(3, 'UTC'),
    dataset_end DateTime64(3, 'UTC'),
    initial_nav_usdt Decimal(38, 8),
    final_nav_usdt Decimal(38, 8),
    total_return Decimal(18, 8),
    annualized_return Nullable(Decimal(18, 8)),
    sharpe Nullable(Decimal(18, 8)),
    sortino Nullable(Decimal(18, 8)),
    calmar Nullable(Decimal(18, 8)),
    max_drawdown Decimal(18, 8),
    gross_pnl_usdt Decimal(38, 8),
    fees_usdt Decimal(38, 8),
    funding_usdt Decimal(38, 8),
    slippage_usdt Decimal(38, 8),
    fills_count UInt64,
    partial_fills_count UInt64,
    portfolio_count UInt64,
    risk_reject_count UInt64,
    config String DEFAULT '',
    metrics String DEFAULT ''
)
ENGINE = ReplacingMergeTree(completed_at)
PARTITION BY toYYYYMM(completed_at)
ORDER BY (run_id);

CREATE TABLE IF NOT EXISTS rustcta.smart_money_latest_wallet_positions
(
    ts DateTime64(3, 'UTC'),
    recv_ts DateTime64(3, 'UTC'),
    wallet_id UUID,
    address String,
    trader_id UUID,
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
    unrealized_pnl_usdt Decimal(38, 8),
    liquidation_price Nullable(Decimal(38, 18)),
    raw_snapshot String
)
ENGINE = ReplacingMergeTree(ts)
ORDER BY (wallet_id, symbol);

CREATE MATERIALIZED VIEW IF NOT EXISTS rustcta.smart_money_latest_wallet_positions_mv
TO rustcta.smart_money_latest_wallet_positions
AS
SELECT
    ts,
    recv_ts,
    wallet_id,
    address,
    trader_id,
    venue,
    symbol,
    direction,
    quantity,
    entry_price,
    mark_price,
    notional_usdt,
    equity_usdt,
    margin_used_usdt,
    leverage,
    unrealized_pnl_usdt,
    liquidation_price,
    raw_snapshot
FROM rustcta.smart_money_wallet_position_snapshots;

CREATE TABLE IF NOT EXISTS rustcta.smart_money_latest_wallet_equity
(
    ts DateTime64(3, 'UTC'),
    recv_ts DateTime64(3, 'UTC'),
    wallet_id UUID,
    address String,
    trader_id UUID,
    venue LowCardinality(String),
    equity_usdt Decimal(38, 8),
    available_margin_usdt Nullable(Decimal(38, 8)),
    total_position_notional_usdt Decimal(38, 8),
    unrealized_pnl_usdt Decimal(38, 8),
    raw_snapshot String
)
ENGINE = ReplacingMergeTree(ts)
ORDER BY (wallet_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS rustcta.smart_money_latest_wallet_equity_mv
TO rustcta.smart_money_latest_wallet_equity
AS
SELECT
    ts,
    recv_ts,
    wallet_id,
    address,
    trader_id,
    venue,
    equity_usdt,
    available_margin_usdt,
    total_position_notional_usdt,
    unrealized_pnl_usdt,
    raw_snapshot
FROM rustcta.smart_money_wallet_equity_snapshots;

CREATE TABLE IF NOT EXISTS rustcta.smart_money_latest_alpha
(
    ts DateTime64(3, 'UTC'),
    run_id Nullable(UUID),
    symbol LowCardinality(String),
    long_pressure Decimal(38, 18),
    short_pressure Decimal(38, 18),
    consensus_strength Decimal(18, 8),
    capital_weighted_consensus Decimal(38, 18),
    cluster_weighted_consensus Decimal(38, 18),
    net_alpha_score Decimal(18, 8),
    alpha_confidence_score Decimal(18, 8),
    contributing_wallets UInt32,
    dominant_wallet_share Decimal(18, 8),
    dominant_cluster_share Decimal(18, 8),
    inputs String
)
ENGINE = ReplacingMergeTree(ts)
ORDER BY (symbol);

CREATE MATERIALIZED VIEW IF NOT EXISTS rustcta.smart_money_latest_alpha_mv
TO rustcta.smart_money_latest_alpha
AS
SELECT
    ts,
    run_id,
    symbol,
    long_pressure,
    short_pressure,
    consensus_strength,
    capital_weighted_consensus,
    cluster_weighted_consensus,
    net_alpha_score,
    alpha_confidence_score,
    contributing_wallets,
    dominant_wallet_share,
    dominant_cluster_share,
    inputs
FROM rustcta.smart_money_alpha_snapshots;

CREATE TABLE IF NOT EXISTS rustcta.smart_money_latest_nav
(
    ts DateTime64(3, 'UTC'),
    run_id UUID,
    portfolio_id Nullable(UUID),
    nav_usdt Decimal(38, 8),
    cash_usdt Decimal(38, 8),
    gross_notional_usdt Decimal(38, 8),
    net_notional_usdt Decimal(38, 8),
    leverage Decimal(18, 8),
    realized_pnl_usdt Decimal(38, 8),
    unrealized_pnl_usdt Decimal(38, 8),
    fees_usdt Decimal(38, 8),
    funding_usdt Decimal(38, 8),
    drawdown Decimal(18, 8),
    positions String
)
ENGINE = ReplacingMergeTree(ts)
ORDER BY (run_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS rustcta.smart_money_latest_nav_mv
TO rustcta.smart_money_latest_nav
AS
SELECT
    ts,
    run_id,
    portfolio_id,
    nav_usdt,
    cash_usdt,
    gross_notional_usdt,
    net_notional_usdt,
    leverage,
    realized_pnl_usdt,
    unrealized_pnl_usdt,
    fees_usdt,
    funding_usdt,
    drawdown,
    positions
FROM rustcta.smart_money_nav_snapshots;
