-- Cross-exchange USDT perpetual arbitrage storage schema.
--
-- This file is intentionally ClickHouse-oriented and side-effect free: it can
-- be reviewed and parsed without requiring a running ClickHouse instance.
-- Runtime code can start with JSONL storage and later map the same event model
-- to these tables.

CREATE DATABASE IF NOT EXISTS rustcta;

CREATE TABLE IF NOT EXISTS rustcta.cross_arb_market_books_sampled
(
    event_ts DateTime64(3, 'UTC'),
    exchange_ts DateTime64(3, 'UTC'),
    recv_ts DateTime64(3, 'UTC'),
    exchange LowCardinality(String),
    canonical_symbol LowCardinality(String),
    exchange_symbol String,
    route LowCardinality(String),
    sequence Nullable(UInt64),
    best_bid Float64,
    best_bid_qty Float64,
    best_ask Float64,
    best_ask_qty Float64,
    mid_price Nullable(Float64),
    spread_bps Nullable(Float64),
    bids Array(Tuple(price Float64, qty Float64)),
    asks Array(Tuple(price Float64, qty Float64)),
    has_bids Bool,
    has_asks Bool,
    crossed Bool,
    has_invalid_levels Bool,
    stale Bool,
    sequence_gap Bool,
    quality_flags Array(LowCardinality(String)),
    source LowCardinality(String) DEFAULT 'websocket'
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (canonical_symbol, exchange, event_ts)
TTL event_ts + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS rustcta.cross_arb_funding_snapshots
(
    event_ts DateTime64(3, 'UTC'),
    exchange_ts Nullable(DateTime64(3, 'UTC')),
    exchange LowCardinality(String),
    canonical_symbol LowCardinality(String),
    exchange_symbol String,
    funding_rate Float64,
    predicted_funding_rate Nullable(Float64),
    funding_interval_hours UInt8,
    next_funding_ts Nullable(DateTime64(3, 'UTC')),
    mark_price Nullable(Float64),
    index_price Nullable(Float64),
    source LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (canonical_symbol, exchange, event_ts)
TTL event_ts + INTERVAL 180 DAY;

CREATE TABLE IF NOT EXISTS rustcta.cross_arb_opportunities
(
    event_ts DateTime64(3, 'UTC'),
    opportunity_id String,
    runtime_mode LowCardinality(String),
    canonical_symbol LowCardinality(String),
    long_exchange LowCardinality(String),
    short_exchange LowCardinality(String),
    buy_exchange LowCardinality(String),
    sell_exchange LowCardinality(String),
    buy_route LowCardinality(String),
    sell_route LowCardinality(String),
    target_qty Float64,
    notional_usdt Float64,
    buy_vwap Float64,
    sell_vwap Float64,
    raw_open_spread_bps Float64,
    maker_fee_bps Float64,
    taker_fee_bps Float64,
    funding_edge_bps Nullable(Float64),
    close_cost_buffer_bps Float64,
    latency_buffer_bps Float64,
    maker_taker_net_edge_bps Float64,
    depth_enough Bool,
    route_healthy Bool,
    accepted Bool,
    reject_reason LowCardinality(String),
    risk_flags Array(LowCardinality(String)),
    source_book_sequences Map(String, UInt64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (canonical_symbol, accepted, event_ts)
TTL event_ts + INTERVAL 180 DAY;

CREATE TABLE IF NOT EXISTS rustcta.cross_arb_signals
(
    event_ts DateTime64(3, 'UTC'),
    signal_id String,
    opportunity_id String,
    runtime_mode LowCardinality(String),
    signal_type LowCardinality(String),
    canonical_symbol LowCardinality(String),
    long_exchange LowCardinality(String),
    short_exchange LowCardinality(String),
    maker_exchange LowCardinality(String),
    taker_exchange LowCardinality(String),
    side LowCardinality(String),
    target_qty Float64,
    target_notional_usdt Float64,
    limit_price Nullable(Float64),
    expected_edge_bps Float64,
    expected_pnl_usdt Nullable(Float64),
    confidence Nullable(Float64),
    expires_at Nullable(DateTime64(3, 'UTC')),
    status LowCardinality(String),
    reject_reason LowCardinality(String),
    risk_flags Array(LowCardinality(String))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (canonical_symbol, signal_id, event_ts)
TTL event_ts + INTERVAL 365 DAY;

CREATE TABLE IF NOT EXISTS rustcta.cross_arb_execution_commands
(
    event_ts DateTime64(3, 'UTC'),
    command_id String,
    bundle_id String,
    signal_id String,
    runtime_mode LowCardinality(String),
    exchange LowCardinality(String),
    canonical_symbol LowCardinality(String),
    exchange_symbol String,
    intent LowCardinality(String),
    side LowCardinality(String),
    position_side LowCardinality(String),
    order_type LowCardinality(String),
    time_in_force LowCardinality(String),
    price Nullable(Float64),
    qty Float64,
    notional_usdt Nullable(Float64),
    post_only Bool,
    reduce_only Bool,
    client_order_id String,
    attempt UInt16,
    idempotency_key String,
    status LowCardinality(String),
    reject_reason LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (bundle_id, exchange, event_ts)
TTL event_ts + INTERVAL 365 DAY;

CREATE TABLE IF NOT EXISTS rustcta.cross_arb_order_events
(
    event_ts DateTime64(3, 'UTC'),
    exchange_ts Nullable(DateTime64(3, 'UTC')),
    exchange LowCardinality(String),
    canonical_symbol LowCardinality(String),
    exchange_symbol String,
    bundle_id String,
    command_id String,
    client_order_id String,
    exchange_order_id String,
    event_type LowCardinality(String),
    order_status LowCardinality(String),
    side LowCardinality(String),
    position_side LowCardinality(String),
    order_type LowCardinality(String),
    price Nullable(Float64),
    qty Float64,
    filled_qty Float64,
    avg_fill_price Nullable(Float64),
    fee_asset LowCardinality(String),
    fee_amount Float64,
    realized_pnl_usdt Nullable(Float64),
    raw_event String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (bundle_id, client_order_id, event_ts)
TTL event_ts + INTERVAL 365 DAY;

CREATE TABLE IF NOT EXISTS rustcta.cross_arb_bundle_events
(
    event_ts DateTime64(3, 'UTC'),
    bundle_id String,
    runtime_mode LowCardinality(String),
    canonical_symbol LowCardinality(String),
    previous_status LowCardinality(String),
    next_status LowCardinality(String),
    long_exchange LowCardinality(String),
    short_exchange LowCardinality(String),
    maker_exchange LowCardinality(String),
    taker_exchange LowCardinality(String),
    qty Float64,
    entry_edge_bps Nullable(Float64),
    realized_pnl_usdt Nullable(Float64),
    unrealized_pnl_usdt Nullable(Float64),
    fees_usdt Float64,
    funding_usdt Float64,
    reason LowCardinality(String),
    detail String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (bundle_id, event_ts)
TTL event_ts + INTERVAL 365 DAY;

CREATE TABLE IF NOT EXISTS rustcta.cross_arb_funding_settlements
(
    event_ts DateTime64(3, 'UTC'),
    bundle_id String,
    exchange LowCardinality(String),
    canonical_symbol LowCardinality(String),
    position_side LowCardinality(String),
    notional_usdt Float64,
    funding_rate Float64,
    funding_pnl_usdt Float64,
    mark_price Nullable(Float64),
    settled_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (bundle_id, exchange, canonical_symbol, settled_at)
TTL event_ts + INTERVAL 365 DAY;

CREATE TABLE IF NOT EXISTS rustcta.cross_arb_reconcile_reports
(
    event_ts DateTime64(3, 'UTC'),
    report_id String,
    runtime_mode LowCardinality(String),
    exchange LowCardinality(String),
    canonical_symbol LowCardinality(String),
    bundle_id String,
    severity LowCardinality(String),
    local_position_qty Float64,
    exchange_position_qty Float64,
    position_drift_qty Float64,
    local_open_order_count UInt32,
    exchange_open_order_count UInt32,
    order_drift_count Int32,
    local_balance_usdt Nullable(Float64),
    exchange_balance_usdt Nullable(Float64),
    balance_drift_usdt Nullable(Float64),
    orphan_exposure_usdt Nullable(Float64),
    recommended_action LowCardinality(String),
    auto_recovery_allowed Bool,
    alert_required Bool,
    detail String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (severity, exchange, canonical_symbol, event_ts)
TTL event_ts + INTERVAL 365 DAY;

CREATE TABLE IF NOT EXISTS rustcta.cross_arb_route_health_events
(
    event_ts DateTime64(3, 'UTC'),
    exchange LowCardinality(String),
    route LowCardinality(String),
    channel LowCardinality(String),
    previous_status LowCardinality(String),
    next_status LowCardinality(String),
    can_open Bool,
    can_close Bool,
    last_message_age_ms Nullable(UInt64),
    reconnect_count UInt64,
    heartbeat_timeout_count UInt64,
    sequence_gap_count UInt64,
    stale_book_count UInt64,
    snapshot_recovery_count UInt64,
    error_code LowCardinality(String),
    reason String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (exchange, route, event_ts)
TTL event_ts + INTERVAL 180 DAY;

CREATE TABLE IF NOT EXISTS rustcta.cross_arb_risk_events
(
    event_ts DateTime64(3, 'UTC'),
    risk_event_id String,
    runtime_mode LowCardinality(String),
    scope LowCardinality(String),
    exchange LowCardinality(String),
    canonical_symbol LowCardinality(String),
    bundle_id String,
    risk_type LowCardinality(String),
    severity LowCardinality(String),
    action LowCardinality(String),
    close_only Bool,
    new_entries_paused Bool,
    threshold Nullable(Float64),
    observed_value Nullable(Float64),
    detail String
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (severity, scope, event_ts)
TTL event_ts + INTERVAL 365 DAY;

CREATE TABLE IF NOT EXISTS rustcta.cross_arb_alerts
(
    event_ts DateTime64(3, 'UTC'),
    alert_id String,
    runtime_mode LowCardinality(String),
    severity LowCardinality(String),
    category LowCardinality(String),
    scope LowCardinality(String),
    exchange LowCardinality(String),
    canonical_symbol LowCardinality(String),
    bundle_id String,
    title String,
    message String,
    dedup_key String,
    status LowCardinality(String),
    acknowledged_by String,
    acknowledged_at Nullable(DateTime64(3, 'UTC')),
    resolved_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_ts)
ORDER BY (severity, status, event_ts)
TTL event_ts + INTERVAL 365 DAY;
