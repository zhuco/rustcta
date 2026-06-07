use chrono::{Duration, Utc};
use std::collections::HashMap;

use super::*;
use crate::exchanges::paper::{PaperExchangeClient, PaperExchangeConfig};
use crate::exchanges::unified::{
    AssetBalance, LiquidityRole, MarketType as UnifiedMarketType, OrderBookLevel,
    OrderBookSnapshot, OrderSide as UnifiedOrderSide,
};
use crate::market::{BookLevel, CanonicalSymbol, ExchangeId};

fn base_config() -> CrossExchangeArbitrageConfig {
    let mut config = CrossExchangeArbitrageConfig::default();
    config.detection.exchanges = vec![ExchangeId::Binance, ExchangeId::Okx];
    config.detection.symbols = vec!["BTCUSDT".to_string()];
    config.detection.estimated_slippage_bps = 2.0;
    config.detection.safety_buffer_bps = 3.0;
    config.detection.min_net_spread_bps = 5.0;
    config.detection.estimated_notional_usdt = 100.0;
    config
}

fn book(exchange: ExchangeId, ask: f64, bid: f64) -> NormalizedDepthSnapshot {
    let now = Utc::now();
    NormalizedDepthSnapshot {
        exchange: exchange.clone(),
        symbol: CanonicalSymbol::new("BTC", "USDT"),
        exchange_symbol: spot_exchange_symbol(&exchange, "BTCUSDT", &HashMap::new()),
        bids: vec![BookLevel::new(bid, 2.0)],
        asks: vec![BookLevel::new(ask, 2.0)],
        exchange_timestamp: Some(now),
        received_at: now,
        sequence: Some(1),
    }
}

#[test]
fn spread_calculation_should_use_buy_ask_and_sell_bid() {
    let engine = SpreadEngine::from_config(&base_config());
    let record = engine.evaluate(
        &book(ExchangeId::Binance, 100.0, 99.0),
        &book(ExchangeId::Okx, 102.0, 101.0),
        Utc::now(),
    );

    assert!((record.raw_spread_bps - 100.0).abs() < 1e-9);
}

#[test]
fn fee_deduction_should_reduce_net_spread() {
    let mut config = base_config();
    config.detection.estimated_slippage_bps = 0.0;
    config.detection.safety_buffer_bps = 0.0;
    let engine = SpreadEngine::from_config(&config);
    let record = engine.evaluate(
        &book(ExchangeId::Binance, 100.0, 99.0),
        &book(ExchangeId::Okx, 102.0, 101.0),
        Utc::now(),
    );

    assert!((record.estimated_net_spread_bps - 93.0).abs() < 1e-9);
}

#[test]
fn slippage_deduction_should_reduce_net_spread() {
    let config = base_config();
    let engine = SpreadEngine::from_config(&config);
    let record = engine.evaluate(
        &book(ExchangeId::Binance, 100.0, 99.0),
        &book(ExchangeId::Okx, 102.0, 101.0),
        Utc::now(),
    );

    assert!((record.estimated_net_spread_bps - 88.0).abs() < 1e-9);
}

#[test]
fn opportunity_threshold_should_accept_or_reject() {
    let mut config = base_config();
    config.detection.min_net_spread_bps = 90.0;
    let engine = SpreadEngine::from_config(&config);

    let rejected = engine.evaluate(
        &book(ExchangeId::Binance, 100.0, 99.0),
        &book(ExchangeId::Okx, 102.0, 101.0),
        Utc::now(),
    );
    assert_eq!(rejected.decision, OpportunityDecision::Rejected);

    config.detection.min_net_spread_bps = 80.0;
    let accepted = SpreadEngine::from_config(&config).evaluate(
        &book(ExchangeId::Binance, 100.0, 99.0),
        &book(ExchangeId::Okx, 102.0, 101.0),
        Utc::now(),
    );
    assert_eq!(accepted.decision, OpportunityDecision::Accepted);
}

#[test]
fn stale_order_book_should_reject_opportunity() {
    let config = base_config();
    let engine = SpreadEngine::from_config(&config);
    let mut stale = book(ExchangeId::Binance, 100.0, 99.0);
    stale.received_at = Utc::now() - Duration::seconds(10);

    let record = engine.evaluate(&stale, &book(ExchangeId::Okx, 102.0, 101.0), Utc::now());

    assert_eq!(record.decision, OpportunityDecision::Rejected);
    assert_eq!(record.reason, "stale_or_unusable_order_book");
}

#[test]
fn symbol_mapping_should_support_compact_and_okx_spot_symbols() {
    assert_eq!(
        normalize_symbol("BTCUSDT").unwrap(),
        CanonicalSymbol::new("BTC", "USDT")
    );
    assert_eq!(
        spot_exchange_symbol(&ExchangeId::Okx, "BTCUSDT", &HashMap::new()),
        "BTC-USDT"
    );

    let mut mappings = HashMap::new();
    mappings.insert(
        ExchangeId::Okx,
        HashMap::from([("BTCUSDT".to_string(), "BTC-USDT".to_string())]),
    );
    assert_eq!(
        spot_exchange_symbol(&ExchangeId::Okx, "BTCUSDT", &mappings),
        "BTC-USDT"
    );
}

#[test]
fn replay_spread_calculation_should_rebuild_opportunities_from_snapshots() {
    let config = base_config();
    let replay = CrossArbReplayEngine::new(config);
    let now = Utc::now();
    let snapshots = vec![
        book(ExchangeId::Binance, 100.0, 99.0),
        book(ExchangeId::Okx, 102.0, 101.0),
    ];

    let report = replay.replay_market_snapshots(&snapshots, now);

    assert_eq!(report.total_opportunities, 2);
    assert_eq!(report.accepted_opportunities, 1);
    assert_eq!(report.rejected_opportunities, 1);
}

#[test]
fn replay_pnl_calculation_should_use_recorded_fills() {
    let now = Utc::now();
    let symbol = CanonicalSymbol::new("BTC", "USDT");
    let fills = vec![
        ArbitrageFillRecord {
            timestamp: now,
            exchange: ExchangeId::Binance,
            symbol: symbol.clone(),
            side: "Buy".to_string(),
            price: 100.0,
            quantity: 1.0,
            fee: 0.05,
            fee_asset: Some("USDT".to_string()),
            liquidity_role: "Maker".to_string(),
            order_id: Some("buy-order".to_string()),
            client_order_id: Some("buy-client".to_string()),
            latency_ms: Some(10),
        },
        ArbitrageFillRecord {
            timestamp: now,
            exchange: ExchangeId::Okx,
            symbol,
            side: "Sell".to_string(),
            price: 102.0,
            quantity: 1.0,
            fee: 0.05,
            fee_asset: Some("USDT".to_string()),
            liquidity_role: "Taker".to_string(),
            order_id: Some("sell-order".to_string()),
            client_order_id: Some("sell-client".to_string()),
            latency_ms: Some(12),
        },
    ];
    let orders = vec![
        ArbitrageOrderRecord {
            timestamp: now,
            exchange: ExchangeId::Binance,
            symbol: CanonicalSymbol::new("BTC", "USDT"),
            side: "Buy".to_string(),
            order_type: "Limit".to_string(),
            status: "Filled".to_string(),
            order_id: Some("buy-order".to_string()),
            client_order_id: Some("buy-client".to_string()),
            price: Some(100.0),
            quantity: 1.0,
            filled_quantity: 1.0,
            average_fill_price: Some(100.0),
            latency_ms: Some(10),
        },
        ArbitrageOrderRecord {
            timestamp: now,
            exchange: ExchangeId::Okx,
            symbol: CanonicalSymbol::new("BTC", "USDT"),
            side: "Sell".to_string(),
            order_type: "Market".to_string(),
            status: "Filled".to_string(),
            order_id: Some("sell-order".to_string()),
            client_order_id: Some("sell-client".to_string()),
            price: None,
            quantity: 1.0,
            filled_quantity: 1.0,
            average_fill_price: Some(102.0),
            latency_ms: Some(12),
        },
    ];

    let report = CrossArbReplayEngine::summary_from_parts(&[], &orders, &fills, &[], &[], now);

    assert!((report.realized_pnl_usdt - 1.9).abs() < 1e-9);
    assert_eq!(report.fill_ratio, 1.0);
    assert_eq!(report.maker_fill_ratio, 0.5);
    assert_eq!(report.taker_hedge_ratio, 0.5);
}

#[test]
fn detection_config_should_parse_requested_cli_template() {
    let raw = include_str!("../../../config/cross_exchange_arbitrage.yml");
    let config: CrossExchangeArbitrageConfig = serde_yaml::from_str(raw).unwrap();

    config.validate().unwrap();
    assert_eq!(config.strategy.name, "cross_exchange_arbitrage");
    assert_eq!(
        config.detection.exchanges,
        vec![ExchangeId::Binance, ExchangeId::Okx]
    );
    assert_eq!(
        config.detection.symbols,
        vec!["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    );
    assert!(config.execution.dry_run);
    assert!(!config.mode.allows_live_orders());
}

fn paper_settings(mode: PaperExecutionMode) -> PaperExecutionSettings {
    PaperExecutionSettings {
        mode,
        maker_timeout_ms: 80,
        stale_book_ms: 1_000,
        min_executable_depth_usdt: 20.0,
        persist_jsonl_path: None,
        risk_config: None,
    }
}

fn paper_client(
    exchange_name: &str,
    balances: Vec<(&str, f64)>,
    bid_price: f64,
    bid_qty: f64,
    ask_price: f64,
    ask_qty: f64,
) -> PaperExchangeClient {
    let client = PaperExchangeClient::with_balances(
        PaperExchangeConfig {
            exchange_name: exchange_name.to_string(),
            maker_fee_rate: 0.0002,
            taker_fee_rate: 0.0005,
            stale_book_after_ms: 1_000,
            ..PaperExchangeConfig::default()
        },
        balances
            .into_iter()
            .map(|(asset, total)| AssetBalance::new(asset, total, total, 0.0))
            .collect(),
    );
    client
        .update_orderbook(test_orderbook(
            exchange_name,
            bid_price,
            bid_qty,
            ask_price,
            ask_qty,
            Utc::now(),
        ))
        .unwrap();
    client
}

fn test_orderbook(
    exchange: &str,
    bid_price: f64,
    bid_qty: f64,
    ask_price: f64,
    ask_qty: f64,
    received_at: chrono::DateTime<Utc>,
) -> OrderBookSnapshot {
    OrderBookSnapshot {
        exchange: exchange.to_string(),
        market_type: UnifiedMarketType::Spot,
        symbol: "BTCUSDT".to_string(),
        best_bid: Some(bid_price),
        best_ask: Some(ask_price),
        bids: vec![OrderBookLevel {
            price: bid_price,
            quantity: bid_qty,
        }],
        asks: vec![OrderBookLevel {
            price: ask_price,
            quantity: ask_qty,
        }],
        exchange_timestamp: Some(received_at),
        received_at,
        latency_ms: Some(0),
        sequence: Some(1),
        is_stale: false,
    }
}

fn paper_opportunity() -> OpportunityRecord {
    OpportunityRecord {
        timestamp: Utc::now(),
        symbol: CanonicalSymbol::new("BTC", "USDT"),
        buy_exchange: ExchangeId::Binance,
        sell_exchange: ExchangeId::Okx,
        buy_price: 100.0,
        sell_price: 102.0,
        raw_spread_bps: 200.0,
        estimated_net_spread_bps: 190.0,
        estimated_notional: 100.0,
        decision: OpportunityDecision::Accepted,
        reason: "accepted".to_string(),
    }
}

fn paper_engine(
    mode: PaperExecutionMode,
    buy_client: PaperExchangeClient,
    sell_client: PaperExchangeClient,
) -> CrossExchangePaperExecutionEngine {
    CrossExchangePaperExecutionEngine::new(
        HashMap::from([
            (ExchangeId::Binance, buy_client),
            (ExchangeId::Okx, sell_client),
        ]),
        paper_settings(mode),
    )
}

#[tokio::test]
async fn paper_taker_taker_should_execute_successful_arbitrage() {
    let buy_client = paper_client("binance", vec![("USDT", 1_000.0)], 99.0, 2.0, 100.0, 2.0);
    let sell_client = paper_client("okx", vec![("BTC", 2.0)], 102.0, 2.0, 103.0, 2.0);
    let mut engine = paper_engine(PaperExecutionMode::TakerTaker, buy_client, sell_client);

    let report = engine
        .execute_opportunity(&paper_opportunity())
        .await
        .unwrap();

    assert_eq!(report.status, PaperExecutionStatus::Filled);
    assert_eq!(report.orders.len(), 2);
    assert_eq!(report.fills.len(), 2);
    assert!(report.realized_pnl_usdt > 0.0);
    assert!(report.fee_paid_usdt > 0.0);
    assert!(
        engine
            .state()
            .net_exposure
            .get(&CanonicalSymbol::new("BTC", "USDT"))
            .unwrap()
            .abs()
            < 1e-9
    );
}

#[tokio::test]
async fn paper_maker_first_should_hedge_after_maker_fill() {
    let buy_client = paper_client("binance", vec![("USDT", 1_000.0)], 99.0, 2.0, 100.0, 2.0);
    let buy_update = buy_client.clone();
    let sell_client = paper_client("okx", vec![("BTC", 2.0)], 102.0, 2.0, 103.0, 2.0);
    let mut engine = paper_engine(
        PaperExecutionMode::MakerFirstThenTakerHedge,
        buy_client,
        sell_client,
    );

    let task = tokio::spawn(async move { engine.execute_opportunity(&paper_opportunity()).await });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    buy_update
        .update_orderbook(test_orderbook("binance", 98.0, 2.0, 99.0, 2.0, Utc::now()))
        .unwrap();
    let report = task.await.unwrap().unwrap();

    assert_eq!(report.status, PaperExecutionStatus::Filled);
    assert!(report
        .fills
        .iter()
        .any(|fill| fill.liquidity == LiquidityRole::Maker));
    assert!(report
        .fills
        .iter()
        .any(|fill| fill.liquidity == LiquidityRole::Taker));
}

#[tokio::test]
async fn paper_maker_first_should_cancel_after_timeout() {
    let buy_client = paper_client("binance", vec![("USDT", 1_000.0)], 99.0, 2.0, 100.0, 2.0);
    let sell_client = paper_client("okx", vec![("BTC", 2.0)], 102.0, 2.0, 103.0, 2.0);
    let mut engine = paper_engine(
        PaperExecutionMode::MakerFirstThenTakerHedge,
        buy_client,
        sell_client,
    );

    let report = engine
        .execute_opportunity(&paper_opportunity())
        .await
        .unwrap();

    assert_eq!(report.status, PaperExecutionStatus::Cancelled);
    assert!(report.fills.is_empty());
    assert_eq!(report.reason.as_deref(), Some("maker_timeout_cancelled"));
}

#[tokio::test]
async fn paper_maker_first_should_hedge_partial_fill_after_timeout() {
    let buy_client = paper_client("binance", vec![("USDT", 1_000.0)], 99.0, 2.0, 100.0, 2.0);
    let buy_update = buy_client.clone();
    let sell_client = paper_client("okx", vec![("BTC", 2.0)], 102.0, 2.0, 103.0, 2.0);
    let mut engine = paper_engine(
        PaperExecutionMode::MakerFirstThenTakerHedge,
        buy_client,
        sell_client,
    );

    let task = tokio::spawn(async move { engine.execute_opportunity(&paper_opportunity()).await });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    buy_update
        .update_orderbook(test_orderbook("binance", 98.0, 2.0, 99.0, 0.4, Utc::now()))
        .unwrap();
    let report = task.await.unwrap().unwrap();

    assert_eq!(report.status, PaperExecutionStatus::PartiallyFilled);
    assert!(report.fills.iter().any(|fill| {
        fill.liquidity == LiquidityRole::Maker && (fill.quantity - 0.4).abs() < 1e-9
    }));
    assert!(report.fills.iter().any(|fill| {
        fill.liquidity == LiquidityRole::Taker && (fill.quantity - 0.4).abs() < 1e-9
    }));
}

#[tokio::test]
async fn paper_one_leg_failure_should_trigger_emergency_hedge() {
    let buy_client = paper_client(
        "binance",
        vec![("BTC", 1.0), ("USDT", 1_000.0)],
        99.0,
        2.0,
        100.0,
        2.0,
    );
    let sell_client = paper_client("okx", vec![("BTC", 2.0)], 102.0, 2.0, 103.0, 2.0);
    let mut engine = paper_engine(PaperExecutionMode::TakerTaker, buy_client, sell_client);

    let report = engine
        .emergency_hedge_residual(PaperResidualExposure {
            exchange: ExchangeId::Binance,
            symbol: CanonicalSymbol::new("BTC", "USDT"),
            side: UnifiedOrderSide::Buy,
            quantity: 0.5,
            reason: "test_one_leg_failure".to_string(),
            created_at: Utc::now(),
        })
        .await
        .unwrap();

    assert_eq!(report.status, PaperExecutionStatus::EmergencyHedged);
    assert_eq!(report.risk_events.len(), 1);
    assert_eq!(report.residual_exposures.len(), 1);
    assert_eq!(report.fills[0].side, UnifiedOrderSide::Sell);
}

#[tokio::test]
async fn paper_execution_should_reject_insufficient_balance() {
    let buy_client = paper_client("binance", vec![("USDT", 10.0)], 99.0, 2.0, 100.0, 2.0);
    let sell_client = paper_client("okx", vec![("BTC", 2.0)], 102.0, 2.0, 103.0, 2.0);
    let mut engine = paper_engine(PaperExecutionMode::TakerTaker, buy_client, sell_client);

    let err = engine
        .execute_opportunity(&paper_opportunity())
        .await
        .unwrap_err();

    assert!(err.to_string().contains("insufficient paper balance"));
}

#[tokio::test]
async fn paper_execution_should_reject_insufficient_depth() {
    let buy_client = paper_client("binance", vec![("USDT", 1_000.0)], 99.0, 0.1, 100.0, 0.1);
    let sell_client = paper_client("okx", vec![("BTC", 2.0)], 102.0, 2.0, 103.0, 2.0);
    let mut engine = paper_engine(PaperExecutionMode::TakerTaker, buy_client, sell_client);

    let err = engine
        .execute_opportunity(&paper_opportunity())
        .await
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("insufficient executable paper depth"));
}

#[tokio::test]
async fn paper_execution_should_reject_stale_data() {
    let stale_at = Utc::now() - Duration::seconds(10);
    let buy_client = PaperExchangeClient::with_balances(
        PaperExchangeConfig {
            exchange_name: "binance".to_string(),
            stale_book_after_ms: 30_000,
            ..PaperExchangeConfig::default()
        },
        vec![AssetBalance::new("USDT", 1_000.0, 1_000.0, 0.0)],
    );
    buy_client
        .update_orderbook(test_orderbook("binance", 99.0, 2.0, 100.0, 2.0, stale_at))
        .unwrap();
    let sell_client = paper_client("okx", vec![("BTC", 2.0)], 102.0, 2.0, 103.0, 2.0);
    let mut engine = paper_engine(PaperExecutionMode::TakerTaker, buy_client, sell_client);

    let err = engine
        .execute_opportunity(&paper_opportunity())
        .await
        .unwrap_err();

    assert!(err.to_string().contains("stale paper order book"));
}
