use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

use async_trait::async_trait;
use chrono::{Duration, Utc};
use tempfile::tempdir;

use super::*;
use crate::exchanges::unified::{
    AssetBalance, BalanceSnapshot, CancelOrderRequest, CancelOrderResponse, ExchangeClientError,
    ExchangeClientResult, FeeRate, FeeRateSource, MarketType, OrderBookLevel, OrderBookSnapshot,
    OrderRequest, OrderResponse, OrderStatus, OrderType, SymbolRule, SymbolStatus, TimeInForce,
};
use crate::exchanges::{coinex, mexc};
use crate::execution::{FeeConfig, FeeModel, FeePairConfig, FeeSource, PlatformTokenDiscount};
use crate::risk::{
    DisabledConfig, DisabledExchangeSymbol, DisabledRegistry, DisabledRegistryConfig,
    DisabledSymbol, UnmanagedPosition,
};

fn config() -> SpotSpotTakerArbitrageConfig {
    let mut initial_balances = HashMap::new();
    initial_balances.insert(
        "mexc".to_string(),
        HashMap::from([("USDT".to_string(), 500.0), ("CUDIS".to_string(), 10_000.0)]),
    );
    initial_balances.insert(
        "coinex".to_string(),
        HashMap::from([("USDT".to_string(), 500.0), ("CUDIS".to_string(), 10_000.0)]),
    );
    SpotSpotTakerArbitrageConfig {
        enabled: true,
        trading_mode: "paper".to_string(),
        exchanges: vec!["mexc".to_string(), "coinex".to_string()],
        symbols: vec!["CUDISUSDT".to_string()],
        quote_asset: "USDT".to_string(),
        max_notional_per_trade: 100.0,
        min_notional_per_trade: 5.0,
        max_notional_per_symbol: 500.0,
        max_total_notional: 1_000.0,
        max_enabled_arbitrage_symbols: 5,
        initial_entry_notional_usdt: 5.0,
        spread_duration_threshold_seconds: 30,
        entry_order_timeout_seconds: 30,
        entry_maker_retries: 5,
        active_taker_notional_usdt: 1.2,
        inactivity_exit_seconds: 3_600,
        exit_order_timeout_seconds: 60,
        exit_maker_retries: 10,
        min_raw_spread_bps: 30.0,
        min_net_spread_bps: 10.0,
        max_raw_spread_bps: 1_000.0,
        taker_fee_bps_override: Some(10.0),
        fee_config_path: "config/fees.yml".to_string(),
        disabled_registry_path: "config/disabled_symbols.yml".to_string(),
        slippage_bps: 2.0,
        safety_buffer_bps: 3.0,
        stale_book_ms: 1_000,
        max_book_latency_ms: 1_000,
        min_depth_notional: 5.0,
        max_active_opportunities_per_symbol: 1,
        cooldown_ms_after_trade: 5_000,
        enable_database_recording: false,
        enable_csv_recording: false,
        report_interval_seconds: 30,
        dry_run: true,
        live_trading_enabled: false,
        scan_interval_ms: 1_000,
        orderbook_depth: 5,
        request_timeout_ms: 10_000,
        max_daily_loss: 100.0,
        max_trade_loss: 10.0,
        max_consecutive_rejections: 20,
        jsonl_path: "logs/test_spot_spot_taker_arbitrage.jsonl".to_string(),
        csv_path: "logs/test_spot_spot_taker_arbitrage.csv".to_string(),
        market_data_mode: MarketDataMode::RestPolling,
        websocket: WebsocketMarketDataConfig::default(),
        rest_polling: RestPollingMarketDataConfig::default(),
        replay: ReplayConfig::default(),
        monitoring: crate::web::MonitoringConfig::default(),
        spot_symbol_control: crate::control::spot_control::SpotSymbolControlConfig::default(),
        live_preflight: crate::live_preflight::LivePreflightConfig::default(),
        live_dry_run: crate::execution::LiveDryRunConfig::default(),
        order_reconciliation: crate::execution::OrderReconciliationConfig::default(),
        kill_switch: crate::risk::KillSwitchConfig::default(),
        small_live_gate: crate::live_preflight::SmallLiveGateConfig::default(),
        arbitrage_scanner: crate::strategies::arbitrage_core::ArbitrageScannerConfig::default(),
        inventory_rebalance: InventoryRebalanceConfig::default(),
        venue_selection: VenueSelectionConfig::default(),
        initial_balances,
        mexc: VenueRuntimeConfig::default(),
        coinex: VenueRuntimeConfig::default(),
        gateio: VenueRuntimeConfig::default(),
        bitget: VenueRuntimeConfig::default(),
        kucoin: crate::exchanges::kucoin::KuCoinSpotConfig::default(),
    }
}

fn live_small_gate_config() -> SpotSpotTakerArbitrageConfig {
    let mut cfg = config();
    cfg.trading_mode = "live".to_string();
    cfg.dry_run = false;
    cfg.live_trading_enabled = true;
    cfg.kill_switch.allow_live_orders = true;
    cfg.small_live_gate.enabled = true;
    cfg.small_live_gate.explicit_live_confirmation = true;
    cfg.small_live_gate.max_notional_per_order = 5.0;
    cfg.small_live_gate.max_total_notional = 1_000.0;
    cfg.small_live_gate.enabled_symbols = vec!["CUDISUSDT".to_string()];
    cfg.small_live_gate.enabled_exchanges = vec!["mexc".to_string()];
    cfg.live_preflight.enabled = true;
    cfg.live_preflight.target_mode = "live".to_string();
    cfg.live_preflight.exchanges = cfg.small_live_gate.enabled_exchanges.clone();
    cfg.live_preflight.symbols = cfg.small_live_gate.enabled_symbols.clone();
    cfg.live_preflight.max_live_notional_per_trade =
        Some(cfg.small_live_gate.max_notional_per_order);
    cfg.live_preflight.max_total_live_notional = Some(cfg.small_live_gate.max_total_notional);
    cfg
}

struct MockSpotOrderClient {
    orders: Mutex<VecDeque<OrderResponse>>,
    get_order_calls: AtomicUsize,
    cancel_calls: AtomicUsize,
}

impl MockSpotOrderClient {
    fn new(orders: impl IntoIterator<Item = OrderResponse>) -> Self {
        Self {
            orders: Mutex::new(orders.into_iter().collect()),
            get_order_calls: AtomicUsize::new(0),
            cancel_calls: AtomicUsize::new(0),
        }
    }

    fn get_order_calls(&self) -> usize {
        self.get_order_calls.load(Ordering::SeqCst)
    }

    fn cancel_calls(&self) -> usize {
        self.cancel_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ExchangeClient for MockSpotOrderClient {
    fn market_type(&self) -> MarketType {
        MarketType::Spot
    }

    fn exchange_name(&self) -> &str {
        "mexc"
    }

    async fn get_balances(&self) -> ExchangeClientResult<BalanceSnapshot> {
        Ok(BalanceSnapshot {
            exchange: "mexc".to_string(),
            market_type: MarketType::Spot,
            balances: Vec::new(),
            timestamp: Utc::now(),
        })
    }

    async fn get_orderbook(
        &self,
        _symbol: &str,
        _depth: u16,
    ) -> ExchangeClientResult<OrderBookSnapshot> {
        Err(ExchangeClientError::Unsupported(
            "mock orderbook is not used by these tests".to_string(),
        ))
    }

    async fn place_order(&self, _request: OrderRequest) -> ExchangeClientResult<OrderResponse> {
        Err(ExchangeClientError::Unsupported(
            "mock place_order is not used by these tests".to_string(),
        ))
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeClientResult<CancelOrderResponse> {
        self.cancel_calls.fetch_add(1, Ordering::SeqCst);
        Ok(CancelOrderResponse {
            exchange: "mexc".to_string(),
            market_type: request.market_type,
            symbol: request.symbol,
            order_id: request.order_id,
            client_order_id: request.client_order_id,
            status: OrderStatus::Cancelled,
            cancelled_at: Utc::now(),
        })
    }

    async fn get_order(
        &self,
        _symbol: &str,
        _order_id: &str,
    ) -> ExchangeClientResult<OrderResponse> {
        self.get_order_calls.fetch_add(1, Ordering::SeqCst);
        self.orders
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| ExchangeClientError::Unsupported("mock order sequence is empty".into()))
    }

    async fn get_open_orders(
        &self,
        _symbol: Option<&str>,
    ) -> ExchangeClientResult<Vec<OrderResponse>> {
        Ok(Vec::new())
    }

    async fn get_fee_rate(&self, _symbol: &str) -> ExchangeClientResult<FeeRate> {
        Ok(FeeRate::new(0.0, 0.0, FeeRateSource::DefaultFallback))
    }
}

fn build_with_models(
    cfg: &SpotSpotTakerArbitrageConfig,
    fee_model: &FeeModel,
    disabled_registry: &DisabledRegistry,
    buy_exchange: SpotVenue,
    sell_exchange: SpotVenue,
    buy_book: &OrderBookSnapshot,
    sell_book: &OrderBookSnapshot,
) -> OpportunityRecord {
    build_opportunity_with_source(
        cfg,
        &rules(),
        &PaperInventory::from_config(cfg).unwrap(),
        &RiskState::new(cfg),
        buy_exchange,
        sell_exchange,
        buy_book,
        sell_book,
        fee_model,
        disabled_registry,
        BookSource::Rest,
        BookSource::Rest,
    )
}

fn rule(exchange: &str) -> SymbolRule {
    SymbolRule {
        exchange: exchange.to_string(),
        market_type: MarketType::Spot,
        internal_symbol: "CUDISUSDT".to_string(),
        exchange_symbol: "CUDISUSDT".to_string(),
        base_asset: "CUDIS".to_string(),
        quote_asset: "USDT".to_string(),
        price_precision: 4,
        quantity_precision: 0,
        tick_size: 0.0001,
        step_size: 1.0,
        min_quantity: 1.0,
        min_notional: 5.0,
        max_quantity: None,
        supported_order_types: vec![OrderType::Market, OrderType::IOC],
        supported_time_in_force: vec![TimeInForce::IOC],
        status: SymbolStatus::Trading,
        raw_metadata: None,
    }
}

fn rules() -> CommonSymbolRules {
    CommonSymbolRules {
        mexc: rule("mexc"),
        coinex: rule("coinex"),
        gateio: None,
        bitget: None,
        kucoin: None,
    }
}

fn test_symbol_rules(symbol: &str) -> CommonSymbolRules {
    let base_asset = symbol.strip_suffix("USDT").unwrap_or(symbol).to_string();
    let rule = SymbolRule {
        exchange: "mexc".to_string(),
        market_type: MarketType::Spot,
        internal_symbol: symbol.to_string(),
        exchange_symbol: symbol.to_string(),
        base_asset,
        quote_asset: "USDT".to_string(),
        price_precision: 4,
        quantity_precision: 0,
        tick_size: 0.0001,
        step_size: 1.0,
        min_quantity: 1.0,
        min_notional: 1.0,
        max_quantity: None,
        supported_order_types: vec![OrderType::Market, OrderType::IOC],
        supported_time_in_force: vec![TimeInForce::IOC],
        status: SymbolStatus::Trading,
        raw_metadata: None,
    };
    CommonSymbolRules {
        mexc: rule.clone(),
        coinex: rule,
        gateio: None,
        bitget: None,
        kucoin: None,
    }
}

fn lifecycle_symbol_rules(symbol: &str) -> CommonSymbolRules {
    let mut mexc = rule("mexc");
    mexc.internal_symbol = symbol.to_string();
    mexc.exchange_symbol = symbol.to_string();
    mexc.step_size = 0.0001;
    mexc.min_notional = 1.0;
    mexc.supported_order_types = vec![OrderType::Market, OrderType::IOC, OrderType::PostOnly];
    mexc.supported_time_in_force = vec![TimeInForce::IOC, TimeInForce::GTC];
    let mut coinex = mexc.clone();
    coinex.exchange = "coinex".to_string();
    let mut gateio = mexc.clone();
    gateio.exchange = "gateio".to_string();
    let mut bitget = mexc.clone();
    bitget.exchange = "bitget".to_string();
    CommonSymbolRules {
        mexc,
        coinex,
        gateio: Some(gateio),
        bitget: Some(bitget),
        kucoin: None,
    }
}

fn test_symbol_book(
    exchange: &str,
    symbol: &str,
    bid: f64,
    ask: f64,
    qty: f64,
) -> OrderBookSnapshot {
    let mut book = book(exchange, bid, ask, qty);
    book.symbol = symbol.to_string();
    book
}

fn two_exchange_entry_balances(usdt_available: f64) -> HashMap<SpotVenue, Vec<AssetBalance>> {
    HashMap::from([
        (
            SpotVenue::Mexc,
            vec![AssetBalance::new(
                "USDT",
                usdt_available,
                usdt_available,
                0.0,
            )],
        ),
        (
            SpotVenue::CoinEx,
            vec![AssetBalance::new(
                "USDT",
                usdt_available,
                usdt_available,
                0.0,
            )],
        ),
    ])
}

#[test]
fn max_enabled_arbitrage_symbols_should_block_new_symbols_after_limit() {
    let mut enabled = std::collections::BTreeSet::new();
    for symbol in ["AUSDT", "BUSDT", "CUSDT", "DUSDT", "EUSDT"] {
        assert!(arbitrage_symbol_allowed(&mut enabled, symbol, 5));
    }
    assert!(arbitrage_symbol_allowed(&mut enabled, "AUSDT", 5));
    assert!(!arbitrage_symbol_allowed(&mut enabled, "FUSDT", 5));
    assert_eq!(enabled.len(), 5);
}

#[test]
fn released_arbitrage_symbol_should_free_capacity_for_new_symbol() {
    let mut enabled = std::collections::BTreeSet::new();
    for symbol in ["AUSDT", "BUSDT", "CUSDT", "DUSDT", "EUSDT"] {
        assert!(arbitrage_symbol_allowed(&mut enabled, symbol, 5));
    }
    let blocked = BTreeSet::from(["CUSDT".to_string()]);
    release_arbitrage_symbols(&mut enabled, &blocked);

    assert!(!enabled.contains("CUSDT"));
    assert!(arbitrage_symbol_allowed(&mut enabled, "FUSDT", 5));
    assert_eq!(enabled.len(), 5);
}

#[test]
fn zero_max_enabled_arbitrage_symbols_should_block_all_execution() {
    let mut enabled = std::collections::BTreeSet::new();
    assert!(!arbitrage_symbol_allowed(&mut enabled, "AUSDT", 0));
    assert!(enabled.is_empty());
}

#[test]
fn arbitrage_scan_gate_should_count_opening_and_arbitraging_symbols() {
    let now = Utc::now();
    let mut states = BTreeMap::new();

    assert_eq!(active_or_opening_arbitrage_symbol_count(&states), 0);
    assert!(should_scan_arbitrage_opportunities(&states, 1));
    assert!(!should_scan_arbitrage_opportunities(&states, 0));

    states.insert(
        "AAAUSDT".to_string(),
        ArbitragePairRuntime::opening("AAAUSDT", now),
    );
    assert_eq!(active_or_opening_arbitrage_symbol_count(&states), 1);
    assert!(!should_scan_arbitrage_opportunities(&states, 1));
    assert!(should_scan_arbitrage_opportunities(&states, 2));

    let mut arbitraging = ArbitragePairRuntime::opening("BBBUSDT", now);
    arbitraging.mark_arbitraging(now);
    states.insert("BBBUSDT".to_string(), arbitraging);
    assert_eq!(active_or_opening_arbitrage_symbol_count(&states), 2);
    assert!(!should_scan_arbitrage_opportunities(&states, 2));

    let mut exiting = ArbitragePairRuntime::opening("CCCUSDT", now);
    exiting.status = ArbitragePairStatus::Exiting;
    states.insert("CCCUSDT".to_string(), exiting);
    let mut closed = ArbitragePairRuntime::opening("DDDUSDT", now);
    closed.status = ArbitragePairStatus::Closed;
    states.insert("DDDUSDT".to_string(), closed);

    assert_eq!(active_or_opening_arbitrage_symbol_count(&states), 2);
    assert!(should_scan_arbitrage_opportunities(&states, 3));
}

#[test]
fn blocked_symbols_should_release_initial_entry_capacity() {
    let blocked = BTreeSet::from(["WLDUSDT".to_string(), "PEPEUSDT".to_string()]);
    let mut completed = BTreeSet::from([
        "WLDUSDT".to_string(),
        "PEPEUSDT".to_string(),
        "HYPEUSDT".to_string(),
    ]);
    let mut planned = BTreeMap::from([
        (
            "WLDUSDT".to_string(),
            BTreeSet::from(["gateio".to_string(), "bitget".to_string()]),
        ),
        (
            "HYPEUSDT".to_string(),
            BTreeSet::from(["gateio".to_string()]),
        ),
    ]);
    let mut failures = BTreeMap::from([
        ("PEPEUSDT:bitget".to_string(), "stopped".to_string()),
        ("HYPEUSDT:gateio".to_string(), "retry later".to_string()),
    ]);
    let mut enabled = completed.clone();
    let mut states = BTreeMap::from([
        (
            "WLDUSDT".to_string(),
            ArbitragePairRuntime::opening("WLDUSDT", Utc::now()),
        ),
        (
            "HYPEUSDT".to_string(),
            ArbitragePairRuntime::opening("HYPEUSDT", Utc::now()),
        ),
    ]);

    release_blocked_initial_entry_symbols(
        &blocked,
        &mut completed,
        &mut planned,
        &mut failures,
        &mut enabled,
        &mut states,
    );

    assert_eq!(completed, BTreeSet::from(["HYPEUSDT".to_string()]));
    assert!(!planned.contains_key("WLDUSDT"));
    assert!(planned.contains_key("HYPEUSDT"));
    assert!(!failures.contains_key("PEPEUSDT:bitget"));
    assert!(failures.contains_key("HYPEUSDT:gateio"));
    assert!(!enabled.contains("WLDUSDT"));
    assert!(!states.contains_key("WLDUSDT"));
    assert!(states.contains_key("HYPEUSDT"));
}

#[tokio::test]
async fn initial_entry_live_dry_run_should_build_capped_five_usdt_plans() {
    let mut cfg = config();
    cfg.trading_mode = "live_dry_run".to_string();
    cfg.max_enabled_arbitrage_symbols = 2;
    cfg.max_notional_per_trade = 1.2;
    cfg.initial_entry_notional_usdt = 5.0;
    cfg.symbols = vec![
        "AAAUSDT".to_string(),
        "BBBUSDT".to_string(),
        "CCCUSDT".to_string(),
    ];
    cfg.initial_balances.insert(
        "mexc".to_string(),
        HashMap::from([("USDT".to_string(), 50.0)]),
    );
    let book_cache = BookCache::default();
    let mut symbol_rules = HashMap::new();
    for symbol in &cfg.symbols {
        book_cache
            .update_book(
                test_symbol_book("mexc", symbol, 0.99, 1.0, 100.0),
                BookSource::Rest,
            )
            .await;
        book_cache
            .update_book(
                test_symbol_book("coinex", symbol, 1.00, 1.01, 100.0),
                BookSource::Rest,
            )
            .await;
        symbol_rules.insert(symbol.clone(), test_symbol_rules(symbol));
    }

    let plans = build_initial_entry_live_dry_run_plans(
        &cfg,
        &symbol_rules,
        &two_exchange_entry_balances(50.0),
        &DisabledRegistry::new(),
        &FeeModel::default(),
        &book_cache,
        &BTreeSet::new(),
        &BTreeSet::new(),
    )
    .await;

    assert_eq!(plans.len(), 4);
    assert!(plans.iter().all(|plan| plan.intent == "initial_entry"));
    assert!(plans.iter().all(|plan| plan.side == OrderSide::Buy));
    assert!(plans.iter().all(|plan| plan.notional <= 5.0 + 1e-12));
    assert!(plans.iter().all(|plan| plan.notional >= 1.0));
    assert_eq!(
        plans
            .iter()
            .map(|plan| plan.symbol.as_str())
            .collect::<Vec<_>>(),
        vec!["AAAUSDT", "AAAUSDT", "BBBUSDT", "BBBUSDT"]
    );
}

#[tokio::test]
async fn initial_entry_live_dry_run_should_split_target_by_entry_allocation() {
    let mut cfg = config();
    cfg.trading_mode = "live".to_string();
    cfg.exchanges = vec!["mexc".to_string(), "coinex".to_string()];
    cfg.max_enabled_arbitrage_symbols = 1;
    cfg.initial_entry_notional_usdt = 20.0;
    cfg.live_dry_run.max_notional_per_order = 14.0;
    cfg.live_dry_run.max_total_notional = 50.0;
    cfg.symbols = vec!["AAAUSDT".to_string()];
    let book_cache = BookCache::default();
    book_cache
        .update_book(
            test_symbol_book("mexc", "AAAUSDT", 1.09, 1.10, 100.0),
            BookSource::Rest,
        )
        .await;
    book_cache
        .update_book(
            test_symbol_book("coinex", "AAAUSDT", 0.99, 1.00, 100.0),
            BookSource::Rest,
        )
        .await;
    let symbol_rules = HashMap::from([("AAAUSDT".to_string(), test_symbol_rules("AAAUSDT"))]);

    let plans = build_initial_entry_live_dry_run_plans(
        &cfg,
        &symbol_rules,
        &two_exchange_entry_balances(50.0),
        &DisabledRegistry::new(),
        &FeeModel::default(),
        &book_cache,
        &BTreeSet::new(),
        &BTreeSet::new(),
    )
    .await;

    assert_eq!(plans.len(), 2);
    assert!(plans.iter().all(|plan| plan.validation_result.passed));
    assert!(plans.iter().all(|plan| plan.notional <= 14.0 + 1e-12));
    let notional_by_exchange = plans
        .iter()
        .map(|plan| (plan.exchange.as_str(), plan.notional.round() as i64))
        .collect::<HashMap<_, _>>();
    assert_eq!(notional_by_exchange.get("coinex"), Some(&13));
    assert_eq!(notional_by_exchange.get("mexc"), Some(&7));
}

#[tokio::test]
async fn live_initial_entry_should_only_build_enabled_small_live_symbols() {
    let mut cfg = live_small_gate_config();
    cfg.exchanges = vec!["mexc".to_string(), "coinex".to_string()];
    cfg.symbols = vec![
        "AAAUSDT".to_string(),
        "BBBUSDT".to_string(),
        "CCCUSDT".to_string(),
    ];
    cfg.small_live_gate.enabled_symbols = vec!["BBBUSDT".to_string()];
    cfg.small_live_gate.enabled_exchanges = vec!["mexc".to_string(), "coinex".to_string()];
    cfg.max_enabled_arbitrage_symbols = 2;
    cfg.max_notional_per_trade = 1.2;
    cfg.initial_entry_notional_usdt = 5.0;
    let book_cache = BookCache::default();
    let mut symbol_rules = HashMap::new();
    for symbol in &cfg.symbols {
        book_cache
            .update_book(
                test_symbol_book("mexc", symbol, 0.99, 1.0, 100.0),
                BookSource::Rest,
            )
            .await;
        book_cache
            .update_book(
                test_symbol_book("coinex", symbol, 1.00, 1.01, 100.0),
                BookSource::Rest,
            )
            .await;
        symbol_rules.insert(symbol.clone(), test_symbol_rules(symbol));
    }

    let plans = build_initial_entry_live_dry_run_plans(
        &cfg,
        &symbol_rules,
        &two_exchange_entry_balances(50.0),
        &DisabledRegistry::new(),
        &FeeModel::default(),
        &book_cache,
        &BTreeSet::new(),
        &BTreeSet::new(),
    )
    .await;

    assert_eq!(plans.len(), 2);
    assert!(plans.iter().all(|plan| plan.symbol == "BBBUSDT"));
    assert!(plans.iter().all(|plan| plan.intent == "initial_entry"));
}

#[tokio::test]
async fn initial_entry_live_dry_run_should_only_build_missing_symbols() {
    let mut cfg = config();
    cfg.trading_mode = "live_dry_run".to_string();
    cfg.max_enabled_arbitrage_symbols = 2;
    cfg.symbols = vec!["AAAUSDT".to_string(), "BBBUSDT".to_string()];
    cfg.initial_balances.insert(
        "mexc".to_string(),
        HashMap::from([("USDT".to_string(), 50.0)]),
    );
    let book_cache = BookCache::default();
    let mut symbol_rules = HashMap::new();
    for symbol in &cfg.symbols {
        book_cache
            .update_book(
                test_symbol_book("mexc", symbol, 0.99, 1.0, 100.0),
                BookSource::Rest,
            )
            .await;
        book_cache
            .update_book(
                test_symbol_book("coinex", symbol, 1.00, 1.01, 100.0),
                BookSource::Rest,
            )
            .await;
        symbol_rules.insert(symbol.clone(), test_symbol_rules(symbol));
    }
    let already_planned = BTreeSet::from(["AAAUSDT".to_string()]);

    let plans = build_initial_entry_live_dry_run_plans(
        &cfg,
        &symbol_rules,
        &two_exchange_entry_balances(50.0),
        &DisabledRegistry::new(),
        &FeeModel::default(),
        &book_cache,
        &already_planned,
        &BTreeSet::new(),
    )
    .await;

    assert_eq!(plans.len(), 2);
    assert!(plans.iter().all(|plan| plan.symbol == "BBBUSDT"));
    assert!(plans.iter().all(|plan| plan.intent == "initial_entry"));
}

#[tokio::test]
async fn initial_entry_live_dry_run_should_skip_control_blocked_symbols() {
    let mut cfg = config();
    cfg.trading_mode = "live_dry_run".to_string();
    cfg.max_enabled_arbitrage_symbols = 1;
    cfg.symbols = vec!["AAAUSDT".to_string()];
    let book_cache = BookCache::default();
    book_cache
        .update_book(
            test_symbol_book("mexc", "AAAUSDT", 1.09, 1.10, 100.0),
            BookSource::Rest,
        )
        .await;
    book_cache
        .update_book(
            test_symbol_book("coinex", "AAAUSDT", 0.99, 1.00, 100.0),
            BookSource::Rest,
        )
        .await;
    let symbol_rules = HashMap::from([("AAAUSDT".to_string(), test_symbol_rules("AAAUSDT"))]);
    let balances = HashMap::from([
        (
            SpotVenue::Mexc,
            vec![AssetBalance::new("USDT", 50.0, 50.0, 0.0)],
        ),
        (
            SpotVenue::CoinEx,
            vec![AssetBalance::new("USDT", 50.0, 50.0, 0.0)],
        ),
    ]);
    let blocked = BTreeSet::from(["AAAUSDT".to_string()]);

    let plans = build_initial_entry_live_dry_run_plans(
        &cfg,
        &symbol_rules,
        &balances,
        &DisabledRegistry::new(),
        &FeeModel::default(),
        &book_cache,
        &BTreeSet::new(),
        &blocked,
    )
    .await;

    assert!(plans.is_empty());
}

#[tokio::test]
async fn initial_entry_live_dry_run_should_build_buy_plans_on_both_exchanges() {
    let mut cfg = config();
    cfg.trading_mode = "live_dry_run".to_string();
    cfg.max_enabled_arbitrage_symbols = 1;
    cfg.symbols = vec!["AAAUSDT".to_string()];
    let book_cache = BookCache::default();
    book_cache
        .update_book(
            test_symbol_book("mexc", "AAAUSDT", 1.09, 1.10, 100.0),
            BookSource::Rest,
        )
        .await;
    book_cache
        .update_book(
            test_symbol_book("coinex", "AAAUSDT", 0.99, 1.00, 100.0),
            BookSource::Rest,
        )
        .await;
    let symbol_rules = HashMap::from([("AAAUSDT".to_string(), test_symbol_rules("AAAUSDT"))]);
    let balances = HashMap::from([
        (
            SpotVenue::Mexc,
            vec![AssetBalance::new("USDT", 50.0, 50.0, 0.0)],
        ),
        (
            SpotVenue::CoinEx,
            vec![AssetBalance::new("USDT", 50.0, 50.0, 0.0)],
        ),
    ]);

    let plans = build_initial_entry_live_dry_run_plans(
        &cfg,
        &symbol_rules,
        &balances,
        &DisabledRegistry::new(),
        &FeeModel::default(),
        &book_cache,
        &BTreeSet::new(),
        &BTreeSet::new(),
    )
    .await;

    assert_eq!(plans.len(), 2);
    assert_eq!(
        plans
            .iter()
            .map(|plan| (plan.exchange.as_str(), plan.side))
            .collect::<Vec<_>>(),
        vec![("mexc", OrderSide::Buy), ("coinex", OrderSide::Buy)]
    );
    assert!(plans.iter().all(|plan| plan.validation_result.passed));
}

#[tokio::test]
async fn initial_entry_live_dry_run_should_reflect_live_balance_shortage() {
    let mut cfg = config();
    cfg.trading_mode = "live_dry_run".to_string();
    cfg.max_enabled_arbitrage_symbols = 1;
    cfg.initial_entry_notional_usdt = 5.0;
    cfg.symbols = vec!["AAAUSDT".to_string()];
    let book_cache = BookCache::default();
    book_cache
        .update_book(
            test_symbol_book("mexc", "AAAUSDT", 0.99, 1.0, 100.0),
            BookSource::Rest,
        )
        .await;
    book_cache
        .update_book(
            test_symbol_book("coinex", "AAAUSDT", 1.00, 1.01, 100.0),
            BookSource::Rest,
        )
        .await;
    let symbol_rules = HashMap::from([("AAAUSDT".to_string(), test_symbol_rules("AAAUSDT"))]);

    let plans = build_initial_entry_live_dry_run_plans(
        &cfg,
        &symbol_rules,
        &two_exchange_entry_balances(1.0),
        &DisabledRegistry::new(),
        &FeeModel::default(),
        &book_cache,
        &BTreeSet::new(),
        &BTreeSet::new(),
    )
    .await;

    assert_eq!(plans.len(), 2);
    assert!(plans.iter().all(|plan| plan.intent == "initial_entry"));
    assert!(plans.iter().all(|plan| !plan.validation_result.passed));
    assert!(plans.iter().all(|plan| plan
        .rejection_reason
        .as_deref()
        .unwrap_or_default()
        .contains("balance_available")));
}

#[tokio::test]
async fn initial_entry_failure_should_not_mark_symbol_completed() {
    let mut cfg = config();
    cfg.trading_mode = "live_dry_run".to_string();
    cfg.max_enabled_arbitrage_symbols = 1;
    cfg.initial_entry_notional_usdt = 5.0;
    cfg.symbols = vec!["AAAUSDT".to_string()];
    let book_cache = BookCache::default();
    book_cache
        .update_book(
            test_symbol_book("mexc", "AAAUSDT", 0.99, 1.0, 100.0),
            BookSource::Rest,
        )
        .await;
    book_cache
        .update_book(
            test_symbol_book("coinex", "AAAUSDT", 1.00, 1.01, 100.0),
            BookSource::Rest,
        )
        .await;
    let symbol_rules = HashMap::from([("AAAUSDT".to_string(), test_symbol_rules("AAAUSDT"))]);
    let failed = build_initial_entry_live_dry_run_plans(
        &cfg,
        &symbol_rules,
        &two_exchange_entry_balances(1.0),
        &DisabledRegistry::new(),
        &FeeModel::default(),
        &book_cache,
        &BTreeSet::new(),
        &BTreeSet::new(),
    )
    .await
    .remove(0);
    let mut planned_legs = BTreeMap::new();
    let mut completed = BTreeSet::new();
    let mut failures = BTreeMap::new();

    assert!(should_record_initial_entry_plan(
        &failed,
        &mut planned_legs,
        &mut completed,
        &mut failures
    ));
    assert!(completed.is_empty());
    assert!(!should_record_initial_entry_plan(
        &failed,
        &mut planned_legs,
        &mut completed,
        &mut failures
    ));

    let passed = build_initial_entry_live_dry_run_plans(
        &cfg,
        &symbol_rules,
        &two_exchange_entry_balances(50.0),
        &DisabledRegistry::new(),
        &FeeModel::default(),
        &book_cache,
        &BTreeSet::new(),
        &BTreeSet::new(),
    )
    .await;
    assert_eq!(passed.len(), 2);
    assert!(should_record_initial_entry_plan(
        &passed[0],
        &mut planned_legs,
        &mut completed,
        &mut failures
    ));
    assert!(completed.is_empty());
    assert!(should_record_initial_entry_plan(
        &passed[1],
        &mut planned_legs,
        &mut completed,
        &mut failures
    ));
    assert!(completed.contains("AAAUSDT"));
    assert!(!failures.contains_key("AAAUSDT"));
}

#[test]
fn spread_duration_tracker_should_trigger_after_configured_seconds() {
    let mut tracker = SpreadDurationTracker::default();
    let mut opportunity = test_opportunity_record(true);
    opportunity.symbol = "AAAUSDT".to_string();
    let start = Utc::now();

    assert!(!tracker.observe(&opportunity, start, 30));
    assert!(!tracker.observe(&opportunity, start + Duration::seconds(29), 30));
    assert!(tracker.observe(&opportunity, start + Duration::seconds(30), 30));

    opportunity.accepted = false;
    assert!(!tracker.observe(&opportunity, start + Duration::seconds(31), 30));
    opportunity.accepted = true;
    assert!(!tracker.observe(&opportunity, start + Duration::seconds(32), 30));
}

#[test]
fn arbitrage_pair_runtime_should_exit_after_no_net_spread_window() {
    let start = Utc::now();
    let mut runtime = ArbitragePairRuntime::opening("AAAUSDT", start);
    runtime.mark_arbitraging(start);

    assert!(!runtime.should_exit_for_inactivity(start + Duration::seconds(299), 300));
    assert!(runtime.should_exit_for_inactivity(start + Duration::seconds(300), 300));

    runtime.mark_opportunity(start + Duration::seconds(250));
    assert!(!runtime.should_exit_for_inactivity(start + Duration::seconds(549), 300));
    assert!(runtime.should_exit_for_inactivity(start + Duration::seconds(550), 300));
}

#[test]
fn entry_allocations_should_follow_low_bid_weight_schedule() {
    let mut cfg = config();
    cfg.initial_entry_notional_usdt = 10.0;
    cfg.exchanges = vec![
        "mexc".to_string(),
        "coinex".to_string(),
        "gateio".to_string(),
        "bitget".to_string(),
    ];
    let books = vec![
        (
            SpotVenue::Mexc,
            test_symbol_book("mexc", "AAAUSDT", 1.02, 1.03, 100.0),
        ),
        (
            SpotVenue::CoinEx,
            test_symbol_book("coinex", "AAAUSDT", 1.00, 1.01, 100.0),
        ),
        (
            SpotVenue::GateIo,
            test_symbol_book("gateio", "AAAUSDT", 0.98, 0.99, 100.0),
        ),
        (
            SpotVenue::Bitget,
            test_symbol_book("bitget", "AAAUSDT", 1.01, 1.02, 100.0),
        ),
    ];

    let allocations = build_entry_allocations(&cfg, &books);

    assert_eq!(
        allocations
            .iter()
            .map(|allocation| allocation.exchange.as_str())
            .collect::<Vec<_>>(),
        vec!["gateio", "coinex", "bitget", "mexc"]
    );
    assert_eq!(
        allocations
            .iter()
            .map(|allocation| allocation.notional)
            .collect::<Vec<_>>(),
        vec![4.0, 3.0, 2.0, 1.0]
    );
}

#[test]
fn entry_allocations_should_sort_by_bid_even_when_ask_order_differs() {
    let mut cfg = config();
    cfg.initial_entry_notional_usdt = 10.0;
    let books = vec![
        (
            SpotVenue::Mexc,
            test_symbol_book("mexc", "AAAUSDT", 1.00, 1.01, 100.0),
        ),
        (
            SpotVenue::CoinEx,
            test_symbol_book("coinex", "AAAUSDT", 0.98, 1.20, 100.0),
        ),
        (
            SpotVenue::GateIo,
            test_symbol_book("gateio", "AAAUSDT", 0.99, 1.00, 100.0),
        ),
    ];

    let allocations = build_entry_allocations(&cfg, &books);

    assert_eq!(
        allocations
            .iter()
            .map(|allocation| allocation.exchange.as_str())
            .collect::<Vec<_>>(),
        vec!["coinex", "gateio", "mexc"]
    );
    assert_eq!(
        allocations
            .iter()
            .map(|allocation| allocation.notional)
            .collect::<Vec<_>>(),
        vec![5.0, 3.0, 2.0]
    );
}

#[test]
fn entry_allocations_should_use_three_and_two_exchange_schedules() {
    let mut cfg = config();
    cfg.initial_entry_notional_usdt = 10.0;
    let three_books = vec![
        (
            SpotVenue::Mexc,
            test_symbol_book("mexc", "AAAUSDT", 1.02, 1.03, 100.0),
        ),
        (
            SpotVenue::CoinEx,
            test_symbol_book("coinex", "AAAUSDT", 0.99, 1.00, 100.0),
        ),
        (
            SpotVenue::GateIo,
            test_symbol_book("gateio", "AAAUSDT", 1.00, 1.01, 100.0),
        ),
    ];
    let two_books = three_books[..2].to_vec();

    assert_eq!(
        build_entry_allocations(&cfg, &three_books)
            .iter()
            .map(|allocation| allocation.notional)
            .collect::<Vec<_>>(),
        vec![5.0, 3.0, 2.0]
    );
    assert_eq!(
        build_entry_allocations(&cfg, &two_books)
            .iter()
            .map(|allocation| allocation.notional)
            .collect::<Vec<_>>(),
        vec![6.5, 3.5]
    );
}

#[test]
fn entry_plans_should_switch_from_bid_maker_to_ask_taker_after_retries() {
    let mut cfg = config();
    cfg.initial_entry_notional_usdt = 10.0;
    cfg.live_dry_run.max_notional_per_order = 10.0;
    cfg.live_dry_run.max_total_notional = 20.0;
    let rules = lifecycle_symbol_rules("AAAUSDT");
    let books = vec![
        (
            SpotVenue::Mexc,
            test_symbol_book("mexc", "AAAUSDT", 0.99, 1.00, 100.0),
        ),
        (
            SpotVenue::CoinEx,
            test_symbol_book("coinex", "AAAUSDT", 1.01, 1.02, 100.0),
        ),
    ];
    let balances = HashMap::from([
        (
            SpotVenue::Mexc,
            vec![AssetBalance::new("USDT", 50.0, 50.0, 0.0)],
        ),
        (
            SpotVenue::CoinEx,
            vec![AssetBalance::new("USDT", 50.0, 50.0, 0.0)],
        ),
    ]);

    let maker = build_entry_live_dry_run_plans(
        &cfg,
        "AAAUSDT",
        &rules,
        &books,
        &balances,
        0,
        &DisabledRegistry::new(),
        &FeeModel::default(),
    );
    let taker = build_entry_live_dry_run_plans(
        &cfg,
        "AAAUSDT",
        &rules,
        &books,
        &balances,
        cfg.entry_maker_retries,
        &DisabledRegistry::new(),
        &FeeModel::default(),
    );

    assert!(maker
        .iter()
        .all(|plan| plan.order_type == OrderType::PostOnly));
    assert!(maker
        .iter()
        .all(|plan| plan.time_in_force == Some(TimeInForce::GTC)));
    assert_eq!(maker[0].price, Some(0.99));
    assert!(taker.iter().all(|plan| plan.order_type == OrderType::IOC));
    assert!(taker
        .iter()
        .all(|plan| plan.time_in_force == Some(TimeInForce::IOC)));
    assert_eq!(taker[0].price, Some(1.00));
}

#[test]
fn dual_taker_arbitrage_plans_should_use_larger_exchange_min_notional() {
    let mut cfg = config();
    cfg.active_taker_notional_usdt = 1.2;
    cfg.live_dry_run.max_notional_per_order = 10.0;
    let mut rules = lifecycle_symbol_rules("CUDISUSDT");
    rules.mexc.min_notional = 3.0;
    rules.coinex.min_notional = 5.0;
    let opportunity = test_opportunity_record(true);
    let buy_book = book("mexc", 0.99, 1.00, 100.0);
    let sell_book = book("coinex", 1.02, 1.03, 100.0);
    let buy_balances = vec![AssetBalance::new("USDT", 50.0, 50.0, 0.0)];
    let sell_balances = vec![AssetBalance::new("CUDIS", 50.0, 50.0, 0.0)];

    let plans = build_dual_taker_arbitrage_plans(
        &cfg,
        &opportunity,
        &rules,
        &buy_book,
        &sell_book,
        &buy_balances,
        &sell_balances,
        &DisabledRegistry::new(),
        &FeeModel::default(),
    )
    .unwrap();

    assert_eq!(plans.len(), 2);
    assert!(plans
        .iter()
        .all(|plan| plan.intent == "dual_taker_arbitrage"));
    assert!(plans.iter().all(|plan| plan.order_type == OrderType::IOC));
    assert!(plans.iter().all(|plan| plan.notional >= 5.0 - 1e-9));
    assert!(plans.iter().all(|plan| plan.notional <= 5.1));
}

#[test]
fn dual_taker_execution_should_order_sell_leg_first() {
    let mut cfg = config();
    cfg.active_taker_notional_usdt = 3.6;
    cfg.live_dry_run.max_notional_per_order = 10.0;
    let rules = lifecycle_symbol_rules("CUDISUSDT");
    let opportunity = test_opportunity_record(true);
    let buy_book = book("mexc", 0.99, 1.00, 100.0);
    let sell_book = book("coinex", 1.02, 1.03, 100.0);
    let buy_balances = vec![AssetBalance::new("USDT", 50.0, 50.0, 0.0)];
    let sell_balances = vec![AssetBalance::new("CUDIS", 50.0, 50.0, 0.0)];

    let mut plans = build_dual_taker_arbitrage_plans(
        &cfg,
        &opportunity,
        &rules,
        &buy_book,
        &sell_book,
        &buy_balances,
        &sell_balances,
        &DisabledRegistry::new(),
        &FeeModel::default(),
    )
    .unwrap();

    assert_eq!(plans[0].side, OrderSide::Buy);
    order_dual_taker_plans_sell_first(&mut plans);
    assert_eq!(plans[0].side, OrderSide::Sell);
    assert_eq!(plans[1].side, OrderSide::Buy);
}

#[test]
fn existing_inventory_should_restore_pair_to_arbitraging() {
    let mut cfg = config();
    cfg.active_taker_notional_usdt = 3.6;
    cfg.live_dry_run.max_notional_per_order = 10.0;
    cfg.live_dry_run.max_total_notional = 50.0;
    let rules = lifecycle_symbol_rules("CUDISUSDT");
    let opportunity = test_opportunity_record(true);
    let buy_book = book("mexc", 0.99, 1.00, 100.0);
    let sell_book = book("coinex", 1.02, 1.03, 100.0);
    let mut cache = LiveInventoryCache::default();
    cache.balances_by_exchange.insert(
        SpotVenue::Mexc,
        vec![AssetBalance::new("USDT", 10.0, 10.0, 0.0)],
    );
    cache.balances_by_exchange.insert(
        SpotVenue::CoinEx,
        vec![AssetBalance::new("CUDIS", 10.0, 10.0, 0.0)],
    );
    let now = Utc::now();
    let mut runtime = ArbitragePairRuntime::opening("CUDISUSDT", now);

    let restored = restore_arbitraging_from_existing_inventory(
        &cfg,
        &opportunity,
        &rules,
        &buy_book,
        &sell_book,
        &cache,
        &PaperInventory::default(),
        &DisabledRegistry::new(),
        &FeeModel::default(),
        now,
        &mut runtime,
    );

    assert!(restored);
    assert_eq!(runtime.status, ArbitragePairStatus::Arbitraging);
}

#[test]
fn missing_sell_inventory_should_not_restore_pair_to_arbitraging() {
    let mut cfg = config();
    cfg.active_taker_notional_usdt = 3.6;
    cfg.live_dry_run.max_notional_per_order = 10.0;
    cfg.live_dry_run.max_total_notional = 50.0;
    let rules = lifecycle_symbol_rules("CUDISUSDT");
    let opportunity = test_opportunity_record(true);
    let buy_book = book("mexc", 0.99, 1.00, 100.0);
    let sell_book = book("coinex", 1.02, 1.03, 100.0);
    let mut cache = LiveInventoryCache::default();
    cache.balances_by_exchange.insert(
        SpotVenue::Mexc,
        vec![AssetBalance::new("USDT", 10.0, 10.0, 0.0)],
    );
    cache.balances_by_exchange.insert(
        SpotVenue::CoinEx,
        vec![AssetBalance::new("CUDIS", 0.0, 0.0, 0.0)],
    );
    let now = Utc::now();
    let mut runtime = ArbitragePairRuntime::opening("CUDISUSDT", now);

    let restored = restore_arbitraging_from_existing_inventory(
        &cfg,
        &opportunity,
        &rules,
        &buy_book,
        &sell_book,
        &cache,
        &PaperInventory::default(),
        &DisabledRegistry::new(),
        &FeeModel::default(),
        now,
        &mut runtime,
    );

    assert!(!restored);
    assert_eq!(runtime.status, ArbitragePairStatus::Opening);
}

#[test]
fn inactive_exit_plans_should_switch_from_ask_maker_to_market_after_retries() {
    let mut cfg = config();
    cfg.initial_entry_notional_usdt = 10.0;
    cfg.live_dry_run.max_notional_per_order = 10.0;
    cfg.live_dry_run.max_total_notional = 20.0;
    let rules = lifecycle_symbol_rules("CUDISUSDT");
    let books = vec![
        (SpotVenue::Mexc, book("mexc", 0.99, 1.00, 100.0)),
        (SpotVenue::CoinEx, book("coinex", 1.01, 1.02, 100.0)),
    ];
    let balances = HashMap::from([
        (
            SpotVenue::Mexc,
            vec![AssetBalance::new("CUDIS", 50.0, 50.0, 0.0)],
        ),
        (
            SpotVenue::CoinEx,
            vec![AssetBalance::new("CUDIS", 50.0, 50.0, 0.0)],
        ),
    ]);

    let maker = build_exit_live_dry_run_plans(
        &cfg,
        "CUDISUSDT",
        &rules,
        &books,
        &balances,
        0,
        &DisabledRegistry::new(),
        &FeeModel::default(),
    );
    let taker = build_exit_live_dry_run_plans(
        &cfg,
        "CUDISUSDT",
        &rules,
        &books,
        &balances,
        cfg.exit_maker_retries,
        &DisabledRegistry::new(),
        &FeeModel::default(),
    );

    assert!(maker
        .iter()
        .all(|plan| plan.intent == "inactive_exit_maker"));
    assert!(maker
        .iter()
        .all(|plan| plan.order_type == OrderType::PostOnly));
    assert_eq!(maker[0].price, Some(1.00));
    assert!(taker
        .iter()
        .all(|plan| plan.intent == "inactive_exit_market_fallback"));
    assert!(taker
        .iter()
        .all(|plan| plan.order_type == OrderType::Market));
    assert_eq!(taker[0].price, None);
    assert_eq!(taker[0].order_request.price, None);
}

#[test]
fn inactive_exit_plans_should_not_create_below_min_notional_chunks() {
    let mut cfg = config();
    cfg.initial_entry_notional_usdt = 3.2;
    cfg.live_dry_run.max_notional_per_order = 3.2;
    cfg.live_dry_run.max_total_notional = 20.0;
    let mut rules = lifecycle_symbol_rules("CUDISUSDT");
    if let Some(gateio) = rules.gateio.as_mut() {
        gateio.min_notional = 3.0;
        gateio.step_size = 0.001;
        gateio.min_quantity = 0.001;
    }
    let books = vec![
        (SpotVenue::GateIo, book("gateio", 0.5177, 0.5180, 100.0)),
        (SpotVenue::Bitget, book("bitget", 0.5176, 0.5179, 100.0)),
    ];
    let balances = HashMap::from([
        (
            SpotVenue::GateIo,
            vec![AssetBalance::new("CUDIS", 6.18094, 6.18094, 0.0)],
        ),
        (
            SpotVenue::Bitget,
            vec![AssetBalance::new("CUDIS", 18.54144, 18.54144, 0.0)],
        ),
    ]);

    let plans = build_exit_live_dry_run_plans(
        &cfg,
        "CUDISUSDT",
        &rules,
        &books,
        &balances,
        0,
        &DisabledRegistry::new(),
        &FeeModel::default(),
    );

    let gateio = plans
        .iter()
        .find(|plan| plan.exchange == "gateio")
        .expect("gateio exit plan should be generated");
    assert!(gateio.notional >= 3.0);
    assert!(gateio.validation_result.passed);
    assert!(gateio.rejection_reason.is_none());
}

#[test]
fn taker_failure_recovery_should_sell_from_other_exchange() {
    let mut cfg = config();
    cfg.live_dry_run.max_notional_per_order = 10.0;
    cfg.inventory_rebalance.allow_emergency_recovery = true;
    let rules = lifecycle_symbol_rules("CUDISUSDT");
    let books = vec![
        (SpotVenue::Mexc, book("mexc", 0.99, 1.00, 100.0)),
        (SpotVenue::CoinEx, book("coinex", 1.02, 1.03, 100.0)),
        (SpotVenue::GateIo, book("gateio", 1.01, 1.02, 100.0)),
    ];
    let balances = HashMap::from([
        (
            SpotVenue::CoinEx,
            vec![AssetBalance::new("CUDIS", 50.0, 50.0, 0.0)],
        ),
        (
            SpotVenue::GateIo,
            vec![AssetBalance::new("CUDIS", 50.0, 50.0, 0.0)],
        ),
    ]);

    let plans = build_taker_failure_recovery_sell_plans(
        &cfg,
        "CUDISUSDT",
        SpotVenue::Mexc,
        &rules,
        &books,
        &balances,
        &DisabledRegistry::new(),
        &FeeModel::default(),
    );

    assert_eq!(plans.len(), 2);
    assert!(plans
        .iter()
        .all(|plan| plan.intent == "taker_failure_recovery_sell"));
    assert_eq!(plans[0].exchange, "coinex");
    assert_eq!(plans[0].side, OrderSide::Sell);
    assert_eq!(plans[0].order_type, OrderType::IOC);
}

#[test]
fn taker_failure_recovery_should_be_disabled_by_default() {
    let mut cfg = config();
    cfg.live_dry_run.max_notional_per_order = 10.0;
    let rules = lifecycle_symbol_rules("CUDISUSDT");
    let books = vec![
        (SpotVenue::Mexc, book("mexc", 0.99, 1.00, 100.0)),
        (SpotVenue::CoinEx, book("coinex", 1.02, 1.03, 100.0)),
    ];
    let balances = HashMap::from([(
        SpotVenue::CoinEx,
        vec![AssetBalance::new("CUDIS", 50.0, 50.0, 0.0)],
    )]);

    let plans = build_taker_failure_recovery_sell_plans(
        &cfg,
        "CUDISUSDT",
        SpotVenue::Mexc,
        &rules,
        &books,
        &balances,
        &DisabledRegistry::new(),
        &FeeModel::default(),
    );

    assert!(plans.is_empty());
}

#[test]
fn inventory_rebalance_should_only_plan_profit_preserving_move_toward_five_usdt() {
    let mut cfg = config();
    cfg.min_notional_per_trade = 1.0;
    cfg.active_taker_notional_usdt = 1.2;
    cfg.live_dry_run.max_notional_per_order = 10.0;
    cfg.taker_fee_bps_override = Some(0.0);
    let rules = lifecycle_symbol_rules("CUDISUSDT");
    let books = vec![
        (SpotVenue::Mexc, book("mexc", 1.09, 1.10, 100.0)),
        (SpotVenue::CoinEx, book("coinex", 0.99, 1.00, 100.0)),
    ];
    let balances = HashMap::from([
        (
            SpotVenue::Mexc,
            vec![AssetBalance::new("CUDIS", 10.0, 10.0, 0.0)],
        ),
        (
            SpotVenue::CoinEx,
            vec![
                AssetBalance::new("CUDIS", 0.0, 0.0, 0.0),
                AssetBalance::new("USDT", 50.0, 50.0, 0.0),
            ],
        ),
    ]);

    let plans = build_inventory_rebalance_plans(
        &cfg,
        "CUDISUSDT",
        &rules,
        &books,
        &balances,
        5.0,
        &DisabledRegistry::new(),
        &FeeModel::default(),
    );

    assert_eq!(plans.len(), 2);
    assert!(plans
        .iter()
        .all(|plan| plan.intent == "inventory_rebalance"));
    assert_eq!(
        plans
            .iter()
            .map(|plan| (plan.exchange.as_str(), plan.side))
            .collect::<Vec<_>>(),
        vec![("mexc", OrderSide::Sell), ("coinex", OrderSide::Buy)]
    );

    let losing_books = vec![
        (SpotVenue::Mexc, book("mexc", 0.99, 1.00, 100.0)),
        (SpotVenue::CoinEx, book("coinex", 1.09, 1.10, 100.0)),
    ];
    assert!(build_inventory_rebalance_plans(
        &cfg,
        "CUDISUSDT",
        &rules,
        &losing_books,
        &balances,
        5.0,
        &DisabledRegistry::new(),
        &FeeModel::default(),
    )
    .is_empty());
}

#[test]
fn inventory_trim_excess_should_sell_when_total_inventory_exceeds_target() {
    let mut cfg = config();
    cfg.min_notional_per_trade = 1.0;
    cfg.active_taker_notional_usdt = 3.6;
    cfg.live_dry_run.max_notional_per_order = 10.0;
    cfg.inventory_rebalance.max_rebalance_notional_usdt = 5.0;
    cfg.taker_fee_bps_override = Some(0.0);
    let rules = lifecycle_symbol_rules("CUDISUSDT");
    let books = vec![
        (SpotVenue::GateIo, book("gateio", 0.02930, 0.02931, 2000.0)),
        (SpotVenue::Bitget, book("bitget", 0.02925, 0.02926, 2000.0)),
    ];
    let balances = HashMap::from([
        (
            SpotVenue::GateIo,
            vec![AssetBalance::new("CUDIS", 1133.0, 1133.0, 0.0)],
        ),
        (
            SpotVenue::Bitget,
            vec![AssetBalance::new("CUDIS", 468.0, 468.0, 0.0)],
        ),
    ]);

    let plans = build_inventory_trim_excess_plans(
        &cfg,
        "CUDISUSDT",
        &rules,
        &books,
        &balances,
        20.0,
        &DisabledRegistry::new(),
        &FeeModel::default(),
    );

    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].intent, "inventory_trim_excess");
    assert_eq!(plans[0].exchange, "gateio");
    assert_eq!(plans[0].side, OrderSide::Sell);
    assert!(plans[0].notional <= cfg.active_taker_notional_usdt + 1e-12);
}

#[test]
fn inventory_trim_excess_should_not_sell_when_only_one_venue_exceeds_target() {
    let mut cfg = config();
    cfg.min_notional_per_trade = 1.0;
    cfg.active_taker_notional_usdt = 3.6;
    cfg.live_dry_run.max_notional_per_order = 10.0;
    cfg.inventory_rebalance.max_rebalance_notional_usdt = 5.0;
    cfg.taker_fee_bps_override = Some(0.0);
    let rules = lifecycle_symbol_rules("CUDISUSDT");
    let books = vec![
        (SpotVenue::GateIo, book("gateio", 0.02930, 0.02931, 2000.0)),
        (SpotVenue::Bitget, book("bitget", 0.02930, 0.02931, 2000.0)),
    ];
    let balances = HashMap::from([
        (
            SpotVenue::GateIo,
            vec![AssetBalance::new("CUDIS", 649.0, 649.0, 0.0)],
        ),
        (
            SpotVenue::Bitget,
            vec![AssetBalance::new("CUDIS", 34.0, 34.0, 0.0)],
        ),
    ]);

    let plans = build_inventory_trim_excess_plans(
        &cfg,
        "CUDISUSDT",
        &rules,
        &books,
        &balances,
        20.0,
        &DisabledRegistry::new(),
        &FeeModel::default(),
    );

    assert!(plans.is_empty());
}

#[test]
fn inventory_top_up_should_buy_when_total_inventory_below_target() {
    let mut cfg = config();
    cfg.min_notional_per_trade = 1.0;
    cfg.active_taker_notional_usdt = 3.6;
    cfg.live_dry_run.max_notional_per_order = 10.0;
    cfg.inventory_rebalance.max_rebalance_notional_usdt = 5.0;
    cfg.inventory_rebalance.allow_auto_initial_entry = true;
    cfg.taker_fee_bps_override = Some(0.0);
    let rules = lifecycle_symbol_rules("CUDISUSDT");
    let books = vec![
        (SpotVenue::GateIo, book("gateio", 0.02927, 0.02928, 2000.0)),
        (SpotVenue::Bitget, book("bitget", 0.02941, 0.02942, 2000.0)),
    ];
    let balances = HashMap::from([
        (
            SpotVenue::GateIo,
            vec![
                AssetBalance::new("CUDIS", 160.0, 160.0, 0.0),
                AssetBalance::new("USDT", 30.0, 30.0, 0.0),
            ],
        ),
        (
            SpotVenue::Bitget,
            vec![
                AssetBalance::new("CUDIS", 276.0, 276.0, 0.0),
                AssetBalance::new("USDT", 30.0, 30.0, 0.0),
            ],
        ),
    ]);

    let plans = build_inventory_top_up_plans(
        &cfg,
        "CUDISUSDT",
        &rules,
        &books,
        &balances,
        20.0,
        &DisabledRegistry::new(),
        &FeeModel::default(),
    );

    assert_eq!(plans.len(), 1);
    assert_eq!(plans[0].intent, "inventory_top_up");
    assert_eq!(plans[0].side, OrderSide::Buy);
    assert_eq!(plans[0].exchange, "gateio");
    assert!(plans[0].notional <= cfg.active_taker_notional_usdt + 1e-12);
}

#[test]
fn blocked_inventory_rebalance_should_allow_small_budgeted_loss() {
    let mut cfg = config();
    cfg.min_notional_per_trade = 1.0;
    cfg.active_taker_notional_usdt = 3.6;
    cfg.live_dry_run.max_notional_per_order = 10.0;
    cfg.taker_fee_bps_override = Some(0.0);
    cfg.inventory_rebalance.allow_lossy_rebalance_when_blocked = true;
    cfg.inventory_rebalance.max_blocked_rebalance_loss_usdt = 0.08;
    let rules = lifecycle_symbol_rules("VSNUSDT");
    let books = vec![
        (SpotVenue::GateIo, book("gateio", 0.02919, 0.02920, 2000.0)),
        (SpotVenue::Bitget, book("bitget", 0.02937, 0.02938, 2000.0)),
    ];
    let balances = HashMap::from([
        (
            SpotVenue::GateIo,
            vec![
                AssetBalance::new("VSN", 1134.372, 1134.372, 0.0),
                AssetBalance::new("USDT", 2.9173, 2.9173, 0.0),
            ],
        ),
        (
            SpotVenue::Bitget,
            vec![
                AssetBalance::new("VSN", 468.10321, 468.10321, 0.0),
                AssetBalance::new("USDT", 24.733, 24.733, 0.0),
            ],
        ),
    ]);
    let opportunity = OpportunityRecord {
        timestamp: Utc::now(),
        symbol: "VSNUSDT".to_string(),
        buy_exchange: "gateio".to_string(),
        sell_exchange: "bitget".to_string(),
        buy_price: 0.02920,
        sell_price: 0.02937,
        raw_spread_bps: 58.0,
        buy_fee_bps: 0.0,
        sell_fee_bps: 0.0,
        fee_source_buy: FeeSource::Fallback,
        fee_source_sell: FeeSource::Fallback,
        platform_discount_applied: false,
        estimated_fee_bps: 0.0,
        estimated_slippage_bps: 0.0,
        safety_buffer_bps: 0.0,
        estimated_net_spread_bps: 58.0,
        estimated_total_fee: 0.0,
        estimated_gross_pnl: 0.02,
        estimated_net_pnl: 0.02,
        executable_notional: 3.6,
        quantity: 122.0,
        accepted: true,
        rejection_reason: None,
        rejection_detail: None,
        buy_book_age_ms: 0,
        sell_book_age_ms: 0,
        buy_book_source: BookSource::Rest,
        sell_book_source: BookSource::Rest,
        buy_latency_ms: None,
        sell_latency_ms: None,
        lifecycle_latency: None,
        capital_cost_bps: 0.0,
        transfer_cost_bps: 0.0,
        transfer_delay_penalty_bps: 0.0,
        inventory_rebalance_cost_bps: 0.0,
        latency_penalty_bps: 0.0,
        effective_min_net_spread_bps: 0.0,
        estimated_slippage_cost: 0.0,
        estimated_capital_cost: 0.0,
        estimated_transfer_cost: 0.0,
        estimated_inventory_rebalance_cost: 0.0,
        estimated_latency_penalty_cost: 0.0,
        estimated_total_cost: 0.0,
    };

    let default_plans = build_inventory_rebalance_plans(
        &cfg,
        "VSNUSDT",
        &rules,
        &books,
        &balances,
        10.0,
        &DisabledRegistry::new(),
        &FeeModel::default(),
    );
    assert!(default_plans.is_empty());

    let blocked_plans = build_blocked_arbitrage_rebalance_plans(
        &cfg,
        &opportunity,
        SpotVenue::GateIo,
        SpotVenue::Bitget,
        &rules,
        &books,
        &balances,
        &DisabledRegistry::new(),
        &FeeModel::default(),
    );

    assert_eq!(blocked_plans.len(), 2);
    assert!(blocked_plans
        .iter()
        .all(|plan| plan.intent == "blocked_inventory_rebalance"));
    assert_eq!(
        blocked_plans
            .iter()
            .map(|plan| (plan.exchange.as_str(), plan.side))
            .collect::<Vec<_>>(),
        vec![("gateio", OrderSide::Sell), ("bitget", OrderSide::Buy)]
    );

    cfg.inventory_rebalance.max_blocked_rebalance_loss_usdt = 0.001;
    assert!(build_blocked_arbitrage_rebalance_plans(
        &cfg,
        &opportunity,
        SpotVenue::GateIo,
        SpotVenue::Bitget,
        &rules,
        &books,
        &balances,
        &DisabledRegistry::new(),
        &FeeModel::default(),
    )
    .is_empty());
}

#[test]
fn long_one_sided_recovery_should_not_sell_below_cost_like_pond() {
    let mut cfg = config();
    cfg.min_notional_per_trade = 1.0;
    cfg.live_dry_run.max_notional_per_order = 25.0;
    cfg.inventory_rebalance.max_rebalance_notional_usdt = 25.0;
    cfg.taker_fee_bps_override = Some(0.0);
    let rules = lifecycle_symbol_rules("CUDISUSDT");
    let losing_books = vec![(SpotVenue::Mexc, book("mexc", 0.001993, 0.002000, 20_000.0))];
    let balances = HashMap::from([(
        SpotVenue::Mexc,
        vec![AssetBalance::new("CUDIS", 9832.8, 9832.8, 0.0)],
    )]);
    let exposure = OneSidedExposure {
        symbol: "CUDISUSDT".to_string(),
        kind: OneSidedExposureKind::LongBase,
        exchange: SpotVenue::Mexc,
        quantity: 9832.8,
        reference_price: 0.002022,
        notional: 19.8819216,
        created_at: Utc::now(),
    };

    let losing_plans = build_one_sided_exposure_recovery_plans(
        &cfg,
        &exposure,
        &rules,
        &losing_books,
        &balances,
        &DisabledRegistry::new(),
        &FeeModel::default(),
        0.0,
    );

    assert!(losing_plans.is_empty());

    let profitable_books = vec![(SpotVenue::Mexc, book("mexc", 0.002040, 0.002050, 20_000.0))];
    let profitable_plans = build_one_sided_exposure_recovery_plans(
        &cfg,
        &exposure,
        &rules,
        &profitable_books,
        &balances,
        &DisabledRegistry::new(),
        &FeeModel::default(),
        0.0,
    );

    assert_eq!(profitable_plans.len(), 1);
    assert_eq!(profitable_plans[0].side, OrderSide::Sell);
    assert_eq!(
        profitable_plans[0].intent,
        "one_sided_recovery_sell_no_loss"
    );
}

#[test]
fn short_one_sided_recovery_should_not_buy_above_sold_price_like_obol() {
    let mut cfg = config();
    cfg.min_notional_per_trade = 1.0;
    cfg.live_dry_run.max_notional_per_order = 25.0;
    cfg.inventory_rebalance.max_rebalance_notional_usdt = 25.0;
    cfg.taker_fee_bps_override = Some(0.0);
    let rules = lifecycle_symbol_rules("CUDISUSDT");
    let losing_books = vec![(
        SpotVenue::GateIo,
        book("gateio", 0.004180, 0.004185, 20_000.0),
    )];
    let balances = HashMap::from([(
        SpotVenue::GateIo,
        vec![AssetBalance::new("USDT", 50.0, 50.0, 0.0)],
    )]);
    let exposure = OneSidedExposure {
        symbol: "CUDISUSDT".to_string(),
        kind: OneSidedExposureKind::ShortBase,
        exchange: SpotVenue::CoinEx,
        quantity: 2496.01,
        reference_price: 0.004145,
        notional: 10.34596145,
        created_at: Utc::now(),
    };

    let losing_plans = build_one_sided_exposure_recovery_plans(
        &cfg,
        &exposure,
        &rules,
        &losing_books,
        &balances,
        &DisabledRegistry::new(),
        &FeeModel::default(),
        0.0,
    );

    assert!(losing_plans.is_empty());

    let profitable_books = vec![(
        SpotVenue::GateIo,
        book("gateio", 0.004100, 0.004105, 20_000.0),
    )];
    let profitable_plans = build_one_sided_exposure_recovery_plans(
        &cfg,
        &exposure,
        &rules,
        &profitable_books,
        &balances,
        &DisabledRegistry::new(),
        &FeeModel::default(),
        0.0,
    );

    assert_eq!(profitable_plans.len(), 1);
    assert_eq!(profitable_plans[0].side, OrderSide::Buy);
    assert_eq!(profitable_plans[0].intent, "one_sided_recovery_buy_no_loss");
}

#[test]
fn inventory_rebalance_config_can_disable_market_rebalance() {
    let mut cfg = config();
    cfg.min_notional_per_trade = 1.0;
    cfg.active_taker_notional_usdt = 1.2;
    cfg.live_dry_run.max_notional_per_order = 10.0;
    cfg.taker_fee_bps_override = Some(0.0);
    cfg.inventory_rebalance.allow_market_rebalance = false;
    let rules = lifecycle_symbol_rules("CUDISUSDT");
    let books = vec![
        (SpotVenue::Mexc, book("mexc", 1.09, 1.10, 100.0)),
        (SpotVenue::CoinEx, book("coinex", 0.99, 1.00, 100.0)),
    ];
    let balances = HashMap::from([
        (
            SpotVenue::Mexc,
            vec![AssetBalance::new("CUDIS", 10.0, 10.0, 0.0)],
        ),
        (
            SpotVenue::CoinEx,
            vec![
                AssetBalance::new("CUDIS", 0.0, 0.0, 0.0),
                AssetBalance::new("USDT", 50.0, 50.0, 0.0),
            ],
        ),
    ]);

    let plans = build_inventory_rebalance_plans(
        &cfg,
        "CUDISUSDT",
        &rules,
        &books,
        &balances,
        5.0,
        &DisabledRegistry::new(),
        &FeeModel::default(),
    );

    assert!(plans.is_empty());
}

fn test_maker_order_plan(
    cfg: &SpotSpotTakerArbitrageConfig,
) -> crate::execution::LiveDryRunOrderPlan {
    let rules = lifecycle_symbol_rules("CUDISUSDT");
    let book = book("mexc", 0.99, 1.00, 100.0);
    let balances = vec![AssetBalance::new("USDT", 50.0, 50.0, 0.0)];
    let mut plan = crate::execution::build_live_dry_run_order_plan_with_style(
        &cfg.live_dry_run,
        crate::execution::LiveDryRunOrderInput {
            exchange: "mexc",
            market_type: MarketType::Spot,
            internal_symbol: "CUDISUSDT",
            side: OrderSide::Buy,
            desired_notional: 1.2,
            book: &book,
            symbol_rule: rules.for_exchange(SpotVenue::Mexc),
            balances: &balances,
            reservations: &crate::exchanges::spot_reservation::BalanceReservationManager::default(),
            disabled_registry: &DisabledRegistry::new(),
            fee_model: &FeeModel::default(),
        },
        Some(crate::execution::LiveDryRunOrderStyle::maker_post_only(
            0.99,
        )),
    )
    .unwrap();
    plan.intent = "initial_entry_maker".to_string();
    plan
}

fn mock_order_for_plan(
    plan: &crate::execution::LiveDryRunOrderPlan,
    status: OrderStatus,
    filled_quantity: f64,
) -> OrderResponse {
    OrderResponse {
        exchange: plan.exchange.clone(),
        market_type: plan.market_type,
        symbol: plan.symbol.clone(),
        order_id: "mock-order-1".to_string(),
        client_order_id: plan.client_order_id.clone(),
        side: plan.side,
        position_side: crate::exchanges::unified::PositionSide::None,
        order_type: plan.order_type,
        status,
        price: plan.price,
        quantity: plan.quantity,
        filled_quantity,
        average_price: plan.price,
        created_at: Utc::now(),
        updated_at: None,
    }
}

#[test]
fn live_inventory_cache_should_apply_filled_buy_and_sell_orders() {
    let cfg = config();
    let buy_plan = test_maker_order_plan(&cfg);
    let mut cache = LiveInventoryCache::default();
    cache.balances_by_exchange.insert(
        SpotVenue::Mexc,
        vec![AssetBalance::new("USDT", 50.0, 50.0, 0.0)],
    );

    cache.apply_filled_order(&buy_plan);

    let balances = cache.balances_by_exchange.get(&SpotVenue::Mexc).unwrap();
    let usdt = balances
        .iter()
        .find(|balance| balance.asset == "USDT")
        .unwrap();
    let cudis = balances
        .iter()
        .find(|balance| balance.asset == "CUDIS")
        .unwrap();
    assert!(usdt.available < 50.0);
    assert!((cudis.available - buy_plan.quantity).abs() < 1e-9);

    let mut sell_plan = buy_plan.clone();
    sell_plan.side = OrderSide::Sell;
    cache.balances_by_exchange.insert(
        SpotVenue::Mexc,
        vec![
            AssetBalance::new("CUDIS", 10.0, 10.0, 0.0),
            AssetBalance::new("USDT", 50.0, 50.0, 0.0),
        ],
    );

    cache.apply_filled_order(&sell_plan);

    let balances = cache.balances_by_exchange.get(&SpotVenue::Mexc).unwrap();
    let usdt = balances
        .iter()
        .find(|balance| balance.asset == "USDT")
        .unwrap();
    let cudis = balances
        .iter()
        .find(|balance| balance.asset == "CUDIS")
        .unwrap();
    assert!(usdt.available > 50.0);
    assert!((cudis.available - (10.0 - sell_plan.quantity)).abs() < 1e-9);
}

#[test]
fn live_order_result_helpers_should_detect_fill_and_strategy_timeouts() {
    let cfg = config();
    let mut plan = test_maker_order_plan(&cfg);

    let filled = mock_order_for_plan(&plan, OrderStatus::New, plan.quantity);

    assert!(order_filled(&filled, plan.quantity));
    assert_eq!(live_order_timeout_ms(&cfg, &plan), 30_000);
    plan.intent = "inactive_exit_maker".to_string();
    assert_eq!(live_order_timeout_ms(&cfg, &plan), 60_000);
    plan.intent = "inactive_exit_market_fallback".to_string();
    plan.order_type = OrderType::Market;
    assert_eq!(live_order_timeout_ms(&cfg, &plan), cfg.request_timeout_ms);
}

#[tokio::test]
async fn live_order_wait_should_poll_until_filled() {
    let cfg = config();
    let plan = test_maker_order_plan(&cfg);
    let initial = mock_order_for_plan(&plan, OrderStatus::New, 0.0);
    let filled = mock_order_for_plan(&plan, OrderStatus::Filled, plan.quantity);
    let client = MockSpotOrderClient::new([filled]);

    let result = wait_for_spot_order_fill(&cfg, &plan, initial, &client).await;

    assert!(result.submitted);
    assert!(result.filled);
    assert!(!result.timed_out);
    assert_eq!(result.order_id.as_deref(), Some("mock-order-1"));
    assert_eq!(client.get_order_calls(), 1);
    assert_eq!(client.cancel_calls(), 0);
}

#[tokio::test]
async fn live_order_wait_should_stop_on_terminal_unfilled_status() {
    let cfg = config();
    let plan = test_maker_order_plan(&cfg);
    let initial = mock_order_for_plan(&plan, OrderStatus::Rejected, 0.0);
    let client = MockSpotOrderClient::new([]);

    let result = wait_for_spot_order_fill(&cfg, &plan, initial, &client).await;

    assert!(result.submitted);
    assert!(!result.filled);
    assert!(!result.timed_out);
    assert!(result
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("terminal unfilled status Rejected"));
    assert_eq!(client.get_order_calls(), 0);
    assert_eq!(client.cancel_calls(), 0);
}

#[tokio::test]
async fn live_order_wait_should_cancel_post_only_after_strategy_timeout() {
    let mut cfg = config();
    cfg.entry_order_timeout_seconds = 0;
    let plan = test_maker_order_plan(&cfg);
    let initial = mock_order_for_plan(&plan, OrderStatus::New, 0.0);
    let client = MockSpotOrderClient::new([]);

    let result = wait_for_spot_order_fill(&cfg, &plan, initial, &client).await;

    assert!(result.submitted);
    assert!(!result.filled);
    assert!(result.timed_out);
    assert!(result.cancelled);
    assert_eq!(client.get_order_calls(), 0);
    assert_eq!(client.cancel_calls(), 1);
}

#[test]
fn risk_auto_stop_should_trigger_on_consecutive_rejections() {
    let mut cfg = config();
    cfg.max_consecutive_rejections = 2;
    let mut risk = RiskState::new(&cfg);
    risk.record_rejection("AUSDT", RejectionReason::InsufficientQuoteBalance);
    assert!(risk_auto_stop_reason(&risk, &cfg).is_none());
    risk.record_rejection("BUSDT", RejectionReason::InsufficientQuoteBalance);
    assert!(matches!(
        risk_auto_stop_reason(&risk, &cfg),
        Some(RiskStopReason::ConsecutiveRejections { count: 2, limit: 2 })
    ));
}

#[test]
fn routine_depth_and_stale_rejections_should_not_blacklist_symbol() {
    let cfg = config();
    let mut risk = RiskState::new(&cfg);
    for _ in 0..25 {
        risk.record_rejection("AUSDT", RejectionReason::InsufficientDepth);
        risk.record_rejection("AUSDT", RejectionReason::StaleBook);
    }

    assert!(!risk.is_symbol_blacklisted("AUSDT"));
}

#[test]
fn trade_loss_limit_should_trigger_auto_stop_reason() {
    let cfg = config();
    let risk = RiskState::new(&cfg);
    let trade = SimulatedTradeRecord {
        timestamp: Utc::now(),
        symbol: "AUSDT".to_string(),
        buy_exchange: "mexc".to_string(),
        sell_exchange: "coinex".to_string(),
        buy_avg_price: 1.0,
        sell_avg_price: 0.9,
        quantity: 100.0,
        notional: 100.0,
        buy_fee: 0.0,
        sell_fee: 0.0,
        gross_pnl: -10.0,
        net_pnl: -11.0,
        pnl_category: TradePnlCategory::Arbitrage,
        slippage_cost: 0.0,
        capital_cost: 0.0,
        transfer_cost: 0.0,
        inventory_rebalance_cost: 0.0,
        latency_penalty_cost: 0.0,
        latency_ms: 10,
        order_book_age_ms: 10,
        execution_mode: "paper".to_string(),
    };
    assert!(risk.trade_loss_limit_hit(&cfg, &trade));
    let reason = RiskStopReason::TradeLoss {
        pnl: trade.net_pnl,
        limit: cfg.max_trade_loss,
        symbol: trade.symbol.clone(),
    };
    assert_eq!(reason.symbol(), "AUSDT");
}

fn book(exchange: &str, bid: f64, ask: f64, qty: f64) -> OrderBookSnapshot {
    OrderBookSnapshot {
        exchange: exchange.to_string(),
        market_type: MarketType::Spot,
        symbol: "CUDISUSDT".to_string(),
        bids: vec![OrderBookLevel {
            price: bid,
            quantity: qty,
        }],
        asks: vec![OrderBookLevel {
            price: ask,
            quantity: qty,
        }],
        best_bid: Some(bid),
        best_ask: Some(ask),
        exchange_timestamp: Some(Utc::now()),
        received_at: Utc::now(),
        latency_ms: Some(10),
        sequence: Some(1),
        is_stale: false,
    }
}

#[test]
fn raw_spread_calculation_should_use_sell_bid_minus_buy_ask() {
    let spread = calculate_spread(1.0, 1.02, 0.0, 0.0, 0.0, 0.0);
    assert!((spread.raw_spread_bps - 200.0).abs() < 1e-9);
}

#[test]
fn net_spread_calculation_should_deduct_fees_slippage_and_buffer() {
    let spread = calculate_spread(1.0, 1.02, 10.0, 10.0, 2.0, 3.0);
    assert!((spread.net_spread_bps - 175.0).abs() < 1e-9);
}

#[test]
fn stale_order_book_should_reject_opportunity() {
    let cfg = config();
    let mut stale = book("mexc", 0.99, 1.0, 100.0);
    stale.received_at = Utc::now() - Duration::milliseconds(2_000);
    let opp = build_opportunity(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &stale,
        &book("coinex", 1.03, 1.04, 100.0),
    );
    assert_eq!(opp.rejection_reason, Some(RejectionReason::StaleBook));
}

#[test]
fn insufficient_depth_should_reject_opportunity() {
    let cfg = config();
    let opp = build_opportunity(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 2.0),
        &book("coinex", 1.03, 1.04, 2.0),
    );
    assert_eq!(
        opp.rejection_reason,
        Some(RejectionReason::InsufficientDepth)
    );
}

#[test]
fn exchange_min_notional_should_raise_active_trade_depth_target() {
    let mut cfg = config();
    cfg.active_taker_notional_usdt = 1.2;
    cfg.min_notional_per_trade = 1.0;
    cfg.min_depth_notional = 1.0;
    cfg.max_notional_per_trade = 10.0;
    let mut rules = rules();
    rules.mexc.min_notional = 3.0;
    rules.coinex.min_notional = 5.0;

    let shallow = build_opportunity(
        &cfg,
        &rules,
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 4.0),
        &book("coinex", 1.03, 1.04, 4.0),
    );
    assert_eq!(
        shallow.rejection_reason,
        Some(RejectionReason::InsufficientDepth)
    );

    let enough = build_opportunity(
        &cfg,
        &rules,
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 10.0),
        &book("coinex", 1.03, 1.04, 10.0),
    );
    assert_eq!(enough.rejection_reason, None);
    assert!(enough.executable_notional >= 5.0 - 1e-12);
}

#[test]
fn step_rounding_should_not_reject_when_depth_covers_target_and_exchange_minimums_pass() {
    let mut cfg = config();
    cfg.active_taker_notional_usdt = 3.6;
    cfg.min_notional_per_trade = 1.0;
    cfg.min_depth_notional = 3.6;
    cfg.max_notional_per_trade = 3.6;
    let mut rules = rules();
    rules.mexc.min_notional = 3.0;
    rules.mexc.min_quantity = 1.0;
    rules.mexc.step_size = 1.0;
    rules.coinex.min_notional = 1.0;
    rules.coinex.min_quantity = 0.0;
    rules.coinex.step_size = 0.01;

    let opp = build_opportunity(
        &cfg,
        &rules,
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 4.0),
        &book("coinex", 1.03, 1.04, 4.0),
    );

    assert!(opp.accepted);
    assert!(opp.executable_notional < cfg.min_depth_notional);
    assert!(opp.executable_notional >= rules.mexc.min_notional);
    assert_eq!(opp.rejection_reason, None);
}

#[test]
fn min_notional_should_reject_opportunity() {
    let mut cfg = config();
    cfg.min_depth_notional = 1.0;
    cfg.min_notional_per_trade = 50.0;
    cfg.max_notional_per_trade = 20.0;
    let opp = build_opportunity(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 20.0),
        &book("coinex", 1.03, 1.04, 20.0),
    );
    assert_eq!(
        opp.rejection_reason,
        Some(RejectionReason::InsufficientDepth)
    );
}

#[test]
fn symbol_rule_validation_should_round_down_common_step() {
    let mut cfg = config();
    cfg.max_notional_per_trade = 10.5;
    let opp = build_opportunity(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );
    assert_eq!(opp.rejection_reason, None);
    assert!((opp.quantity - 6.0).abs() < 1e-12);
    assert!(opp.executable_notional <= cfg.max_notional_per_trade + 1e-12);
}

#[test]
fn abnormal_raw_spread_should_be_rejected() {
    let mut cfg = config();
    cfg.min_net_spread_bps = 0.0;
    let opp = build_opportunity(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.25, 1.26, 100.0),
    );
    assert_eq!(opp.rejection_reason, Some(RejectionReason::AbnormalSpread));
}

#[test]
fn edgeusdt_raw_spread_above_ten_percent_should_be_rejected() {
    let mut cfg = config();
    cfg.symbols = vec!["EDGEUSDT".to_string()];
    cfg.min_notional_per_trade = 1.0;
    cfg.min_depth_notional = 1.0;
    cfg.min_net_spread_bps = 0.0;
    cfg.max_raw_spread_bps = 1_000.0;
    cfg.initial_balances.insert(
        "coinex".to_string(),
        HashMap::from([("EDGE".to_string(), 10_000.0), ("USDT".to_string(), 500.0)]),
    );

    let opp = build_opportunity(
        &cfg,
        &test_symbol_rules("EDGEUSDT"),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &test_symbol_book("mexc", "EDGEUSDT", 0.99, 1.0, 100.0),
        &test_symbol_book("coinex", "EDGEUSDT", 1.1002, 1.1010, 100.0),
    );

    assert_eq!(opp.rejection_reason, Some(RejectionReason::AbnormalSpread));
    assert!(opp.raw_spread_bps > 1_000.0);
}

#[test]
fn raw_bid_ask_spread_below_thirty_bps_should_reject() {
    let mut cfg = config();
    cfg.min_raw_spread_bps = 30.0;
    cfg.min_net_spread_bps = 0.0;
    cfg.taker_fee_bps_override = Some(0.0);
    cfg.slippage_bps = 0.0;
    cfg.safety_buffer_bps = 0.0;
    let opp = build_opportunity(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.0029, 1.0040, 100.0),
    );

    assert_eq!(
        opp.rejection_reason,
        Some(RejectionReason::NetSpreadBelowThreshold)
    );
    assert!(opp
        .rejection_detail
        .as_deref()
        .unwrap_or_default()
        .contains("raw_spread_bps"));
}

#[test]
fn raw_bid_ask_spread_above_thirty_bps_should_pass_raw_gate() {
    let mut cfg = config();
    cfg.min_raw_spread_bps = 30.0;
    cfg.min_net_spread_bps = 0.0;
    cfg.taker_fee_bps_override = Some(0.0);
    cfg.slippage_bps = 0.0;
    cfg.safety_buffer_bps = 0.0;
    let opp = build_opportunity(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.0031, 1.0040, 100.0),
    );

    assert!(opp.accepted);
    assert!(opp.raw_spread_bps > 30.0);
}

#[test]
fn taker_buy_simulation_should_consume_asks() {
    let leg = simulate_taker_buy(
        &[
            OrderBookLevel {
                price: 1.0,
                quantity: 5.0,
            },
            OrderBookLevel {
                price: 1.1,
                quantity: 5.0,
            },
        ],
        10.0,
        10.0,
    )
    .unwrap();
    assert!((leg.average_price - 1.05).abs() < 1e-9);
    assert!((leg.fee - 0.0105).abs() < 1e-9);
}

#[test]
fn taker_sell_simulation_should_consume_bids() {
    let leg = simulate_taker_sell(
        &[
            OrderBookLevel {
                price: 1.1,
                quantity: 5.0,
            },
            OrderBookLevel {
                price: 1.0,
                quantity: 5.0,
            },
        ],
        10.0,
        10.0,
    )
    .unwrap();
    assert!((leg.average_price - 1.05).abs() < 1e-9);
}

#[test]
fn full_taker_taker_paper_trade_should_settle_inventory() {
    let cfg = config();
    let mut inventory = PaperInventory::from_config(&cfg).unwrap();
    let mexc = book("mexc", 0.99, 1.0, 100.0);
    let coinex = book("coinex", 1.03, 1.04, 100.0);
    let opp = build_opportunity(
        &cfg,
        &rules(),
        &inventory,
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &mexc,
        &coinex,
    );
    assert!(opp.accepted);
    let trade =
        execute_paper_taker_taker(&cfg, &mut inventory, &opp, &rules(), &mexc, &coinex).unwrap();
    assert!(trade.net_pnl > 0.0);
    assert!(inventory.realized_pnl > 0.0);
}

#[test]
fn spot_spot_taker_arbitrage_should_use_fee_model_for_net_spread() {
    let mut cfg = config();
    cfg.taker_fee_bps_override = None;
    let fee_model = FeeModel::default();
    let opp = build_with_models(
        &cfg,
        &fee_model,
        &DisabledRegistry::new(),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );
    assert_eq!(opp.buy_fee_bps, 5.0);
    assert_eq!(opp.sell_fee_bps, 20.0);
    assert_eq!(opp.fee_source_buy, FeeSource::ConfigDefault);
    assert_eq!(opp.fee_source_sell, FeeSource::ConfigDefault);
}

#[test]
fn opportunity_should_be_accepted_when_fees_allow_it() {
    let cfg = config();
    let opp = build_with_models(
        &cfg,
        &fee_model_from_strategy_config(&cfg),
        &DisabledRegistry::new(),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );
    assert!(opp.accepted);
    assert!(opp.estimated_net_pnl > 0.0);
}

#[test]
fn opportunity_should_be_rejected_when_fee_model_makes_spread_negative() {
    let mut cfg = config();
    cfg.taker_fee_bps_override = None;
    cfg.min_net_spread_bps = 0.0;
    let fee_model = FeeModel::from_config(FeeConfig {
        fallback: HashMap::new(),
        defaults: HashMap::from([
            (
                "mexc".to_string(),
                HashMap::from([(
                    "spot".to_string(),
                    FeePairConfig {
                        maker_bps: 0.0,
                        taker_bps: 200.0,
                        fee_asset: Some("quote".to_string()),
                        rebate_ratio: None,
                    },
                )]),
            ),
            (
                "coinex".to_string(),
                HashMap::from([(
                    "spot".to_string(),
                    FeePairConfig {
                        maker_bps: 0.0,
                        taker_bps: 200.0,
                        fee_asset: Some("quote".to_string()),
                        rebate_ratio: None,
                    },
                )]),
            ),
        ]),
        symbol_overrides: Vec::new(),
        side_overrides: Vec::new(),
        vip_overrides: Vec::new(),
        exchange_api: Vec::new(),
        platform_tokens: Vec::new(),
        prefer_exchange_api_fees: false,
    });
    let opp = build_with_models(
        &cfg,
        &fee_model,
        &DisabledRegistry::new(),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );
    assert_eq!(
        opp.rejection_reason,
        Some(RejectionReason::NetSpreadBelowThreshold)
    );
}

#[test]
fn task8_different_fee_sources_should_change_net_spread() {
    let mut cfg = config();
    cfg.taker_fee_bps_override = None;
    cfg.min_net_spread_bps = -10_000.0;
    cfg.venue_selection.enabled = false;
    let mut fee_config = FeeConfig::default();
    fee_config
        .symbol_overrides
        .push(crate::execution::SymbolFeeOverride {
            exchange: "mexc".to_string(),
            market_type: "spot".to_string(),
            symbol: Some("CUDISUSDT".to_string()),
            maker_bps: 0.0,
            taker_bps: 0.0,
            fee_asset: Some("quote".to_string()),
            rebate_ratio: None,
            reason: Some("zero fee campaign".to_string()),
        });
    let default_fee = build_with_models(
        &cfg,
        &FeeModel::default(),
        &DisabledRegistry::new(),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );
    let symbol_fee = build_with_models(
        &cfg,
        &FeeModel::from_config(fee_config),
        &DisabledRegistry::new(),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );

    assert_eq!(default_fee.fee_source_buy, FeeSource::ConfigDefault);
    assert_eq!(symbol_fee.fee_source_buy, FeeSource::SymbolOverride);
    assert!(symbol_fee.estimated_net_spread_bps > default_fee.estimated_net_spread_bps);
    assert!(symbol_fee.estimated_net_pnl > default_fee.estimated_net_pnl);
}

#[test]
fn task8_inventory_shortage_should_block_executable_opportunity() {
    let mut cfg = config();
    cfg.min_net_spread_bps = 0.0;
    cfg.initial_balances
        .get_mut("coinex")
        .unwrap()
        .insert("CUDIS".to_string(), 0.0);
    let opp = build_opportunity(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );

    assert_eq!(
        opp.rejection_reason,
        Some(RejectionReason::InsufficientBaseBalance)
    );
}

#[test]
fn task8_slow_venue_latency_penalty_should_raise_execution_threshold() {
    let mut cfg = config();
    cfg.exchanges = vec!["gateio".to_string(), "bitget".to_string()];
    cfg.websocket.exchanges = cfg.exchanges.clone();
    cfg.taker_fee_bps_override = Some(0.0);
    cfg.min_raw_spread_bps = 0.0;
    cfg.min_net_spread_bps = 10.0;
    cfg.slippage_bps = 0.0;
    cfg.safety_buffer_bps = 0.0;
    cfg.initial_balances = HashMap::from([
        (
            "gateio".to_string(),
            HashMap::from([("USDT".to_string(), 500.0), ("CUDIS".to_string(), 10_000.0)]),
        ),
        (
            "bitget".to_string(),
            HashMap::from([("USDT".to_string(), 500.0), ("CUDIS".to_string(), 10_000.0)]),
        ),
    ]);
    cfg.venue_selection.venue_overrides.insert(
        "gateio".to_string(),
        VenueCostConfig {
            latency_penalty_bps: Some(40.0),
            min_net_spread_extra_bps: Some(40.0),
            ..VenueCostConfig::default()
        },
    );
    let gate_rule = rule("gateio");
    let bitget_rule = rule("bitget");
    let rules = CommonSymbolRules {
        mexc: gate_rule.clone(),
        coinex: bitget_rule.clone(),
        gateio: Some(gate_rule),
        bitget: Some(bitget_rule),
        kucoin: None,
    };

    let opp = build_opportunity(
        &cfg,
        &rules,
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::GateIo,
        SpotVenue::Bitget,
        &book("gateio", 0.9999, 1.0, 100.0),
        &book("bitget", 1.0050, 1.0060, 100.0),
    );

    assert_eq!(opp.latency_penalty_bps, 40.0);
    assert_eq!(opp.effective_min_net_spread_bps, 90.0);
    assert_eq!(
        opp.rejection_reason,
        Some(RejectionReason::NetSpreadBelowThreshold)
    );
    assert!(opp
        .rejection_detail
        .as_deref()
        .unwrap_or_default()
        .contains("effective_min_net_spread_bps"));
}

#[test]
fn opportunity_should_be_rejected_by_disabled_symbol() {
    let cfg = config();
    let disabled = DisabledRegistry::from_config(DisabledRegistryConfig {
        disabled: DisabledConfig {
            symbols: vec![DisabledSymbol {
                symbol: "CUDISUSDT".to_string(),
                reason: "manual disabled".to_string(),
                expires_at: None,
            }],
            exchanges: Vec::new(),
            exchange_symbols: Vec::new(),
        },
        unmanaged_positions: Vec::new(),
    });
    let opp = build_with_models(
        &cfg,
        &fee_model_from_strategy_config(&cfg),
        &disabled,
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );
    assert_eq!(opp.rejection_reason, Some(RejectionReason::DisabledSymbol));
    assert_eq!(opp.rejection_detail.as_deref(), Some("manual disabled"));
}

#[test]
fn opportunity_should_be_rejected_by_disabled_exchange_symbol() {
    let cfg = config();
    let disabled = DisabledRegistry::from_config(DisabledRegistryConfig {
        disabled: DisabledConfig {
            symbols: Vec::new(),
            exchanges: Vec::new(),
            exchange_symbols: vec![DisabledExchangeSymbol {
                exchange: "mexc".to_string(),
                market_type: MarketType::Spot,
                symbol: "CUDISUSDT".to_string(),
                reason: "oversold errors".to_string(),
                expires_at: None,
            }],
        },
        unmanaged_positions: Vec::new(),
    });
    let opp = build_with_models(
        &cfg,
        &fee_model_from_strategy_config(&cfg),
        &disabled,
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );
    assert_eq!(
        opp.rejection_reason,
        Some(RejectionReason::DisabledExchangeSymbol)
    );
    assert_eq!(opp.rejection_detail.as_deref(), Some("oversold errors"));
}

#[test]
fn unmanaged_position_should_be_excluded_from_inventory() {
    let cfg = config();
    let disabled = DisabledRegistry::from_config(DisabledRegistryConfig {
        disabled: DisabledConfig::default(),
        unmanaged_positions: vec![UnmanagedPosition {
            exchange: "coinex".to_string(),
            market_type: MarketType::Spot,
            symbol: "CUDISUSDT".to_string(),
            asset: "CUDIS".to_string(),
            quantity: 9_950.0,
            reason: "legacy residual".to_string(),
            created_at: Utc::now(),
        }],
    });
    let mut inventory = PaperInventory::from_config(&cfg).unwrap();
    inventory.exclude_unmanaged_positions(&disabled);
    assert_eq!(
        inventory
            .balance(SpotVenue::CoinEx, "CUDIS")
            .effective_available(),
        50.0
    );
}

#[test]
fn small_live_unmanaged_overlap_should_ignore_positions_outside_gate_scope() {
    let mut cfg = live_small_gate_config();
    cfg.small_live_gate.enabled_symbols = vec!["WLDUSDT".to_string(), "PEPEUSDT".to_string()];
    cfg.small_live_gate.enabled_exchanges = vec!["gateio".to_string(), "bitget".to_string()];
    let positions = vec![crate::web::UnmanagedPositionView {
        exchange: "coinex".to_string(),
        market_type: MarketType::Spot,
        symbol: "PONDUSDT".to_string(),
        asset: "POND".to_string(),
        quantity: 5_000.0,
        reason: "legacy residual".to_string(),
        created_at: Utc::now(),
    }];

    assert!(!has_small_live_unmanaged_inventory_overlap(
        &cfg.small_live_gate,
        &positions
    ));
}

#[test]
fn small_live_unmanaged_overlap_should_match_enabled_exchange_and_symbol() {
    let mut cfg = live_small_gate_config();
    cfg.small_live_gate.enabled_symbols = vec!["WLDUSDT".to_string(), "PEPEUSDT".to_string()];
    cfg.small_live_gate.enabled_exchanges = vec!["gateio".to_string(), "bitget".to_string()];
    let positions = vec![crate::web::UnmanagedPositionView {
        exchange: "gate.io".to_string(),
        market_type: MarketType::Spot,
        symbol: "wldusdt".to_string(),
        asset: "WLD".to_string(),
        quantity: 1.0,
        reason: "manual".to_string(),
        created_at: Utc::now(),
    }];

    assert!(has_small_live_unmanaged_inventory_overlap(
        &cfg.small_live_gate,
        &positions
    ));
}

#[test]
fn opportunity_record_should_include_platform_discount_fee_source() {
    let mut cfg = config();
    cfg.taker_fee_bps_override = None;
    let mut fee_config = FeeConfig::default();
    fee_config.platform_tokens.push(PlatformTokenDiscount {
        exchange: "mexc".to_string(),
        token: "MX".to_string(),
        enabled: true,
        discount_multiplier: Some(0.5),
    });
    let opp = build_with_models(
        &cfg,
        &FeeModel::from_config(fee_config),
        &DisabledRegistry::new(),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );
    assert!(opp.platform_discount_applied);
    assert_eq!(opp.fee_source_buy, FeeSource::PlatformTokenDiscount);
}

#[test]
fn insufficient_quote_balance_should_reject() {
    let mut cfg = config();
    cfg.initial_balances
        .get_mut("mexc")
        .unwrap()
        .insert("USDT".to_string(), 1.0);
    let opp = build_opportunity(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );
    assert_eq!(
        opp.rejection_reason,
        Some(RejectionReason::InsufficientQuoteBalance)
    );
}

#[test]
fn insufficient_base_balance_should_reject() {
    let mut cfg = config();
    cfg.initial_balances
        .get_mut("coinex")
        .unwrap()
        .insert("CUDIS".to_string(), 1.0);
    let opp = build_opportunity(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );
    assert_eq!(
        opp.rejection_reason,
        Some(RejectionReason::InsufficientBaseBalance)
    );
}

#[test]
fn live_lifecycle_initial_entry_candidate_should_not_require_existing_base() {
    let mut cfg = config();
    cfg.trading_mode = "live_dry_run".to_string();
    cfg.min_net_spread_bps = 0.0;
    cfg.initial_balances
        .get_mut("coinex")
        .unwrap()
        .insert("CUDIS".to_string(), 0.0);
    let opp = build_opportunity(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &book("mexc", 0.99, 1.0, 100.0),
        &book("coinex", 1.03, 1.04, 100.0),
    );

    assert_eq!(opp.rejection_reason, None);
    assert!(opp.accepted);
}

#[test]
fn fee_deduction_should_reduce_net_pnl() {
    let leg = simulate_taker_buy(
        &[OrderBookLevel {
            price: 1.0,
            quantity: 10.0,
        }],
        10.0,
        10.0,
    )
    .unwrap();
    assert_eq!(leg.notional, 10.0);
    assert_eq!(leg.fee, 0.01);
}

#[test]
fn inventory_settlement_should_update_balances() {
    let cfg = config();
    let mut inventory = PaperInventory::from_config(&cfg).unwrap();
    inventory.settle_buy(SpotVenue::Mexc, "CUDIS", "USDT", 10.0, 10.0, 0.01);
    assert!((inventory.balance(SpotVenue::Mexc, "USDT").available - 489.99).abs() < 1e-9);
    assert!((inventory.balance(SpotVenue::Mexc, "CUDIS").available - 10_010.0).abs() < 1e-9);
}

#[test]
fn opportunity_recording_should_write_jsonl() {
    let path = format!(
        "{}/spot_spot_taker_{}.jsonl",
        std::env::temp_dir().display(),
        Utc::now().timestamp_nanos_opt().unwrap()
    );
    let event = RecorderEvent::Opportunity(OpportunityRecord {
        timestamp: Utc::now(),
        symbol: "CUDISUSDT".to_string(),
        buy_exchange: "mexc".to_string(),
        sell_exchange: "coinex".to_string(),
        buy_price: 1.0,
        sell_price: 1.03,
        raw_spread_bps: 300.0,
        buy_fee_bps: 10.0,
        sell_fee_bps: 10.0,
        fee_source_buy: FeeSource::ConfigDefault,
        fee_source_sell: FeeSource::ConfigDefault,
        platform_discount_applied: false,
        estimated_fee_bps: 20.0,
        estimated_slippage_bps: 2.0,
        safety_buffer_bps: 3.0,
        estimated_net_spread_bps: 275.0,
        estimated_total_fee: 0.2,
        estimated_gross_pnl: 3.0,
        estimated_net_pnl: 2.8,
        capital_cost_bps: 0.0,
        transfer_cost_bps: 0.0,
        transfer_delay_penalty_bps: 0.0,
        inventory_rebalance_cost_bps: 0.0,
        latency_penalty_bps: 0.0,
        effective_min_net_spread_bps: 0.0,
        estimated_slippage_cost: 0.0,
        estimated_capital_cost: 0.0,
        estimated_transfer_cost: 0.0,
        estimated_inventory_rebalance_cost: 0.0,
        estimated_latency_penalty_cost: 0.0,
        estimated_total_cost: 0.2,
        executable_notional: 100.0,
        quantity: 100.0,
        accepted: true,
        rejection_reason: None,
        rejection_detail: None,
        buy_book_age_ms: 1,
        sell_book_age_ms: 1,
        buy_book_source: BookSource::Rest,
        sell_book_source: BookSource::Rest,
        buy_latency_ms: Some(1),
        sell_latency_ms: Some(1),
        lifecycle_latency: None,
    });
    append_jsonl_for_test(&path, &event).unwrap();
    let raw = std::fs::read_to_string(path).unwrap();
    assert!(raw.contains("\"event_type\":\"opportunity\""));
}

#[test]
fn rejection_reason_recording_should_group_reasons() {
    let mut report = SummaryReport::default();
    report.record_rejection(RejectionReason::StaleBook);
    report.record_rejection(RejectionReason::StaleBook);
    assert_eq!(
        report
            .rejection_reasons
            .get(&RejectionReason::StaleBook)
            .copied(),
        Some(2)
    );
}

#[test]
fn legacy_summary_report_should_use_sdk_core_statistics() {
    let mut report = SummaryReport::default();
    report.symbols_scanned = 1;
    report.record_opportunity(&OpportunityRecord {
        timestamp: Utc::now(),
        symbol: "CUDISUSDT".to_string(),
        buy_exchange: "mexc".to_string(),
        sell_exchange: "coinex".to_string(),
        buy_price: 1.0,
        sell_price: 1.03,
        raw_spread_bps: 300.0,
        buy_fee_bps: 10.0,
        sell_fee_bps: 10.0,
        fee_source_buy: FeeSource::ConfigDefault,
        fee_source_sell: FeeSource::ConfigDefault,
        platform_discount_applied: false,
        estimated_fee_bps: 20.0,
        estimated_slippage_bps: 2.0,
        safety_buffer_bps: 3.0,
        estimated_net_spread_bps: 275.0,
        estimated_total_fee: 0.2,
        estimated_gross_pnl: 3.0,
        estimated_net_pnl: 2.8,
        capital_cost_bps: 0.0,
        transfer_cost_bps: 0.0,
        transfer_delay_penalty_bps: 0.0,
        inventory_rebalance_cost_bps: 0.0,
        latency_penalty_bps: 0.0,
        effective_min_net_spread_bps: 0.0,
        estimated_slippage_cost: 0.0,
        estimated_capital_cost: 0.0,
        estimated_transfer_cost: 0.0,
        estimated_inventory_rebalance_cost: 0.0,
        estimated_latency_penalty_cost: 0.0,
        estimated_total_cost: 0.2,
        executable_notional: 100.0,
        quantity: 100.0,
        accepted: false,
        rejection_reason: Some(RejectionReason::NetSpreadBelowThreshold),
        rejection_detail: Some("net spread below threshold".to_string()),
        buy_book_age_ms: 4,
        sell_book_age_ms: 8,
        buy_book_source: BookSource::Rest,
        sell_book_source: BookSource::Rest,
        buy_latency_ms: Some(1),
        sell_latency_ms: Some(1),
        lifecycle_latency: None,
    });
    report.record_trade(&SimulatedTradeRecord {
        timestamp: Utc::now(),
        symbol: "CUDISUSDT".to_string(),
        buy_exchange: "mexc".to_string(),
        sell_exchange: "coinex".to_string(),
        buy_avg_price: 1.0,
        sell_avg_price: 1.03,
        quantity: 100.0,
        notional: 100.0,
        buy_fee: 0.1,
        sell_fee: 0.1,
        gross_pnl: 3.0,
        net_pnl: 2.8,
        pnl_category: TradePnlCategory::Arbitrage,
        slippage_cost: 0.0,
        capital_cost: 0.0,
        transfer_cost: 0.0,
        inventory_rebalance_cost: 0.0,
        latency_penalty_cost: 0.0,
        latency_ms: 12,
        order_book_age_ms: 8,
        execution_mode: "paper".to_string(),
    });

    assert_eq!(report.opportunities_detected, 1);
    assert_eq!(report.opportunities_rejected, 1);
    assert_eq!(
        report
            .rejection_reasons
            .get(&RejectionReason::NetSpreadBelowThreshold),
        Some(&1)
    );
    assert_eq!(report.total_fees, 0.2);
    assert_eq!(report.symbol_net_pnl("CUDISUSDT"), 2.8);
    assert!(report.render().contains("avg_book_age_ms=8.0"));
}

#[test]
fn task8_summary_report_should_split_arbitrage_and_inventory_recovery_pnl() {
    let mut report = SummaryReport::default();
    let mut arbitrage_trade = simulated_trade_for_report(2.0);
    arbitrage_trade.pnl_category = TradePnlCategory::Arbitrage;
    arbitrage_trade.slippage_cost = 0.05;
    arbitrage_trade.capital_cost = 0.02;
    report.record_trade(&arbitrage_trade);

    let mut recovery_trade = simulated_trade_for_report(-0.4);
    recovery_trade.pnl_category = TradePnlCategory::OneSidedExposure;
    recovery_trade.inventory_rebalance_cost = 0.03;
    report.record_trade(&recovery_trade);

    assert_eq!(report.total_arbitrage_net_pnl, 2.0);
    assert_eq!(report.total_inventory_recovery_pnl, -0.4);
    assert_eq!(report.total_one_sided_exposure_pnl, -0.4);
    assert_eq!(report.total_slippage_cost, 0.05);
    assert_eq!(report.total_capital_cost, 0.02);
    assert_eq!(report.total_inventory_rebalance_cost, 0.03);
    let rendered = report.render();
    assert!(rendered.contains("arbitrage_net_pnl=2.000000"));
    assert!(rendered.contains("inventory_recovery_pnl=-0.400000"));
}

#[test]
fn live_trading_config_requires_explicit_real_order_flags() {
    let mut cfg = config();
    cfg.live_trading_enabled = true;
    assert!(cfg.validate_safe_mode().is_err());
    cfg.live_trading_enabled = false;
    cfg.trading_mode = "live".to_string();
    assert!(cfg.validate_safe_mode().is_err());
    cfg.live_trading_enabled = true;
    assert!(cfg.validate_safe_mode().is_err());
    cfg.small_live_gate.enabled = true;
    cfg.small_live_gate.explicit_live_confirmation = true;
    cfg.small_live_gate.enabled_symbols = cfg.symbols.clone();
    cfg.small_live_gate.enabled_exchanges = cfg.exchanges.clone();
    cfg.kill_switch.allow_live_orders = true;
    cfg.live_preflight.enabled = true;
    cfg.live_preflight.target_mode = "live".to_string();
    cfg.live_preflight.exchanges = cfg.small_live_gate.enabled_exchanges.clone();
    cfg.live_preflight.symbols = cfg.small_live_gate.enabled_symbols.clone();
    cfg.live_preflight.max_live_notional_per_trade =
        Some(cfg.small_live_gate.max_notional_per_order);
    cfg.live_preflight.max_total_live_notional = Some(cfg.small_live_gate.max_total_notional);
    cfg.dry_run = false;
    assert!(cfg.validate_safe_mode().is_ok());
    cfg.live_trading_enabled = false;
    assert!(cfg.validate_safe_mode().is_err());
    cfg.live_trading_enabled = true;
    cfg.dry_run = true;
    assert!(cfg.validate_safe_mode().is_err());
    cfg.dry_run = false;
    cfg.trading_mode = "live_dry_run".to_string();
    cfg.live_trading_enabled = false;
    cfg.dry_run = true;
    assert!(cfg.validate_safe_mode().is_ok());
    cfg.live_dry_run.submit_orders = true;
    assert!(cfg.validate_safe_mode().is_err());
}

#[test]
fn live_trading_config_requires_preflight_and_kill_switch_alignment() {
    let mut cfg = live_small_gate_config();
    assert!(cfg.validate_safe_mode().is_ok());

    cfg.live_preflight.enabled = false;
    assert!(cfg.validate_safe_mode().is_err());
    cfg.live_preflight.enabled = true;

    cfg.kill_switch.allow_live_orders = false;
    assert!(cfg.validate_safe_mode().is_err());
    cfg.kill_switch.allow_live_orders = true;

    cfg.live_dry_run.build_order_requests = false;
    assert!(cfg.validate_safe_mode().is_err());
    cfg.live_dry_run.build_order_requests = true;

    cfg.live_preflight.max_live_notional_per_trade =
        Some(cfg.small_live_gate.max_notional_per_order + 0.01);
    assert!(cfg.validate_safe_mode().is_err());
    cfg.live_preflight.max_live_notional_per_trade =
        Some(cfg.small_live_gate.max_notional_per_order);

    cfg.live_preflight.exchanges = vec!["coinex".to_string()];
    assert!(cfg.validate_safe_mode().is_err());
}

#[test]
fn live_trading_config_caps_raw_spread_filter_at_ten_percent() {
    let mut cfg = live_small_gate_config();
    cfg.max_raw_spread_bps = 1_000.0;
    assert!(cfg.validate_safe_mode().is_ok());

    cfg.max_raw_spread_bps = 1_000.0001;
    assert!(cfg.validate_safe_mode().is_err());
}

#[test]
fn live_trading_config_caps_enabled_arbitrage_symbols_at_five() {
    let mut cfg = live_small_gate_config();
    cfg.max_enabled_arbitrage_symbols = 5;
    assert!(cfg.validate_safe_mode().is_ok());

    cfg.max_enabled_arbitrage_symbols = 6;
    assert!(cfg.validate_safe_mode().is_err());

    cfg.max_enabled_arbitrage_symbols = 0;
    assert!(cfg.validate_safe_mode().is_err());
}

#[test]
fn live_trading_config_requires_small_live_symbols_to_be_bounded_subset() {
    let mut cfg = live_small_gate_config();
    cfg.symbols = vec!["AAAUSDT".to_string(), "BBBUSDT".to_string()];
    cfg.small_live_gate.enabled_symbols = vec!["AAAUSDT".to_string(), "BBBUSDT".to_string()];
    cfg.live_preflight.symbols = cfg.small_live_gate.enabled_symbols.clone();
    cfg.max_enabled_arbitrage_symbols = 2;
    assert!(cfg.validate_safe_mode().is_ok());

    cfg.small_live_gate
        .enabled_symbols
        .push("CCCUSDT".to_string());
    assert!(cfg.validate_safe_mode().is_err());

    cfg.small_live_gate.enabled_symbols = vec![
        "AAAUSDT".to_string(),
        "BBBUSDT".to_string(),
        "AAAUSDT".to_string(),
    ];
    cfg.live_preflight.symbols = vec![
        "AAAUSDT".to_string(),
        "BBBUSDT".to_string(),
        "AAAUSDT".to_string(),
    ];
    assert!(cfg.validate_safe_mode().is_ok());

    cfg.small_live_gate.enabled_symbols = vec!["AAAUSDT".to_string(), "BBBUSDT".to_string()];
    cfg.live_preflight.symbols = cfg.small_live_gate.enabled_symbols.clone();
    cfg.max_enabled_arbitrage_symbols = 1;
    assert!(cfg.validate_safe_mode().is_ok());
}

#[test]
fn websocket_symbol_override_must_match_strategy_symbols() {
    let mut cfg = config();
    cfg.market_data_mode = MarketDataMode::WebsocketCache;
    cfg.websocket.enabled = true;
    cfg.symbols = vec!["EDGEUSDT".to_string(), "BTRUSDT".to_string()];
    cfg.websocket.symbols = vec!["EDGEUSDT".to_string()];
    assert!(cfg.validate_safe_mode().is_err());

    cfg.websocket.symbols = vec!["BTR-USDT".to_string(), "EDGE_USDT".to_string()];
    assert!(cfg.validate_safe_mode().is_ok());
}

#[test]
fn websocket_default_should_not_hardcode_exchange_pair() {
    assert!(WebsocketMarketDataConfig::default().exchanges.is_empty());
}

#[test]
fn live_order_submission_should_not_require_small_live_env_confirmation() {
    let cfg = live_small_gate_config();
    let plan = test_maker_order_plan(&cfg);

    assert!(small_live_order_block_reason(&cfg, &plan).is_none());
}

#[test]
fn live_order_submission_should_enforce_small_live_notional_limit() {
    let cfg = live_small_gate_config();
    let mut plan = test_maker_order_plan(&cfg);
    plan.notional = cfg.small_live_gate.max_notional_per_order + 0.01;

    let reason = small_live_order_block_reason(&cfg, &plan).unwrap();
    assert!(reason.contains("max_notional_per_order"));
}

#[test]
fn live_order_safety_should_return_all_actual_rejection_reasons() {
    let cfg = live_small_gate_config();
    let mut plan = test_maker_order_plan(&cfg);
    plan.notional = cfg.small_live_gate.max_notional_per_order + 0.01;
    plan.validation_result.passed = false;
    plan.validation_result
        .checks
        .push(crate::execution::LiveDryRunCheck {
            name: "balance_available".to_string(),
            passed: false,
            message: "USDT available=0 required=3.21".to_string(),
        });

    apply_live_order_safety_checks(&cfg, &mut plan);
    let reason = plan.rejection_reason.as_deref().unwrap_or_default();
    assert!(reason.contains("balance_available: USDT available=0 required=3.21"));
    assert!(reason.contains("small_live_gate: plan notional"));
    assert!(reason.contains("max_notional_per_order"));
    assert!(!plan.would_submit);
}

#[test]
fn live_plan_group_should_skip_when_any_leg_fails_safety() {
    let cfg = live_small_gate_config();
    let buy_plan = test_maker_order_plan(&cfg);
    let mut sell_plan = test_maker_order_plan(&cfg);
    sell_plan.side = OrderSide::Sell;
    sell_plan.exchange = "coinex".to_string();
    sell_plan.intent = "dual_taker_arbitrage".to_string();

    let plans = live_plan_group_ready_for_submit(&cfg, vec![buy_plan, sell_plan]);

    assert!(plans.is_empty());
}

#[test]
fn control_stop_liquidation_should_bypass_entry_notional_limit() {
    let cfg = live_small_gate_config();
    let mut plan = test_maker_order_plan(&cfg);
    plan.intent = "control_stop_liquidate".to_string();
    plan.side = OrderSide::Sell;
    plan.order_type = OrderType::IOC;
    plan.notional = cfg.small_live_gate.max_notional_per_order + 100.0;

    assert!(small_live_order_block_reason(&cfg, &plan).is_none());
}

#[test]
fn live_order_submission_should_not_apply_total_entry_notional_limit_to_exit_plans() {
    let mut cfg = live_small_gate_config();
    cfg.small_live_gate.max_notional_per_order = 5.0;
    cfg.small_live_gate.max_total_notional = 3.0;
    let mut plan = test_maker_order_plan(&cfg);
    plan.notional = 4.0;

    let reason = small_live_order_block_reason(&cfg, &plan).unwrap();
    assert!(reason.contains("max_total_notional"));

    plan.intent = "inactive_exit_maker".to_string();
    assert!(small_live_order_block_reason(&cfg, &plan).is_none());
}

#[test]
fn small_live_opening_notional_should_release_on_sell_fill() {
    if let Ok(mut total) = SPOT_SPOT_LIVE_OPENING_NOTIONAL_USDT
        .get_or_init(|| Mutex::new(0.0))
        .lock()
    {
        *total = 0.0;
    }
    let mut cfg = live_small_gate_config();
    cfg.small_live_gate.max_total_notional = 10.0;
    let mut buy_plan = test_maker_order_plan(&cfg);
    buy_plan.notional = 8.0;
    adjust_live_opening_notional(&buy_plan);

    let mut next_buy = test_maker_order_plan(&cfg);
    next_buy.notional = 3.0;
    let blocked = small_live_order_block_reason(&cfg, &next_buy).unwrap();
    assert!(blocked.contains("open live entry notional"));

    let mut sell_plan = test_maker_order_plan(&cfg);
    sell_plan.side = OrderSide::Sell;
    sell_plan.intent = "dual_taker_arbitrage".to_string();
    sell_plan.notional = 6.0;
    adjust_live_opening_notional(&sell_plan);

    assert!(small_live_order_block_reason(&cfg, &next_buy).is_none());
}

#[test]
fn live_order_submission_should_pass_when_small_live_gate_matches_plan() {
    let cfg = live_small_gate_config();
    let plan = test_maker_order_plan(&cfg);

    assert!(small_live_order_block_reason(&cfg, &plan).is_none());
}

#[tokio::test]
async fn book_cache_insert_and_read_should_return_normalized_book() {
    let cache = BookCache::default();
    cache
        .update_book(book("MEXC", 0.99, 1.0, 100.0), BookSource::Websocket)
        .await;

    let cached = cache.get_book("mexc", "cudisusdt").await.unwrap();
    assert_eq!(cached.exchange, "mexc");
    assert_eq!(cached.symbol, "CUDISUSDT");
    assert_eq!(cached.source, BookSource::Websocket);
    assert_eq!(
        cache.get_best_bid_ask("MEXC", "CUDISUSDT").await,
        Some((0.99, 1.0))
    );
}

#[tokio::test]
async fn book_cache_stale_detection_should_use_age_and_flags() {
    let cache = BookCache::default();
    let mut snapshot = book("mexc", 0.99, 1.0, 100.0);
    snapshot.received_at = Utc::now() - Duration::milliseconds(2_000);
    cache.update_book(snapshot, BookSource::Websocket).await;

    assert!(!cache.is_fresh("mexc", "CUDISUSDT", 1_000).await);
    assert!(cache.get_book_age_ms("mexc", "CUDISUSDT").await.unwrap() >= 2_000);

    cache
        .update_book(book("mexc", 0.99, 1.0, 100.0), BookSource::Websocket)
        .await;
    assert!(cache.is_fresh("mexc", "CUDISUSDT", 1_000).await);
    cache.mark_stale("mexc", "CUDISUSDT").await;
    assert!(!cache.is_fresh("mexc", "CUDISUSDT", 1_000).await);
}

#[test]
fn mexc_websocket_update_normalization_should_parse_depth() {
    let ts = Utc::now().timestamp_millis();
    let message = serde_json::json!({
        "s": "CUDISUSDT",
        "d": {
            "bids": [["1.00", "10"]],
            "asks": [["1.01", "11"]],
            "E": ts,
            "u": 42
        }
    })
    .to_string();

    let snapshot = mexc::parse_ws_orderbook_message(&message, 1_000)
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.exchange, "mexc");
    assert_eq!(snapshot.symbol, "CUDISUSDT");
    assert_eq!(snapshot.best_bid, Some(1.0));
    assert_eq!(snapshot.best_ask, Some(1.01));
    assert_eq!(snapshot.sequence, Some(42));
}

#[test]
fn coinex_websocket_update_normalization_should_parse_depth() {
    let ts = Utc::now().timestamp_millis();
    let message = serde_json::json!({
        "params": {
            "market": "CUDISUSDT",
            "bids": [["1.00", "10"]],
            "asks": [["1.01", "11"]],
            "updated_at": ts,
            "last": 43
        }
    })
    .to_string();

    let snapshot = coinex::parse_ws_orderbook_message(&message, 1_000)
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.exchange, "coinex");
    assert_eq!(snapshot.symbol, "CUDISUSDT");
    assert_eq!(snapshot.best_bid, Some(1.0));
    assert_eq!(snapshot.best_ask, Some(1.01));
    assert_eq!(snapshot.sequence, Some(43));
}

#[test]
fn book_recorder_should_write_jsonl() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("books.jsonl");
    let cached = CachedBook {
        exchange: "mexc".to_string(),
        symbol: "CUDISUSDT".to_string(),
        bids: vec![OrderBookLevel {
            price: 1.0,
            quantity: 10.0,
        }],
        asks: vec![OrderBookLevel {
            price: 1.01,
            quantity: 10.0,
        }],
        best_bid: Some(1.0),
        best_ask: Some(1.01),
        exchange_timestamp: None,
        local_timestamp: Utc::now(),
        latency_ms: None,
        sequence: Some(1),
        source: BookSource::Websocket,
        is_stale: false,
    };
    append_book_record(
        path.to_str().unwrap(),
        &BookRecord::from_cached(&cached, true),
    )
    .unwrap();

    let raw = std::fs::read_to_string(path).unwrap();
    assert!(raw.contains("\"event_type\":\"top_of_book\""));
    assert!(raw.contains("\"source\":\"websocket\""));
}

#[test]
fn replay_should_load_jsonl_records() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("books.jsonl");
    let record = test_book_record("mexc", "CUDISUSDT", 1.0, 1.01, 10.0, 1);
    append_book_record(path.to_str().unwrap(), &record).unwrap();

    let records = load_book_records(path.to_str().unwrap()).unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].timestamp_local(), record.timestamp_local());
}

#[tokio::test]
async fn replay_should_reconstruct_book_state() {
    let cache = BookCache::default();
    let record = test_book_record("coinex", "CUDISUSDT", 1.03, 1.04, 10.0, 2);
    let cached = record.into_cached();
    cache
        .update_book(cached.into_snapshot(), BookSource::Replay)
        .await;

    let replayed = cache.get_book("coinex", "CUDISUSDT").await.unwrap();
    assert_eq!(replayed.source, BookSource::Replay);
    assert_eq!(replayed.best_bid, Some(1.03));
    assert_eq!(replayed.sequence, Some(2));
}

#[tokio::test]
async fn opportunity_detection_from_replayed_books_should_accept_spread() {
    let cfg = replay_config_with_paths("unused", "unused");
    let cache = BookCache::default();
    cache
        .update_book(
            test_book_record("mexc", "CUDISUSDT", 0.99, 1.0, 200.0, 1)
                .into_cached()
                .into_snapshot(),
            BookSource::Replay,
        )
        .await;
    cache
        .update_book(
            test_book_record("coinex", "CUDISUSDT", 1.03, 1.04, 200.0, 1)
                .into_cached()
                .into_snapshot(),
            BookSource::Replay,
        )
        .await;

    let mexc_book = cache
        .get_book("mexc", "CUDISUSDT")
        .await
        .unwrap()
        .into_snapshot();
    let coinex_book = cache
        .get_book("coinex", "CUDISUSDT")
        .await
        .unwrap()
        .into_snapshot();
    let fee_model = fee_model_from_strategy_config(&cfg);
    let disabled_registry = DisabledRegistry::new();
    let opportunities = detect_opportunities_for_pair_with_source(
        &cfg,
        &rules(),
        &mexc_book,
        &coinex_book,
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        &fee_model,
        &disabled_registry,
        BookSource::Replay,
        BookSource::Replay,
    );
    assert!(opportunities.iter().any(|opportunity| opportunity.accepted));
    assert!(opportunities
        .iter()
        .all(|opportunity| opportunity.buy_book_source == BookSource::Replay));
}

#[test]
fn stale_books_should_be_rejected_in_websocket_cache_mode() {
    let mut cfg = config();
    cfg.market_data_mode = MarketDataMode::WebsocketCache;
    cfg.websocket.enabled = true;
    let mut stale = book("mexc", 0.99, 1.0, 100.0);
    stale.is_stale = true;
    let fee_model = fee_model_from_strategy_config(&cfg);
    let disabled_registry = DisabledRegistry::new();
    let opp = build_opportunity_with_source(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &stale,
        &book("coinex", 1.03, 1.04, 100.0),
        &fee_model,
        &disabled_registry,
        BookSource::Websocket,
        BookSource::Websocket,
    );
    assert_eq!(opp.rejection_reason, Some(RejectionReason::StaleBook));
    assert_eq!(opp.buy_book_source, BookSource::Websocket);
}

#[tokio::test]
async fn replay_mode_should_run_without_network_clients() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("books.jsonl");
    let output = dir.path().join("report.jsonl");
    append_book_record(
        input.to_str().unwrap(),
        &test_book_record("mexc", "CUDISUSDT", 0.99, 1.0, 200.0, 1),
    )
    .unwrap();
    append_book_record(
        input.to_str().unwrap(),
        &test_book_record("coinex", "CUDISUSDT", 1.03, 1.04, 200.0, 2),
    )
    .unwrap();
    let cfg = replay_config_with_paths(input.to_str().unwrap(), output.to_str().unwrap());

    ensure_replay_is_network_free(&cfg).unwrap();
    let report = run_replay_mode(cfg).await.unwrap();
    assert_eq!(report.total_book_events, 2);
    assert!(report.opportunities_detected > 0);
    assert!(output.exists());
}

#[tokio::test]
async fn replay_output_should_be_deterministic_for_same_input() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("books.jsonl");
    append_book_record(
        input.to_str().unwrap(),
        &test_book_record("mexc", "CUDISUSDT", 0.99, 1.0, 200.0, 1),
    )
    .unwrap();
    append_book_record(
        input.to_str().unwrap(),
        &test_book_record("coinex", "CUDISUSDT", 1.03, 1.04, 200.0, 2),
    )
    .unwrap();

    let first_output = dir.path().join("report_1.jsonl");
    let second_output = dir.path().join("report_2.jsonl");
    let first = run_replay_mode(replay_config_with_paths(
        input.to_str().unwrap(),
        first_output.to_str().unwrap(),
    ))
    .await
    .unwrap();
    let second = run_replay_mode(replay_config_with_paths(
        input.to_str().unwrap(),
        second_output.to_str().unwrap(),
    ))
    .await
    .unwrap();

    assert_eq!(first.total_book_events, second.total_book_events);
    assert_eq!(first.opportunities_detected, second.opportunities_detected);
    assert_eq!(first.opportunities_accepted, second.opportunities_accepted);
    assert!((first.simulated_net_pnl - second.simulated_net_pnl).abs() < 1e-9);
}

#[tokio::test]
async fn replay_latency_should_make_opportunity_disappear() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("books.jsonl");
    let output = dir.path().join("report.jsonl");
    let base = Utc::now();
    append_book_record(
        input.to_str().unwrap(),
        &test_book_record_at("mexc", "CUDISUSDT", 0.99, 1.00, 200.0, 1, base),
    )
    .unwrap();
    append_book_record(
        input.to_str().unwrap(),
        &test_book_record_at("coinex", "CUDISUSDT", 1.08, 1.09, 200.0, 2, base),
    )
    .unwrap();
    append_book_record(
        input.to_str().unwrap(),
        &test_book_record_at(
            "coinex",
            "CUDISUSDT",
            0.99,
            1.00,
            200.0,
            3,
            base + Duration::milliseconds(1),
        ),
    )
    .unwrap();

    let mut cfg = replay_config_with_paths(input.to_str().unwrap(), output.to_str().unwrap());
    cfg.max_raw_spread_bps = 2_000.0;
    cfg.replay.speed = "fixed:market=0,decision=0,submit=10,timeout=1000,seed=7".to_string();

    let report = run_replay_mode(cfg).await.unwrap();

    assert!(report.theoretical_opportunities > 0);
    assert!(report.gross_theoretical_pnl > 0.0);
    assert_eq!(report.latency_adjusted_accepted, 0);
    assert_eq!(report.actual_fill_opportunities, 0);
    assert!(report.one_sided_risk_count > 0);
}

#[tokio::test]
async fn replay_ioc_should_report_partial_fill() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("books.jsonl");
    let output = dir.path().join("report.jsonl");
    let base = Utc::now();
    append_book_record(
        input.to_str().unwrap(),
        &test_book_record_at("mexc", "CUDISUSDT", 0.99, 1.00, 200.0, 1, base),
    )
    .unwrap();
    append_book_record(
        input.to_str().unwrap(),
        &test_book_record_at("coinex", "CUDISUSDT", 1.08, 1.09, 200.0, 2, base),
    )
    .unwrap();

    let mut cfg = replay_config_with_paths(input.to_str().unwrap(), output.to_str().unwrap());
    cfg.max_raw_spread_bps = 2_000.0;
    cfg.replay.speed =
        "fixed:market=0,decision=0,submit=0,partial_prob=1,partial_min=0.5,partial_max=0.5,seed=11"
            .to_string();

    let report = run_replay_mode(cfg).await.unwrap();

    assert!(report.gross_theoretical_pnl > 0.0);
    assert!(report.actual_fill_opportunities > 0);
    assert!(report.partial_fill_count > 0);
    assert!(report.latency_adjusted_realized_net_pnl < report.gross_theoretical_pnl);
}

#[tokio::test]
async fn replay_random_model_should_be_stable_with_fixed_seed() {
    let dir = tempdir().unwrap();
    let input = dir.path().join("books.jsonl");
    let base = Utc::now();
    append_book_record(
        input.to_str().unwrap(),
        &test_book_record_at("mexc", "CUDISUSDT", 0.99, 1.00, 200.0, 1, base),
    )
    .unwrap();
    append_book_record(
        input.to_str().unwrap(),
        &test_book_record_at("coinex", "CUDISUSDT", 1.08, 1.09, 200.0, 2, base),
    )
    .unwrap();

    let first_output = dir.path().join("report_seed_1.jsonl");
    let second_output = dir.path().join("report_seed_2.jsonl");
    let mut first_cfg =
        replay_config_with_paths(input.to_str().unwrap(), first_output.to_str().unwrap());
    first_cfg.max_raw_spread_bps = 2_000.0;
    first_cfg.replay.speed = "uniform:market_min=0,market_max=5,decision_min=0,decision_max=3,submit_min=0,submit_max=7,partial_prob=1,partial_min=0.2,partial_max=0.8,seed=99".to_string();
    let mut second_cfg =
        replay_config_with_paths(input.to_str().unwrap(), second_output.to_str().unwrap());
    second_cfg.max_raw_spread_bps = 2_000.0;
    second_cfg.replay.speed = first_cfg.replay.speed.clone();

    let first = run_replay_mode(first_cfg).await.unwrap();
    let second = run_replay_mode(second_cfg).await.unwrap();

    assert_eq!(
        first.latency_adjusted_opportunities,
        second.latency_adjusted_opportunities
    );
    assert_eq!(
        first.actual_fill_opportunities,
        second.actual_fill_opportunities
    );
    assert_eq!(first.partial_fill_count, second.partial_fill_count);
    assert!(
        (first.latency_adjusted_realized_net_pnl - second.latency_adjusted_realized_net_pnl).abs()
            < 1e-9
    );
}

#[test]
fn opportunity_duration_tracker_should_measure_continuity() {
    let mut first = test_opportunity_record(true);
    first.timestamp = Utc::now();
    let mut second = first.clone();
    second.timestamp = first.timestamp + Duration::milliseconds(125);
    second.estimated_net_spread_bps = 50.0;
    second.executable_notional = 75.0;
    let mut rejected = second.clone();
    rejected.timestamp = second.timestamp + Duration::milliseconds(1);
    rejected.accepted = false;
    rejected.rejection_reason = Some(RejectionReason::NetSpreadBelowThreshold);

    let mut tracker = OpportunityDurationTracker::default();
    tracker.observe(&first);
    tracker.observe(&second);
    tracker.observe(&rejected);
    let durations = tracker.finish();

    assert_eq!(durations.len(), 1);
    assert_eq!(durations[0].duration_ms, 125);
    assert_eq!(durations[0].max_net_spread_bps, 50.0);
    assert_eq!(durations[0].max_executable_notional, 75.0);
}

#[tokio::test]
async fn reconnect_should_mark_books_stale() {
    let cache = BookCache::default();
    cache
        .update_book(book("mexc", 0.99, 1.0, 100.0), BookSource::Websocket)
        .await;
    mark_reconnect_stale_for_test(&cache, SpotVenue::Mexc, &["CUDISUSDT".to_string()]).await;

    assert!(!cache.is_fresh("mexc", "CUDISUSDT", 1_000).await);
    assert!(cache.get_book("mexc", "CUDISUSDT").await.unwrap().is_stale);
}

#[test]
fn opportunity_record_should_include_book_age_ms() {
    let cfg = config();
    let mut mexc = book("mexc", 0.99, 1.0, 100.0);
    mexc.received_at = Utc::now() - Duration::milliseconds(25);
    let fee_model = fee_model_from_strategy_config(&cfg);
    let disabled_registry = DisabledRegistry::new();
    let opp = build_opportunity_with_source(
        &cfg,
        &rules(),
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        SpotVenue::Mexc,
        SpotVenue::CoinEx,
        &mexc,
        &book("coinex", 1.03, 1.04, 100.0),
        &fee_model,
        &disabled_registry,
        BookSource::Websocket,
        BookSource::Websocket,
    );

    assert!(opp.buy_book_age_ms >= 25);
    assert!(opp.sell_book_age_ms >= 0);
}

#[test]
fn gateio_bitget_pair_should_build_live_dry_run_opportunities() {
    let mut cfg = config();
    cfg.trading_mode = "live_dry_run".to_string();
    cfg.exchanges = vec!["gateio".to_string(), "bitget".to_string()];
    cfg.websocket.exchanges = cfg.exchanges.clone();
    cfg.initial_balances = HashMap::from([
        (
            "gateio".to_string(),
            HashMap::from([("USDT".to_string(), 500.0), ("CUDIS".to_string(), 10_000.0)]),
        ),
        (
            "bitget".to_string(),
            HashMap::from([("USDT".to_string(), 500.0), ("CUDIS".to_string(), 10_000.0)]),
        ),
    ]);
    let gate_rule = rule("gateio");
    let bitget_rule = rule("bitget");
    let rules = CommonSymbolRules {
        mexc: gate_rule.clone(),
        coinex: bitget_rule.clone(),
        gateio: Some(gate_rule),
        bitget: Some(bitget_rule),
        kucoin: None,
    };
    let gate_book = book("gateio", 0.9999, 1.0000, 1_000.0);
    let bitget_book = book("bitget", 1.0300, 1.0301, 1_000.0);

    let opportunities = detect_opportunities_for_pair_with_source(
        &cfg,
        &rules,
        &gate_book,
        &bitget_book,
        &PaperInventory::from_config(&cfg).unwrap(),
        &RiskState::new(&cfg),
        &fee_model_from_strategy_config(&cfg),
        &DisabledRegistry::new(),
        BookSource::Websocket,
        BookSource::Websocket,
    );

    assert_eq!(opportunities[0].buy_exchange, "gateio");
    assert_eq!(opportunities[0].sell_exchange, "bitget");
    assert!(opportunities[0].accepted);
}

#[test]
fn stopped_control_states_with_residual_inventory_should_replan_liquidation() {
    assert!(control_lifecycle_should_replan_market_liquidation(
        SpotSymbolLifecycleState::DisabledClean
    ));
    assert!(control_lifecycle_should_replan_market_liquidation(
        SpotSymbolLifecycleState::DisabledWithInventory
    ));
    assert!(control_lifecycle_should_replan_market_liquidation(
        SpotSymbolLifecycleState::DustRemaining
    ));
    assert!(!control_lifecycle_should_replan_market_liquidation(
        SpotSymbolLifecycleState::ManualInterventionRequired
    ));
    assert!(!control_lifecycle_should_replan_market_liquidation(
        SpotSymbolLifecycleState::Active
    ));
    assert!(!control_lifecycle_should_replan_market_liquidation(
        SpotSymbolLifecycleState::LiquidationPlanning
    ));
}

#[test]
fn safe_liquidation_quantity_should_leave_balance_buffer() {
    let quantity = safe_liquidation_quantity(0.017982, 0.001);
    assert_eq!(quantity, 0.017);
    assert!(quantity < 0.017982);
}

#[test]
fn residual_base_balance_should_only_liquidate_when_order_is_sellable() {
    let mut rule = rule("gateio");
    rule.base_asset = "WLD".to_string();
    rule.step_size = 0.001;
    rule.min_quantity = 0.01;
    rule.min_notional = 1.0;

    let balances = vec![AssetBalance::new("WLD", 7.5, 7.5, 0.0)];
    assert!(balances_have_liquidatable_base(&balances, &rule, 0.42));

    let dust_balances = vec![AssetBalance::new("WLD", 0.001, 0.001, 0.0)];
    assert!(!balances_have_liquidatable_base(
        &dust_balances,
        &rule,
        0.42
    ));

    let below_notional = vec![AssetBalance::new("WLD", 0.5, 0.5, 0.0)];
    assert!(!balances_have_liquidatable_base(
        &below_notional,
        &rule,
        0.42
    ));
}

fn test_book_record(
    exchange: &str,
    symbol: &str,
    bid: f64,
    ask: f64,
    quantity: f64,
    sequence: u64,
) -> BookRecord {
    test_book_record_at(exchange, symbol, bid, ask, quantity, sequence, Utc::now())
}

fn test_book_record_at(
    exchange: &str,
    symbol: &str,
    bid: f64,
    ask: f64,
    quantity: f64,
    sequence: u64,
    timestamp: chrono::DateTime<Utc>,
) -> BookRecord {
    BookRecord::BookSnapshot {
        timestamp_local: timestamp,
        timestamp_exchange: Some(timestamp),
        exchange: exchange.to_string(),
        symbol: symbol.to_string(),
        bids: vec![OrderBookLevel {
            price: bid,
            quantity,
        }],
        asks: vec![OrderBookLevel {
            price: ask,
            quantity,
        }],
        sequence: Some(sequence),
        source: BookSource::Replay,
        latency_ms: Some(1),
    }
}

fn replay_config_with_paths(input_path: &str, output_path: &str) -> SpotSpotTakerArbitrageConfig {
    let mut cfg = config();
    cfg.market_data_mode = MarketDataMode::Replay;
    cfg.replay.enabled = true;
    cfg.replay.input_path = input_path.to_string();
    cfg.replay.output_path = output_path.to_string();
    cfg.disabled_registry_path = "__missing_replay_test_disabled_registry.yml".to_string();
    cfg.mexc.base_url = "http://127.0.0.1:1".to_string();
    cfg.coinex.base_url = "http://127.0.0.1:1".to_string();
    cfg
}

fn test_opportunity_record(accepted: bool) -> OpportunityRecord {
    OpportunityRecord {
        timestamp: Utc::now(),
        symbol: "CUDISUSDT".to_string(),
        buy_exchange: "mexc".to_string(),
        sell_exchange: "coinex".to_string(),
        buy_price: 1.0,
        sell_price: 1.03,
        raw_spread_bps: 300.0,
        buy_fee_bps: 10.0,
        sell_fee_bps: 10.0,
        fee_source_buy: FeeSource::ConfigDefault,
        fee_source_sell: FeeSource::ConfigDefault,
        platform_discount_applied: false,
        estimated_fee_bps: 20.0,
        estimated_slippage_bps: 2.0,
        safety_buffer_bps: 3.0,
        estimated_net_spread_bps: 25.0,
        estimated_total_fee: 0.1,
        estimated_gross_pnl: 1.5,
        estimated_net_pnl: 1.4,
        capital_cost_bps: 0.0,
        transfer_cost_bps: 0.0,
        transfer_delay_penalty_bps: 0.0,
        inventory_rebalance_cost_bps: 0.0,
        latency_penalty_bps: 0.0,
        effective_min_net_spread_bps: 0.0,
        estimated_slippage_cost: 0.0,
        estimated_capital_cost: 0.0,
        estimated_transfer_cost: 0.0,
        estimated_inventory_rebalance_cost: 0.0,
        estimated_latency_penalty_cost: 0.0,
        estimated_total_cost: 0.1,
        executable_notional: 50.0,
        quantity: 50.0,
        accepted,
        rejection_reason: (!accepted).then_some(RejectionReason::NetSpreadBelowThreshold),
        rejection_detail: None,
        buy_book_age_ms: 1,
        sell_book_age_ms: 1,
        buy_book_source: BookSource::Replay,
        sell_book_source: BookSource::Replay,
        buy_latency_ms: Some(1),
        sell_latency_ms: Some(1),
        lifecycle_latency: None,
    }
}

fn simulated_trade_for_report(net_pnl: f64) -> SimulatedTradeRecord {
    SimulatedTradeRecord {
        timestamp: Utc::now(),
        symbol: "CUDISUSDT".to_string(),
        buy_exchange: "mexc".to_string(),
        sell_exchange: "coinex".to_string(),
        buy_avg_price: 1.0,
        sell_avg_price: 1.03,
        quantity: 10.0,
        notional: 10.0,
        buy_fee: 0.01,
        sell_fee: 0.01,
        gross_pnl: net_pnl + 0.02,
        net_pnl,
        pnl_category: TradePnlCategory::Arbitrage,
        slippage_cost: 0.0,
        capital_cost: 0.0,
        transfer_cost: 0.0,
        inventory_rebalance_cost: 0.0,
        latency_penalty_cost: 0.0,
        latency_ms: 1,
        order_book_age_ms: 1,
        execution_mode: "paper".to_string(),
    }
}
