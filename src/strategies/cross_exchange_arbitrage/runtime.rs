//! Runtime wiring helpers for cross-exchange arbitrage live execution.
//!
//! This module owns strategy-specific adapter construction so binaries do not
//! duplicate exchange credential, position-mode, and private stream wiring.

use super::market_data::{
    build_subscription_plan, normalize_orderbook_snapshot, normalize_symbol, MarketDataBookStore,
};
use super::recorder::OpportunityRecorder;
use super::spread_engine::SpreadEngine;
use super::types::{DetectionMetrics, OpportunityDecision};
use super::{CrossExchangeArbitrageConfig, OpenExecutionStyle, TradingMode};
use crate::core::exchange::Exchange;
use crate::exchanges::private_perp::{PrivatePerpExchange, PrivateWsAuth, PrivateWsRunConfig};
pub use crate::exchanges::registry::{
    build_core_exchange_for_exchange, build_trading_adapter_for_exchange,
    build_trading_adapter_for_exchange_with_instruments, configured_position_mode,
    private_perp_exchange, private_rest_auth_for_exchange, private_ws_auth_for_exchange,
};
use crate::exchanges::unified::{ExchangeClient, OrderBookSnapshot};
use crate::exchanges::{BinanceSpotClient, BinanceSpotConfig, OkxSpotClient, OkxSpotConfig};
use crate::execution::{ExecutionRouter, TradingAdapter};
use crate::market::{
    exchange_symbol_for, CanonicalSymbol, ExchangeId, ExchangeSymbol, InstrumentMeta,
};
use anyhow::{anyhow, Context, Result};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq)]
pub struct PrivateWsRuntimeSpec {
    pub exchange: PrivatePerpExchange,
    pub auth: PrivateWsAuth,
    pub symbols: Vec<ExchangeSymbol>,
    pub instruments: Vec<InstrumentMeta>,
    pub config: PrivateWsRunConfig,
    pub url: String,
}

#[derive(Clone)]
pub struct BinancePrivateWsRuntimeSpec {
    pub exchange: Arc<dyn Exchange>,
    pub config: PrivateWsRunConfig,
}

pub struct CrossArbLiveRuntimeParts {
    pub router: ExecutionRouter,
    pub adapters: Vec<Arc<dyn TradingAdapter>>,
    pub private_ws_specs: Vec<PrivateWsRuntimeSpec>,
    pub binance_private_ws_specs: Vec<BinancePrivateWsRuntimeSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LiveTradingSafetyError {
    #[error("live trading is disabled")]
    LiveTradingDisabled,
    #[error("live trading requires enable_live_trading=true")]
    LiveTradingNotEnabled,
    #[error("max_live_notional_per_trade is missing or invalid")]
    MissingMaxLiveNotional,
    #[error("enabled_symbols is empty")]
    MissingEnabledSymbols,
    #[error("enabled_exchanges is empty")]
    MissingEnabledExchanges,
    #[error("api keys are missing or invalid")]
    MissingApiKeys,
    #[error("exchange connectivity check failed")]
    ExchangeConnectivity,
    #[error("public websocket data is stale")]
    PublicWebSocketStale,
    #[error("private user stream is unhealthy")]
    PrivateStreamUnhealthy,
    #[error("REST order endpoint dry-run check failed")]
    RestOrderDryRun,
    #[error("balances are unavailable")]
    BalancesUnavailable,
    #[error("balance reconciliation failed")]
    BalanceReconciliationFailed,
    #[error("symbol mapping check failed")]
    SymbolMapping,
    #[error("min order size check failed")]
    MinOrderSize,
    #[error("tick size check failed")]
    TickSize,
    #[error("lot size check failed")]
    LotSize,
    #[error("fee model is not loaded")]
    FeeModelNotLoaded,
    #[error("database recorder is unhealthy")]
    RecorderUnhealthy,
    #[error("live trading starts with taker-taker only")]
    MakerFirstDisabled,
    #[error("notional exceeds max_live_notional_per_trade")]
    MaxNotionalExceeded,
    #[error("symbol is not live-enabled")]
    SymbolNotEnabled,
    #[error("exchange is not live-enabled")]
    ExchangeNotEnabled,
    #[error("symbol already has an active live trade")]
    ActiveSymbolTrade,
    #[error("global active trade limit reached")]
    ActiveTradeLimit,
    #[error("kill switch is active")]
    KillSwitch,
    #[error("critical risk event is active")]
    CriticalRiskEvent,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveTradingSafetyReport {
    pub enabled_symbols: Vec<CanonicalSymbol>,
    pub enabled_exchanges: Vec<ExchangeId>,
    pub max_live_notional_per_trade: String,
    pub max_total_active_trades: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LiveTradingPreflightState {
    pub api_keys_valid: bool,
    pub exchange_connectivity_ok: bool,
    pub public_ws_fresh: bool,
    pub private_stream_fresh: bool,
    pub rest_order_dry_run_ok: bool,
    pub balances_loaded: bool,
    pub balance_reconciliation_passed: bool,
    pub symbol_mapping_valid: bool,
    pub min_order_size_valid: bool,
    pub tick_size_valid: bool,
    pub lot_size_valid: bool,
    pub fee_model_loaded: bool,
    pub database_recorder_healthy: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LivePreTradeCheck {
    pub symbol: CanonicalSymbol,
    pub buy_exchange: ExchangeId,
    pub sell_exchange: ExchangeId,
    pub notional_usdt: f64,
    pub active_trades_for_symbol: usize,
    pub active_trades_total: usize,
    pub kill_switch_active: bool,
    pub critical_risk_event_active: bool,
}

pub fn validate_live_trading_preflight(
    config: &CrossExchangeArbitrageConfig,
    state: &LiveTradingPreflightState,
) -> std::result::Result<LiveTradingSafetyReport, LiveTradingSafetyError> {
    if config.trading_mode != TradingMode::Live {
        return Err(LiveTradingSafetyError::LiveTradingDisabled);
    }
    if !config.enable_live_trading {
        return Err(LiveTradingSafetyError::LiveTradingNotEnabled);
    }
    let max_live_notional = config
        .max_live_notional_per_trade
        .filter(|value| value.is_finite() && *value > 0.0)
        .ok_or(LiveTradingSafetyError::MissingMaxLiveNotional)?;
    if config.enabled_symbols.is_empty() {
        return Err(LiveTradingSafetyError::MissingEnabledSymbols);
    }
    if config.enabled_exchanges.is_empty() {
        return Err(LiveTradingSafetyError::MissingEnabledExchanges);
    }
    if config.execution.open_execution_style != OpenExecutionStyle::DualTaker {
        return Err(LiveTradingSafetyError::MakerFirstDisabled);
    }
    for exchange in &config.enabled_exchanges {
        if !config.fees.per_exchange.contains_key(exchange) {
            return Err(LiveTradingSafetyError::FeeModelNotLoaded);
        }
    }
    if !state.api_keys_valid {
        return Err(LiveTradingSafetyError::MissingApiKeys);
    }
    if !state.exchange_connectivity_ok {
        return Err(LiveTradingSafetyError::ExchangeConnectivity);
    }
    if !state.public_ws_fresh {
        return Err(LiveTradingSafetyError::PublicWebSocketStale);
    }
    if !state.private_stream_fresh {
        return Err(LiveTradingSafetyError::PrivateStreamUnhealthy);
    }
    if !state.rest_order_dry_run_ok {
        return Err(LiveTradingSafetyError::RestOrderDryRun);
    }
    if !state.balances_loaded {
        return Err(LiveTradingSafetyError::BalancesUnavailable);
    }
    if !state.balance_reconciliation_passed {
        return Err(LiveTradingSafetyError::BalanceReconciliationFailed);
    }
    if !state.symbol_mapping_valid {
        return Err(LiveTradingSafetyError::SymbolMapping);
    }
    if !state.min_order_size_valid {
        return Err(LiveTradingSafetyError::MinOrderSize);
    }
    if !state.tick_size_valid {
        return Err(LiveTradingSafetyError::TickSize);
    }
    if !state.lot_size_valid {
        return Err(LiveTradingSafetyError::LotSize);
    }
    if !state.fee_model_loaded {
        return Err(LiveTradingSafetyError::FeeModelNotLoaded);
    }
    if !state.database_recorder_healthy {
        return Err(LiveTradingSafetyError::RecorderUnhealthy);
    }
    log_live_safety_startup(config, max_live_notional);
    Ok(LiveTradingSafetyReport {
        enabled_symbols: config.enabled_symbols.clone(),
        enabled_exchanges: config.enabled_exchanges.clone(),
        max_live_notional_per_trade: format!("{max_live_notional:.8}"),
        max_total_active_trades: config.risk.max_open_positions,
    })
}

pub fn evaluate_live_pre_trade_safety(
    config: &CrossExchangeArbitrageConfig,
    state: &LiveTradingPreflightState,
    check: &LivePreTradeCheck,
) -> std::result::Result<(), LiveTradingSafetyError> {
    validate_live_trading_preflight(config, state)?;
    log::info!(
        "cross-arb live pre-trade risk check symbol={} buy_exchange={} sell_exchange={} notional={} active_symbol={} active_total={} kill_switch={} critical_risk={}",
        check.symbol,
        check.buy_exchange,
        check.sell_exchange,
        check.notional_usdt,
        check.active_trades_for_symbol,
        check.active_trades_total,
        check.kill_switch_active,
        check.critical_risk_event_active
    );
    if check.kill_switch_active {
        return Err(LiveTradingSafetyError::KillSwitch);
    }
    if check.critical_risk_event_active {
        return Err(LiveTradingSafetyError::CriticalRiskEvent);
    }
    if !config.enabled_symbols.contains(&check.symbol) {
        return Err(LiveTradingSafetyError::SymbolNotEnabled);
    }
    if !config.enabled_exchanges.contains(&check.buy_exchange)
        || !config.enabled_exchanges.contains(&check.sell_exchange)
    {
        return Err(LiveTradingSafetyError::ExchangeNotEnabled);
    }
    if check.notional_usdt
        > config
            .max_live_notional_per_trade
            .expect("preflight checked max_live_notional_per_trade")
    {
        return Err(LiveTradingSafetyError::MaxNotionalExceeded);
    }
    if check.active_trades_for_symbol > 0 {
        return Err(LiveTradingSafetyError::ActiveSymbolTrade);
    }
    if check.active_trades_total >= config.risk.max_open_positions {
        return Err(LiveTradingSafetyError::ActiveTradeLimit);
    }
    Ok(())
}

fn log_live_safety_startup(config: &CrossExchangeArbitrageConfig, max_live_notional: f64) {
    log::warn!(
        "cross-arb live safety limits trading_mode={:?} enable_live_trading={} max_live_notional_per_trade={} enabled_symbols={:?} enabled_exchanges={:?} max_open_positions={} max_total_notional={} max_daily_loss={} max_drawdown={} taker_ioc_slippage_limit_pct={}",
        config.trading_mode,
        config.enable_live_trading,
        max_live_notional,
        config.enabled_symbols,
        config.enabled_exchanges,
        config.risk.max_open_positions,
        config.risk.max_total_notional_usdt,
        config.risk.max_daily_loss_usdt,
        config.risk.max_drawdown_usdt,
        config.execution.taker_ioc_slippage_limit_pct
    );
}

pub async fn run_cross_exchange_arbitrage_detection_only(
    config: CrossExchangeArbitrageConfig,
) -> Result<()> {
    config
        .validate()
        .context("invalid cross-exchange arbitrage config")?;
    if config.mode.allows_live_orders() {
        return Err(anyhow!(
            "cross_exchange_arbitrage CLI path is detection-only; live modes are disabled"
        ));
    }
    if !config.execution.dry_run {
        return Err(anyhow!(
            "cross_exchange_arbitrage detection requires execution.dry_run=true"
        ));
    }
    if !config.detection.enabled {
        log::warn!("cross_exchange_arbitrage detection is disabled by config");
        return futures_util::future::pending::<Result<()>>().await;
    }

    let plan = build_subscription_plan(&config);
    for subscription in &plan {
        log::info!(
            "cross_exchange_arbitrage subscribing public spot books: exchange={} symbols={:?}",
            subscription.exchange,
            subscription.symbols
        );
    }
    if config.detection.paper_trading.enabled {
        log::info!(
            "paper trading flag is enabled for future execution simulation; live orders remain disabled"
        );
    }

    let (tx, mut rx) = mpsc::channel::<(ExchangeId, OrderBookSnapshot)>(4096);
    start_spot_orderbook_subscriptions(&plan, tx).await?;

    let mut store = MarketDataBookStore::new();
    let engine = SpreadEngine::from_config(&config);
    let mut recorder = OpportunityRecorder::new(config.detection.recorder.clone());
    let mut metrics = DetectionMetrics::default();
    let symbols = config
        .detection
        .symbols
        .iter()
        .filter_map(|symbol| normalize_symbol(symbol))
        .collect::<Vec<_>>();

    log::info!(
        "cross_exchange_arbitrage detection started: exchanges={:?} symbols={:?} min_net_spread_bps={} live_orders=false",
        config.detection.exchanges,
        config.detection.symbols,
        config.detection.min_net_spread_bps
    );

    while let Some((exchange, snapshot)) = rx.recv().await {
        metrics.observe_book();
        let Some(book) = normalize_orderbook_snapshot(
            exchange.clone(),
            snapshot,
            &config.detection.symbol_mappings,
        ) else {
            log::warn!("ignored unmapped order book from {}", exchange);
            continue;
        };
        let symbol = book.symbol.clone();
        store.update(book);
        for opportunity in engine.scan_symbol(
            &store,
            &symbol,
            &config.detection.exchanges,
            chrono::Utc::now(),
        ) {
            if opportunity.decision == OpportunityDecision::Rejected
                && !config.detection.record_rejected
            {
                continue;
            }
            if opportunity.accepted() {
                log::info!(
                    "cross_exchange_arbitrage opportunity accepted symbol={} buy={}@{} sell={}@{} net_bps={:.4} notional={:.2}",
                    opportunity.symbol,
                    opportunity.buy_exchange,
                    opportunity.buy_price,
                    opportunity.sell_exchange,
                    opportunity.sell_price,
                    opportunity.estimated_net_spread_bps,
                    opportunity.estimated_notional
                );
            } else {
                log::debug!(
                    "cross_exchange_arbitrage opportunity rejected symbol={} buy={} sell={} net_bps={:.4} reason={}",
                    opportunity.symbol,
                    opportunity.buy_exchange,
                    opportunity.sell_exchange,
                    opportunity.estimated_net_spread_bps,
                    opportunity.reason
                );
            }
            recorder
                .record(opportunity)
                .context("failed to record cross-exchange arbitrage opportunity")?;
        }
        log::trace!(
            "cross_exchange_arbitrage metrics books={} recorded={} accepted={} rejected={} symbols={}",
            metrics.books_received,
            recorder.metrics().opportunities_recorded,
            recorder.metrics().opportunities_accepted,
            recorder.metrics().opportunities_rejected,
            symbols.len()
        );
    }

    Ok(())
}

async fn start_spot_orderbook_subscriptions(
    plan: &[super::market_data::MarketDataSubscription],
    tx: mpsc::Sender<(ExchangeId, OrderBookSnapshot)>,
) -> Result<()> {
    for subscription in plan {
        match subscription.exchange {
            ExchangeId::Binance => {
                let client = BinanceSpotClient::new(BinanceSpotConfig::default());
                let mut rx = client
                    .subscribe_orderbook(subscription.symbols.clone())
                    .await
                    .context("failed to subscribe Binance Spot order book")?;
                let tx = tx.clone();
                tokio::spawn(async move {
                    while let Some(book) = rx.recv().await {
                        if tx.send((ExchangeId::Binance, book)).await.is_err() {
                            break;
                        }
                    }
                });
            }
            ExchangeId::Okx => {
                let client = OkxSpotClient::new(OkxSpotConfig::default());
                let mut rx = client
                    .subscribe_orderbook(subscription.symbols.clone())
                    .await
                    .context("failed to subscribe OKX Spot order book")?;
                let tx = tx.clone();
                tokio::spawn(async move {
                    while let Some(book) = rx.recv().await {
                        if tx.send((ExchangeId::Okx, book)).await.is_err() {
                            break;
                        }
                    }
                });
            }
            ref other => {
                log::warn!(
                    "cross_exchange_arbitrage detection currently skips unsupported spot exchange {}",
                    other
                );
            }
        }
    }
    Ok(())
}

pub fn build_cross_arb_execution_router(
    config: &CrossExchangeArbitrageConfig,
) -> Result<ExecutionRouter> {
    build_cross_arb_execution_router_with_instruments(config, Vec::new())
}

pub fn build_cross_arb_execution_router_with_instruments(
    config: &CrossExchangeArbitrageConfig,
    instruments: impl IntoIterator<Item = InstrumentMeta>,
) -> Result<ExecutionRouter> {
    let instruments = instruments.into_iter().collect::<Vec<_>>();
    let mut router = ExecutionRouter::new(config.execution.dry_run);
    for exchange in live_enabled_exchanges(config) {
        let adapter = build_trading_adapter_for_exchange_with_instruments(
            config,
            &exchange,
            instruments
                .iter()
                .filter(|item| item.exchange == exchange)
                .cloned(),
        )?;
        router.register_adapter(adapter);
    }
    Ok(router)
}

pub fn build_cross_arb_live_runtime_parts(
    config: &CrossExchangeArbitrageConfig,
    instruments: impl IntoIterator<Item = InstrumentMeta>,
) -> Result<CrossArbLiveRuntimeParts> {
    let instruments = instruments.into_iter().collect::<Vec<_>>();
    let mut router = ExecutionRouter::new(config.execution.dry_run);
    let mut adapters = Vec::new();
    let mut binance_private_ws_specs = Vec::new();
    for exchange in live_enabled_exchanges(config) {
        let adapter = build_trading_adapter_for_exchange_with_instruments(
            config,
            &exchange,
            instruments
                .iter()
                .filter(|item| item.exchange == exchange)
                .cloned(),
        )?;
        router.register_adapter(adapter.clone());
        adapters.push(adapter);
        if exchange == ExchangeId::Binance
            && config
                .exchanges
                .get(&exchange)
                .map(|runtime| runtime.private_ws_enabled)
                .unwrap_or(false)
        {
            binance_private_ws_specs.push(BinancePrivateWsRuntimeSpec {
                exchange: build_core_exchange_for_exchange(config, &exchange)?,
                config: config
                    .exchanges
                    .get(&exchange)
                    .map(|runtime| runtime.private_ws_run.clone())
                    .unwrap_or_default()
                    .into(),
            });
        }
    }
    let private_ws_specs = build_private_ws_runtime_specs(
        config,
        live_enabled_exchanges(config).into_iter().map(|exchange| {
            let ws_instruments = instruments
                .iter()
                .filter(|item| item.exchange == exchange)
                .cloned()
                .collect::<Vec<_>>();
            let symbols = private_ws_symbols_for_exchange(config, &exchange, &ws_instruments);
            (exchange, symbols, ws_instruments)
        }),
    )?;
    Ok(CrossArbLiveRuntimeParts {
        router,
        adapters,
        private_ws_specs,
        binance_private_ws_specs,
    })
}

fn private_ws_symbols_for_exchange(
    config: &CrossExchangeArbitrageConfig,
    exchange: &ExchangeId,
    instruments: &[InstrumentMeta],
) -> Vec<ExchangeSymbol> {
    let universe = config
        .universe
        .symbols
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    let mut seen = HashSet::new();
    let mut symbols = instruments
        .iter()
        .filter(|instrument| instrument.exchange == *exchange)
        .filter(|instrument| instrument.status.allows_closes())
        .filter(|instrument| universe.contains(&instrument.canonical_symbol))
        .filter(|instrument| seen.insert(instrument.exchange_symbol.clone()))
        .map(|instrument| instrument.exchange_symbol.clone())
        .collect::<Vec<_>>();
    if symbols.is_empty() {
        symbols = config
            .universe
            .symbols
            .iter()
            .map(|symbol| exchange_symbol_for(exchange, symbol))
            .filter(|symbol| seen.insert(symbol.clone()))
            .collect();
    }
    symbols
}

pub fn build_private_ws_runtime_specs(
    config: &CrossExchangeArbitrageConfig,
    symbols_by_exchange: impl IntoIterator<
        Item = (ExchangeId, Vec<ExchangeSymbol>, Vec<InstrumentMeta>),
    >,
) -> Result<Vec<PrivateWsRuntimeSpec>> {
    let mut specs = Vec::new();
    for (exchange, symbols, instruments) in symbols_by_exchange {
        let Some(private_exchange) = private_perp_exchange(&exchange) else {
            continue;
        };
        let runtime = config.exchanges.get(&exchange);
        if !runtime
            .map(|runtime| runtime.private_ws_enabled)
            .unwrap_or(false)
        {
            continue;
        }
        specs.push(PrivateWsRuntimeSpec {
            exchange: private_exchange,
            auth: private_ws_auth_for_exchange(config, &exchange)?,
            symbols,
            instruments,
            config: runtime
                .map(|runtime| runtime.private_ws_run.clone())
                .unwrap_or_default()
                .into(),
            url: runtime
                .and_then(|runtime| runtime.private_ws_url.clone())
                .unwrap_or_else(|| private_exchange.private_ws_url().to_string()),
        });
    }
    Ok(specs)
}

pub fn live_enabled_exchanges(config: &CrossExchangeArbitrageConfig) -> Vec<ExchangeId> {
    let configured =
        if config.trading_mode == TradingMode::Live && !config.enabled_exchanges.is_empty() {
            &config.enabled_exchanges
        } else {
            &config.universe.enabled_exchanges
        };
    configured
        .iter()
        .filter(|exchange| {
            config
                .exchanges
                .get(*exchange)
                .map(|runtime| !runtime.is_disabled())
                .unwrap_or(true)
        })
        .cloned()
        .collect()
}

pub fn exchange_route_status(
    config: &CrossExchangeArbitrageConfig,
    exchange: &ExchangeId,
) -> crate::market::RouteStatus {
    config
        .exchanges
        .get(exchange)
        .map(|runtime| runtime.route_status())
        .unwrap_or(crate::market::RouteStatus::Healthy)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cross_exchange_arbitrage::ExchangeOperatingMode;
    use crate::execution::PositionMode;
    use crate::market::{CanonicalSymbol, ContractType, ExchangeSymbol, InstrumentStatus};

    #[test]
    fn runtime_should_filter_disabled_exchanges() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config
            .exchanges
            .entry(ExchangeId::Bitget)
            .or_default()
            .enabled = Some(false);

        let exchanges = live_enabled_exchanges(&config);

        assert!(exchanges.contains(&ExchangeId::Binance));
        assert!(!exchanges.contains(&ExchangeId::Bitget));
    }

    #[test]
    fn runtime_should_keep_close_only_exchanges_attached() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config
            .exchanges
            .entry(ExchangeId::Bitget)
            .or_default()
            .operating_mode = ExchangeOperatingMode::CloseOnly;

        let exchanges = live_enabled_exchanges(&config);

        assert!(exchanges.contains(&ExchangeId::Bitget));
        assert_eq!(
            exchange_route_status(&config, &ExchangeId::Bitget),
            crate::market::RouteStatus::CloseOnly
        );
    }

    #[test]
    fn live_runtime_should_use_explicit_enabled_exchange_allowlist() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.trading_mode = TradingMode::Live;
        config.universe.enabled_exchanges = vec![ExchangeId::Binance, ExchangeId::Okx];
        config.enabled_exchanges = vec![ExchangeId::Binance];

        let exchanges = live_enabled_exchanges(&config);

        assert_eq!(exchanges, vec![ExchangeId::Binance]);
    }

    #[test]
    fn runtime_should_parse_position_mode_for_private_adapter() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config
            .exchanges
            .entry(ExchangeId::Bitget)
            .or_default()
            .position_mode = Some("hedge".to_string());

        assert_eq!(
            configured_position_mode(&config, &ExchangeId::Bitget),
            PositionMode::Hedge
        );
        assert_eq!(
            configured_position_mode(&config, &ExchangeId::Gate),
            PositionMode::OneWay
        );

        config
            .exchanges
            .entry(ExchangeId::Gate)
            .or_default()
            .position_mode = Some("hedge".to_string());
        assert_eq!(
            configured_position_mode(&config, &ExchangeId::Gate),
            PositionMode::Hedge
        );
    }

    #[test]
    fn runtime_should_build_private_ws_specs_from_config() {
        let mut config = CrossExchangeArbitrageConfig::default();
        let gate = config.exchanges.entry(ExchangeId::Gate).or_default();
        gate.private_ws_enabled = true;
        gate.account_id = Some("20011".to_string());
        gate.env_prefix = Some("GATE_TEST_RUNTIME".to_string());
        gate.demo_trading = true;
        gate.private_ws_url = Some("wss://ws-testnet.gate.com/v4/ws/futures/usdt".to_string());
        std::env::set_var("GATE_TEST_RUNTIME_API_KEY", "key");
        std::env::set_var("GATE_TEST_RUNTIME_API_SECRET", "secret");

        let specs = build_private_ws_runtime_specs(
            &config,
            [(
                ExchangeId::Gate,
                vec![ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT")],
                Vec::new(),
            )],
        )
        .unwrap();

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].exchange, PrivatePerpExchange::Gate);
        assert_eq!(specs[0].auth.account_id.as_deref(), Some("20011"));
        assert!(specs[0].auth.demo_trading);
        assert_eq!(specs[0].url, "wss://ws-testnet.gate.com/v4/ws/futures/usdt");

        std::env::remove_var("GATE_TEST_RUNTIME_API_KEY");
        std::env::remove_var("GATE_TEST_RUNTIME_API_SECRET");
    }

    #[test]
    fn runtime_should_build_private_rest_auth_with_env_prefix() {
        let mut config = CrossExchangeArbitrageConfig::default();
        let bitget = config.exchanges.entry(ExchangeId::Bitget).or_default();
        bitget.env_prefix = Some("BITGET_TEST_RUNTIME".to_string());
        bitget.demo_trading = true;
        std::env::set_var("BITGET_TEST_RUNTIME_API_KEY", "key");
        std::env::set_var("BITGET_TEST_RUNTIME_API_SECRET", "secret");
        std::env::set_var("BITGET_TEST_RUNTIME_PASSPHRASE", "pass");

        let auth = private_rest_auth_for_exchange(&config, &ExchangeId::Bitget).unwrap();

        assert_eq!(auth.api_key, "key");
        assert_eq!(auth.passphrase.as_deref(), Some("pass"));
        assert!(auth.demo_trading);

        std::env::remove_var("BITGET_TEST_RUNTIME_API_KEY");
        std::env::remove_var("BITGET_TEST_RUNTIME_API_SECRET");
        std::env::remove_var("BITGET_TEST_RUNTIME_PASSPHRASE");
    }

    #[test]
    fn runtime_should_register_private_adapter_with_symbol_rules() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.universe.enabled_exchanges = vec![ExchangeId::Bitget];
        let bitget = config.exchanges.entry(ExchangeId::Bitget).or_default();
        bitget.enabled = Some(true);
        bitget.env_prefix = Some("BITGET_ROUTER_TEST".to_string());
        std::env::set_var("BITGET_ROUTER_TEST_API_KEY", "key");
        std::env::set_var("BITGET_ROUTER_TEST_API_SECRET", "secret");
        std::env::set_var("BITGET_ROUTER_TEST_PASSPHRASE", "pass");

        let router = build_cross_arb_execution_router_with_instruments(
            &config,
            [InstrumentMeta::new(
                ExchangeId::Bitget,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
                "BTC",
                "USDT",
                "USDT",
                ContractType::LinearPerpetual,
                1.0,
                0.1,
                0.001,
                0.001,
                10.0,
                1,
                3,
                InstrumentStatus::Trading,
            )],
        )
        .unwrap();

        assert!(router.has_adapter(&ExchangeId::Bitget));

        std::env::remove_var("BITGET_ROUTER_TEST_API_KEY");
        std::env::remove_var("BITGET_ROUTER_TEST_API_SECRET");
        std::env::remove_var("BITGET_ROUTER_TEST_PASSPHRASE");
    }

    #[test]
    fn runtime_should_build_live_parts_with_router_adapters_and_private_ws_specs() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.universe.enabled_exchanges =
            vec![ExchangeId::Binance, ExchangeId::Bitget, ExchangeId::Gate];
        config.universe.symbols = vec![CanonicalSymbol::new("BTC", "USDT")];
        let binance = config.exchanges.entry(ExchangeId::Binance).or_default();
        binance.enabled = Some(true);
        binance.private_ws_enabled = true;
        binance.env_prefix = Some("BINANCE_LIVE_PARTS_TEST".to_string());
        let bitget = config.exchanges.entry(ExchangeId::Bitget).or_default();
        bitget.enabled = Some(true);
        bitget.private_ws_enabled = true;
        bitget.env_prefix = Some("BITGET_LIVE_PARTS_TEST".to_string());
        let gate = config.exchanges.entry(ExchangeId::Gate).or_default();
        gate.enabled = Some(true);
        gate.private_ws_enabled = true;
        gate.account_id = Some("20011".to_string());
        gate.env_prefix = Some("GATE_LIVE_PARTS_TEST".to_string());
        std::env::set_var("BITGET_LIVE_PARTS_TEST_API_KEY", "key");
        std::env::set_var("BITGET_LIVE_PARTS_TEST_API_SECRET", "secret");
        std::env::set_var("BITGET_LIVE_PARTS_TEST_PASSPHRASE", "pass");
        std::env::set_var("GATE_LIVE_PARTS_TEST_API_KEY", "key");
        std::env::set_var("GATE_LIVE_PARTS_TEST_API_SECRET", "secret");
        std::env::set_var("BINANCE_LIVE_PARTS_TEST_API_KEY", "key");
        std::env::set_var("BINANCE_LIVE_PARTS_TEST_API_SECRET", "secret");

        let parts = build_cross_arb_live_runtime_parts(&config, Vec::new()).unwrap();

        assert!(parts.router.has_adapter(&ExchangeId::Binance));
        assert!(parts.router.has_adapter(&ExchangeId::Bitget));
        assert!(parts.router.has_adapter(&ExchangeId::Gate));
        assert_eq!(parts.adapters.len(), 3);
        assert_eq!(parts.private_ws_specs.len(), 2);
        assert_eq!(parts.binance_private_ws_specs.len(), 1);

        std::env::remove_var("BITGET_LIVE_PARTS_TEST_API_KEY");
        std::env::remove_var("BITGET_LIVE_PARTS_TEST_API_SECRET");
        std::env::remove_var("BITGET_LIVE_PARTS_TEST_PASSPHRASE");
        std::env::remove_var("GATE_LIVE_PARTS_TEST_API_KEY");
        std::env::remove_var("GATE_LIVE_PARTS_TEST_API_SECRET");
        std::env::remove_var("BINANCE_LIVE_PARTS_TEST_API_KEY");
        std::env::remove_var("BINANCE_LIVE_PARTS_TEST_API_SECRET");
    }

    #[test]
    fn runtime_should_filter_private_ws_symbols_to_loaded_instruments() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.universe.enabled_exchanges = vec![ExchangeId::Gate];
        config.universe.symbols = vec![
            crate::market::CanonicalSymbol::new("BTC", "USDT"),
            crate::market::CanonicalSymbol::new("FAKE", "USDT"),
        ];
        let gate = config.exchanges.entry(ExchangeId::Gate).or_default();
        gate.enabled = Some(true);
        gate.private_ws_enabled = true;
        gate.account_id = Some("20011".to_string());
        gate.env_prefix = Some("GATE_FILTER_TEST".to_string());
        std::env::set_var("GATE_FILTER_TEST_API_KEY", "key");
        std::env::set_var("GATE_FILTER_TEST_API_SECRET", "secret");
        let instruments = vec![crate::market::InstrumentMeta::new(
            ExchangeId::Gate,
            crate::market::CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
            "BTC",
            "USDT",
            "USDT",
            crate::market::ContractType::LinearPerpetual,
            1.0,
            0.1,
            0.001,
            0.001,
            5.0,
            1,
            3,
            crate::market::InstrumentStatus::Trading,
        )];

        let parts = build_cross_arb_live_runtime_parts(&config, instruments).unwrap();

        assert_eq!(parts.private_ws_specs.len(), 1);
        assert_eq!(
            parts.private_ws_specs[0].symbols,
            vec![ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT")]
        );

        std::env::remove_var("GATE_FILTER_TEST_API_KEY");
        std::env::remove_var("GATE_FILTER_TEST_API_SECRET");
    }

    #[test]
    fn runtime_should_map_exchange_symbols_for_ws_specs() {
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let exchange_symbol = crate::market::exchange_symbol_for(&ExchangeId::Gate, &symbol);

        assert_eq!(exchange_symbol.symbol, "BTC_USDT");
    }

    fn live_config() -> CrossExchangeArbitrageConfig {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.mode = crate::market::RuntimeMode::LiveSmall;
        config.strategy.mode = Some(crate::market::RuntimeMode::LiveSmall);
        config.trading_mode = TradingMode::Live;
        config.enable_live_trading = true;
        config.max_live_notional_per_trade = Some(100.0);
        config.enabled_symbols = vec![CanonicalSymbol::new("BTC", "USDT")];
        config.enabled_exchanges = vec![ExchangeId::Binance, ExchangeId::Okx];
        config.universe.enabled_exchanges = config.enabled_exchanges.clone();
        config.universe.symbols = config.enabled_symbols.clone();
        config.execution.open_execution_style = OpenExecutionStyle::DualTaker;
        for exchange in config.universe.enabled_exchanges.clone() {
            let runtime = config.exchanges.entry(exchange).or_default();
            runtime.enabled = Some(true);
            runtime.private_rest_enabled = true;
            runtime.private_ws_enabled = true;
        }
        config
    }

    fn healthy_preflight() -> LiveTradingPreflightState {
        LiveTradingPreflightState {
            api_keys_valid: true,
            exchange_connectivity_ok: true,
            public_ws_fresh: true,
            private_stream_fresh: true,
            rest_order_dry_run_ok: true,
            balances_loaded: true,
            balance_reconciliation_passed: true,
            symbol_mapping_valid: true,
            min_order_size_valid: true,
            tick_size_valid: true,
            lot_size_valid: true,
            fee_model_loaded: true,
            database_recorder_healthy: true,
        }
    }

    fn pre_trade(notional_usdt: f64) -> LivePreTradeCheck {
        LivePreTradeCheck {
            symbol: CanonicalSymbol::new("BTC", "USDT"),
            buy_exchange: ExchangeId::Binance,
            sell_exchange: ExchangeId::Okx,
            notional_usdt,
            active_trades_for_symbol: 0,
            active_trades_total: 0,
            kill_switch_active: false,
            critical_risk_event_active: false,
        }
    }

    #[test]
    fn live_mode_should_be_disabled_by_default() {
        let config = CrossExchangeArbitrageConfig::default();

        assert_eq!(config.trading_mode, TradingMode::Paper);
        assert!(!config.enable_live_trading);
        assert_eq!(
            validate_live_trading_preflight(&config, &healthy_preflight()).unwrap_err(),
            LiveTradingSafetyError::LiveTradingDisabled
        );
    }

    #[test]
    fn missing_api_keys_should_prevent_live_startup() {
        let config = live_config();
        let mut state = healthy_preflight();
        state.api_keys_valid = false;

        assert_eq!(
            validate_live_trading_preflight(&config, &state).unwrap_err(),
            LiveTradingSafetyError::MissingApiKeys
        );
    }

    #[test]
    fn unhealthy_private_stream_should_prevent_live_startup() {
        let config = live_config();
        let mut state = healthy_preflight();
        state.private_stream_fresh = false;

        assert_eq!(
            validate_live_trading_preflight(&config, &state).unwrap_err(),
            LiveTradingSafetyError::PrivateStreamUnhealthy
        );
    }

    #[test]
    fn balance_mismatch_should_prevent_live_startup() {
        let config = live_config();
        let mut state = healthy_preflight();
        state.balance_reconciliation_passed = false;

        assert_eq!(
            validate_live_trading_preflight(&config, &state).unwrap_err(),
            LiveTradingSafetyError::BalanceReconciliationFailed
        );
    }

    #[test]
    fn max_notional_should_be_enforced_before_live_order() {
        let config = live_config();

        assert_eq!(
            evaluate_live_pre_trade_safety(&config, &healthy_preflight(), &pre_trade(101.0))
                .unwrap_err(),
            LiveTradingSafetyError::MaxNotionalExceeded
        );
    }

    #[test]
    fn kill_switch_should_block_new_live_orders() {
        let config = live_config();
        let mut check = pre_trade(50.0);
        check.kill_switch_active = true;

        assert_eq!(
            evaluate_live_pre_trade_safety(&config, &healthy_preflight(), &check).unwrap_err(),
            LiveTradingSafetyError::KillSwitch
        );
    }
}
