use std::future::Future;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use rustcta::core::config::{ApiKeys, Config as CoreExchangeConfig};
use rustcta::core::exchange::Exchange;
use rustcta::core::types::MarketType;
use rustcta::exchanges::adapters::{
    private_trading_support_for, BinanceMarketAdapter, BitgetMarketAdapter, BybitMarketAdapter,
    GateMarketAdapter, HtxMarketAdapter, MexcMarketAdapter, OkxMarketAdapter,
};
use rustcta::exchanges::{BinanceExchange, OkxExchange};
use rustcta::execution::FillQuery;
use rustcta::market::{exchange_symbol_for, ExchangeId, MarketDataAdapter, RuntimeMode};
use rustcta::strategies::cross_exchange_arbitrage::{
    build_trading_adapter_for_exchange, live_enabled_exchanges, CrossExchangeArbitrageConfig,
};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use tokio::time::{timeout, Duration};

#[derive(Parser, Debug)]
#[command(
    name = "cross_arb_preflight",
    version,
    about = "Safe preflight checks before enabling cross-exchange arbitrage live orders"
)]
struct Args {
    #[arg(long, default_value = "config/cross_exchange_arbitrage_usdt.yml")]
    config: PathBuf,
    #[arg(long, alias = "private-readonly", default_value_t = false)]
    private: bool,
    #[arg(long, default_value_t = 5_000)]
    timeout_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum CheckStatus {
    Pass,
    Warn,
    Fail,
    Skipped,
}

#[derive(Debug, Serialize)]
struct PreflightReport {
    generated_at: DateTime<Utc>,
    config_path: String,
    configured_mode: RuntimeMode,
    private_checks_requested: bool,
    private_readonly_required_for_live: bool,
    live_orders_allowed_by_mode: bool,
    live_ready: bool,
    blocking_reasons: Vec<String>,
    overall_status: CheckStatus,
    checks: Vec<PreflightCheck>,
}

#[derive(Debug, Serialize)]
struct PreflightCheck {
    scope: String,
    status: CheckStatus,
    latency_ms: Option<u128>,
    message: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config = load_config(&args.config)?;
    let mut checks = Vec::new();

    match config.validate() {
        Ok(()) => checks.push(pass("config.validate", "cross-arb config validates")),
        Err(err) => checks.push(fail("config.validate", err.to_string())),
    }

    checks.push(
        if config.mode.allows_live_orders() && config.execution.dry_run {
            warn(
                "execution.dry_run",
                "live-capable mode is configured, but execution.dry_run remains true",
            )
        } else if config.mode.allows_live_orders() {
            warn(
                "runtime.mode",
                "live-capable mode is configured; use LiveSmall before LiveScaled",
            )
        } else {
            pass("runtime.mode", "mode does not allow live orders")
        },
    );

    let mut coverage_by_symbol: HashMap<String, HashSet<ExchangeId>> = HashMap::new();
    let active_exchanges = live_enabled_exchanges(&config);
    for exchange in &active_exchanges {
        match market_adapter(exchange) {
            Some(adapter) => {
                checks.extend(
                    check_public_market_adapter(
                        adapter.as_ref(),
                        &config,
                        args.timeout_ms,
                        &mut coverage_by_symbol,
                    )
                    .await,
                );
            }
            None => checks.push(fail(
                format!("{}.market_adapter", exchange.as_str()),
                "no public market adapter is registered",
            )),
        }

        checks.push(check_private_trading_support(exchange));

        if args.private {
            checks.extend(check_private_exchange(exchange, &config, args.timeout_ms).await);
        } else {
            checks.push(skipped(
                format!("{}.private", exchange.as_str()),
                "private checks skipped; pass --private to read account state",
            ));
        }
    }

    for exchange in config
        .universe
        .enabled_exchanges
        .iter()
        .filter(|exchange| !active_exchanges.contains(exchange))
    {
        checks.push(warn(
            format!("{}.runtime.disabled", exchange.as_str()),
            "exchange is present in universe.enabled_exchanges but disabled in exchanges.<venue>; runtime will not register it",
        ));
    }

    for symbol in &config.universe.symbols {
        let venues = coverage_by_symbol
            .get(&symbol.to_string())
            .map(HashSet::len)
            .unwrap_or_default();
        if venues >= config.market.min_common_exchanges {
            checks.push(pass(
                format!("universe.{}.coverage", symbol),
                format!("available on {venues} exchange(s)"),
            ));
        } else {
            checks.push(fail(
                format!("universe.{}.coverage", symbol),
                format!(
                    "available on {venues} exchange(s), requires at least {}",
                    config.market.min_common_exchanges
                ),
            ));
        }
    }

    let overall_status = overall_status(&checks);
    let live_gate = evaluate_live_gate(&config, args.private, &checks);
    let report = PreflightReport {
        generated_at: Utc::now(),
        config_path: args.config.display().to_string(),
        configured_mode: config.mode,
        private_checks_requested: args.private,
        private_readonly_required_for_live: live_gate.private_readonly_required_for_live,
        live_orders_allowed_by_mode: config.mode.allows_live_orders(),
        live_ready: live_gate.live_ready,
        blocking_reasons: live_gate.blocking_reasons,
        overall_status,
        checks,
    };

    println!("{}", serde_json::to_string_pretty(&report)?);
    if report.live_orders_allowed_by_mode && !report.live_ready {
        return Err(anyhow!(
            "live preflight gate blocked; inspect blocking_reasons"
        ));
    }
    Ok(())
}

fn load_config(path: &PathBuf) -> Result<CrossExchangeArbitrageConfig> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    serde_yaml::from_str::<CrossExchangeArbitrageConfig>(&raw)
        .with_context(|| format!("failed to parse config {}", path.display()))
}

fn market_adapter(exchange: &ExchangeId) -> Option<Box<dyn MarketDataAdapter + Send + Sync>> {
    match exchange {
        ExchangeId::Binance => Some(Box::new(BinanceMarketAdapter)),
        ExchangeId::Okx => Some(Box::new(OkxMarketAdapter)),
        ExchangeId::Bitget => Some(Box::new(BitgetMarketAdapter)),
        ExchangeId::Gate => Some(Box::new(GateMarketAdapter)),
        ExchangeId::Bybit => Some(Box::new(BybitMarketAdapter)),
        ExchangeId::Mexc => Some(Box::new(MexcMarketAdapter)),
        ExchangeId::Htx => Some(Box::new(HtxMarketAdapter)),
        ExchangeId::Other(_) => None,
    }
}

fn check_private_trading_support(exchange: &ExchangeId) -> PreflightCheck {
    let support = private_trading_support_for(exchange);
    if support.private_trading_enabled {
        pass(
            format!("{}.private.support", exchange.as_str()),
            "private trading/read-only adapter is registered",
        )
    } else {
        warn(
            format!("{}.private.support", exchange.as_str()),
            support
                .disabled_reason
                .unwrap_or("private trading/read-only adapter is not registered"),
        )
    }
}

async fn check_public_market_adapter(
    adapter: &(dyn MarketDataAdapter + Send + Sync),
    config: &CrossExchangeArbitrageConfig,
    timeout_ms: u64,
    coverage_by_symbol: &mut HashMap<String, HashSet<ExchangeId>>,
) -> Vec<PreflightCheck> {
    let exchange = adapter.exchange();
    let mut checks = Vec::new();
    let mut supported_symbols = HashSet::new();

    checks.push(
        timed(
            format!("{}.public.instruments", exchange.as_str()),
            timeout_ms,
            adapter.load_instruments(),
        )
        .await
        .map_message(|instruments| {
            for instrument in instruments {
                if instrument.is_tradeable_usdt_perpetual() {
                    supported_symbols.insert(instrument.canonical_symbol);
                }
            }
            format!(
                "loaded {} tradeable USDT perpetual instruments",
                supported_symbols.len()
            )
        }),
    );

    let symbols_for_exchange = config
        .universe
        .symbols
        .iter()
        .filter(|symbol| supported_symbols.contains(*symbol))
        .cloned()
        .collect::<Vec<_>>();

    checks.push(pass(
        format!("{}.public.symbol_filter", exchange.as_str()),
        format!(
            "{} of {} configured symbols are listed on {}",
            symbols_for_exchange.len(),
            config.universe.symbols.len(),
            exchange.as_str()
        ),
    ));

    for symbol in &symbols_for_exchange {
        coverage_by_symbol
            .entry(symbol.to_string())
            .or_default()
            .insert(exchange.clone());
        let exchange_symbol = exchange_symbol_for(&exchange, symbol);
        checks.push(
            timed(
                format!("{}.{}.public.orderbook", exchange.as_str(), symbol),
                timeout_ms,
                adapter.fetch_orderbook_snapshot(&exchange_symbol, 5),
            )
            .await
            .map_message(|book| {
                format!(
                    "book quality={:?}, bids={}, asks={}",
                    book.quality,
                    book.bids.len(),
                    book.asks.len()
                )
            }),
        );
    }

    checks.push(
        timed(
            format!("{}.public.funding", exchange.as_str()),
            timeout_ms,
            adapter.load_funding(&symbols_for_exchange),
        )
        .await
        .map_message(|funding| format!("loaded {} funding snapshots", funding.len())),
    );

    checks
}

async fn check_private_exchange(
    exchange: &ExchangeId,
    config: &CrossExchangeArbitrageConfig,
    timeout_ms: u64,
) -> Vec<PreflightCheck> {
    if matches!(exchange, ExchangeId::Bitget | ExchangeId::Gate) {
        return check_private_trading_adapter(exchange, config, timeout_ms).await;
    }

    let mut checks = Vec::new();
    let private_exchange = match build_private_exchange(exchange) {
        Ok(Some(exchange)) => exchange,
        Ok(None) => {
            checks.push(fail(
                format!("{}.private.adapter", exchange.as_str()),
                "no core private Exchange implementation is registered for this venue",
            ));
            return checks;
        }
        Err(err) => {
            checks.push(fail(
                format!("{}.private.credentials", exchange.as_str()),
                err.to_string(),
            ));
            return checks;
        }
    };

    checks.push(
        timed(
            format!("{}.private.ping", exchange.as_str()),
            timeout_ms,
            async {
                private_exchange
                    .ping()
                    .await
                    .map_err(|err| anyhow!(err.to_string()))
            },
        )
        .await
        .into(),
    );
    checks.push(
        timed(
            format!("{}.private.balance", exchange.as_str()),
            timeout_ms,
            async {
                private_exchange
                    .get_balance(MarketType::Futures)
                    .await
                    .map_err(|err| anyhow!(err.to_string()))
            },
        )
        .await
        .map_message(|balances| format!("loaded {} futures balances", balances.len())),
    );

    for symbol in &config.universe.symbols {
        let symbol_pair = symbol.as_pair();
        checks.push(
            timed(
                format!("{}.{}.private.positions", exchange.as_str(), symbol),
                timeout_ms,
                async {
                    private_exchange
                        .get_positions(Some(&symbol_pair))
                        .await
                        .map_err(|err| anyhow!(err.to_string()))
                },
            )
            .await
            .map_message(|positions| format!("loaded {} positions", positions.len())),
        );
        let symbol_pair = symbol.as_pair();
        checks.push(
            timed(
                format!("{}.{}.private.open_orders", exchange.as_str(), symbol),
                timeout_ms,
                async {
                    private_exchange
                        .get_open_orders(Some(&symbol_pair), MarketType::Futures)
                        .await
                        .map_err(|err| anyhow!(err.to_string()))
                },
            )
            .await
            .map_message(|orders| format!("loaded {} open orders", orders.len())),
        );
        let symbol_pair = symbol.as_pair();
        checks.push(
            timed(
                format!("{}.{}.private.trade_fee", exchange.as_str(), symbol),
                timeout_ms,
                async {
                    private_exchange
                        .get_trade_fee(&symbol_pair, MarketType::Futures)
                        .await
                        .map_err(|err| anyhow!(err.to_string()))
                },
            )
            .await
            .map_message(|fee| {
                format!(
                    "maker_fee={:?}, taker_fee={:?}",
                    fee.maker_fee, fee.taker_fee
                )
            }),
        );
        let symbol_pair = symbol.as_pair();
        checks.push(
            timed(
                format!("{}.{}.private.fills", exchange.as_str(), symbol),
                timeout_ms,
                async {
                    private_exchange
                        .get_my_trades(Some(&symbol_pair), MarketType::Futures, Some(5))
                        .await
                        .map_err(|err| anyhow!(err.to_string()))
                },
            )
            .await
            .map_message(|trades| format!("loaded {} recent fills", trades.len())),
        );
    }

    checks.push(warn(
        format!("{}.private.position_mode", exchange.as_str()),
        "position mode readback is not exposed by the core Exchange trait; verify manually or add venue-specific readback before live",
    ));
    checks.push(warn(
        format!("{}.private.leverage", exchange.as_str()),
        "leverage readback is not exposed by the core Exchange trait; verify manually or add venue-specific readback before live",
    ));

    checks
}

async fn check_private_trading_adapter(
    exchange: &ExchangeId,
    config: &CrossExchangeArbitrageConfig,
    timeout_ms: u64,
) -> Vec<PreflightCheck> {
    let mut checks = Vec::new();
    let adapter = match build_trading_adapter_for_exchange(config, exchange) {
        Ok(adapter) => adapter,
        Err(err) => {
            checks.push(fail(
                format!("{}.private.credentials", exchange.as_str()),
                err.to_string(),
            ));
            return checks;
        }
    };

    checks.push(pass(
        format!("{}.private.adapter", exchange.as_str()),
        "private USDT perpetual TradingAdapter is registered",
    ));
    checks.push(
        timed(
            format!("{}.private.balance", exchange.as_str()),
            timeout_ms,
            adapter.get_balances(),
        )
        .await
        .map_message(|balances| format!("loaded {} futures balances", balances.len())),
    );

    for symbol in &config.universe.symbols {
        let exchange_symbol = exchange_symbol_for(exchange, symbol);
        checks.push(
            timed(
                format!("{}.{}.private.positions", exchange.as_str(), symbol),
                timeout_ms,
                adapter.get_positions(Some(&exchange_symbol)),
            )
            .await
            .map_message(|positions| format!("loaded {} positions", positions.len())),
        );
        checks.push(
            timed(
                format!("{}.{}.private.open_orders", exchange.as_str(), symbol),
                timeout_ms,
                adapter.get_open_orders(Some(&exchange_symbol)),
            )
            .await
            .map_message(|orders| format!("loaded {} open orders", orders.len())),
        );

        let mut fill_query =
            FillQuery::for_symbol(exchange.clone(), symbol.clone(), exchange_symbol);
        fill_query.limit = Some(5);
        checks.push(
            timed(
                format!("{}.{}.private.fills", exchange.as_str(), symbol),
                timeout_ms,
                adapter.get_fills(fill_query),
            )
            .await
            .map_message(|trades| format!("loaded {} recent fills", trades.len())),
        );
    }

    checks.push(warn(
        format!("{}.private.position_mode", exchange.as_str()),
        "position mode write is supported by the adapter when the venue supports it; live readback still requires venue-specific confirmation",
    ));
    checks.push(warn(
        format!("{}.private.leverage", exchange.as_str()),
        "leverage write is supported by the adapter; live readback still requires venue-specific confirmation",
    ));

    checks
}

fn build_private_exchange(exchange: &ExchangeId) -> Result<Option<Box<dyn Exchange>>> {
    let Some(config) = core_config(exchange) else {
        return Ok(None);
    };
    let api_keys = ApiKeys::from_env(exchange.as_str()).map_err(|err| anyhow!(err.to_string()))?;
    match exchange {
        ExchangeId::Binance => Ok(Some(Box::new(BinanceExchange::new(config, api_keys)))),
        ExchangeId::Okx => Ok(Some(Box::new(OkxExchange::new(config, api_keys)))),
        ExchangeId::Bitget
        | ExchangeId::Gate
        | ExchangeId::Bybit
        | ExchangeId::Mexc
        | ExchangeId::Htx
        | ExchangeId::Other(_) => Ok(None),
    }
}

fn core_config(exchange: &ExchangeId) -> Option<CoreExchangeConfig> {
    match exchange {
        ExchangeId::Binance => Some(CoreExchangeConfig {
            name: "binance".to_string(),
            testnet: false,
            spot_base_url: "https://api.binance.com".to_string(),
            futures_base_url: "https://fapi.binance.com".to_string(),
            ws_spot_url: "wss://stream.binance.com:9443".to_string(),
            ws_futures_url: "wss://fstream.binance.com".to_string(),
        }),
        ExchangeId::Okx => Some(CoreExchangeConfig {
            name: "okx".to_string(),
            testnet: false,
            spot_base_url: "https://www.okx.com".to_string(),
            futures_base_url: "https://www.okx.com".to_string(),
            ws_spot_url: "wss://ws.okx.com:8443/ws/v5/public".to_string(),
            ws_futures_url: "wss://ws.okx.com:8443/ws/v5/public".to_string(),
        }),
        _ => None,
    }
}

async fn timed<F, T>(scope: impl Into<String>, timeout_ms: u64, future: F) -> PreflightTypedCheck<T>
where
    F: Future<Output = Result<T>>,
{
    let scope = scope.into();
    let started = Instant::now();
    match timeout(Duration::from_millis(timeout_ms), future).await {
        Ok(Ok(value)) => PreflightTypedCheck {
            scope,
            status: CheckStatus::Pass,
            latency_ms: Some(started.elapsed().as_millis()),
            value: Some(value),
            message: "ok".to_string(),
        },
        Ok(Err(err)) => PreflightTypedCheck {
            scope,
            status: CheckStatus::Fail,
            latency_ms: Some(started.elapsed().as_millis()),
            value: None,
            message: err.to_string(),
        },
        Err(_) => PreflightTypedCheck {
            scope,
            status: CheckStatus::Fail,
            latency_ms: Some(started.elapsed().as_millis()),
            value: None,
            message: format!("timed out after {} ms", timeout_ms),
        },
    }
}

struct PreflightTypedCheck<T> {
    scope: String,
    status: CheckStatus,
    latency_ms: Option<u128>,
    value: Option<T>,
    message: String,
}

impl<T> PreflightTypedCheck<T> {
    fn map_message(self, f: impl FnOnce(T) -> String) -> PreflightCheck {
        PreflightCheck {
            scope: self.scope,
            status: self.status,
            latency_ms: self.latency_ms,
            message: self.value.map(f).unwrap_or(self.message),
        }
    }
}

impl<T> From<PreflightTypedCheck<T>> for PreflightCheck {
    fn from(check: PreflightTypedCheck<T>) -> Self {
        Self {
            scope: check.scope,
            status: check.status,
            latency_ms: check.latency_ms,
            message: check.message,
        }
    }
}

fn pass(scope: impl Into<String>, message: impl Into<String>) -> PreflightCheck {
    PreflightCheck {
        scope: scope.into(),
        status: CheckStatus::Pass,
        latency_ms: None,
        message: message.into(),
    }
}

fn warn(scope: impl Into<String>, message: impl Into<String>) -> PreflightCheck {
    PreflightCheck {
        scope: scope.into(),
        status: CheckStatus::Warn,
        latency_ms: None,
        message: message.into(),
    }
}

fn fail(scope: impl Into<String>, message: impl Into<String>) -> PreflightCheck {
    PreflightCheck {
        scope: scope.into(),
        status: CheckStatus::Fail,
        latency_ms: None,
        message: message.into(),
    }
}

fn skipped(scope: impl Into<String>, message: impl Into<String>) -> PreflightCheck {
    PreflightCheck {
        scope: scope.into(),
        status: CheckStatus::Skipped,
        latency_ms: None,
        message: message.into(),
    }
}

fn overall_status(checks: &[PreflightCheck]) -> CheckStatus {
    if checks.iter().any(|check| check.status == CheckStatus::Fail) {
        CheckStatus::Fail
    } else if checks.iter().any(|check| check.status == CheckStatus::Warn) {
        CheckStatus::Warn
    } else {
        CheckStatus::Pass
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveGateDecision {
    private_readonly_required_for_live: bool,
    live_ready: bool,
    blocking_reasons: Vec<String>,
}

fn evaluate_live_gate(
    config: &CrossExchangeArbitrageConfig,
    private_checks_requested: bool,
    checks: &[PreflightCheck],
) -> LiveGateDecision {
    let private_readonly_required_for_live = config.mode.allows_live_orders();
    let mut blocking_reasons = Vec::new();

    if private_readonly_required_for_live {
        if !private_checks_requested {
            blocking_reasons.push(
                "live-capable mode requires --private-readonly to verify balances, positions, open orders, fees, and recent fills".to_string(),
            );
        }

        for exchange in &config.universe.enabled_exchanges {
            let support = private_trading_support_for(exchange);
            if !support.private_trading_enabled {
                blocking_reasons.push(format!(
                    "{} private trading/read-only adapter disabled: {}",
                    exchange.as_str(),
                    support
                        .disabled_reason
                        .unwrap_or("private adapter is not registered")
                ));
            }
        }

        for check in checks
            .iter()
            .filter(|check| check.status == CheckStatus::Fail)
        {
            blocking_reasons.push(format!("{} failed: {}", check.scope, check.message));
        }
    }

    LiveGateDecision {
        private_readonly_required_for_live,
        live_ready: private_readonly_required_for_live && blocking_reasons.is_empty(),
        blocking_reasons,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preflight_cli_should_accept_private_readonly_alias() {
        let args = Args::try_parse_from(["cross_arb_preflight", "--private-readonly"])
            .expect("private-readonly alias should parse");

        assert!(args.private);
    }

    #[test]
    fn live_gate_should_require_private_readonly_for_live_mode() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.mode = RuntimeMode::LiveSmall;
        config.strategy.mode = Some(RuntimeMode::LiveSmall);
        config.universe.enabled_exchanges = vec![ExchangeId::Binance, ExchangeId::Okx];

        let decision = evaluate_live_gate(&config, false, &[]);

        assert!(decision.private_readonly_required_for_live);
        assert!(!decision.live_ready);
        assert!(decision
            .blocking_reasons
            .iter()
            .any(|reason| reason.contains("--private-readonly")));
    }

    #[test]
    fn live_gate_should_allow_bitget_gate_private_adapters() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.mode = RuntimeMode::LiveSmall;
        config.strategy.mode = Some(RuntimeMode::LiveSmall);
        config.universe.enabled_exchanges =
            vec![ExchangeId::Binance, ExchangeId::Bitget, ExchangeId::Gate];

        let decision = evaluate_live_gate(&config, true, &[]);

        assert!(decision.live_ready);
        assert!(decision.blocking_reasons.is_empty());
    }

    #[test]
    fn live_gate_should_block_exchanges_without_private_adapter() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.mode = RuntimeMode::LiveSmall;
        config.strategy.mode = Some(RuntimeMode::LiveSmall);
        config.universe.enabled_exchanges = vec![
            ExchangeId::Binance,
            ExchangeId::Other("venue-x".to_string()),
        ];

        let decision = evaluate_live_gate(&config, true, &[]);

        assert!(!decision.live_ready);
        assert!(decision
            .blocking_reasons
            .iter()
            .any(|reason| reason.contains("venue-x private")));
    }

    #[test]
    fn live_gate_should_block_failed_checks() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.mode = RuntimeMode::LiveSmall;
        config.strategy.mode = Some(RuntimeMode::LiveSmall);
        config.universe.enabled_exchanges = vec![ExchangeId::Binance, ExchangeId::Okx];
        let checks = vec![fail(
            "binance.private.balance",
            "authentication failed for readonly key",
        )];

        let decision = evaluate_live_gate(&config, true, &checks);

        assert!(!decision.live_ready);
        assert!(decision
            .blocking_reasons
            .iter()
            .any(|reason| reason.contains("binance.private.balance failed")));
    }

    #[test]
    fn live_gate_should_pass_when_live_private_checks_have_no_failures() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.mode = RuntimeMode::LiveSmall;
        config.strategy.mode = Some(RuntimeMode::LiveSmall);
        config.universe.enabled_exchanges = vec![ExchangeId::Binance, ExchangeId::Okx];
        let checks = vec![
            pass("binance.private.balance", "loaded 1 futures balances"),
            warn(
                "binance.private.position_mode",
                "manual venue-specific readback still required",
            ),
        ];

        let decision = evaluate_live_gate(&config, true, &checks);

        assert!(decision.live_ready);
        assert!(decision.blocking_reasons.is_empty());
    }

    #[test]
    fn non_live_gate_should_not_claim_live_ready() {
        let config = CrossExchangeArbitrageConfig::default();

        let decision = evaluate_live_gate(&config, false, &[]);

        assert!(!decision.private_readonly_required_for_live);
        assert!(!decision.live_ready);
        assert!(decision.blocking_reasons.is_empty());
    }
}
