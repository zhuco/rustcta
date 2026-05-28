use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use rustcta::core::exchange::Exchange;
use rustcta::core::types::MarketType;
use rustcta::exchanges::adapters::{
    private_trading_support_for, BinanceMarketAdapter, BitgetMarketAdapter, BybitMarketAdapter,
    GateMarketAdapter, HtxMarketAdapter, MexcMarketAdapter, OkxMarketAdapter,
};
use rustcta::market::{exchange_symbol_for, ExchangeId, MarketDataAdapter, RuntimeMode};
use rustcta::strategies::cross_exchange_arbitrage::{
    build_core_exchange_for_exchange, build_trading_adapter_for_exchange, live_enabled_exchanges,
    private_ws_auth_for_exchange, CrossExchangeArbitrageConfig,
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
    #[arg(long, default_value_t = 8)]
    private_symbol_sample: usize,
    #[arg(long, default_value_t = 24)]
    public_orderbook_sample: usize,
    #[arg(long, default_value_t = false)]
    full_symbol_checks: bool,
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
    dotenv::dotenv().ok();
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
                        if args.full_symbol_checks {
                            usize::MAX
                        } else {
                            args.public_orderbook_sample
                        },
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
            checks.extend(
                check_private_exchange(
                    exchange,
                    &config,
                    args.timeout_ms,
                    if args.full_symbol_checks {
                        usize::MAX
                    } else {
                        args.private_symbol_sample
                    },
                )
                .await,
            );
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
    orderbook_sample_limit: usize,
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

    let orderbook_symbols = sampled_symbols(&symbols_for_exchange, orderbook_sample_limit);
    for symbol in &symbols_for_exchange {
        coverage_by_symbol
            .entry(symbol.to_string())
            .or_default()
            .insert(exchange.clone());
    }
    checks.push(pass(
        format!("{}.public.orderbook_sample", exchange.as_str()),
        format!(
            "checking {} of {} listed configured symbols; use --full-symbol-checks for exhaustive checks",
            orderbook_symbols.len(),
            symbols_for_exchange.len()
        ),
    ));
    for symbol in &orderbook_symbols {
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
    symbol_sample_limit: usize,
) -> Vec<PreflightCheck> {
    if rustcta::strategies::cross_exchange_arbitrage::private_perp_exchange(exchange).is_some() {
        return check_private_trading_adapter(exchange, config, timeout_ms, symbol_sample_limit)
            .await;
    }

    let mut checks = Vec::new();
    let private_exchange = match build_private_exchange(config, exchange) {
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
    checks.push(
        timed(
            format!("{}.private.positions_all", exchange.as_str()),
            timeout_ms,
            async {
                private_exchange
                    .get_positions(None)
                    .await
                    .map_err(|err| anyhow!(err.to_string()))
            },
        )
        .await
        .map_message(|positions| {
            let nonzero = positions
                .iter()
                .filter(|position| {
                    position.contracts.abs() > 1e-12
                        || position.size.abs() > 1e-12
                        || position.amount.abs() > 1e-12
                })
                .count();
            format!(
                "loaded {} account positions, nonzero={nonzero}",
                positions.len()
            )
        }),
    );
    checks.push(
        timed(
            format!("{}.private.open_orders_all", exchange.as_str()),
            timeout_ms,
            async {
                private_exchange
                    .get_open_orders(None, MarketType::Futures)
                    .await
                    .map_err(|err| anyhow!(err.to_string()))
            },
        )
        .await
        .map_message(|orders| format!("loaded {} account open orders", orders.len())),
    );

    let private_symbols = sampled_symbols(&config.universe.symbols, symbol_sample_limit);
    checks.push(pass(
        format!("{}.private.symbol_sample", exchange.as_str()),
        format!(
            "checking {} of {} configured symbols; use --full-symbol-checks for exhaustive private symbol checks",
            private_symbols.len(),
            config.universe.symbols.len()
        ),
    ));
    for symbol in &private_symbols {
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
        checks.push(skipped(
            format!("{}.{}.private.fills", exchange.as_str(), symbol),
            "recent fills are private WebSocket/runtime reconciliation data; skipped to avoid REST rate-limit pressure",
        ));
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
    symbol_sample_limit: usize,
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
    checks.extend(check_private_ws_identity(exchange, config));
    checks.push(
        timed(
            format!("{}.private.balance", exchange.as_str()),
            timeout_ms,
            adapter.get_balances(),
        )
        .await
        .map_message(|balances| format!("loaded {} futures balances", balances.len())),
    );
    checks.push(
        timed(
            format!("{}.private.positions_all", exchange.as_str()),
            timeout_ms,
            adapter.get_positions(None),
        )
        .await
        .map_message(|positions| {
            let nonzero = positions
                .iter()
                .filter(|position| position.quantity.abs() > 1e-12)
                .count();
            format!(
                "loaded {} account positions, nonzero={nonzero}",
                positions.len()
            )
        }),
    );
    checks.push(
        timed(
            format!("{}.private.open_orders_all", exchange.as_str()),
            timeout_ms,
            adapter.get_open_orders(None),
        )
        .await
        .map_message(|orders| format!("loaded {} account open orders", orders.len())),
    );

    let private_symbols = sampled_symbols(&config.universe.symbols, symbol_sample_limit);
    checks.push(pass(
        format!("{}.private.symbol_sample", exchange.as_str()),
        format!(
            "checking {} of {} configured symbols; use --full-symbol-checks for exhaustive private symbol checks",
            private_symbols.len(),
            config.universe.symbols.len()
        ),
    ));
    for symbol in &private_symbols {
        let exchange_symbol = exchange_symbol_for(exchange, symbol);
        checks.push(skipped(
            format!("{}.{}.private.fills", exchange.as_str(), symbol),
            "recent fills are private WebSocket/runtime reconciliation data; skipped to avoid REST rate-limit pressure",
        ));
        checks.push(
            timed(
                format!("{}.{}.private.trade_fee", exchange.as_str(), symbol),
                timeout_ms,
                adapter.get_trade_fee(&exchange_symbol),
            )
            .await
            .map_message(|fee| format!("maker_fee={}, taker_fee={}", fee.maker, fee.taker)),
        );
        checks.push(
            timed(
                format!(
                    "{}.{}.private.symbol_account_config",
                    exchange.as_str(),
                    symbol
                ),
                timeout_ms,
                adapter.get_symbol_account_config(&exchange_symbol),
            )
            .await
            .map_message(|config| {
                format!(
                    "position_mode={:?}, margin_mode={:?}, leverage={:?}, max_leverage={:?}",
                    config.position_mode, config.margin_mode, config.leverage, config.max_leverage
                )
            }),
        );
    }

    checks.push(warn(
        format!("{}.private.position_mode", exchange.as_str()),
        "position mode write/readback is venue-specific; verify account hedge/one-way mode on each venue before live-small",
    ));
    checks.push(warn(
        format!("{}.private.leverage", exchange.as_str()),
        "startup does not write leverage; strategy will use each exchange account's current/default leverage unless you run a separate leverage script",
    ));

    checks
}

fn check_private_ws_identity(
    exchange: &ExchangeId,
    config: &CrossExchangeArbitrageConfig,
) -> Vec<PreflightCheck> {
    let private_ws_enabled = config
        .exchanges
        .get(exchange)
        .map(|runtime| runtime.private_ws_enabled)
        .unwrap_or(false);
    if !private_ws_enabled {
        return vec![skipped(
            format!("{}.private_ws.identity", exchange.as_str()),
            "private WebSocket is disabled for this exchange",
        )];
    }

    if *exchange == ExchangeId::Gate {
        match private_ws_auth_for_exchange(config, exchange) {
            Ok(_) => vec![pass(
                "gate.private_ws.account_id",
                format!(
                    "numeric Gate user id configured for private WebSocket; {} symbol subscriptions will use this identity",
                    config.universe.symbols.len()
                ),
            )],
            Err(err) => vec![fail("gate.private_ws.account_id", err.to_string())],
        }
    } else {
        vec![pass(
            format!("{}.private_ws.identity", exchange.as_str()),
            "private WebSocket identity can be built from configured credentials",
        )]
    }
}

fn build_private_exchange(
    config: &CrossExchangeArbitrageConfig,
    exchange: &ExchangeId,
) -> Result<Option<Arc<dyn Exchange>>> {
    match exchange {
        ExchangeId::Binance | ExchangeId::Okx => {
            build_core_exchange_for_exchange(config, exchange).map(Some)
        }
        ExchangeId::Bitget
        | ExchangeId::Gate
        | ExchangeId::Bybit
        | ExchangeId::Mexc
        | ExchangeId::Htx
        | ExchangeId::Other(_) => Ok(None),
    }
}

fn sampled_symbols<T: Clone>(symbols: &[T], limit: usize) -> Vec<T> {
    if limit == 0 || symbols.is_empty() {
        return Vec::new();
    }
    if symbols.len() <= limit {
        return symbols.to_vec();
    }
    symbols.iter().take(limit).cloned().collect()
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

        if config.risk.block_on_external_account_exposure {
            for check in checks.iter().filter(|check| {
                check.scope.ends_with(".private.open_orders_all")
                    && !check.message.contains("loaded 0 account open orders")
            }) {
                blocking_reasons.push(format!(
                    "{} found existing open orders: {}",
                    check.scope, check.message
                ));
            }

            for check in checks.iter().filter(|check| {
                check.scope.ends_with(".private.positions_all")
                    && !check.message.contains("nonzero=0")
            }) {
                blocking_reasons.push(format!(
                    "{} found existing nonzero positions: {}",
                    check.scope, check.message
                ));
            }
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
    fn live_gate_should_allow_registered_private_adapters() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.mode = RuntimeMode::LiveSmall;
        config.strategy.mode = Some(RuntimeMode::LiveSmall);
        config.universe.enabled_exchanges = vec![
            ExchangeId::Binance,
            ExchangeId::Bitget,
            ExchangeId::Gate,
            ExchangeId::Bybit,
            ExchangeId::Mexc,
            ExchangeId::Htx,
        ];

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
