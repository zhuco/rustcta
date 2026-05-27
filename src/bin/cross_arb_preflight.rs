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
    BinanceMarketAdapter, BitgetMarketAdapter, GateMarketAdapter, OkxMarketAdapter,
};
use rustcta::exchanges::{BinanceExchange, OkxExchange};
use rustcta::market::{exchange_symbol_for, ExchangeId, MarketDataAdapter, RuntimeMode};
use rustcta::strategies::cross_exchange_arbitrage::CrossExchangeArbitrageConfig;
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
    live_orders_allowed_by_mode: bool,
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
    for exchange in &config.universe.enabled_exchanges {
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

        if args.private {
            checks.extend(check_private_exchange(exchange, &config, args.timeout_ms).await);
        } else {
            checks.push(skipped(
                format!("{}.private", exchange.as_str()),
                "private checks skipped; pass --private to read account state",
            ));
        }
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
    let report = PreflightReport {
        generated_at: Utc::now(),
        config_path: args.config.display().to_string(),
        configured_mode: config.mode,
        private_checks_requested: args.private,
        live_orders_allowed_by_mode: config.mode.allows_live_orders(),
        overall_status,
        checks,
    };

    println!("{}", serde_json::to_string_pretty(&report)?);
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
        ExchangeId::Other(_) => None,
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

fn build_private_exchange(exchange: &ExchangeId) -> Result<Option<Box<dyn Exchange>>> {
    let Some(config) = core_config(exchange) else {
        return Ok(None);
    };
    let api_keys = ApiKeys::from_env(exchange.as_str()).map_err(|err| anyhow!(err.to_string()))?;
    match exchange {
        ExchangeId::Binance => Ok(Some(Box::new(BinanceExchange::new(config, api_keys)))),
        ExchangeId::Okx => Ok(Some(Box::new(OkxExchange::new(config, api_keys)))),
        ExchangeId::Bitget | ExchangeId::Gate | ExchangeId::Other(_) => Ok(None),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preflight_cli_should_accept_private_readonly_alias() {
        let args = Args::try_parse_from(["cross_arb_preflight", "--private-readonly"])
            .expect("private-readonly alias should parse");

        assert!(args.private);
    }
}
