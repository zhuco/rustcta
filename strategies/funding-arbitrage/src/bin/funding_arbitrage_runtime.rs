use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, CanonicalSymbol, ExchangeClient, ExchangeId, ExchangeSymbol, FeesRequest,
    FundingRatesRequest, FundingRatesResponse, MarketType, OrderBookRequest, RequestContext,
    ResponseMetadata, RunId, SymbolRulesRequest, SymbolRulesResponse, SymbolScope, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_exchange_gateway::{
    AdapterBackedGateway, GatewayExchangeClient, InProcessGatewayClient,
};
use rustcta_strategy_funding_arbitrage::{
    build_report_from_gateway_scan_bundles, core::ExchangeScanError, FundingArbitrageRuntime,
    FundingCoreConfig, FundingScanReport, GatewayFundingScanBundle, STRATEGY_KIND,
};
use rustcta_strategy_sdk::{
    ExecutionCancelAck, ExecutionCancelCommand, ExecutionIntent, ExecutionIntentAck,
    ExecutionOrderAck, ExecutionOrderCommand, SdkResult, StrategyContext, StrategyExecutionClient,
    StrategyInstanceId, StrategyRuntime,
};
use serde::Serialize;
use serde_json::{json, Value};

#[derive(Debug)]
struct Args {
    config: PathBuf,
    strategy_id: String,
    run_id: String,
    tenant_id: String,
    account_id: String,
    once: bool,
    snapshot_interval_ms: u64,
}

#[derive(Debug, Serialize)]
struct RuntimeReport {
    generated_at: chrono::DateTime<Utc>,
    strategy_kind: &'static str,
    strategy_id: String,
    run_id: String,
    config_path: String,
    root_free_runtime: bool,
    live_orders_enabled: bool,
    concrete_exchange_adapter_loaded: bool,
    funding_scan_report: Option<FundingScanReport>,
    snapshot: serde_json::Value,
}

struct RuntimeGatewayScan {
    adapter_loaded: bool,
    report: FundingScanReport,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;
    let config = read_yaml_config(&args.config)?;
    let core_config = funding_core_config(&config)?;
    let mut runtime = FundingArbitrageRuntime::new();
    runtime.start(strategy_context(&args, config)).await?;
    emit_report(&args, &runtime, &core_config).await?;
    if args.once {
        runtime.stop().await?;
        return Ok(());
    }
    let mut interval = tokio::time::interval(Duration::from_millis(args.snapshot_interval_ms));
    loop {
        interval.tick().await;
        emit_report(&args, &runtime, &core_config).await?;
    }
}

fn parse_args() -> Result<Args> {
    let mut values = std::env::args().skip(1);
    let mut args = Args {
        config: PathBuf::from("config/funding_rate_arbitrage_live_usdt.yml"),
        strategy_id: "funding_arb_live".to_string(),
        run_id: "local".to_string(),
        tenant_id: "local".to_string(),
        account_id: "default".to_string(),
        once: false,
        snapshot_interval_ms: 30_000,
    };
    while let Some(arg) = values.next() {
        match arg.as_str() {
            "--config" => args.config = PathBuf::from(next_value(&mut values, "--config")?),
            "--strategy-id" => args.strategy_id = next_value(&mut values, "--strategy-id")?,
            "--run-id" => args.run_id = next_value(&mut values, "--run-id")?,
            "--tenant-id" => args.tenant_id = next_value(&mut values, "--tenant-id")?,
            "--account-id" => args.account_id = next_value(&mut values, "--account-id")?,
            "--snapshot-interval-ms" => {
                args.snapshot_interval_ms = next_value(&mut values, "--snapshot-interval-ms")?
                    .parse()
                    .context("--snapshot-interval-ms must be a positive integer")?
            }
            "--once" => args.once = true,
            "--help" | "-h" => {
                println!("funding-arbitrage-runtime --config <path> [--once]");
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    Ok(args)
}

fn next_value(values: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    values
        .next()
        .with_context(|| format!("{flag} requires a value"))
}

fn read_yaml_config(path: &PathBuf) -> Result<serde_json::Value> {
    let raw = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let yaml: serde_yaml::Value =
        serde_yaml::from_str(&raw).with_context(|| format!("parse {}", path.display()))?;
    serde_json::to_value(yaml).context("convert runtime config to json")
}

fn funding_core_config(config: &Value) -> Result<FundingCoreConfig> {
    serde_json::from_value(config.clone()).context("parse funding arbitrage core config")
}

fn strategy_context(args: &Args, config: Value) -> StrategyContext {
    StrategyContext::new(
        StrategyInstanceId::new(format!("{}:{}", args.strategy_id, args.run_id)),
        args.tenant_id.clone(),
        args.account_id.clone(),
        args.strategy_id.clone(),
        args.run_id.clone(),
        config,
        Arc::new(NoopExecutionClient),
    )
}

async fn emit_report(
    args: &Args,
    runtime: &FundingArbitrageRuntime,
    core_config: &FundingCoreConfig,
) -> Result<()> {
    let gateway_scan = scan_gateway_funding(args, core_config).await?;
    let report = RuntimeReport {
        generated_at: Utc::now(),
        strategy_kind: STRATEGY_KIND,
        strategy_id: args.strategy_id.clone(),
        run_id: args.run_id.clone(),
        config_path: args.config.display().to_string(),
        root_free_runtime: true,
        live_orders_enabled: false,
        concrete_exchange_adapter_loaded: gateway_scan.adapter_loaded,
        funding_scan_report: Some(gateway_scan.report),
        snapshot: serde_json::to_value(runtime.snapshot().await?)?,
    };
    println!("{}", serde_json::to_string(&json!(report))?);
    Ok(())
}

async fn scan_gateway_funding(
    args: &Args,
    config: &FundingCoreConfig,
) -> Result<RuntimeGatewayScan> {
    let generated_at = Utc::now();
    let gateway = Arc::new(AdapterBackedGateway::new("funding-arbitrage-runtime"));
    let mut registered = Vec::new();
    let mut bundles = Vec::new();

    for raw_exchange in scanner_exchanges(config) {
        match normalize_funding_exchange(&raw_exchange) {
            Ok(exchange) => match gateway.register_named_adapter(&exchange) {
                Ok(()) => registered.push(exchange),
                Err(error) => bundles.push(empty_scan_bundle(
                    exchange,
                    generated_at,
                    scan_error(&raw_exchange, "adapter", error),
                )?),
            },
            Err(message) => bundles.push(empty_scan_bundle(
                raw_exchange.clone(),
                generated_at,
                scan_error(&raw_exchange, "adapter", message),
            )?),
        }
    }

    registered.sort();
    registered.dedup();
    let gateway_client = Arc::new(InProcessGatewayClient::new(Arc::clone(&gateway)));
    let tenant_id =
        TenantId::new(args.tenant_id.clone()).context("build exchange gateway tenant id")?;
    let account_id =
        Some(AccountId::new(args.account_id.clone()).context("build exchange gateway account id")?);

    for exchange in &registered {
        let exchange_id =
            ExchangeId::new(exchange.clone()).context("build exchange gateway exchange id")?;
        let client = GatewayExchangeClient::new(
            Arc::clone(&gateway_client),
            tenant_id.clone(),
            account_id.clone(),
            exchange_id,
        );
        bundles.push(scan_exchange(args, config, &client, exchange, generated_at).await?);
    }

    Ok(RuntimeGatewayScan {
        adapter_loaded: !registered.is_empty(),
        report: build_report_from_gateway_scan_bundles(config, generated_at, bundles),
    })
}

async fn scan_exchange(
    args: &Args,
    config: &FundingCoreConfig,
    client: &GatewayExchangeClient,
    exchange: &str,
    generated_at: chrono::DateTime<Utc>,
) -> Result<GatewayFundingScanBundle> {
    let exchange_id = ExchangeId::new(exchange.to_string())?;
    let metadata = ResponseMetadata::new(exchange_id.clone(), generated_at);
    let mut errors = Vec::new();
    let symbols = match symbol_scopes_for_exchange(exchange, config) {
        Ok(symbols) => symbols,
        Err(error) => {
            errors.push(scan_error(exchange, "symbols", error));
            Vec::new()
        }
    };

    let symbol_rules = match client
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: request_context(args, exchange, "symbol-rules"),
            symbols: symbols.clone(),
        })
        .await
    {
        Ok(response) => response,
        Err(error) => {
            errors.push(scan_error(exchange, "symbol_rules", error));
            SymbolRulesResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: metadata.clone(),
                rules: Vec::new(),
            }
        }
    };

    let funding_rates = match client
        .get_funding_rates(FundingRatesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: request_context(args, exchange, "funding-rates"),
            symbols: symbols.clone(),
        })
        .await
    {
        Ok(response) => response,
        Err(error) => {
            errors.push(scan_error(exchange, "funding_rates", error));
            FundingRatesResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: metadata.clone(),
                rates: Vec::new(),
            }
        }
    };

    let fees = match client
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: request_context(args, exchange, "fees"),
            symbols: symbols.clone(),
        })
        .await
    {
        Ok(response) => response.fees,
        Err(error) => {
            errors.push(scan_error(exchange, "fees", error));
            Vec::new()
        }
    };

    let mut order_books = Vec::new();
    for symbol in symbols {
        match client
            .get_order_book(OrderBookRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: request_context(args, exchange, "order-book"),
                symbol,
                depth: Some(50),
            })
            .await
        {
            Ok(response) => order_books.push(response.order_book),
            Err(error) => errors.push(scan_error(exchange, "order_book", error)),
        }
    }

    Ok(GatewayFundingScanBundle {
        exchange: exchange.to_string(),
        symbol_rules,
        funding_rates,
        fees,
        order_books,
        errors,
    })
}

fn request_context(args: &Args, exchange: &str, stage: &str) -> RequestContext {
    let mut context = RequestContext::new(Utc::now());
    context.tenant_id = TenantId::new(args.tenant_id.clone()).ok();
    context.account_id = AccountId::new(args.account_id.clone()).ok();
    context.run_id = RunId::new(args.run_id.clone()).ok();
    context.request_id = Some(format!(
        "{}:{}:{}:{}",
        STRATEGY_KIND, args.run_id, exchange, stage
    ));
    context
}

fn scanner_exchanges(config: &FundingCoreConfig) -> Vec<String> {
    config
        .universe
        .enabled_exchanges
        .iter()
        .map(|exchange| exchange.trim().to_string())
        .filter(|exchange| !exchange.is_empty())
        .collect()
}

fn normalize_funding_exchange(exchange: &str) -> Result<String, String> {
    let normalized = exchange.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "" => Err("exchange id must not be empty".to_string()),
        "asterdex" | "aster_dex" | "aster-dex" => Ok("aster".to_string()),
        "kucoin" => Err(
            "`kucoin` is the spot adapter; use `kucoinfutures` for contract funding scans"
                .to_string(),
        ),
        "kucoin_futures" | "kucoin-futures" => Ok("kucoinfutures".to_string()),
        other => Ok(other.to_string()),
    }
}

fn symbol_scopes_for_exchange(
    exchange: &str,
    config: &FundingCoreConfig,
) -> Result<Vec<SymbolScope>> {
    configured_scan_symbols(config)
        .into_iter()
        .map(|symbol| symbol_scope_for_pair(exchange, &symbol))
        .collect()
}

fn configured_scan_symbols(config: &FundingCoreConfig) -> Vec<String> {
    let quote = config.universe.quote_asset.trim().to_ascii_uppercase();
    let fallback = format!("BTC/{quote}");
    let mut symbols = if config.universe.symbol_allowlist.is_empty() {
        vec![fallback]
    } else {
        config
            .universe
            .symbol_allowlist
            .iter()
            .map(|symbol| symbol.trim().to_string())
            .filter(|symbol| !symbol.is_empty())
            .collect::<Vec<_>>()
    };
    let blocked = config
        .universe
        .symbol_blocklist
        .iter()
        .map(|symbol| symbol.trim().to_ascii_uppercase())
        .collect::<std::collections::HashSet<_>>();
    symbols.retain(|symbol| !blocked.contains(&symbol.to_ascii_uppercase()));
    symbols
}

fn symbol_scope_for_pair(exchange: &str, pair: &str) -> Result<SymbolScope> {
    let (base, quote) = split_symbol_pair(pair)?;
    let exchange_id = ExchangeId::new(exchange.to_string())?;
    let market_type = MarketType::Perpetual;
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new(&base, &quote)?),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id,
            market_type,
            exchange_symbol_for_pair(exchange, &base, &quote),
        )?,
    })
}

fn split_symbol_pair(pair: &str) -> Result<(String, String)> {
    let normalized = pair.trim().to_ascii_uppercase();
    for separator in ['/', '-', '_'] {
        if let Some((base, quote)) = normalized.split_once(separator) {
            if !base.is_empty() && !quote.is_empty() {
                return Ok((base.to_string(), quote.to_string()));
            }
        }
    }
    for quote in ["USDT", "USDC", "USD"] {
        if let Some(base) = normalized.strip_suffix(quote) {
            if !base.is_empty() {
                return Ok((base.to_string(), quote.to_string()));
            }
        }
    }
    bail!("symbol {pair:?} must include base and quote assets")
}

fn exchange_symbol_for_pair(exchange: &str, base: &str, quote: &str) -> String {
    match exchange {
        "mexc" => format!("{base}_{quote}"),
        "kucoinfutures" if base == "BTC" && quote == "USDT" => "XBTUSDTM".to_string(),
        "kucoinfutures" => format!("{base}{quote}M"),
        _ => format!("{base}{quote}"),
    }
}

fn empty_scan_bundle(
    exchange: String,
    generated_at: chrono::DateTime<Utc>,
    error: ExchangeScanError,
) -> Result<GatewayFundingScanBundle> {
    let exchange_id = ExchangeId::new(exchange.clone())?;
    let metadata = ResponseMetadata::new(exchange_id, generated_at);
    Ok(GatewayFundingScanBundle {
        exchange,
        symbol_rules: SymbolRulesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata: metadata.clone(),
            rules: Vec::new(),
        },
        funding_rates: FundingRatesResponse {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            metadata,
            rates: Vec::new(),
        },
        fees: Vec::new(),
        order_books: Vec::new(),
        errors: vec![error],
    })
}

fn scan_error(
    exchange: &str,
    stage: &'static str,
    error: impl std::fmt::Display,
) -> ExchangeScanError {
    ExchangeScanError {
        exchange: exchange.to_string(),
        stage,
        message: error.to_string(),
    }
}

struct NoopExecutionClient;

#[async_trait]
impl StrategyExecutionClient for NoopExecutionClient {
    async fn submit_order(&self, command: ExecutionOrderCommand) -> SdkResult<ExecutionOrderAck> {
        Ok(ExecutionOrderAck {
            schema_version: command.schema_version,
            accepted: false,
            client_order_id: command.client_order_id,
            execution_order_id: None,
            reason: Some("root-free supervisor wrapper has no execution client".to_string()),
            received_at: Utc::now(),
        })
    }

    async fn cancel_order(&self, command: ExecutionCancelCommand) -> SdkResult<ExecutionCancelAck> {
        Ok(ExecutionCancelAck {
            schema_version: command.schema_version,
            accepted: false,
            client_order_id: command.client_order_id,
            execution_order_id: command.execution_order_id,
            reason: Some("root-free supervisor wrapper has no execution client".to_string()),
            received_at: Utc::now(),
        })
    }

    async fn submit_raw_intent(&self, intent: ExecutionIntent) -> SdkResult<ExecutionIntentAck> {
        Ok(ExecutionIntentAck {
            schema_version: intent.schema_version,
            accepted: false,
            intent_kind: intent.intent_kind,
            reason: Some("root-free supervisor wrapper has no execution client".to_string()),
            received_at: Utc::now(),
            payload: json!({}),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_symbols(symbols: Vec<&str>) -> FundingCoreConfig {
        let mut config = FundingCoreConfig::default();
        config.universe.enabled_exchanges = vec![
            "aster".to_string(),
            "mexc".to_string(),
            "kucoinfutures".to_string(),
            "bybit".to_string(),
        ];
        config.universe.symbol_allowlist = symbols
            .into_iter()
            .map(std::string::ToString::to_string)
            .collect();
        config
    }

    #[test]
    fn normalize_funding_exchange_should_route_kucoin_contract_aliases_only() {
        assert_eq!(
            normalize_funding_exchange("asterdex").expect("aster alias"),
            "aster"
        );
        assert_eq!(
            normalize_funding_exchange("kucoin-futures").expect("futures alias"),
            "kucoinfutures"
        );
        assert!(normalize_funding_exchange("kucoin")
            .expect_err("spot kucoin should not be accepted")
            .contains("spot adapter"));
    }

    #[test]
    fn symbol_scopes_should_match_target_contract_symbol_shapes() {
        let config = config_with_symbols(vec!["BTC/USDT"]);

        let aster = symbol_scopes_for_exchange("aster", &config).expect("aster symbols");
        let mexc = symbol_scopes_for_exchange("mexc", &config).expect("mexc symbols");
        let kucoin = symbol_scopes_for_exchange("kucoinfutures", &config).expect("kucoin symbols");
        let bybit = symbol_scopes_for_exchange("bybit", &config).expect("bybit symbols");

        assert_eq!(aster[0].exchange_symbol.symbol, "BTCUSDT");
        assert_eq!(mexc[0].exchange_symbol.symbol, "BTC_USDT");
        assert_eq!(kucoin[0].exchange_symbol.symbol, "XBTUSDTM");
        assert_eq!(bybit[0].exchange_symbol.symbol, "BTCUSDT");
        for scope in [aster, mexc, kucoin, bybit].into_iter().flatten() {
            assert_eq!(scope.market_type, MarketType::Perpetual);
            assert_eq!(
                scope
                    .canonical_symbol
                    .as_ref()
                    .expect("canonical symbol")
                    .as_str(),
                "BTC/USDT"
            );
        }
    }

    #[test]
    fn configured_scan_symbols_should_default_to_quote_asset_pair_and_apply_blocklist() {
        let mut config = config_with_symbols(Vec::new());
        config.universe.quote_asset = "USDT".to_string();
        assert_eq!(configured_scan_symbols(&config), vec!["BTC/USDT"]);

        config.universe.symbol_allowlist = vec!["BTC/USDT".to_string(), "ETH/USDT".to_string()];
        config.universe.symbol_blocklist = vec!["btc/usdt".to_string()];
        assert_eq!(configured_scan_symbols(&config), vec!["ETH/USDT"]);
    }
}
