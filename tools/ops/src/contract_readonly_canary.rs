use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use clap::Args;
use rustcta_exchange_api::{
    AccountId, BalancesRequest, CanonicalSymbol, ExchangeClient, ExchangeId, ExchangeSymbol,
    FeesRequest, FundingRatesRequest, MarketType, OrderBookRequest, PositionsRequest,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    RequestContext, RunId, SymbolRulesRequest, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_exchange_gateway::{
    AdapterBackedGateway, GatewayExchangeClient, InProcessGatewayClient,
};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::private_ws_observe::{
    run_private_ws_observe_once, PrivateWsObserveConfig, PrivateWsObserveEvent,
};

#[derive(Debug, Clone, Args)]
pub struct ContractReadonlyCanaryArgs {
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "aster,mexc,kucoinfutures,bybit"
    )]
    pub exchanges: Vec<String>,
    #[arg(long, default_value = "BTC/USDT")]
    pub symbol: String,
    #[arg(long, default_value = "local")]
    pub tenant_id: String,
    #[arg(long, default_value = "default")]
    pub account_id: String,
    #[arg(long, default_value = "contract_readonly_canary")]
    pub run_id: String,
    #[arg(long, default_value_t = 50)]
    pub order_book_depth: u32,
    #[arg(long, default_value_t = 0)]
    pub private_ws_observe_duration_ms: u64,
    #[arg(long, default_value_t = 30_000)]
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContractReadonlyCanaryReport {
    pub generated_at: DateTime<Utc>,
    pub read_only: bool,
    pub live_orders_enabled: bool,
    pub symbol: String,
    pub exchanges: Vec<ContractReadonlyExchangeReport>,
    pub ready: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContractReadonlyExchangeReport {
    pub exchange: String,
    pub symbol: String,
    pub ready: bool,
    pub stages: Vec<ContractReadonlyStageReport>,
    pub private_ws_status: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContractReadonlyStageReport {
    pub stage: &'static str,
    pub ok: bool,
    pub details: Value,
    pub error: Option<String>,
}

pub async fn run_contract_readonly_canary(
    args: ContractReadonlyCanaryArgs,
) -> Result<ContractReadonlyCanaryReport> {
    dotenv::dotenv().ok();
    let exchanges = normalized_exchanges(&args.exchanges)?;
    let tenant_id = TenantId::new(args.tenant_id.clone()).context("tenant id")?;
    let account_id = AccountId::new(args.account_id.clone()).context("account id")?;
    let gateway = Arc::new(AdapterBackedGateway::new("contract-readonly-canary"));
    let mut reports = Vec::new();

    for exchange in exchanges {
        let mut stages = Vec::new();
        let normalized = match normalize_contract_exchange(&exchange) {
            Ok(exchange) => exchange,
            Err(error) => {
                stages.push(stage_error("adapter", error));
                reports.push(ContractReadonlyExchangeReport {
                    exchange,
                    symbol: args.symbol.clone(),
                    ready: false,
                    stages,
                    private_ws_status: None,
                });
                continue;
            }
        };

        match gateway.register_named_adapter(&normalized) {
            Ok(()) => stages.push(stage_ok("adapter", json!({"registered": true}))),
            Err(error) => {
                stages.push(stage_error("adapter", error));
                reports.push(ContractReadonlyExchangeReport {
                    exchange: normalized,
                    symbol: args.symbol.clone(),
                    ready: false,
                    stages,
                    private_ws_status: None,
                });
                continue;
            }
        }

        let exchange_id = ExchangeId::new(normalized.clone())?;
        let gateway_client = Arc::new(InProcessGatewayClient::new(Arc::clone(&gateway)));
        let client = GatewayExchangeClient::new(
            gateway_client,
            tenant_id.clone(),
            Some(account_id.clone()),
            exchange_id.clone(),
        );
        let symbol = symbol_scope_for_pair(&normalized, &args.symbol)?;

        stages.push(
            call_stage("symbol_rules", || async {
                client
                    .get_symbol_rules(SymbolRulesRequest {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        context: request_context(&args, &normalized, "symbol-rules"),
                        symbols: vec![symbol.clone()],
                    })
                    .await
                    .map(|response| json!({"rules": response.rules.len()}))
            })
            .await,
        );
        stages.push(
            call_stage("funding_rates", || async {
                client
                    .get_funding_rates(FundingRatesRequest {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        context: request_context(&args, &normalized, "funding-rates"),
                        symbols: vec![symbol.clone()],
                    })
                    .await
                    .map(|response| {
                        json!({
                            "rates": response.rates.len(),
                            "has_next_funding_time": response.rates.iter().any(|rate| rate.next_funding_time.is_some()),
                            "has_mark_price": response.rates.iter().any(|rate| rate.mark_price.is_some()),
                            "has_index_price": response.rates.iter().any(|rate| rate.index_price.is_some()),
                        })
                    })
            })
            .await,
        );
        stages.push(
            call_stage("fees", || async {
                client
                    .get_fees(FeesRequest {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        context: request_context(&args, &normalized, "fees"),
                        symbols: vec![symbol.clone()],
                    })
                    .await
                    .map(|response| json!({"fees": response.fees.len()}))
            })
            .await,
        );
        stages.push(
            call_stage("order_book", || async {
                client
                    .get_order_book(OrderBookRequest {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        context: request_context(&args, &normalized, "order-book"),
                        symbol: symbol.clone(),
                        depth: Some(args.order_book_depth),
                    })
                    .await
                    .map(|response| {
                        json!({
                            "bids": response.order_book.bids.len(),
                            "asks": response.order_book.asks.len(),
                            "sequence": response.order_book.sequence,
                        })
                    })
            })
            .await,
        );
        stages.push(
            call_stage("balances", || async {
                client
                    .get_balances(BalancesRequest {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        context: request_context(&args, &normalized, "balances"),
                        exchange: exchange_id.clone(),
                        market_type: Some(MarketType::Perpetual),
                        assets: vec!["USDT".to_string()],
                    })
                    .await
                    .map(|response| json!({"balances": response.balances.len()}))
            })
            .await,
        );
        stages.push(
            call_stage("positions", || async {
                client
                    .get_positions(PositionsRequest {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        context: request_context(&args, &normalized, "positions"),
                        exchange: exchange_id.clone(),
                        market_type: Some(MarketType::Perpetual),
                        symbols: vec![symbol.exchange_symbol.clone()],
                    })
                    .await
                    .map(|response| json!({"positions": response.positions.len()}))
            })
            .await,
        );
        stages.push(
            call_stage("public_ws_order_book_subscription", || async {
                client
                    .subscribe_public_stream(PublicStreamSubscription {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        context: request_context(&args, &normalized, "public-ws-book"),
                        symbol: symbol.clone(),
                        kind: PublicStreamKind::OrderBookDelta,
                    })
                    .await
                    .map(|subscription_id| json!({"subscription_id": subscription_id}))
            })
            .await,
        );
        stages.push(
            call_stage("private_ws_subscription", || async {
                client
                    .subscribe_private_stream(PrivateStreamSubscription {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        context: request_context(&args, &normalized, "private-ws-account"),
                        exchange: exchange_id.clone(),
                        market_type: Some(MarketType::Perpetual),
                        account_id: account_id.clone(),
                        kind: PrivateStreamKind::Account,
                    })
                    .await
                    .map(|subscription_id| json!({"subscription_id": subscription_id}))
            })
            .await,
        );

        let private_ws_status = if args.private_ws_observe_duration_ms > 0 {
            observe_private_ws_once(&normalized, &args).await
        } else {
            None
        };
        let ready = stages.iter().all(|stage| stage.ok)
            && private_ws_status
                .as_ref()
                .map(private_ws_status_ready)
                .unwrap_or(true);
        reports.push(ContractReadonlyExchangeReport {
            exchange: normalized,
            symbol: args.symbol.clone(),
            ready,
            stages,
            private_ws_status,
        });
    }

    Ok(ContractReadonlyCanaryReport {
        generated_at: Utc::now(),
        read_only: true,
        live_orders_enabled: false,
        symbol: args.symbol,
        ready: reports.iter().all(|report| report.ready),
        exchanges: reports,
    })
}

async fn observe_private_ws_once(
    exchange: &str,
    args: &ContractReadonlyCanaryArgs,
) -> Option<Value> {
    let (tx, mut rx) = mpsc::channel::<PrivateWsObserveEvent>(128);
    let exchanges = vec![exchange.to_string()];
    let config = PrivateWsObserveConfig {
        timeout_ms: args.timeout_ms,
        reconnect_delay_ms: 1_000,
        gateio_user_id: None,
    };
    let handle = tokio::spawn(async move {
        run_private_ws_observe_once(&exchanges, config, tx).await;
    });
    let deadline = tokio::time::sleep(Duration::from_millis(
        args.private_ws_observe_duration_ms.max(1_000),
    ));
    tokio::pin!(deadline);
    let mut latest = None;
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            event = rx.recv() => {
                let Some(event) = event else {
                    break;
                };
                if let PrivateWsObserveEvent::Status { row, .. } = event {
                    latest = Some(row);
                }
            }
        }
    }
    handle.abort();
    latest
}

fn call_stage<F, Fut>(
    stage: &'static str,
    call: F,
) -> impl std::future::Future<Output = ContractReadonlyStageReport>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = rustcta_exchange_api::ExchangeApiResult<Value>>,
{
    async move {
        match call().await {
            Ok(details) => stage_ok(stage, details),
            Err(error) => stage_error(stage, error),
        }
    }
}

fn stage_ok(stage: &'static str, details: Value) -> ContractReadonlyStageReport {
    ContractReadonlyStageReport {
        stage,
        ok: true,
        details,
        error: None,
    }
}

fn stage_error(stage: &'static str, error: impl std::fmt::Display) -> ContractReadonlyStageReport {
    ContractReadonlyStageReport {
        stage,
        ok: false,
        details: json!({}),
        error: Some(error.to_string()),
    }
}

fn request_context(
    args: &ContractReadonlyCanaryArgs,
    exchange: &str,
    stage: &str,
) -> RequestContext {
    let mut context = RequestContext::new(Utc::now());
    context.tenant_id = TenantId::new(args.tenant_id.clone()).ok();
    context.account_id = AccountId::new(args.account_id.clone()).ok();
    context.run_id = RunId::new(args.run_id.clone()).ok();
    context.request_id = Some(format!("contract-readonly-canary:{exchange}:{stage}"));
    context
}

fn normalized_exchanges(exchanges: &[String]) -> Result<Vec<String>> {
    let mut normalized = Vec::new();
    for exchange in exchanges {
        let exchange = exchange.trim();
        if exchange.is_empty() {
            continue;
        }
        let exchange = normalize_contract_exchange(exchange)?;
        if !normalized.contains(&exchange) {
            normalized.push(exchange);
        }
    }
    if normalized.is_empty() {
        bail!("at least one exchange is required");
    }
    Ok(normalized)
}

fn normalize_contract_exchange(exchange: &str) -> Result<String> {
    Ok(match exchange.trim().to_ascii_lowercase().as_str() {
        "" => bail!("exchange id must not be empty"),
        "asterdex" | "aster_dex" | "aster-dex" => "aster".to_string(),
        "kucoin" => bail!("kucoin is the spot adapter; use kucoinfutures for contract canary"),
        "kucoin_futures" | "kucoin-futures" => "kucoinfutures".to_string(),
        other => other.to_string(),
    })
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

fn private_ws_status_ready(row: &Value) -> bool {
    row.get("connected")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        && row.get("login_ok").and_then(Value::as_bool).unwrap_or(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contract_canary_should_normalize_target_contract_venues() {
        assert_eq!(
            normalized_exchanges(&[
                "asterdex".to_string(),
                "mexc".to_string(),
                "kucoin-futures".to_string(),
                "bybit".to_string(),
            ])
            .expect("normalize"),
            vec!["aster", "mexc", "kucoinfutures", "bybit"]
        );
        assert!(normalize_contract_exchange("kucoin").is_err());
    }

    #[test]
    fn contract_canary_symbol_scope_should_use_contract_exchange_symbols() {
        assert_eq!(
            symbol_scope_for_pair("mexc", "BTC/USDT")
                .expect("mexc")
                .exchange_symbol
                .symbol,
            "BTC_USDT"
        );
        assert_eq!(
            symbol_scope_for_pair("kucoinfutures", "BTC/USDT")
                .expect("kucoin")
                .exchange_symbol
                .symbol,
            "XBTUSDTM"
        );
    }
}
