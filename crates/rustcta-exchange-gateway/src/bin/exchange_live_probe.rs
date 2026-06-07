use std::future::Future;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, BalancesRequest, CanonicalSymbol, ExchangeId, ExchangeSymbol, FeesRequest,
    MarketType, OpenOrdersRequest, OrderBookRequest, PositionsRequest, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, RecentFillsRequest,
    RequestContext, SymbolRulesRequest, SymbolScope, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_exchange_gateway::{
    AdapterBackedGateway, BinanceGatewayConfig, GatewayClient, GatewayError,
    GetCapabilitiesRequest, InProcessGatewayClient, OkxGatewayConfig, SubscribeBooksRequest,
    SubscribePrivateRequest, GATEWAY_PROTOCOL_SCHEMA_VERSION,
};
use serde::Serialize;
use serde_json::json;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputFormat {
    Json,
    Text,
}

#[derive(Debug)]
struct Args {
    exchange: String,
    symbol: Option<String>,
    read_only: bool,
    fail_on_error: bool,
    format: OutputFormat,
}

#[derive(Debug, Serialize)]
struct ProbeReport {
    schema_version: u16,
    exchange: String,
    read_only: bool,
    symbol: String,
    generated_at: String,
    credentials: CredentialReport,
    checks: Vec<ProbeCheck>,
}

#[derive(Debug, Serialize)]
struct CredentialReport {
    private_credentials_detected: bool,
    private_read_checks_enabled: bool,
    source: &'static str,
}

#[derive(Debug, Serialize)]
struct ProbeCheck {
    name: &'static str,
    status: CheckStatus,
    detail: String,
    data: serde_json::Value,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CheckStatus {
    Ok,
    Failed,
    Skipped,
    Unsupported,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok((has_failures, fail_on_error)) => {
            if has_failures && fail_on_error {
                ExitCode::from(2)
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(1)
        }
    }
}

async fn run() -> Result<(bool, bool), String> {
    let args = parse_args(std::env::args().skip(1))?;
    if !args.read_only {
        return Err(
            "exchange_live_probe requires --read-only; no live checks were executed".to_string(),
        );
    }

    let exchange = normalize_exchange(&args.exchange);
    if !matches!(exchange.as_str(), "binance" | "okx") {
        let report = unsupported_report(&exchange, args.symbol.as_deref().unwrap_or("BTC/USDT"));
        emit_report(&report, args.format)?;
        return Ok((false, args.fail_on_error));
    }

    let symbol_text = args
        .symbol
        .unwrap_or_else(|| default_symbol(&exchange).to_string());
    let exchange_id = ExchangeId::new(exchange.clone()).map_err(|error| error.to_string())?;
    let symbol = symbol_scope(&exchange_id, &exchange, &symbol_text)?;
    let credentials = credential_report(&exchange);
    let gateway = build_gateway(&exchange)?;
    let client = InProcessGatewayClient::new(Arc::new(gateway));
    let tenant_id = TenantId::new("exchange_live_probe").map_err(|error| error.to_string())?;
    let account_id = AccountId::new("read_only_probe").map_err(|error| error.to_string())?;

    let mut report = ProbeReport {
        schema_version: 1,
        exchange: exchange.clone(),
        read_only: true,
        symbol: symbol_text,
        generated_at: Utc::now().to_rfc3339(),
        credentials,
        checks: Vec::new(),
    };

    report
        .checks
        .push(server_time_check(&exchange, rest_base_url(&exchange)).await);
    report
        .checks
        .push(capabilities_check(&client, &tenant_id, &exchange_id).await);
    report
        .checks
        .push(symbol_rules_check(&client, &tenant_id, &account_id, &symbol).await);
    report
        .checks
        .push(order_book_check(&client, &tenant_id, &account_id, &symbol).await);
    report
        .checks
        .push(public_stream_check(&client, &tenant_id, &account_id, &symbol).await);

    if report.credentials.private_read_checks_enabled {
        report
            .checks
            .push(fees_check(&client, &tenant_id, &account_id, &symbol).await);
        report
            .checks
            .push(balances_check(&client, &tenant_id, &account_id, &exchange_id).await);
        report
            .checks
            .push(positions_check(&client, &tenant_id, &account_id, &exchange_id).await);
        report
            .checks
            .push(open_orders_check(&client, &tenant_id, &account_id, &exchange_id, &symbol).await);
        report.checks.push(
            recent_fills_check(&client, &tenant_id, &account_id, &exchange_id, &symbol).await,
        );
        report
            .checks
            .push(private_stream_check(&client, &tenant_id, &account_id, &exchange_id).await);
    } else {
        for name in [
            "fee_readback",
            "balances",
            "positions",
            "open_orders",
            "recent_fills_dry_read",
            "private_ws_auth_subscribe",
        ] {
            report.checks.push(ProbeCheck {
                name,
                status: CheckStatus::Skipped,
                detail: "private read check skipped because credentials are not present"
                    .to_string(),
                data: json!({}),
            });
        }
    }

    let has_failures = report
        .checks
        .iter()
        .any(|check| check.status == CheckStatus::Failed);
    emit_report(&report, args.format)?;
    Ok((has_failures, args.fail_on_error))
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Args, String> {
    let mut exchange = None;
    let mut symbol = None;
    let mut read_only = false;
    let mut fail_on_error = false;
    let mut format = OutputFormat::Json;
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--read-only" => read_only = true,
            "--fail-on-error" => fail_on_error = true,
            "--exchange" => {
                exchange = Some(
                    args.next()
                        .ok_or_else(|| "--exchange requires a value".to_string())?,
                );
            }
            "--symbol" => {
                symbol = Some(
                    args.next()
                        .ok_or_else(|| "--symbol requires a value".to_string())?,
                );
            }
            "--format" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--format requires json or text".to_string())?;
                format = match value.as_str() {
                    "json" => OutputFormat::Json,
                    "text" => OutputFormat::Text,
                    other => return Err(format!("unsupported --format {other}; use json or text")),
                };
            }
            "-h" | "--help" => return Err(usage()),
            other => return Err(format!("unsupported argument {other}\n{}", usage())),
        }
    }

    Ok(Args {
        exchange: exchange.ok_or_else(usage)?,
        symbol,
        read_only,
        fail_on_error,
        format,
    })
}

fn usage() -> String {
    "usage: exchange_live_probe --read-only --exchange <binance|okx> [--symbol BTC/USDT] [--format json|text] [--fail-on-error]".to_string()
}

fn normalize_exchange(exchange: &str) -> String {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "okex" => "okx".to_string(),
        other => other.to_string(),
    }
}

fn default_symbol(exchange: &str) -> &'static str {
    match exchange {
        "okx" => "BTC/USDT",
        _ => "BTC/USDT",
    }
}

fn credential_report(exchange: &str) -> CredentialReport {
    let detected = match exchange {
        "binance" => {
            env_any(["BINANCE_SPOT_API_KEY", "BINANCE_API_KEY"])
                && env_any(["BINANCE_SPOT_API_SECRET", "BINANCE_API_SECRET"])
        }
        "okx" => {
            env_any(["OKX_API_KEY", "OKX_SPOT_API_KEY"])
                && env_any(["OKX_API_SECRET", "OKX_SPOT_API_SECRET"])
                && env_any(["OKX_PASSPHRASE", "OKX_SPOT_PASSPHRASE"])
        }
        _ => false,
    };
    CredentialReport {
        private_credentials_detected: detected,
        private_read_checks_enabled: detected,
        source: "environment_presence_only",
    }
}

fn env_any<const N: usize>(keys: [&str; N]) -> bool {
    keys.into_iter()
        .filter_map(|key| std::env::var(key).ok())
        .any(|value| !value.trim().is_empty())
}

fn rest_base_url(exchange: &str) -> &'static str {
    match exchange {
        "okx" => "https://www.okx.com",
        _ => "https://api.binance.com",
    }
}

fn build_gateway(exchange: &str) -> Result<AdapterBackedGateway, String> {
    let gateway = AdapterBackedGateway::new("exchange_live_probe");
    match exchange {
        "binance" => gateway
            .register_binance_adapter(BinanceGatewayConfig::default())
            .map_err(|error| error.to_string())?,
        "okx" => gateway
            .register_okx_adapter(OkxGatewayConfig::default())
            .map_err(|error| error.to_string())?,
        other => {
            return Err(format!(
                "exchange {other} is unsupported by exchange_live_probe"
            ))
        }
    }
    Ok(gateway)
}

fn symbol_scope(
    exchange_id: &ExchangeId,
    exchange: &str,
    symbol: &str,
) -> Result<SymbolScope, String> {
    let canonical_symbol = CanonicalSymbol::parse(symbol).map_err(|error| error.to_string())?;
    let exchange_symbol = match exchange {
        "okx" => symbol.replace('/', "-"),
        _ => symbol.replace('/', ""),
    };
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Spot,
            exchange_symbol,
        )
        .map_err(|error| error.to_string())?,
    })
}

fn context(
    request_id: &str,
    tenant_id: &TenantId,
    account_id: Option<&AccountId>,
) -> RequestContext {
    let mut context = RequestContext::new(Utc::now());
    context.tenant_id = Some(tenant_id.clone());
    context.account_id = account_id.cloned();
    context.request_id = Some(request_id.to_string());
    context
}

async fn capture<T, F, Fut, S>(name: &'static str, future: F, summarize: S) -> ProbeCheck
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, GatewayError>>,
    S: FnOnce(T) -> (String, serde_json::Value),
{
    match future().await {
        Ok(response) => {
            let (detail, data) = summarize(response);
            ProbeCheck {
                name,
                status: CheckStatus::Ok,
                detail,
                data,
            }
        }
        Err(error) if is_unsupported(&error) => ProbeCheck {
            name,
            status: CheckStatus::Unsupported,
            detail: error.to_string(),
            data: json!({}),
        },
        Err(error) => ProbeCheck {
            name,
            status: CheckStatus::Failed,
            detail: error.to_string(),
            data: json!({}),
        },
    }
}

fn is_unsupported(error: &GatewayError) -> bool {
    error
        .to_string()
        .to_ascii_lowercase()
        .contains("unsupported")
}

async fn server_time_check(exchange: &str, base_url: &str) -> ProbeCheck {
    let path = match exchange {
        "okx" => "/api/v5/public/time",
        _ => "/api/v3/time",
    };
    let url = format!("{base_url}{path}");
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .user_agent("RustCTA-Gateway/0.3 exchange_live_probe")
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            return ProbeCheck {
                name: "server_time",
                status: CheckStatus::Failed,
                detail: error.to_string(),
                data: json!({}),
            }
        }
    };

    match client.get(&url).send().await {
        Ok(response) => {
            let status = response.status();
            match response.json::<serde_json::Value>().await {
                Ok(value) if status.is_success() => ProbeCheck {
                    name: "server_time",
                    status: CheckStatus::Ok,
                    detail: format!("{exchange} server time endpoint responded"),
                    data: json!({ "http_status": status.as_u16(), "response": value }),
                },
                Ok(value) => ProbeCheck {
                    name: "server_time",
                    status: CheckStatus::Failed,
                    detail: format!("{exchange} server time endpoint returned HTTP {status}"),
                    data: json!({ "http_status": status.as_u16(), "response": value }),
                },
                Err(error) => ProbeCheck {
                    name: "server_time",
                    status: CheckStatus::Failed,
                    detail: error.to_string(),
                    data: json!({ "http_status": status.as_u16() }),
                },
            }
        }
        Err(error) => ProbeCheck {
            name: "server_time",
            status: CheckStatus::Failed,
            detail: error.to_string(),
            data: json!({}),
        },
    }
}

async fn capabilities_check(
    client: &InProcessGatewayClient<AdapterBackedGateway>,
    tenant_id: &TenantId,
    exchange_id: &ExchangeId,
) -> ProbeCheck {
    capture(
        "capabilities",
        || {
            client.get_capabilities(
                "exchange_live_probe.capabilities".to_string(),
                tenant_id.clone(),
                None,
                GetCapabilitiesRequest {
                    schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                    context: context("exchange_live_probe.capabilities", tenant_id, None),
                    exchanges: vec![exchange_id.clone()],
                },
            )
        },
        |response| {
            let Some(capabilities) = response.capabilities.first() else {
                return (
                    "no capabilities returned".to_string(),
                    json!({ "capabilities_count": 0 }),
                );
            };
            (
                "capabilities loaded".to_string(),
                json!({
                    "market_types": &capabilities.market_types,
                    "supports_public_rest": capabilities.supports_public_rest,
                    "supports_private_rest": capabilities.supports_private_rest,
                    "supports_symbol_rules": capabilities.supports_symbol_rules,
                    "supports_order_book_snapshot": capabilities.supports_order_book_snapshot,
                    "supports_balances": capabilities.supports_balances,
                    "supports_fees": capabilities.supports_fees,
                    "supports_positions": capabilities.supports_positions,
                    "supports_open_orders": capabilities.supports_open_orders,
                    "supports_recent_fills": capabilities.supports_recent_fills,
                    "v2": &capabilities.capabilities_v2,
                }),
            )
        },
    )
    .await
}

async fn symbol_rules_check(
    client: &InProcessGatewayClient<AdapterBackedGateway>,
    tenant_id: &TenantId,
    account_id: &AccountId,
    symbol: &SymbolScope,
) -> ProbeCheck {
    capture(
        "market_rules",
        || {
            client.get_symbol_rules(
                "exchange_live_probe.market_rules".to_string(),
                tenant_id.clone(),
                Some(account_id.clone()),
                SymbolRulesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context(
                        "exchange_live_probe.market_rules",
                        tenant_id,
                        Some(account_id),
                    ),
                    symbols: vec![symbol.clone()],
                },
            )
        },
        |response| {
            (
                format!("{} market rule row(s)", response.rules.len()),
                json!({ "rules_count": response.rules.len(), "metadata": response.metadata }),
            )
        },
    )
    .await
}

async fn order_book_check(
    client: &InProcessGatewayClient<AdapterBackedGateway>,
    tenant_id: &TenantId,
    account_id: &AccountId,
    symbol: &SymbolScope,
) -> ProbeCheck {
    capture(
        "order_book",
        || {
            client.get_order_book(
                "exchange_live_probe.order_book".to_string(),
                tenant_id.clone(),
                Some(account_id.clone()),
                OrderBookRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context(
                        "exchange_live_probe.order_book",
                        tenant_id,
                        Some(account_id),
                    ),
                    symbol: symbol.clone(),
                    depth: Some(5),
                },
            )
        },
        |response| {
            let book = response.order_book;
            (
                format!("{} bid(s), {} ask(s)", book.bids.len(), book.asks.len()),
                json!({
                    "bids": book.bids.len(),
                    "asks": book.asks.len(),
                    "best_bid": book.best_bid(),
                    "best_ask": book.best_ask(),
                    "metadata": response.metadata,
                }),
            )
        },
    )
    .await
}

async fn public_stream_check(
    client: &InProcessGatewayClient<AdapterBackedGateway>,
    tenant_id: &TenantId,
    account_id: &AccountId,
    symbol: &SymbolScope,
) -> ProbeCheck {
    capture(
        "public_ws_subscribe",
        || {
            client.subscribe_books(
                "exchange_live_probe.public_ws".to_string(),
                tenant_id.clone(),
                Some(account_id.clone()),
                SubscribeBooksRequest {
                    schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                    context: context("exchange_live_probe.public_ws", tenant_id, Some(account_id)),
                    subscriptions: vec![PublicStreamSubscription {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        context: context(
                            "exchange_live_probe.public_ws",
                            tenant_id,
                            Some(account_id),
                        ),
                        symbol: symbol.clone(),
                        kind: PublicStreamKind::OrderBookSnapshot,
                    }],
                },
            )
        },
        |response| {
            (
                format!(
                    "{} public stream subscription ack(s)",
                    response.subscriptions.len()
                ),
                json!({ "subscriptions": response.subscriptions }),
            )
        },
    )
    .await
}

async fn fees_check(
    client: &InProcessGatewayClient<AdapterBackedGateway>,
    tenant_id: &TenantId,
    account_id: &AccountId,
    symbol: &SymbolScope,
) -> ProbeCheck {
    capture(
        "fee_readback",
        || {
            client.get_fees(
                "exchange_live_probe.fees".to_string(),
                tenant_id.clone(),
                Some(account_id.clone()),
                FeesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("exchange_live_probe.fees", tenant_id, Some(account_id)),
                    symbols: vec![symbol.clone()],
                },
            )
        },
        |response| {
            (
                format!("{} fee row(s)", response.fees.len()),
                json!({ "fees_count": response.fees.len(), "metadata": response.metadata }),
            )
        },
    )
    .await
}

async fn balances_check(
    client: &InProcessGatewayClient<AdapterBackedGateway>,
    tenant_id: &TenantId,
    account_id: &AccountId,
    exchange_id: &ExchangeId,
) -> ProbeCheck {
    capture(
        "balances",
        || {
            client.get_balances(
                "exchange_live_probe.balances".to_string(),
                tenant_id.clone(),
                Some(account_id.clone()),
                BalancesRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("exchange_live_probe.balances", tenant_id, Some(account_id)),
                    exchange: exchange_id.clone(),
                    market_type: Some(MarketType::Spot),
                    assets: Vec::new(),
                },
            )
        },
        |response| {
            (
                format!("{} balance snapshot(s)", response.balances.len()),
                json!({ "balances_count": response.balances.len(), "metadata": response.metadata }),
            )
        },
    )
    .await
}

async fn positions_check(
    client: &InProcessGatewayClient<AdapterBackedGateway>,
    tenant_id: &TenantId,
    account_id: &AccountId,
    exchange_id: &ExchangeId,
) -> ProbeCheck {
    capture(
        "positions",
        || {
            client.get_positions(
                "exchange_live_probe.positions".to_string(),
                tenant_id.clone(),
                Some(account_id.clone()),
                PositionsRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("exchange_live_probe.positions", tenant_id, Some(account_id)),
                    exchange: exchange_id.clone(),
                    market_type: Some(MarketType::Spot),
                    symbols: Vec::new(),
                },
            )
        },
        |response| {
            (
                format!("{} position(s)", response.positions.len()),
                json!({ "positions_count": response.positions.len(), "metadata": response.metadata }),
            )
        },
    )
    .await
}

async fn open_orders_check(
    client: &InProcessGatewayClient<AdapterBackedGateway>,
    tenant_id: &TenantId,
    account_id: &AccountId,
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
) -> ProbeCheck {
    capture(
        "open_orders",
        || {
            client.get_open_orders(
                "exchange_live_probe.open_orders".to_string(),
                tenant_id.clone(),
                Some(account_id.clone()),
                OpenOrdersRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context(
                        "exchange_live_probe.open_orders",
                        tenant_id,
                        Some(account_id),
                    ),
                    exchange: exchange_id.clone(),
                    market_type: Some(MarketType::Spot),
                    symbol: Some(symbol.clone()),
                    page: None,
                },
            )
        },
        |response| {
            (
                format!("{} open order(s)", response.orders.len()),
                json!({ "orders_count": response.orders.len(), "metadata": response.metadata }),
            )
        },
    )
    .await
}

async fn recent_fills_check(
    client: &InProcessGatewayClient<AdapterBackedGateway>,
    tenant_id: &TenantId,
    account_id: &AccountId,
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
) -> ProbeCheck {
    capture(
        "recent_fills_dry_read",
        || {
            client.get_recent_fills(
                "exchange_live_probe.recent_fills".to_string(),
                tenant_id.clone(),
                Some(account_id.clone()),
                RecentFillsRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context(
                        "exchange_live_probe.recent_fills",
                        tenant_id,
                        Some(account_id),
                    ),
                    exchange: exchange_id.clone(),
                    market_type: Some(MarketType::Spot),
                    symbol: Some(symbol.clone()),
                    client_order_id: None,
                    exchange_order_id: None,
                    from_trade_id: None,
                    start_time: None,
                    end_time: None,
                    limit: Some(10),
                    page: None,
                },
            )
        },
        |response| {
            (
                format!("{} recent fill(s)", response.fills.len()),
                json!({ "fills_count": response.fills.len(), "metadata": response.metadata }),
            )
        },
    )
    .await
}

async fn private_stream_check(
    client: &InProcessGatewayClient<AdapterBackedGateway>,
    tenant_id: &TenantId,
    account_id: &AccountId,
    exchange_id: &ExchangeId,
) -> ProbeCheck {
    capture(
        "private_ws_auth_subscribe",
        || {
            client.subscribe_private(
                "exchange_live_probe.private_ws".to_string(),
                tenant_id.clone(),
                Some(account_id.clone()),
                SubscribePrivateRequest {
                    schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                    context: context(
                        "exchange_live_probe.private_ws",
                        tenant_id,
                        Some(account_id),
                    ),
                    subscriptions: vec![PrivateStreamSubscription {
                        schema_version: EXCHANGE_API_SCHEMA_VERSION,
                        context: context(
                            "exchange_live_probe.private_ws",
                            tenant_id,
                            Some(account_id),
                        ),
                        exchange: exchange_id.clone(),
                        market_type: Some(MarketType::Spot),
                        account_id: account_id.clone(),
                        kind: PrivateStreamKind::Account,
                    }],
                },
            )
        },
        |response| {
            (
                format!(
                    "{} private stream subscription ack(s)",
                    response.subscriptions.len()
                ),
                json!({ "subscriptions": response.subscriptions }),
            )
        },
    )
    .await
}

fn unsupported_report(exchange: &str, symbol: &str) -> ProbeReport {
    ProbeReport {
        schema_version: 1,
        exchange: exchange.to_string(),
        read_only: true,
        symbol: symbol.to_string(),
        generated_at: Utc::now().to_rfc3339(),
        credentials: CredentialReport {
            private_credentials_detected: false,
            private_read_checks_enabled: false,
            source: "unsupported_exchange",
        },
        checks: vec![ProbeCheck {
            name: "exchange_support",
            status: CheckStatus::Unsupported,
            detail: "initial live probe supports binance and okx only".to_string(),
            data: json!({ "supported_exchanges": ["binance", "okx"] }),
        }],
    }
}

fn emit_report(report: &ProbeReport, format: OutputFormat) -> Result<(), String> {
    match format {
        OutputFormat::Json => {
            let value = serde_json::to_string_pretty(report).map_err(|error| error.to_string())?;
            println!("{value}");
        }
        OutputFormat::Text => {
            println!(
                "exchange_live_probe exchange={} read_only={} symbol={} generated_at={}",
                report.exchange, report.read_only, report.symbol, report.generated_at
            );
            println!(
                "credentials private_detected={} private_read_checks_enabled={}",
                report.credentials.private_credentials_detected,
                report.credentials.private_read_checks_enabled
            );
            for check in &report.checks {
                println!("{} {:?}: {}", check.name, check.status, check.detail);
            }
        }
    }
    Ok(())
}
