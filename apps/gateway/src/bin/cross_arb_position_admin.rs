use std::sync::Arc;

use anyhow::{bail, Context, Result};
use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OpenOrdersRequest,
    OrderSide, OrderType, PlaceOrderRequest, PositionSide, PositionsRequest, QueryOrderRequest,
    RecentFillsRequest, RequestContext, RunId, SymbolScope, TenantId, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_exchange_gateway::{GatewayClient, InProcessGatewayClient};
use rustcta_gateway_app::GatewayAppConfig;
use serde_json::json;
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Action {
    List,
    Close,
}

#[derive(Debug)]
struct Args {
    exchange: String,
    symbol: String,
    action: Action,
    position_side: Option<PositionSide>,
    quantity: Option<String>,
    execute: bool,
    confirm_close: bool,
    client_order_id: Option<String>,
    exchange_order_id: Option<String>,
    tenant_id: String,
    account_id: String,
    run_id: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args(std::env::args().skip(1))?;
    let exchange = normalize_exchange(&args.exchange);
    if !matches!(exchange.as_str(), "binance" | "bitget" | "gateio") {
        bail!("unsupported --exchange {exchange}; use binance, bitget, or gateio");
    }

    let exchange_id = ExchangeId::new(exchange.clone())?;
    let tenant_id = TenantId::new(args.tenant_id.clone())?;
    let account_id = AccountId::new(args.account_id.clone())?;
    let run_id = RunId::new(args.run_id.clone())?;
    let symbol = symbol_scope(&exchange_id, &exchange, &args.symbol)?;

    let config = gateway_config_from_env();
    let gateway = config.build_gateway()?;
    let client = InProcessGatewayClient::new(Arc::new(gateway));

    let before = read_state(
        &client,
        &tenant_id,
        &account_id,
        &run_id,
        &exchange_id,
        &symbol,
        "before",
    )
    .await?;

    let mut close_result = None;
    if args.action == Action::Close {
        if !args.execute || !args.confirm_close {
            bail!("close requires both --execute and --confirm-close");
        }
        let position_side = args
            .position_side
            .context("--position-side is required for --action close")?;
        let quantity = args
            .quantity
            .as_deref()
            .context("--quantity is required for --action close")?;
        let request_id = format!("cross-arb-admin-close-{}", Utc::now().timestamp_millis());
        let close = client
            .place_order(
                request_id.clone(),
                tenant_id.clone(),
                Some(account_id.clone()),
                close_order_request(
                    &request_id,
                    &tenant_id,
                    &account_id,
                    &run_id,
                    symbol.clone(),
                    &exchange,
                    position_side,
                    quantity,
                )?,
            )
            .await
            .context("place close order")?;
        close_result = Some(close);
        sleep(Duration::from_secs(2)).await;
    }

    let query_client_order_id = close_result
        .as_ref()
        .and_then(|response| response.order.client_order_id.clone())
        .or_else(|| args.client_order_id.clone());
    let query_exchange_order_id = close_result
        .as_ref()
        .and_then(|response| response.order.exchange_order_id.clone())
        .or_else(|| args.exchange_order_id.clone());
    let order_reconciliation =
        if query_client_order_id.is_some() || query_exchange_order_id.is_some() {
            Some(
                read_order_reconciliation(
                    &client,
                    &tenant_id,
                    &account_id,
                    &run_id,
                    &exchange_id,
                    &symbol,
                    query_client_order_id,
                    query_exchange_order_id,
                )
                .await?,
            )
        } else {
            None
        };

    let after = read_state(
        &client,
        &tenant_id,
        &account_id,
        &run_id,
        &exchange_id,
        &symbol,
        "after",
    )
    .await?;

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema_version": 1,
            "tool": "cross-arb-position-admin",
            "generated_at": Utc::now(),
            "exchange": exchange,
            "symbol": args.symbol,
            "exchange_symbol": symbol.exchange_symbol.symbol,
            "action": match args.action {
                Action::List => "list",
                Action::Close => "close",
            },
            "executed": close_result.is_some(),
            "before": before,
            "close_result": close_result,
            "order_reconciliation": order_reconciliation,
            "after": after,
        }))?
    );
    Ok(())
}

async fn read_state<G>(
    client: &InProcessGatewayClient<G>,
    tenant_id: &TenantId,
    account_id: &AccountId,
    run_id: &RunId,
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    label: &str,
) -> Result<serde_json::Value>
where
    InProcessGatewayClient<G>: GatewayClient,
{
    let position_request_id = format!("{label}-positions-{}", Utc::now().timestamp_millis());
    let positions_result = client
        .get_positions(
            position_request_id.clone(),
            tenant_id.clone(),
            Some(account_id.clone()),
            PositionsRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context(&position_request_id, tenant_id, account_id, run_id),
                exchange: exchange_id.clone(),
                market_type: Some(MarketType::Perpetual),
                symbols: vec![symbol.exchange_symbol.clone()],
            },
        )
        .await;

    let open_orders_request_id = format!("{label}-open-orders-{}", Utc::now().timestamp_millis());
    let open_orders_result = client
        .get_open_orders(
            open_orders_request_id.clone(),
            tenant_id.clone(),
            Some(account_id.clone()),
            OpenOrdersRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context(&open_orders_request_id, tenant_id, account_id, run_id),
                exchange: exchange_id.clone(),
                market_type: Some(MarketType::Perpetual),
                symbol: Some(symbol.clone()),
                page: None,
            },
        )
        .await;

    let positions = match positions_result {
        Ok(response) => json!({
            "status": "ok",
            "count": response.positions.len(),
            "items": response.positions,
        }),
        Err(error) => json!({
            "status": "error",
            "error": error.to_string(),
        }),
    };
    let open_orders = match open_orders_result {
        Ok(response) => json!({
            "status": "ok",
            "count": response.orders.len(),
            "items": response.orders,
        }),
        Err(error) => json!({
            "status": "error",
            "error": error.to_string(),
        }),
    };

    Ok(json!({
        "positions": positions,
        "open_orders": open_orders,
    }))
}

async fn read_order_reconciliation<G>(
    client: &InProcessGatewayClient<G>,
    tenant_id: &TenantId,
    account_id: &AccountId,
    run_id: &RunId,
    exchange_id: &ExchangeId,
    symbol: &SymbolScope,
    client_order_id: Option<String>,
    exchange_order_id: Option<String>,
) -> Result<serde_json::Value>
where
    InProcessGatewayClient<G>: GatewayClient,
{
    let query_request_id = format!("query-order-{}", Utc::now().timestamp_millis());
    let query_order_result = client
        .query_order(
            query_request_id.clone(),
            tenant_id.clone(),
            Some(account_id.clone()),
            QueryOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context(&query_request_id, tenant_id, account_id, run_id),
                symbol: symbol.clone(),
                client_order_id: client_order_id.clone(),
                exchange_order_id: exchange_order_id.clone(),
            },
        )
        .await;

    let fills_request_id = format!("recent-fills-{}", Utc::now().timestamp_millis());
    let fills_result = client
        .get_recent_fills(
            fills_request_id.clone(),
            tenant_id.clone(),
            Some(account_id.clone()),
            RecentFillsRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context(&fills_request_id, tenant_id, account_id, run_id),
                exchange: exchange_id.clone(),
                market_type: Some(MarketType::Perpetual),
                symbol: Some(symbol.clone()),
                client_order_id,
                exchange_order_id,
                from_trade_id: None,
                start_time: None,
                end_time: None,
                limit: Some(20),
                page: None,
            },
        )
        .await;

    let query_order = match query_order_result {
        Ok(response) => json!({
            "status": "ok",
            "order": response.order,
        }),
        Err(error) => json!({
            "status": "error",
            "error": error.to_string(),
        }),
    };
    let recent_fills = match fills_result {
        Ok(response) => json!({
            "status": "ok",
            "count": response.fills.len(),
            "items": response.fills,
        }),
        Err(error) => json!({
            "status": "error",
            "error": error.to_string(),
        }),
    };

    Ok(json!({
        "query_order": query_order,
        "recent_fills": recent_fills,
    }))
}

fn close_order_request(
    request_id: &str,
    tenant_id: &TenantId,
    account_id: &AccountId,
    run_id: &RunId,
    symbol: SymbolScope,
    exchange: &str,
    position_side: PositionSide,
    quantity: &str,
) -> Result<PlaceOrderRequest> {
    let side = match position_side {
        PositionSide::Long => OrderSide::Sell,
        PositionSide::Short => OrderSide::Buy,
        PositionSide::Net | PositionSide::None => {
            bail!("--position-side must be long or short for close")
        }
    };
    let reduce_only = match exchange {
        "binance" => false,
        "bitget" | "gateio" => true,
        _ => true,
    };
    Ok(PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(request_id, tenant_id, account_id, run_id),
        symbol,
        client_order_id: Some(safe_client_order_id("ca-admin")),
        side,
        position_side: Some(position_side),
        order_type: OrderType::Market,
        time_in_force: None::<TimeInForce>,
        quantity: quantity.trim().to_string(),
        price: None,
        quote_quantity: None,
        reduce_only,
        post_only: false,
    })
}

fn context(
    request_id: &str,
    tenant_id: &TenantId,
    account_id: &AccountId,
    run_id: &RunId,
) -> RequestContext {
    let mut context = RequestContext::new(Utc::now());
    context.tenant_id = Some(tenant_id.clone());
    context.account_id = Some(account_id.clone());
    context.run_id = Some(run_id.clone());
    context.request_id = Some(request_id.to_string());
    context
}

fn symbol_scope(exchange_id: &ExchangeId, exchange: &str, symbol: &str) -> Result<SymbolScope> {
    let canonical_symbol = CanonicalSymbol::parse(symbol)?;
    let exchange_symbol = match exchange {
        "gateio" => symbol.replace('/', "_").to_ascii_uppercase(),
        "bitget" | "binance" => symbol.replace(['/', '-', '_'], "").to_ascii_uppercase(),
        _ => symbol.replace('/', "").to_ascii_uppercase(),
    };
    Ok(SymbolScope {
        exchange: exchange_id.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(canonical_symbol),
        exchange_symbol: ExchangeSymbol::new(
            exchange_id.clone(),
            MarketType::Perpetual,
            exchange_symbol,
        )?,
    })
}

fn safe_client_order_id(prefix: &str) -> String {
    let millis = Utc::now().timestamp_millis().rem_euclid(1_000_000_000_000);
    format!("{prefix}-{millis}")
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Args> {
    let mut exchange = None;
    let mut symbol = None;
    let mut action = Action::List;
    let mut position_side = None;
    let mut quantity = None;
    let mut execute = false;
    let mut confirm_close = false;
    let mut client_order_id = None;
    let mut exchange_order_id = None;
    let mut tenant_id = "cross_arb".to_string();
    let mut account_id = "cross_arb_3venues".to_string();
    let mut run_id = "cross_arb_position_admin".to_string();
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--exchange" => exchange = Some(next_value(&mut args, "--exchange")?),
            "--symbol" => symbol = Some(next_value(&mut args, "--symbol")?),
            "--action" => {
                action = match next_value(&mut args, "--action")?.as_str() {
                    "list" => Action::List,
                    "close" => Action::Close,
                    other => bail!("unsupported --action {other}; use list or close"),
                };
            }
            "--position-side" => {
                position_side = Some(match next_value(&mut args, "--position-side")?.as_str() {
                    "long" => PositionSide::Long,
                    "short" => PositionSide::Short,
                    other => bail!("unsupported --position-side {other}; use long or short"),
                });
            }
            "--quantity" => quantity = Some(next_value(&mut args, "--quantity")?),
            "--execute" => execute = true,
            "--confirm-close" => confirm_close = true,
            "--client-order-id" => {
                client_order_id = Some(next_value(&mut args, "--client-order-id")?)
            }
            "--exchange-order-id" => {
                exchange_order_id = Some(next_value(&mut args, "--exchange-order-id")?)
            }
            "--tenant-id" => tenant_id = next_value(&mut args, "--tenant-id")?,
            "--account-id" => account_id = next_value(&mut args, "--account-id")?,
            "--run-id" => run_id = next_value(&mut args, "--run-id")?,
            "-h" | "--help" => bail!("{}", usage()),
            other => bail!("unsupported argument {other}\n{}", usage()),
        }
    }

    Ok(Args {
        exchange: exchange.context("--exchange is required")?,
        symbol: symbol.context("--symbol is required")?,
        action,
        position_side,
        quantity,
        execute,
        confirm_close,
        client_order_id,
        exchange_order_id,
        tenant_id,
        account_id,
        run_id,
    })
}

fn next_value(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String> {
    args.next()
        .with_context(|| format!("{name} requires a value"))
}

fn usage() -> &'static str {
    "usage: cross-arb-position-admin --exchange <binance|bitget|gateio> --symbol ESPORTS/USDT [--action list|close] [--position-side long|short --quantity 80 --execute --confirm-close]"
}

fn normalize_exchange(exchange: &str) -> String {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "gate" | "gate.io" | "gate_io" => "gateio".to_string(),
        other => other.to_string(),
    }
}

fn gateway_config_from_env() -> GatewayAppConfig {
    GatewayAppConfig::from_env_reader(gateway_env)
}

fn gateway_env(key: &str) -> Option<String> {
    match key {
        "RUSTCTA_GATEWAY_ADAPTERS" => {
            first_env(&[key]).or_else(|| Some("binance,bitget,gateio".to_string()))
        }
        "RUSTCTA_BINANCE_API_KEY" => first_env(&[key, "BINANCE_0_API_KEY", "BINANCE_API_KEY"]),
        "RUSTCTA_BINANCE_API_SECRET" => {
            first_env(&[key, "BINANCE_0_API_SECRET", "BINANCE_API_SECRET"])
        }
        "RUSTCTA_BITGET_API_KEY" => first_env(&[key, "BITGET_API_KEY"]),
        "RUSTCTA_BITGET_API_SECRET" => first_env(&[key, "BITGET_API_SECRET"]),
        "RUSTCTA_BITGET_API_PASSPHRASE" => {
            first_env(&[key, "BITGET_PASSPHRASE", "BITGET_API_PASSPHRASE"])
        }
        "RUSTCTA_GATEIO_API_KEY" => first_env(&[
            key,
            "GATEIO_API_KEY",
            "GATE_API_KEY",
            "GATE__16076371__API_KEY",
            "GATE__53636022__API_KEY",
        ]),
        "RUSTCTA_GATEIO_API_SECRET" => first_env(&[
            key,
            "GATEIO_API_SECRET",
            "GATE_API_SECRET",
            "GATE__16076371__API_SECRET",
            "GATE__53636022__API_SECRET",
        ]),
        _ => std::env::var(key)
            .ok()
            .filter(|value| !value.trim().is_empty()),
    }
}

fn first_env(keys: &[&str]) -> Option<String> {
    keys.iter()
        .filter_map(|key| std::env::var(key).ok())
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
}
