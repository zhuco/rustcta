use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, CancelOrderRequest, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType,
    OrderSide, OrderStatus, OrderType, PlaceOrderRequest, PositionSide, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, QueryOrderRequest,
    RequestContext, SymbolScope, TenantId, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_exchange_gateway::{
    GatewayClient, GetCapabilitiesRequest, InProcessGatewayClient, MockExchangeGateway,
    SubscribeBooksRequest, SubscribePrivateRequest, GATEWAY_PROTOCOL_SCHEMA_VERSION,
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let tenant_id = TenantId::new("local-gateway-smoke")?;
    let account_id = AccountId::new("local-test-account")?;
    let exchange = ExchangeId::new("mock")?;
    let symbol = symbol_scope(&exchange)?;
    let gateway = Arc::new(MockExchangeGateway::with_exchanges(
        "local-gateway-smoke",
        [exchange.clone()],
    ));
    let client = InProcessGatewayClient::new(gateway);

    let capabilities = client
        .get_capabilities(
            "local-smoke-capabilities".to_string(),
            tenant_id.clone(),
            Some(account_id.clone()),
            GetCapabilitiesRequest {
                schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                context: context("local-smoke-capabilities", &tenant_id, &account_id),
                exchanges: vec![exchange.clone()],
            },
        )
        .await
        .context("get mock gateway capabilities")?;
    let capability = capabilities
        .capabilities
        .first()
        .context("mock gateway returned no capability row")?;
    ensure!(
        capability.supports_place_order,
        "mock place_order unsupported"
    );
    ensure!(
        capability.supports_query_order,
        "mock query_order unsupported"
    );
    ensure!(
        capability.supports_public_streams,
        "mock public streams unsupported"
    );
    ensure!(
        capability.supports_private_streams,
        "mock private streams unsupported"
    );

    let client_order_id = format!("local-smoke-{}", Utc::now().timestamp_millis());
    let placed = client
        .place_order(
            "local-smoke-place-order".to_string(),
            tenant_id.clone(),
            Some(account_id.clone()),
            PlaceOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("local-smoke-place-order", &tenant_id, &account_id),
                symbol: symbol.clone(),
                client_order_id: Some(client_order_id.clone()),
                side: OrderSide::Buy,
                position_side: Some(PositionSide::Long),
                order_type: OrderType::IOC,
                time_in_force: Some(TimeInForce::IOC),
                quantity: "0.001".to_string(),
                price: Some("100.00".to_string()),
                quote_quantity: None,
                reduce_only: false,
                post_only: false,
            },
        )
        .await
        .context("place mock order")?;
    ensure!(
        placed.order.status == OrderStatus::Open,
        "mock order was not accepted/open: {:?}",
        placed.order.status
    );

    let queried = client
        .query_order(
            "local-smoke-query-order".to_string(),
            tenant_id.clone(),
            Some(account_id.clone()),
            QueryOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("local-smoke-query-order", &tenant_id, &account_id),
                symbol: symbol.clone(),
                client_order_id: Some(client_order_id.clone()),
                exchange_order_id: None,
            },
        )
        .await
        .context("query mock order")?;
    let queried_order = queried
        .order
        .context("mock query_order returned no order")?;
    ensure!(
        queried_order.client_order_id.as_deref() == Some(client_order_id.as_str()),
        "query_order returned the wrong client order id"
    );

    let public_ws = client
        .subscribe_books(
            "local-smoke-subscribe-books".to_string(),
            tenant_id.clone(),
            Some(account_id.clone()),
            SubscribeBooksRequest {
                schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                context: context("local-smoke-subscribe-books", &tenant_id, &account_id),
                subscriptions: vec![PublicStreamSubscription {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("local-smoke-subscribe-books", &tenant_id, &account_id),
                    symbol: symbol.clone(),
                    kind: PublicStreamKind::OrderBookSnapshot,
                }],
            },
        )
        .await
        .context("subscribe mock public order-book stream")?;
    ensure!(
        public_ws.subscriptions.len() == 1,
        "public stream subscription ack missing"
    );

    let private_ws = client
        .subscribe_private(
            "local-smoke-subscribe-private".to_string(),
            tenant_id.clone(),
            Some(account_id.clone()),
            SubscribePrivateRequest {
                schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                context: context("local-smoke-subscribe-private", &tenant_id, &account_id),
                subscriptions: vec![PrivateStreamSubscription {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("local-smoke-subscribe-private", &tenant_id, &account_id),
                    exchange: exchange.clone(),
                    market_type: Some(MarketType::Perpetual),
                    account_id: account_id.clone(),
                    kind: PrivateStreamKind::Orders,
                }],
            },
        )
        .await
        .context("subscribe mock private order stream")?;
    ensure!(
        private_ws.subscriptions.len() == 1,
        "private stream subscription ack missing"
    );

    let cancelled = client
        .cancel_order(
            "local-smoke-cancel-order".to_string(),
            tenant_id.clone(),
            Some(account_id.clone()),
            CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("local-smoke-cancel-order", &tenant_id, &account_id),
                symbol,
                client_order_id: Some(client_order_id.clone()),
                exchange_order_id: None,
            },
        )
        .await
        .context("cancel mock order after smoke")?;
    ensure!(cancelled.cancelled, "mock order cleanup cancel failed");

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "schema_version": 1,
            "mode": "local_mock_gateway",
            "generated_at": Utc::now(),
            "passed": true,
            "checks": [
                {"name": "capabilities", "status": "ok"},
                {"name": "place_order", "status": "ok"},
                {"name": "query_order", "status": "ok"},
                {"name": "subscribe_public_orderbook", "status": "ok"},
                {"name": "subscribe_private_orders", "status": "ok"},
                {"name": "cancel_order_cleanup", "status": "ok"}
            ],
            "order": {
                "client_order_id": client_order_id,
                "exchange_order_id": placed.order.exchange_order_id,
                "placed_status": placed.order.status,
                "query_status": queried_order.status,
                "cancel_status": cancelled.order.status
            },
            "subscriptions": {
                "public_orderbook": public_ws.subscriptions.len(),
                "private_orders": private_ws.subscriptions.len()
            }
        }))?
    );
    Ok(())
}

fn context(request_id: &str, tenant_id: &TenantId, account_id: &AccountId) -> RequestContext {
    let mut context = RequestContext::new(Utc::now());
    context.tenant_id = Some(tenant_id.clone());
    context.account_id = Some(account_id.clone());
    context.request_id = Some(request_id.to_string());
    context
}

fn symbol_scope(exchange: &ExchangeId) -> Result<SymbolScope> {
    let canonical = CanonicalSymbol::new("BTC", "USDT")?;
    Ok(SymbolScope {
        exchange: exchange.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(canonical),
        exchange_symbol: ExchangeSymbol::new(exchange.clone(), MarketType::Perpetual, "BTCUSDT")?,
    })
}
