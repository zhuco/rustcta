use std::sync::Arc;

use chrono::Utc;
use rustcta_exchange_api::{
    BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelAllOrdersRequest,
    CancelOrderRequest, ExchangeClient, OrderBookRequest, OrderSide, OrderType, PlaceOrderRequest,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    RequestContext, SymbolScope, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangeSymbol,
    MarketType, OrderBookLevel, OrderBookSnapshot, OrderStatus, SchemaVersion, TenantId,
};

use super::paper::PaperGatewayAdapter;
use crate::{
    AdapterBackedGateway, GatewayOperation, GatewayProtocolRequest, GatewayRequestPayload,
    GatewayResponsePayload, LocalGateway, GATEWAY_PROTOCOL_SCHEMA_VERSION,
};

fn tenant_id() -> TenantId {
    TenantId::new("tenant").expect("tenant id")
}

fn account_id() -> AccountId {
    AccountId::new("account").expect("account id")
}

fn exchange_id() -> ExchangeId {
    ExchangeId::new("paper").expect("paper exchange id")
}

fn canonical_symbol() -> CanonicalSymbol {
    CanonicalSymbol::new("BTC", "USDT").expect("canonical symbol")
}

fn exchange_symbol() -> ExchangeSymbol {
    ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTCUSDT").expect("exchange symbol")
}

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(tenant_id()),
        account_id: Some(account_id()),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: Utc::now(),
    }
}

fn symbol_scope() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical_symbol()),
        exchange_symbol: exchange_symbol(),
    }
}

fn paper_adapter() -> PaperGatewayAdapter {
    let adapter = PaperGatewayAdapter::default_paper().expect("paper adapter");
    adapter
        .insert_balance_snapshot(ExchangeBalance {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id(),
            account_id: account_id(),
            exchange_id: exchange_id(),
            market_type: MarketType::Spot,
            balances: vec![AssetBalance::new("USDT", 100.0, 100.0, 0.0).expect("balance")],
            observed_at: Utc::now(),
        })
        .expect("seed balance");
    let mut book = OrderBookSnapshot::new(
        exchange_id(),
        MarketType::Spot,
        canonical_symbol(),
        vec![OrderBookLevel::new(99.0, 1.0).expect("bid")],
        vec![OrderBookLevel::new(101.0, 1.0).expect("ask")],
        Utc::now(),
    )
    .expect("book");
    book.exchange_symbol = Some(exchange_symbol());
    adapter.insert_order_book(book).expect("seed book");
    adapter
}

fn place_order_request(request_id: &str) -> PlaceOrderRequest {
    place_order_request_with_client_id(request_id, Some("paper-cli-1"))
}

fn place_order_request_with_client_id(
    request_id: &str,
    client_order_id: Option<&str>,
) -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(request_id),
        symbol: symbol_scope(),
        client_order_id: client_order_id.map(str::to_string),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("100.00".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}

#[tokio::test]
async fn paper_adapter_should_implement_exchange_client_contract() {
    let adapter = paper_adapter();
    assert_eq!(adapter.exchange(), exchange_id());
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_place_order);
    assert!(capabilities.supports_batch_place_order);
    assert!(capabilities.supports_batch_cancel_order);
    assert!(capabilities.supports_cancel_all_orders);

    let balances = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: vec!["usdt".to_string()],
        })
        .await
        .expect("balances");
    assert_eq!(balances.balances[0].balances[0].asset, "USDT");

    let book = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(1),
        })
        .await
        .expect("book");
    assert_eq!(book.order_book.asks.len(), 1);

    let placed = adapter
        .place_order(place_order_request("place"))
        .await
        .expect("place");
    assert_eq!(placed.order.status, OrderStatus::Open);

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: symbol_scope(),
            client_order_id: Some("paper-cli-1".to_string()),
            exchange_order_id: None,
        })
        .await
        .expect("cancel");
    assert!(cancelled.cancelled);
    assert_eq!(cancelled.order.status, OrderStatus::Cancelled);
}

#[tokio::test]
async fn paper_adapter_should_batch_place_cancel_and_cancel_all_orders() {
    let adapter = paper_adapter();

    let batch = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![
                place_order_request_with_client_id("batch-place-1", Some("paper-batch-1")),
                place_order_request_with_client_id("batch-place-2", Some("paper-batch-2")),
                place_order_request_with_client_id("batch-place-3", Some("paper-batch-3")),
            ],
        })
        .await
        .expect("batch place");
    assert_eq!(batch.orders.len(), 3);
    assert!(batch
        .orders
        .iter()
        .all(|order| order.status == OrderStatus::Open));

    let cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-1"),
                    symbol: symbol_scope(),
                    client_order_id: Some("paper-batch-1".to_string()),
                    exchange_order_id: None,
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-2"),
                    symbol: symbol_scope(),
                    client_order_id: Some("paper-batch-2".to_string()),
                    exchange_order_id: None,
                },
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(cancelled.cancelled_count, 2);
    assert!(cancelled
        .orders
        .iter()
        .all(|order| order.status == OrderStatus::Cancelled));

    let cancel_all = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
        })
        .await
        .expect("cancel all");
    assert_eq!(cancel_all.cancelled_count, 1);
    assert_eq!(
        cancel_all.orders[0].client_order_id.as_deref(),
        Some("paper-batch-3")
    );
    assert_eq!(cancel_all.orders[0].status, OrderStatus::Cancelled);
}

#[tokio::test]
async fn adapter_backed_gateway_should_route_requests_to_paper_adapter() {
    let adapter = Arc::new(paper_adapter());
    let gateway = AdapterBackedGateway::new("gateway");
    gateway.register_adapter(adapter.clone()).expect("register");

    let response = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "gateway-place".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::PlaceOrder,
            payload: GatewayRequestPayload::PlaceOrder(place_order_request("gateway-place")),
            requested_at: Utc::now(),
        })
        .await
        .expect("gateway place");
    assert!(response.accepted);
    assert!(matches!(
        response.payload,
        GatewayResponsePayload::PlaceOrder(_)
    ));

    let response = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "gateway-subscribe".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::SubscribeBooks,
            payload: GatewayRequestPayload::SubscribeBooks(crate::SubscribeBooksRequest {
                schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                context: context("gateway-subscribe"),
                subscriptions: vec![PublicStreamSubscription {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("gateway-subscribe"),
                    symbol: symbol_scope(),
                    kind: PublicStreamKind::OrderBookSnapshot,
                }],
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect("gateway subscribe");

    let subscriptions = match response.payload {
        GatewayResponsePayload::BooksSubscribed(response) => response.subscriptions,
        other => panic!("unexpected payload {other:?}"),
    };
    assert_eq!(subscriptions[0].exchange, exchange_id());
    assert_eq!(
        adapter
            .recorded_public_subscriptions()
            .expect("subscriptions")
            .len(),
        1
    );

    let response = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "gateway-capabilities".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::GetCapabilities,
            payload: GatewayRequestPayload::GetCapabilities(crate::GetCapabilitiesRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("gateway-capabilities"),
                exchanges: vec![exchange_id()],
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect("gateway capabilities");
    assert!(matches!(
        response.payload,
        GatewayResponsePayload::Capabilities(response)
            if response.capabilities.len() == 1
                && response.capabilities[0].supports_private_streams
    ));

    let response = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "gateway-private-subscribe".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::SubscribePrivate,
            payload: GatewayRequestPayload::SubscribePrivate(crate::SubscribePrivateRequest {
                schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                context: context("gateway-private-subscribe"),
                subscriptions: vec![PrivateStreamSubscription {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("gateway-private-subscribe"),
                    exchange: exchange_id(),
                    market_type: Some(MarketType::Spot),
                    account_id: account_id(),
                    kind: PrivateStreamKind::Orders,
                }],
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect("gateway private subscribe");
    assert!(matches!(
        response.payload,
        GatewayResponsePayload::PrivateSubscribed(response)
            if response.subscriptions.len() == 1
                && response.subscriptions[0].subscription_id == "paper-private-sub-2"
    ));
}

#[tokio::test]
async fn adapter_backed_gateway_should_route_batch_and_cancel_all_to_paper_adapter() {
    let gateway = AdapterBackedGateway::paper_only("gateway").expect("paper gateway");

    let response = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "gateway-batch-place".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::BatchPlaceOrders,
            payload: GatewayRequestPayload::BatchPlaceOrders(BatchPlaceOrdersRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("gateway-batch-place"),
                exchange: exchange_id(),
                orders: vec![
                    place_order_request_with_client_id(
                        "gateway-batch-place",
                        Some("gateway-paper-1"),
                    ),
                    place_order_request_with_client_id(
                        "gateway-batch-place",
                        Some("gateway-paper-2"),
                    ),
                ],
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect("gateway batch place");
    assert!(matches!(
        response.payload,
        GatewayResponsePayload::BatchPlaceOrders(response) if response.orders.len() == 2
    ));

    let response = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "gateway-batch-cancel".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::BatchCancelOrders,
            payload: GatewayRequestPayload::BatchCancelOrders(BatchCancelOrdersRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("gateway-batch-cancel"),
                exchange: exchange_id(),
                cancels: vec![CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("gateway-batch-cancel"),
                    symbol: symbol_scope(),
                    client_order_id: Some("gateway-paper-1".to_string()),
                    exchange_order_id: None,
                }],
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect("gateway batch cancel");
    assert!(matches!(
        response.payload,
        GatewayResponsePayload::BatchCancelOrders(response) if response.cancelled_count == 1
    ));

    let response = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "gateway-cancel-all".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::CancelAllOrders,
            payload: GatewayRequestPayload::CancelAllOrders(CancelAllOrdersRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("gateway-cancel-all"),
                exchange: exchange_id(),
                market_type: Some(MarketType::Spot),
                symbol: Some(symbol_scope()),
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect("gateway cancel all");
    assert!(matches!(
        response.payload,
        GatewayResponsePayload::CancelAllOrders(response) if response.cancelled_count == 1
    ));
}
