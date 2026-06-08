use std::sync::Arc;

use chrono::Utc;
use rustcta_exchange_api::{
    BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelAllOrdersRequest,
    CancelOrderRequest, ExchangeClient, ExchangeClientCapabilities, FeesRequest, OpenOrdersRequest,
    OrderBookRequest, OrderSide, OrderType, PlaceOrderRequest, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, QueryOrderRequest,
    RecentFillsRequest, RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_exchange_gateway::{
    AdapterBackedGateway, GatewayClient, GatewayError, GatewayExchangeClient,
    InProcessGatewayClient, MockExchangeGateway, SubscribeBooksRequest, SubscribePrivateRequest,
    GATEWAY_PROTOCOL_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangeSymbol,
    MarketType, OrderBookLevel, OrderBookSnapshot, SchemaVersion, TenantId,
};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("paper").expect("paper exchange id")
}

fn tenant_id() -> TenantId {
    TenantId::new("tenant-a").expect("tenant id")
}

fn account_id() -> AccountId {
    AccountId::new("acct-a").expect("account id")
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
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical symbol")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTCUSDT")
            .expect("exchange symbol"),
    }
}

fn order_request(request_id: &str, client_order_id: &str) -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context(request_id),
        symbol: symbol_scope(),
        client_order_id: Some(client_order_id.to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("50000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}

fn order_book() -> OrderBookSnapshot {
    let mut snapshot = OrderBookSnapshot::new(
        exchange_id(),
        MarketType::Spot,
        CanonicalSymbol::new("BTC", "USDT").expect("canonical symbol"),
        vec![OrderBookLevel::new(49999.0, 1.0).expect("bid")],
        vec![OrderBookLevel::new(50001.0, 1.0).expect("ask")],
        Utc::now(),
    )
    .expect("order book");
    snapshot.exchange_symbol =
        Some(ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTCUSDT").expect("symbol"));
    snapshot
}

fn balance_snapshot() -> ExchangeBalance {
    ExchangeBalance {
        schema_version: SchemaVersion::current(),
        tenant_id: tenant_id(),
        account_id: account_id(),
        exchange_id: exchange_id(),
        market_type: MarketType::Spot,
        balances: vec![AssetBalance::new("USDT", 1000.0, 1000.0, 0.0).expect("balance")],
        observed_at: Utc::now(),
    }
}

fn active_runtime_capabilities() -> ExchangeClientCapabilities {
    let mut capabilities = ExchangeClientCapabilities::new(exchange_id());
    capabilities.supports_public_rest = true;
    capabilities.supports_private_rest = true;
    capabilities.supports_public_streams = true;
    capabilities.supports_private_streams = true;
    capabilities.supports_order_book_snapshot = true;
    capabilities.supports_balances = true;
    capabilities.supports_positions = true;
    capabilities.supports_place_order = true;
    capabilities.supports_cancel_order = true;
    capabilities.supports_query_order = true;
    capabilities.supports_open_orders = true;
    capabilities.supports_recent_fills = true;
    capabilities.supports_batch_place_order = true;
    capabilities.supports_batch_cancel_order = true;
    capabilities.supports_cancel_all_orders = true;
    capabilities
}

async fn paper_exchange_client() -> GatewayExchangeClient {
    let gateway = MockExchangeGateway::with_exchanges("task3-mock", [exchange_id()]);
    gateway
        .insert_order_book(order_book())
        .expect("seed order book");
    gateway
        .insert_balance_snapshot(balance_snapshot())
        .expect("seed balance");

    let gateway_client = Arc::new(InProcessGatewayClient::new(Arc::new(gateway)));
    GatewayExchangeClient::new(
        gateway_client,
        tenant_id(),
        Some(account_id()),
        exchange_id(),
    )
    .with_capabilities(active_runtime_capabilities())
}

#[tokio::test]
async fn task3_active_runtime_surface_should_route_core_exchange_requests() {
    let client = paper_exchange_client().await;
    let scope = symbol_scope();

    let capabilities = client.capabilities();
    assert!(capabilities.supports_public_streams);
    assert!(capabilities.supports_private_streams);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(capabilities.supports_balances);
    assert!(capabilities.supports_positions);
    assert!(capabilities.supports_place_order);
    assert!(capabilities.supports_cancel_order);
    assert!(capabilities.supports_query_order);
    assert!(capabilities.supports_open_orders);
    assert!(capabilities.supports_recent_fills);
    assert!(capabilities.supports_batch_place_order);
    assert!(capabilities.supports_batch_cancel_order);
    assert!(capabilities.supports_cancel_all_orders);

    let book = client
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("task3-book"),
            symbol: scope.clone(),
            depth: Some(1),
        })
        .await
        .expect("order book through gateway");
    assert_eq!(book.order_book.best_bid().expect("bid").price, 49999.0);

    let balances = client
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("task3-balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect("balances through gateway");
    assert_eq!(balances.balances.len(), 1);

    let positions = client
        .get_positions(rustcta_exchange_api::PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("task3-positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbols: vec![scope.exchange_symbol.clone()],
        })
        .await
        .expect("positions through gateway");
    assert!(positions.positions.is_empty());

    let fees = client
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("task3-fees"),
            symbols: vec![scope.clone()],
        })
        .await
        .expect("fees through gateway");
    assert!(fees.fees.is_empty());

    let placed = client
        .place_order(order_request("task3-place", "task3-cli-1"))
        .await
        .expect("place order through gateway");
    assert_eq!(placed.order.client_order_id.as_deref(), Some("task3-cli-1"));

    let queried = client
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("task3-query"),
            symbol: scope.clone(),
            client_order_id: Some("task3-cli-1".to_string()),
            exchange_order_id: None,
        })
        .await
        .expect("query order through gateway");
    assert!(queried.order.is_some());

    let open_orders = client
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("task3-open-orders"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(scope.clone()),
            page: None,
        })
        .await
        .expect("open orders through gateway");
    assert_eq!(open_orders.orders.len(), 1);

    let fills = client
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("task3-fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(scope.clone()),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(10),
            page: None,
        })
        .await
        .expect("recent fills through gateway");
    assert!(fills.fills.is_empty());

    let cancelled = client
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("task3-cancel"),
            symbol: scope.clone(),
            client_order_id: Some("task3-cli-1".to_string()),
            exchange_order_id: None,
        })
        .await
        .expect("cancel order through gateway");
    assert!(cancelled.cancelled);

    let batch = client
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("task3-batch-place"),
            exchange: exchange_id(),
            orders: vec![
                order_request("task3-batch-place", "task3-cli-2"),
                order_request("task3-batch-place", "task3-cli-3"),
            ],
        })
        .await
        .expect("batch place through gateway");
    assert_eq!(batch.orders.len(), 2);

    let batch_cancel = client
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("task3-batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("task3-batch-cancel"),
                symbol: scope.clone(),
                client_order_id: Some("task3-cli-2".to_string()),
                exchange_order_id: None,
            }],
        })
        .await
        .expect("batch cancel through gateway");
    assert_eq!(batch_cancel.cancelled_count, 1);

    let cancel_all = client
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("task3-cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(scope.clone()),
        })
        .await
        .expect("cancel all through gateway");
    assert_eq!(cancel_all.cancelled_count, 1);
}

#[tokio::test]
async fn task3_active_runtime_surface_should_route_stream_subscriptions() {
    let gateway = Arc::new(AdapterBackedGateway::paper_only("task3-paper").expect("paper gateway"));
    let gateway_client = InProcessGatewayClient::new(gateway);

    let books = gateway_client
        .subscribe_books(
            "task3-subscribe-books".to_string(),
            tenant_id(),
            Some(account_id()),
            SubscribeBooksRequest {
                schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                context: context("task3-subscribe-books"),
                subscriptions: vec![PublicStreamSubscription {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("task3-subscribe-books"),
                    symbol: symbol_scope(),
                    kind: PublicStreamKind::OrderBookDelta,
                }],
            },
        )
        .await
        .expect("public stream subscription");
    assert_eq!(books.subscriptions.len(), 1);

    let private = gateway_client
        .subscribe_private(
            "task3-subscribe-private".to_string(),
            tenant_id(),
            Some(account_id()),
            SubscribePrivateRequest {
                schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                context: context("task3-subscribe-private"),
                subscriptions: vec![PrivateStreamSubscription {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("task3-subscribe-private"),
                    exchange: exchange_id(),
                    market_type: Some(MarketType::Spot),
                    account_id: account_id(),
                    kind: PrivateStreamKind::Orders,
                }],
            },
        )
        .await
        .expect("private stream subscription");
    assert_eq!(private.subscriptions.len(), 1);
    assert!(private.subscriptions[0].capabilities.is_some());
}

#[tokio::test]
async fn task3_adapter_dispatcher_should_reject_empty_private_subscriptions() {
    let gateway = InProcessGatewayClient::new(Arc::new(
        AdapterBackedGateway::paper_only("task3-paper").expect("paper gateway"),
    ));

    let error = gateway
        .subscribe_private(
            "task3-empty-private".to_string(),
            tenant_id(),
            Some(account_id()),
            SubscribePrivateRequest {
                schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                context: context("task3-empty-private"),
                subscriptions: Vec::new(),
            },
        )
        .await
        .expect_err("empty private stream subscriptions must be rejected");

    assert!(
        matches!(error, GatewayError::Rejected(message) if message.contains("subscribe_private requires at least one subscription"))
    );
}
