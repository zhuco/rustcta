use super::*;

#[tokio::test]
async fn mock_gateway_should_handle_typed_place_and_cancel_order() {
    let gateway = MockExchangeGateway::with_exchanges("mock", vec![exchange_id()]);
    let place_response = gateway
        .handle_typed(place_order_request("req-place"))
        .await
        .expect("place order response");

    assert!(place_response.accepted);
    let order = match place_response.payload {
        GatewayResponsePayload::PlaceOrder(response) => response.order,
        other => panic!("unexpected payload: {other:?}"),
    };
    assert_eq!(order.client_order_id.as_deref(), Some("cli-1"));
    assert_eq!(order.status, OrderStatus::Open);

    let cancel_response = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "req-cancel".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::CancelOrder,
            payload: GatewayRequestPayload::CancelOrder(CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("req-cancel"),
                symbol: symbol_scope(),
                client_order_id: Some("cli-1".to_string()),
                exchange_order_id: None,
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect("cancel order response");

    let cancelled = match cancel_response.payload {
        GatewayResponsePayload::CancelOrder(response) => response,
        other => panic!("unexpected payload: {other:?}"),
    };
    assert!(cancelled.cancelled);
    assert_eq!(cancelled.order.status, OrderStatus::Cancelled);
}

#[tokio::test]
async fn mock_gateway_should_handle_batch_place_batch_cancel_and_cancel_all() {
    let gateway = MockExchangeGateway::with_exchanges("mock", vec![exchange_id()]);
    let first = match place_order_request("req-batch-place-a").payload {
        GatewayRequestPayload::PlaceOrder(request) => request,
        other => panic!("unexpected payload: {other:?}"),
    };
    let mut first = first;
    first.context = context("req-batch-place");
    let mut second = first.clone();
    second.context = context("req-batch-place");
    second.client_order_id = Some("cli-2".to_string());

    let batch_place = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "req-batch-place".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::BatchPlaceOrders,
            payload: GatewayRequestPayload::BatchPlaceOrders(BatchPlaceOrdersRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("req-batch-place"),
                exchange: exchange_id(),
                orders: vec![first, second],
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect("batch place response");
    let placed = match batch_place.payload {
        GatewayResponsePayload::BatchPlaceOrders(response) => response.orders,
        other => panic!("unexpected payload: {other:?}"),
    };
    assert_eq!(placed.len(), 2);

    let batch_cancel = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "req-batch-cancel".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::BatchCancelOrders,
            payload: GatewayRequestPayload::BatchCancelOrders(BatchCancelOrdersRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("req-batch-cancel"),
                exchange: exchange_id(),
                cancels: vec![CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("req-batch-cancel"),
                    symbol: symbol_scope(),
                    client_order_id: Some("cli-1".to_string()),
                    exchange_order_id: None,
                }],
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect("batch cancel response");
    assert!(matches!(
        batch_cancel.payload,
        GatewayResponsePayload::BatchCancelOrders(response)
            if response.cancelled_count == 1 && response.orders[0].status == OrderStatus::Cancelled
    ));

    let cancel_all = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "req-cancel-all".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::CancelAllOrders,
            payload: GatewayRequestPayload::CancelAllOrders(CancelAllOrdersRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("req-cancel-all"),
                exchange: exchange_id(),
                market_type: Some(MarketType::Spot),
                symbol: Some(symbol_scope()),
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect("cancel all response");
    assert!(matches!(
        cancel_all.payload,
        GatewayResponsePayload::CancelAllOrders(response)
            if response.cancelled_count == 1 && response.orders[0].client_order_id.as_deref() == Some("cli-2")
    ));
}

#[tokio::test]
async fn mock_gateway_should_handle_readback_gateway_operations() {
    let gateway = MockExchangeGateway::with_exchanges("mock", vec![exchange_id()]);
    gateway
        .handle_typed(place_order_request("req-place-readback"))
        .await
        .expect("place order response");

    let query = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "req-query".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::QueryOrder,
            payload: GatewayRequestPayload::QueryOrder(QueryOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("req-query"),
                symbol: symbol_scope(),
                client_order_id: Some("cli-1".to_string()),
                exchange_order_id: None,
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect("query order response");
    let query = match query.payload {
        GatewayResponsePayload::QueryOrder(response) => response,
        other => panic!("unexpected payload: {other:?}"),
    };
    assert_eq!(
        query.order.and_then(|order| order.client_order_id),
        Some("cli-1".to_string())
    );

    let open_orders = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "req-open".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::GetOpenOrders,
            payload: GatewayRequestPayload::GetOpenOrders(OpenOrdersRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("req-open"),
                exchange: exchange_id(),
                market_type: Some(MarketType::Spot),
                symbol: Some(symbol_scope()),
                page: None,
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect("open orders response");
    let open_orders = match open_orders.payload {
        GatewayResponsePayload::OpenOrders(response) => response.orders,
        other => panic!("unexpected payload: {other:?}"),
    };
    assert_eq!(open_orders.len(), 1);

    let positions = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "req-positions".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::GetPositions,
            payload: GatewayRequestPayload::GetPositions(PositionsRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("req-positions"),
                exchange: exchange_id(),
                market_type: Some(MarketType::Spot),
                symbols: vec![exchange_symbol()],
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect("positions response");
    assert!(matches!(
        positions.payload,
        GatewayResponsePayload::Positions(response) if response.positions.is_empty()
    ));

    let fees = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "req-fees".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::GetFees,
            payload: GatewayRequestPayload::GetFees(FeesRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("req-fees"),
                symbols: vec![symbol_scope()],
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect("fees response");
    assert!(matches!(
        fees.payload,
        GatewayResponsePayload::Fees(response) if response.fees.is_empty()
    ));

    let fills = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "req-fills".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::GetRecentFills,
            payload: GatewayRequestPayload::GetRecentFills(RecentFillsRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("req-fills"),
                exchange: exchange_id(),
                market_type: Some(MarketType::Spot),
                symbol: Some(symbol_scope()),
                client_order_id: Some("cli-1".to_string()),
                exchange_order_id: None,
                from_trade_id: None,
                start_time: None,
                end_time: None,
                limit: Some(10),
                page: None,
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect("fills response");
    assert!(matches!(
        fills.payload,
        GatewayResponsePayload::RecentFills(response) if response.fills.is_empty()
    ));
}

#[tokio::test]
async fn mock_gateway_should_return_seeded_balances_and_depth_limited_books() {
    let gateway = MockExchangeGateway::with_exchanges("mock", vec![exchange_id()]);
    gateway
        .insert_balance_snapshot(ExchangeBalance {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id(),
            account_id: account_id(),
            exchange_id: exchange_id(),
            market_type: MarketType::Spot,
            balances: vec![
                AssetBalance::new("BTC", 1.0, 0.9, 0.1).expect("btc balance"),
                AssetBalance::new("USDT", 100.0, 100.0, 0.0).expect("usdt balance"),
            ],
            observed_at: Utc::now(),
        })
        .expect("seed balances");
    let mut book = OrderBookSnapshot::new(
        exchange_id(),
        MarketType::Spot,
        canonical_symbol(),
        vec![
            OrderBookLevel::new(99.0, 1.0).expect("bid"),
            OrderBookLevel::new(98.0, 2.0).expect("bid"),
        ],
        vec![
            OrderBookLevel::new(101.0, 1.0).expect("ask"),
            OrderBookLevel::new(102.0, 2.0).expect("ask"),
        ],
        Utc::now(),
    )
    .expect("order book");
    book.exchange_symbol = Some(exchange_symbol());
    gateway.insert_order_book(book).expect("seed book");

    let balances = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "req-balances".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::GetBalances,
            payload: GatewayRequestPayload::GetBalances(BalancesRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("req-balances"),
                exchange: exchange_id(),
                market_type: Some(MarketType::Spot),
                assets: vec!["btc".to_string()],
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect("balances response");
    let balances = match balances.payload {
        GatewayResponsePayload::Balances(response) => response.balances,
        other => panic!("unexpected payload: {other:?}"),
    };
    assert_eq!(balances.len(), 1);
    assert_eq!(balances[0].balances.len(), 1);
    assert_eq!(balances[0].balances[0].asset, "BTC");

    let book = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "req-book".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::GetOrderBook,
            payload: GatewayRequestPayload::GetOrderBook(OrderBookRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("req-book"),
                symbol: symbol_scope(),
                depth: Some(1),
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect("order book response");
    let book = match book.payload {
        GatewayResponsePayload::OrderBook(response) => response.order_book,
        other => panic!("unexpected payload: {other:?}"),
    };
    assert_eq!(book.bids.len(), 1);
    assert_eq!(book.asks.len(), 1);
}

#[tokio::test]
async fn mock_gateway_should_accept_order_book_subscriptions_only() {
    let gateway = MockExchangeGateway::with_exchanges("mock", vec![exchange_id()]);
    let response = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "req-subscribe".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::SubscribeBooks,
            payload: GatewayRequestPayload::SubscribeBooks(SubscribeBooksRequest {
                schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                context: context("req-subscribe"),
                subscriptions: vec![PublicStreamSubscription {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("req-subscribe"),
                    symbol: symbol_scope(),
                    kind: PublicStreamKind::OrderBookSnapshot,
                }],
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect("subscription response");

    let response = match response.payload {
        GatewayResponsePayload::BooksSubscribed(response) => response,
        other => panic!("unexpected payload: {other:?}"),
    };
    assert_eq!(response.subscriptions.len(), 1);
    assert_eq!(
        gateway
            .recorded_book_subscriptions()
            .expect("recorded subscriptions")
            .len(),
        1
    );

    let rejected = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "req-subscribe-reject".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::SubscribeBooks,
            payload: GatewayRequestPayload::SubscribeBooks(SubscribeBooksRequest {
                schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                context: context("req-subscribe-reject"),
                subscriptions: vec![PublicStreamSubscription {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("req-subscribe-reject"),
                    symbol: symbol_scope(),
                    kind: PublicStreamKind::Trades,
                }],
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect_err("non-book subscription should reject");
    assert!(rejected
        .to_string()
        .contains("only accepts order-book stream kinds"));
}

#[tokio::test]
async fn mock_gateway_should_return_capabilities_and_accept_private_subscriptions() {
    let gateway = MockExchangeGateway::with_exchanges("mock", vec![exchange_id()]);
    let capabilities = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "req-capabilities".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::GetCapabilities,
            payload: GatewayRequestPayload::GetCapabilities(GetCapabilitiesRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("req-capabilities"),
                exchanges: vec![exchange_id()],
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect("capabilities response");
    assert!(matches!(
        capabilities.payload,
        GatewayResponsePayload::Capabilities(response)
            if response.capabilities.len() == 1
                && response.capabilities[0].supports_private_streams
                && response.capabilities[0].supports_cancel_all_orders
    ));

    let subscribed = gateway
        .handle_typed(GatewayProtocolRequest {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: "req-private-subscribe".to_string(),
            tenant_id: tenant_id(),
            account_id: Some(account_id()),
            operation: GatewayOperation::SubscribePrivate,
            payload: GatewayRequestPayload::SubscribePrivate(SubscribePrivateRequest {
                schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                context: context("req-private-subscribe"),
                subscriptions: vec![PrivateStreamSubscription {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("req-private-subscribe"),
                    exchange: exchange_id(),
                    market_type: Some(MarketType::Spot),
                    account_id: account_id(),
                    kind: PrivateStreamKind::Orders,
                }],
            }),
            requested_at: Utc::now(),
        })
        .await
        .expect("private subscription response");
    assert!(matches!(
        subscribed.payload,
        GatewayResponsePayload::PrivateSubscribed(response)
            if response.subscriptions.len() == 1
                && response.subscriptions[0].subscription_id == "private-sub-1"
                && response.subscriptions[0]
                    .capabilities
                    .as_ref()
                    .is_some_and(|capabilities| capabilities.supports_orders
                        && capabilities.supports_client_order_id
                        && capabilities.supports_exchange_order_id)
    ));
    assert_eq!(
        gateway
            .recorded_private_subscriptions()
            .expect("private subscriptions")
            .len(),
        1
    );
}

#[tokio::test]
async fn legacy_gateway_request_should_dispatch_to_typed_status() {
    let gateway = MockExchangeGateway::new("mock");
    let response = gateway
        .handle(GatewayRequest {
            request_id: "req-legacy".to_string(),
            tenant_id: "tenant".to_string(),
            account_id: Some("account".to_string()),
            operation: "status".to_string(),
            payload: json!({}),
            requested_at: Utc::now(),
        })
        .await
        .expect("legacy status response");

    assert!(response.accepted);
    assert_eq!(response.request_id, "req-legacy");
    assert_eq!(response.payload["payload_type"], "status");
}
