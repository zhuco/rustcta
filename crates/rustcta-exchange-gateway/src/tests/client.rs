use super::*;

#[tokio::test]
async fn in_process_gateway_client_should_expose_capabilities_and_private_subscriptions() {
    let gateway = Arc::new(MockExchangeGateway::with_exchanges(
        "mock",
        vec![exchange_id()],
    ));
    let client = InProcessGatewayClient::new(gateway.clone());

    let capabilities = client
        .get_capabilities(
            "client-capabilities".to_string(),
            tenant_id(),
            Some(account_id()),
            GetCapabilitiesRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("client-capabilities"),
                exchanges: vec![exchange_id()],
            },
        )
        .await
        .expect("client capabilities");
    assert_eq!(capabilities.capabilities.len(), 1);
    assert!(capabilities.capabilities[0].supports_private_streams);

    let subscribed = client
        .subscribe_private(
            "client-private-subscribe".to_string(),
            tenant_id(),
            Some(account_id()),
            SubscribePrivateRequest {
                schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                context: context("client-private-subscribe"),
                subscriptions: vec![PrivateStreamSubscription {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("client-private-subscribe"),
                    exchange: exchange_id(),
                    market_type: Some(MarketType::Spot),
                    account_id: account_id(),
                    kind: PrivateStreamKind::Orders,
                }],
            },
        )
        .await
        .expect("client private subscribe");
    assert_eq!(subscribed.subscriptions.len(), 1);
    assert_eq!(subscribed.subscriptions[0].subscription_id, "private-sub-1");
    assert!(subscribed.subscriptions[0]
        .capabilities
        .as_ref()
        .is_some_and(|capabilities| capabilities.supports_orders
            && capabilities.supports_client_order_id
            && capabilities.supports_exchange_order_id));
    assert_eq!(
        gateway
            .recorded_private_subscriptions()
            .expect("private subscriptions")
            .len(),
        1
    );
}

#[tokio::test]
async fn in_process_gateway_client_should_cover_read_write_and_public_stream_helpers() {
    let gateway = Arc::new(MockExchangeGateway::with_exchanges(
        "mock",
        vec![exchange_id()],
    ));
    gateway
        .insert_balance_snapshot(ExchangeBalance {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id(),
            account_id: account_id(),
            exchange_id: exchange_id(),
            market_type: MarketType::Spot,
            balances: vec![AssetBalance::new("BTC", 1.0, 0.9, 0.1).expect("btc balance")],
            observed_at: Utc::now(),
        })
        .expect("seed balances");
    let client = InProcessGatewayClient::new(gateway.clone());

    let balances = client
        .get_balances(
            "client-balances".to_string(),
            tenant_id(),
            Some(account_id()),
            BalancesRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("client-balances"),
                exchange: exchange_id(),
                market_type: Some(MarketType::Spot),
                assets: vec!["btc".to_string()],
            },
        )
        .await
        .expect("client balances");
    assert_eq!(balances.balances.len(), 1);
    assert_eq!(balances.balances[0].balances[0].asset, "BTC");

    let first = match place_order_request("client-batch-place-a").payload {
        GatewayRequestPayload::PlaceOrder(request) => request,
        other => panic!("unexpected payload: {other:?}"),
    };
    let mut second = first.clone();
    second.client_order_id = Some("cli-2".to_string());
    second.context = context("client-batch-place");
    let mut first = first;
    first.context = context("client-batch-place");
    let placed = client
        .batch_place_orders(
            "client-batch-place".to_string(),
            tenant_id(),
            Some(account_id()),
            BatchPlaceOrdersRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("client-batch-place"),
                exchange: exchange_id(),
                orders: vec![first, second],
            },
        )
        .await
        .expect("client batch place");
    assert_eq!(placed.orders.len(), 2);

    let subscribed = client
        .subscribe_books(
            "client-books".to_string(),
            tenant_id(),
            Some(account_id()),
            SubscribeBooksRequest {
                schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                context: context("client-books"),
                subscriptions: vec![PublicStreamSubscription {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("client-books"),
                    symbol: symbol_scope(),
                    kind: PublicStreamKind::OrderBookDelta,
                }],
            },
        )
        .await
        .expect("client books subscription");
    assert_eq!(subscribed.subscriptions.len(), 1);
    assert_eq!(
        gateway
            .recorded_book_subscriptions()
            .expect("book subscriptions")
            .len(),
        1
    );
}

#[tokio::test]
async fn gateway_exchange_client_should_implement_exchange_api_trait() {
    use rustcta_exchange_api::ExchangeClient as _;

    let gateway = Arc::new(MockExchangeGateway::with_exchanges(
        "mock",
        vec![exchange_id()],
    ));
    gateway
        .insert_balance_snapshot(ExchangeBalance {
            schema_version: SchemaVersion::current(),
            tenant_id: tenant_id(),
            account_id: account_id(),
            exchange_id: exchange_id(),
            market_type: MarketType::Spot,
            balances: vec![AssetBalance::new("USDT", 100.0, 100.0, 0.0).expect("usdt balance")],
            observed_at: Utc::now(),
        })
        .expect("seed balances");
    let gateway_client = Arc::new(InProcessGatewayClient::new(gateway.clone()));
    let exchange_client = GatewayExchangeClient::new(
        gateway_client,
        tenant_id(),
        Some(account_id()),
        exchange_id(),
    );

    let balances = exchange_client
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("exchange-client-balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect("gateway exchange client balances");
    assert_eq!(balances.balances.len(), 1);

    let order = match place_order_request("exchange-client-place").payload {
        GatewayRequestPayload::PlaceOrder(request) => request,
        other => panic!("unexpected payload: {other:?}"),
    };
    let placed = exchange_client
        .place_order(order)
        .await
        .expect("gateway exchange client place order");
    assert_eq!(placed.order.client_order_id.as_deref(), Some("cli-1"));
    assert_eq!(gateway.recorded_orders().expect("recorded orders").len(), 1);
}

#[tokio::test]
async fn gateway_exchange_client_should_expose_account_control_provider_boundary() {
    let gateway = Arc::new(MockExchangeGateway::with_exchanges(
        "mock",
        vec![exchange_id()],
    ));
    let gateway_client = Arc::new(InProcessGatewayClient::new(gateway));
    let exchange_client = GatewayExchangeClient::new(
        gateway_client,
        tenant_id(),
        Some(account_id()),
        exchange_id(),
    );
    let provider: &dyn rustcta_exchange_api::PerpAccountControlProvider = &exchange_client;

    let capabilities = provider.account_control_capabilities();
    assert_eq!(capabilities.exchange, exchange_id());
    assert!(!capabilities.supports_leverage);
    assert!(!capabilities.supports_position_mode_change);
    assert!(!capabilities.supports_close_position);

    let result = provider
        .set_countdown_cancel_all(CountdownCancelAllRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("gateway-countdown-cancel-all"),
            exchange: exchange_id(),
            symbol: Some(symbol_scope()),
            timeout_secs: 30,
        })
        .await;
    assert!(matches!(
        result,
        Err(ExchangeApiError::Unsupported {
            operation: "set_countdown_cancel_all"
        })
    ));
}

#[tokio::test]
async fn in_process_gateway_client_should_route_account_control_payloads_as_unsupported() {
    let gateway = Arc::new(MockExchangeGateway::with_exchanges(
        "mock",
        vec![exchange_id()],
    ));
    let client = InProcessGatewayClient::new(gateway);

    let result = client
        .set_leverage(
            "client-set-leverage".to_string(),
            tenant_id(),
            Some(account_id()),
            rustcta_exchange_api::SetLeverageRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("client-set-leverage"),
                symbol: symbol_scope(),
                leverage: 3,
            },
        )
        .await;

    assert!(matches!(
        result,
        Err(GatewayError::UnsupportedOperation { operation })
            if operation == "mock.set_leverage"
    ));
}
