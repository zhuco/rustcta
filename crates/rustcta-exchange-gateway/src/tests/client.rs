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
