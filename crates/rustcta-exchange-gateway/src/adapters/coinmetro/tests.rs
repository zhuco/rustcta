use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelAllOrdersRequest,
    ExchangeApiError, ExchangeClient, OrderBookStrictness, OrderListConditionalLeg,
    OrderListLegType, OrderListRequest, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
};

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::private::coinmetro_order_body;
use super::private_parser::{
    parse_balances, parse_open_orders, parse_order_state, parse_recent_fills,
};
use super::signing::{bearer_header, websocket_token_query};
use super::streams::CoinmetroSequenceDecision;
use super::{streams, CoinmetroGatewayAdapter, CoinmetroGatewayConfig};

fn coinmetro_exchange() -> ExchangeId {
    ExchangeId::new("coinmetro").expect("exchange")
}

fn symbol_scope() -> rustcta_exchange_api::SymbolScope {
    rustcta_exchange_api::SymbolScope {
        exchange: coinmetro_exchange(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "EUR").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(coinmetro_exchange(), MarketType::Spot, "BTCEUR")
            .expect("symbol"),
    }
}

fn fixture(path: &str) -> serde_json::Value {
    let text = match path {
        "markets.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinmetro/markets.json"
        ),
        "orderbook.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinmetro/orderbook.json"
        ),
        "balances.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinmetro/balances.json"
        ),
        "open_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinmetro/open_orders.json"
        ),
        "order_status.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinmetro/order_status.json"
        ),
        "fills.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinmetro/fills.json"
        ),
        "unsupported_boundary.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinmetro/unsupported_boundary.json"
        ),
        "signing_vectors/bearer_authorization.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinmetro/signing_vectors/bearer_authorization.json"
        ),
        "request_specs/place_order_limit.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinmetro/request_specs/place_order_limit.json"
        ),
        "request_specs/cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinmetro/request_specs/cancel_order.json"
        ),
        "request_specs/query_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinmetro/request_specs/query_order.json"
        ),
        "ws/book_update.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinmetro/ws/book_update.json"
        ),
        "ws/tick.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinmetro/ws/tick.json"
        ),
        "ws/private_url.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/coinmetro/ws/private_url.json"
        ),
        _ => panic!("unknown coinmetro fixture {path}"),
    };
    serde_json::from_str(text).expect("fixture")
}

#[test]
fn coinmetro_signing_should_build_bearer_and_ws_tokens() {
    let vector = fixture("signing_vectors/bearer_authorization.json");
    assert_eq!(
        bearer_header(vector["input_token"].as_str().unwrap()).as_deref(),
        vector["expected_authorization"].as_str()
    );
    assert_eq!(
        bearer_header(vector["already_prefixed_token"].as_str().unwrap()).as_deref(),
        vector["expected_already_prefixed_authorization"].as_str()
    );
    assert_eq!(
        websocket_token_query(
            vector["device_id"].as_str(),
            vector["input_token"].as_str().unwrap()
        )
        .as_deref(),
        vector["expected_ws_token_query"].as_str()
    );
}

#[test]
fn coinmetro_parser_should_normalize_markets_and_book() {
    let exchange = coinmetro_exchange();
    let rules = parse_symbol_rules(&exchange, &fixture("markets.json")).expect("rules");
    assert_eq!(rules.len(), 3);
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTCEUR");
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "EUR");
    assert_eq!(rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(
        rules[1].symbol.canonical_symbol.as_ref().unwrap().as_str(),
        "ETH/BTC"
    );
    assert_eq!(
        rules[2].symbol.canonical_symbol.as_ref().unwrap().as_str(),
        "USDT/EUR"
    );

    let snapshot = parse_orderbook_snapshot(
        &exchange,
        rules[0].symbol.clone(),
        &fixture("orderbook.json"),
    )
    .expect("book");
    assert_eq!(snapshot.sequence, Some(16890578157));
    assert_eq!(snapshot.bids[0].price, 54225.43);
    assert_eq!(snapshot.asks[0].price, 54300.27);
    assert!(
        streams::coinmetro_book_checksum_matches(&fixture("orderbook.json")["book"])
            .expect("rest book checksum")
    );
}

#[test]
fn coinmetro_private_parsers_should_cover_balances_orders_and_fills() {
    let exchange = coinmetro_exchange();
    let balances = parse_balances(
        &exchange,
        rustcta_exchange_api::TenantId::new("tenant").expect("tenant"),
        rustcta_exchange_api::AccountId::new("account").expect("account"),
        &[],
        &fixture("balances.json"),
    )
    .expect("balances");
    assert_eq!(balances[0].balances.len(), 2);
    assert_eq!(balances[0].balances[0].asset, "BTC");

    let symbol = symbol_scope();
    let filled =
        parse_order_state(&exchange, Some(&symbol), &fixture("order_status.json")).expect("filled");
    assert_eq!(filled.side, OrderSide::Buy);
    assert_eq!(filled.status, rustcta_types::OrderStatus::Filled);
    assert_eq!(filled.price.as_deref(), Some("65000"));

    let open =
        parse_open_orders(&exchange, Some(&symbol), &fixture("open_orders.json")).expect("open");
    assert_eq!(open[0].side, OrderSide::Sell);
    assert_eq!(open[0].quantity, "0.01");

    let fills = parse_recent_fills(
        &exchange,
        rustcta_exchange_api::TenantId::new("tenant").expect("tenant"),
        rustcta_exchange_api::AccountId::new("account").expect("account"),
        &symbol,
        &fixture("fills.json"),
    )
    .expect("fills");
    assert_eq!(fills[0].fill_id.as_deref(), Some("96444450"));
    assert_eq!(fills[0].side, OrderSide::Sell);
}

#[test]
fn coinmetro_order_body_and_request_specs_should_match_limit_order() {
    let body = coinmetro_order_body(&rustcta_exchange_api::PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: rustcta_exchange_api::RequestContext::new(Utc::now()),
        symbol: symbol_scope(),
        client_order_id: None,
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(rustcta_types::TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    })
    .expect("body");
    assert_eq!(body["buyingCurrency"], "BTC");
    assert_eq!(body["sellingCurrency"], "EUR");
    assert_eq!(body["sellingQty"], "650");

    let spec = fixture("request_specs/place_order_limit.json");
    assert_eq!(spec["method"], "POST");
    assert_eq!(spec["path"], "/exchange/orders/create");
    assert_eq!(spec["body"], body);
    assert_eq!(
        fixture("request_specs/cancel_order.json")["path"],
        "/exchange/orders/cancel/fixture-order-1"
    );
    assert_eq!(
        fixture("request_specs/query_order.json")["path"],
        "/exchange/orders/status/fixture-order-1"
    );
}

#[test]
fn coinmetro_stream_helpers_should_cover_urls_and_parser_samples() {
    let symbol = symbol_scope().exchange_symbol;
    let public =
        streams::public_stream_url("wss://api.coinmetro.com/ws", std::slice::from_ref(&symbol))
            .expect("public url");
    assert_eq!(public, "wss://api.coinmetro.com/ws?pairs=BTCEUR");
    let private = streams::private_stream_url(
        "wss://api.coinmetro.com/ws",
        Some("fixture-device"),
        "fixture.jwt.token",
        &[symbol],
    )
    .expect("private url");
    assert_eq!(
        private,
        fixture("ws/private_url.json")["ws_url"].as_str().unwrap()
    );

    assert_eq!(
        streams::parse_ws_message(&fixture("ws/book_update.json")).expect("book update"),
        streams::CoinmetroWsMessage::BookUpdate {
            pair: "BTCEUR".to_string(),
            sequence: Some(96445847)
        }
    );
    assert_eq!(
        streams::parse_ws_message(&fixture("ws/tick.json")).expect("tick"),
        streams::CoinmetroWsMessage::Tick {
            pair: "BTCEUR".to_string(),
            sequence: Some(96444450)
        }
    );
}

#[test]
fn coinmetro_public_ws_order_book_policy_should_capture_structured_details() {
    let policy = streams::coinmetro_public_order_book_ws_policy();

    assert_eq!(policy.subscription, "query string pairs=BTCEUR,LTCEUR");
    assert_eq!(policy.book_update_event, "bookUpdate");
    assert_eq!(policy.tick_event, "tick");
    assert_eq!(policy.fixed_update_interval_ms, None);
    assert_eq!(policy.depth, None);
    assert_eq!(policy.sequence_field, "bookUpdate.seqNumber");
    assert_eq!(policy.checksum_algorithm, "crc32_unsigned");
    assert_eq!(policy.rest_snapshot_endpoint, "GET /exchange/book/{pair}");

    let json = policy.as_json();
    assert_eq!(json["subscription"], "query string pairs=BTCEUR,LTCEUR");
    assert_eq!(json["checksum"]["resync_on_mismatch"], true);
    assert_eq!(json["events"]["bbo"], "tick");
}

#[test]
fn coinmetro_public_ws_parsers_should_capture_book_update_checksum_and_bbo() {
    let update =
        streams::parse_book_update(&fixture("ws/book_update.json")).expect("bookUpdate parser");
    assert_eq!(update.pair, "BTCEUR");
    assert_eq!(update.sequence, 96445847);
    assert_eq!(update.asks[0].price, "10123.45");
    assert_eq!(update.asks[0].quantity_delta, 0.5);
    assert!(update.bids.is_empty());
    assert_eq!(update.checksum, 1_519_698_522);
    assert!(update.checksum_matches_payload);
    assert_eq!(
        streams::coinmetro_book_checksum_string(&fixture("ws/book_update.json")["bookUpdate"])
            .expect("checksum string"),
        "10123.450.5"
    );

    let tick = streams::parse_tick_bbo(&fixture("ws/tick.json")).expect("tick bbo");
    assert_eq!(tick.pair, "BTCEUR");
    assert_eq!(tick.sequence, Some(96444450));
    assert_eq!(tick.bid, Some(64990.0));
    assert_eq!(tick.ask, Some(65010.0));
    assert_eq!(tick.timestamp_ms, Some(1567791175553));
}

#[test]
fn coinmetro_sequence_helper_should_detect_regression_or_duplicate() {
    assert_eq!(
        streams::coinmetro_sequence_decision(None, 10),
        CoinmetroSequenceDecision::Accept
    );
    assert_eq!(
        streams::coinmetro_sequence_decision(Some(10), 11),
        CoinmetroSequenceDecision::Accept
    );
    assert_eq!(
        streams::coinmetro_sequence_decision(Some(10), 10),
        CoinmetroSequenceDecision::Resync
    );
    assert_eq!(
        streams::coinmetro_sequence_decision(Some(10), 9),
        CoinmetroSequenceDecision::Resync
    );
}

#[test]
fn coinmetro_unsupported_boundary_and_named_registration_should_be_explicit() {
    let boundary = fixture("unsupported_boundary.json");
    assert_eq!(boundary["funding_enabled"], false);
    assert!(boundary["unsupported_operations"]
        .as_array()
        .expect("operations")
        .iter()
        .any(|operation| operation == "margin_orders"));

    let adapter = CoinmetroGatewayAdapter::new(CoinmetroGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_positions);
    assert!(!capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_batch_cancel_order);
    assert!(!capabilities.supports_cancel_all_orders);
    assert!(!capabilities.supports_post_only);
    assert_eq!(
        capabilities.order_book.strictness,
        OrderBookStrictness::StrictDelta
    );
    assert!(capabilities.order_book.supports_sequence);
    assert!(capabilities.order_book.supports_checksum);

    let error = coinmetro_order_body(&rustcta_exchange_api::PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: rustcta_exchange_api::RequestContext::new(Utc::now()),
        symbol: symbol_scope(),
        client_order_id: Some("client-order-id".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    })
    .expect_err("client ids unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "coinmetro.client_order_id_unsupported"
        }
    ));

    let gateway = crate::adapters::AdapterBackedGateway::with_named_adapters("test", ["coinmetro"])
        .expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}

#[tokio::test]
async fn coinmetro_advanced_orders_should_stay_explicitly_unsupported() {
    let adapter = CoinmetroGatewayAdapter::new(CoinmetroGatewayConfig::default()).expect("adapter");

    let amend_error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: rustcta_exchange_api::RequestContext::new(Utc::now()),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("fixture-order-1".to_string()),
            new_client_order_id: Some("fixture-order-1-replace".to_string()),
            new_quantity: "0.02".to_string(),
        })
        .await
        .expect_err("amend unsupported");
    assert_unsupported_operation(
        amend_error,
        "coinmetro.modify_order_unmapped_requires_native_qty_fields",
    );

    let order_list_error = adapter
        .place_order_list(OrderListRequest::Oco {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: rustcta_exchange_api::RequestContext::new(Utc::now()),
            symbol: symbol_scope(),
            list_client_order_id: Some("fixture-oco".to_string()),
            side: OrderSide::Sell,
            quantity: "0.01".to_string(),
            above: OrderListConditionalLeg {
                order_type: OrderListLegType::Limit,
                price: Some("68000".to_string()),
                stop_price: None,
                time_in_force: None,
                client_order_id: Some("fixture-oco-above".to_string()),
            },
            below: OrderListConditionalLeg {
                order_type: OrderListLegType::StopLossLimit,
                price: Some("62000".to_string()),
                stop_price: Some("63000".to_string()),
                time_in_force: None,
                client_order_id: Some("fixture-oco-below".to_string()),
            },
        })
        .await
        .expect_err("order-list unsupported");
    assert_unsupported_operation(order_list_error, "coinmetro.order_list_unsupported");

    let batch_place_error = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: rustcta_exchange_api::RequestContext::new(Utc::now()),
            exchange: coinmetro_exchange(),
            orders: vec![],
        })
        .await
        .expect_err("batch place unsupported");
    assert_unsupported_operation(
        batch_place_error,
        "coinmetro.batch_place_orders_composed_not_exposed",
    );

    let batch_cancel_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: rustcta_exchange_api::RequestContext::new(Utc::now()),
            exchange: coinmetro_exchange(),
            cancels: vec![],
        })
        .await
        .expect_err("batch cancel unsupported");
    assert_unsupported_operation(
        batch_cancel_error,
        "coinmetro.batch_cancel_orders_composed_not_exposed",
    );

    let cancel_all_error = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: rustcta_exchange_api::RequestContext::new(Utc::now()),
            exchange: coinmetro_exchange(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
        })
        .await
        .expect_err("cancel-all unsupported");
    assert_unsupported_operation(
        cancel_all_error,
        "coinmetro.cancel_all_orders_no_native_endpoint",
    );

    let boundary = fixture("unsupported_boundary.json");
    assert_eq!(
        boundary["advanced_order_boundary"]["support"],
        "unsupported"
    );
    assert!(boundary["unsupported_operations"]
        .as_array()
        .expect("operations")
        .iter()
        .any(|operation| operation == "amend_order"));
}

#[test]
fn coinmetro_advanced_order_mapping_should_stay_unsupported() {
    let mapping = include_str!("endpoint_mapping.yaml");
    for operation in [
        "amend_order",
        "place_order_list",
        "batch_place_orders",
        "batch_cancel_orders",
        "cancel_all_orders",
    ] {
        assert!(
            mapping.contains(&format!("operation: {operation}")),
            "missing {operation} endpoint"
        );
    }
    assert!(mapping.contains("support: unsupported"));
    assert!(mapping.contains("auth: unsupported"));
    assert!(mapping.contains("native_batch: false"));
}

#[tokio::test]
async fn coinmetro_public_stream_subscription_should_use_pairs_query() {
    let adapter = CoinmetroGatewayAdapter::new(CoinmetroGatewayConfig::default()).expect("adapter");
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: rustcta_exchange_api::RequestContext::new(Utc::now()),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let stream_id = adapter
        .subscribe_public_stream(subscription)
        .await
        .expect("public stream id");
    assert_eq!(stream_id, "wss://api.coinmetro.com/ws?pairs=BTCEUR");
}

#[test]
fn coinmetro_private_capabilities_should_enable_with_bearer_token() {
    let adapter = CoinmetroGatewayAdapter::new(CoinmetroGatewayConfig {
        api_token: "fixture.jwt.token".to_string(),
        device_id: Some("fixture-device".to_string()),
        enabled_private_rest: true,
        ..CoinmetroGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_private_rest);
    assert!(capabilities.supports_balances);
    assert!(capabilities.supports_place_order);
    assert!(capabilities.supports_private_streams);
}

fn assert_unsupported_operation(error: ExchangeApiError, expected: &'static str) {
    match error {
        ExchangeApiError::Unsupported { operation } => assert_eq!(operation, expected),
        other => panic!("expected unsupported {expected}, got {other:?}"),
    }
}
