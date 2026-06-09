use rustcta_exchange_api::{
    BalancesRequest, BatchAtomicity, BatchCancelOrdersRequest, BatchExecutionMode,
    BatchPlaceOrdersRequest, CancelAllOrdersRequest, CancelOrderRequest, ExchangeApiError,
    ExchangeClient, FeesRequest, OpenOrdersRequest, OrderBookRequest, PlaceOrderRequest,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    QueryOrderRequest, RecentFillsRequest, SymbolRulesRequest, SymbolScope, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeSymbol, MarketType, OrderSide, OrderStatus, OrderType,
    PositionSide,
};

use super::private::{
    batch_cancel_body, batch_place_body, build_signed_private_request_spec, cancel_order_body,
    request_spec_body_from_order, LATOKEN_BALANCES_PATH, LATOKEN_BATCH_CANCEL_PATH,
    LATOKEN_BATCH_PLACE_PATH, LATOKEN_CANCEL_ALL_PATH, LATOKEN_CANCEL_ORDER_PATH,
    LATOKEN_FEE_PATH_PREFIX, LATOKEN_OPEN_ORDERS_PATH, LATOKEN_PLACE_ORDER_PATH,
    LATOKEN_RECENT_FILLS_PATH,
};
use super::private_parser::{
    parse_balances_ack, parse_batch_cancel_orders_ack, parse_batch_place_orders_ack,
    parse_fee_snapshot,
};
use super::signing::{rest_signature, LatokenDigest};
use super::streams::{
    heartbeat_policy, latoken_check_nonce_continuity, latoken_public_book_subscribe_frame,
    latoken_public_order_book_ws_policy, parse_public_book_message, public_book_destination,
    public_book_nonce, rest_reconciliation_fallback, stomp_subscribe_frame,
    stomp_unsubscribe_frame, ws_auth_headers, LatokenNonceContinuity,
};
use super::test_support::{context, exchange_id, private_adapter, spawn_rest_server, symbol_scope};
use super::{LatokenGatewayAdapter, LatokenGatewayConfig};
use crate::adapters::AdapterBackedGateway;

fn fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "pairs_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/latoken/pairs_success.json")
        }
        "currencies_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/currencies_success.json"
        ),
        "orderbook_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/orderbook_success.json"
        ),
        "request_specs/place_order_limit_buy.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/request_specs/place_order_limit_buy.json"
        ),
        "request_specs/get_balances.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/request_specs/get_balances.json"
        ),
        "request_specs/get_fees.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/request_specs/get_fees.json"
        ),
        "request_specs/batch_place_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/request_specs/batch_place_orders.json"
        ),
        "request_specs/batch_cancel_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/request_specs/batch_cancel_orders.json"
        ),
        "request_specs/product_line_source_boundary.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/request_specs/product_line_source_boundary.json"
        ),
        "signing_vectors/rest_place_order_hmac_sha256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/signing_vectors/rest_place_order_hmac_sha256.json"
        ),
        "signing_vectors/rest_get_fees_hmac_sha256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/signing_vectors/rest_get_fees_hmac_sha256.json"
        ),
        "signing_vectors/rest_batch_place_hmac_sha256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/signing_vectors/rest_batch_place_hmac_sha256.json"
        ),
        "signing_vectors/rest_batch_cancel_hmac_sha256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/signing_vectors/rest_batch_cancel_hmac_sha256.json"
        ),
        "signing_vectors/ws_auth_hmac_sha256.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/signing_vectors/ws_auth_hmac_sha256.json"
        ),
        "parser/batch_place_orders_ack.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/parser/batch_place_orders_ack.json"
        ),
        "parser/batch_cancel_orders_ack.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/parser/batch_cancel_orders_ack.json"
        ),
        "parser/balances_ack.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/parser/balances_ack.json"
        ),
        "parser/fees_ack.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/parser/fees_ack.json"
        ),
        "ws/public_orderbook_frame.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/ws/public_orderbook_frame.json"
        ),
        "unsupported_boundary.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/latoken/unsupported_boundary.json"
        ),
        _ => panic!("unknown LATOKEN fixture {name}"),
    };
    serde_json::from_str(text).expect("LATOKEN fixture")
}

#[test]
fn latoken_capabilities_should_expose_scan_only_spot_public_rest() {
    let adapter = LatokenGatewayAdapter::default_public().expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
    assert!(capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(capabilities.capabilities_v2.public_rest.is_supported());
    assert!(capabilities.capabilities_v2.public_streams.is_supported());
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.atomicity,
        BatchAtomicity::Partial
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.max_items,
        Some(50)
    );
    assert!(!capabilities
        .capabilities_v2
        .batch_place_orders
        .support
        .is_supported());
    assert_eq!(
        capabilities.capabilities_v2.batch_cancel_orders.mode,
        BatchExecutionMode::Native
    );
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "batch_place_orders"
            && endpoint.path.as_deref() == Some(LATOKEN_BATCH_PLACE_PATH)));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "batch_cancel_orders"
            && endpoint.path.as_deref() == Some(LATOKEN_BATCH_CANCEL_PATH)));
    for operation in ["contract_product", "futures_product"] {
        let endpoint = capabilities
            .capabilities_v2
            .endpoints
            .iter()
            .find(|endpoint| endpoint.operation == operation)
            .expect("product-line boundary endpoint");
        assert!(!endpoint.support.is_supported());
        assert_eq!(
            endpoint.market_types,
            vec![MarketType::Futures, MarketType::Perpetual]
        );
        assert_eq!(endpoint.method, None);
        assert_eq!(endpoint.path, None);
    }
    assert!(capabilities.order_book.supports_sequence);

    let boundary = fixture("unsupported_boundary.json");
    assert_eq!(boundary["scan_only"], true);
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["public_streams"], "native_stomp_public_book");
}

#[test]
fn latoken_product_line_boundary_should_pin_futures_gap() {
    let boundary = fixture("request_specs/product_line_source_boundary.json");
    assert_eq!(boundary["exchange"], "latoken");
    assert_eq!(boundary["status"], "project_unimplemented");
    assert_eq!(boundary["current_adapter_scope"][0], "spot");
    assert_eq!(boundary["official_product_lines"][0], "futures");
    assert_eq!(boundary["official_product_lines"][1], "perpetual");
    assert_eq!(
        boundary["runtime_isolation"],
        "Keep LATOKEN futures clues outside the spot REST/STOMP adapter until endpoint specs, symbols and risk semantics are verified."
    );
    assert_eq!(
        boundary["required_endpoints"]["market_metadata"][0],
        "futures/perpetual instruments"
    );
    assert_eq!(
        boundary["required_endpoints"]["private_account"][1],
        "positions"
    );
    assert_eq!(
        boundary["required_endpoints"]["private_order_lifecycle"][0],
        "futures/perpetual place order"
    );
    assert_eq!(
        boundary["required_auth_and_scope"][0],
        "confirm whether futures uses LATOKEN v2 HMAC headers or a separate auth flow"
    );
    assert_eq!(
        boundary["unsafe_to_promote_without"][0],
        "stable official futures/perpetual endpoint specs"
    );
}

#[test]
fn latoken_named_registration_should_accept_adapter_id() {
    AdapterBackedGateway::with_named_adapters("latoken-test", ["latoken"]).expect("latoken");
}

#[tokio::test]
async fn latoken_should_load_symbol_rules_from_pairs_and_currencies() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture("pairs_success.json"),
        fixture("currencies_success.json"),
    ])
    .await;
    let adapter = LatokenGatewayAdapter::new(LatokenGatewayConfig {
        rest_base_url: base_url,
        ..LatokenGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    let rule = &response.rules[0];
    assert_eq!(rule.base_asset, "BTC");
    assert_eq!(rule.quote_asset, "USDT");
    assert_eq!(rule.symbol.exchange_symbol.symbol, "BTC_USDT");
    assert_eq!(rule.price_increment.as_deref(), Some("0.010000000"));
    assert_eq!(rule.quantity_increment.as_deref(), Some("0.000001000"));
    assert_eq!(rule.min_quantity.as_deref(), Some("0.00001"));
    assert_eq!(rule.min_notional.as_deref(), Some("5"));

    let seen = seen.lock().unwrap();
    assert_eq!(seen[0].method, "GET");
    assert_eq!(seen[0].path, "/v2/pair");
    assert_eq!(seen[1].path, "/v2/currency");
}

#[tokio::test]
async fn latoken_should_load_order_book_snapshot() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("orderbook_success.json")]).await;
    let adapter = LatokenGatewayAdapter::new(LatokenGatewayConfig {
        rest_base_url: base_url,
        ..LatokenGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(2),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids.len(), 2);
    assert_eq!(response.order_book.asks.len(), 2);
    assert_eq!(response.order_book.bids[0].price, 62679.15);
    assert_eq!(response.order_book.asks[0].quantity, 0.1177424);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/v2/book/BTC/USDT");
    assert_eq!(request.query.get("limit").map(String::as_str), Some("2"));
}

#[tokio::test]
async fn latoken_private_capabilities_should_enable_signed_runtime_with_credentials() {
    let adapter = LatokenGatewayAdapter::new(LatokenGatewayConfig {
        api_key: Some("latoken_public_test_key".to_string()),
        api_secret: Some("latoken_private_test_secret".to_string()),
        enabled_private_rest: true,
        ..LatokenGatewayConfig::default()
    })
    .expect("adapter");

    assert!(adapter.config.private_rest_configured());
    assert!(adapter.config.private_rest_enabled());
    assert!(adapter.capabilities().supports_private_rest);
    assert!(adapter.capabilities().supports_balances);
    assert!(adapter.capabilities().supports_batch_place_order);
    assert!(adapter.capabilities().supports_batch_cancel_order);
}

#[tokio::test]
async fn latoken_balances_runtime_should_send_signed_private_rest_and_parse_readback() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("parser/balances_ack.json")]).await;
    let adapter = private_adapter(base_url);

    let response = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: vec!["BTC".to_string()],
        })
        .await
        .expect("balances runtime");
    assert_eq!(response.balances.len(), 1);
    assert_eq!(response.balances[0].balances.len(), 1);
    assert_eq!(response.balances[0].balances[0].asset, "BTC");
    assert_eq!(response.balances[0].balances[0].total, 0.012);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, LATOKEN_BALANCES_PATH);
    assert_eq!(
        requests[0].query.get("zeros").map(String::as_str),
        Some("false")
    );
    assert_eq!(requests[0].header("x-la-apikey"), Some("test-key"));
    assert_eq!(requests[0].header("x-la-digest"), Some("HMAC-SHA256"));
    assert!(requests[0].header("x-la-signature").is_some());
}

#[tokio::test]
async fn latoken_fees_runtime_should_send_signed_private_rest_and_parse_rates() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("parser/fees_ack.json")]).await;
    let adapter = private_adapter(base_url);

    let response = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("fees runtime");
    assert_eq!(response.fees.len(), 1);
    assert_eq!(response.fees[0].maker_rate, "0.001");
    assert_eq!(response.fees[0].taker_rate, "0.002");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/v2/auth/trade/fee/BTC/USDT");
    assert_eq!(requests[0].header("x-la-apikey"), Some("test-key"));
    assert_eq!(requests[0].header("x-la-digest"), Some("HMAC-SHA256"));
    assert!(requests[0].header("x-la-signature").is_some());
}

#[tokio::test]
async fn latoken_batch_place_should_be_explicitly_request_spec_only() {
    let adapter = LatokenGatewayAdapter::default_public().expect("adapter");
    let request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch"),
        exchange: exchange_id(),
        orders: vec![btc_batch_place_order_request(), eth_place_order_request()],
    };

    let error = adapter
        .batch_place_orders(request)
        .await
        .expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "latoken.batch_place_orders_private_rest_not_enabled"
        }
    ));
}

#[tokio::test]
async fn latoken_batch_cancel_should_be_explicitly_request_spec_only() {
    let adapter = LatokenGatewayAdapter::default_public().expect("adapter");
    let request = BatchCancelOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-cancel"),
        exchange: exchange_id(),
        cancels: vec![cancel_request("latoken_test_order_id_001")],
    };

    let error = adapter
        .batch_cancel_orders(request)
        .await
        .expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "latoken.batch_cancel_orders_private_rest_not_enabled"
        }
    ));
}

#[tokio::test]
async fn latoken_batch_runtime_should_send_signed_private_rest_and_parse_reports() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture("parser/batch_place_orders_ack.json"),
        fixture("parser/batch_cancel_orders_ack.json"),
    ])
    .await;
    let adapter = private_adapter(base_url);
    let place_request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place-runtime"),
        exchange: exchange_id(),
        orders: vec![btc_batch_place_order_request(), eth_place_order_request()],
    };
    let placed = adapter
        .batch_place_orders(place_request)
        .await
        .expect("batch place runtime");
    assert_eq!(placed.orders.len(), 1);
    assert_eq!(
        placed.report.as_ref().expect("place report").failed_count(),
        1
    );

    let cancel_request = BatchCancelOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-cancel-runtime"),
        exchange: exchange_id(),
        cancels: vec![
            cancel_request("latoken_test_order_id_001"),
            cancel_request("latoken_test_order_id_002"),
        ],
    };
    let cancelled = adapter
        .batch_cancel_orders(cancel_request)
        .await
        .expect("batch cancel runtime");
    assert_eq!(cancelled.cancelled_count, 1);
    assert_eq!(
        cancelled
            .report
            .as_ref()
            .expect("cancel report")
            .failed_count(),
        1
    );

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, LATOKEN_BATCH_PLACE_PATH);
    assert_eq!(requests[0].header("x-la-apikey"), Some("test-key"));
    assert_eq!(requests[0].header("x-la-digest"), Some("HMAC-SHA256"));
    assert!(requests[0].header("x-la-signature").is_some());
    assert_eq!(
        requests[0].body.as_ref().expect("body")["orders"][0]["clientOrderId"],
        "latoken_batch_client_order_001"
    );
    assert_eq!(requests[1].method, "POST");
    assert_eq!(requests[1].path, LATOKEN_BATCH_CANCEL_PATH);
    assert_eq!(
        requests[1].body.as_ref().expect("body")["orders"][0]["id"],
        "latoken_test_order_id_001"
    );
}

#[tokio::test]
async fn latoken_core_runtime_should_send_signed_private_rest_and_parse_readbacks() {
    let (base_url, seen) = spawn_rest_server(vec![
        serde_json::json!({
            "status": "SUCCESS",
            "message": { "id": "latoken_created_order_id_001", "clientOrderId": "latoken_test_client_order_001", "status": "SUCCESS" }
        }),
        serde_json::json!({
            "status": "SUCCESS",
            "message": { "id": "latoken_created_order_id_001", "status": "SUCCESS" }
        }),
        serde_json::json!({
            "status": "SUCCESS",
            "message": [{ "id": "latoken_created_order_id_002", "status": "SUCCESS" }]
        }),
        serde_json::json!({
            "id": "latoken_created_order_id_001",
            "clientOrderId": "latoken_test_client_order_001",
            "side": "BUY",
            "type": "LIMIT",
            "status": "ACTIVE",
            "quantity": "0.01",
            "price": "64000"
        }),
        serde_json::json!([{
            "id": "latoken_created_order_id_001",
            "clientOrderId": "latoken_test_client_order_001",
            "side": "BUY",
            "type": "LIMIT",
            "status": "ACTIVE",
            "quantity": "0.01",
            "price": "64000"
        }]),
        serde_json::json!([{
            "id": "trade-1",
            "orderId": "latoken_created_order_id_001",
            "clientOrderId": "latoken_test_client_order_001",
            "side": "BUY",
            "price": "64000",
            "quantity": "0.01",
            "fee": "0.1",
            "feeCurrency": "USDT",
            "liquidity": "taker",
            "timestamp": 1713200525092_i64
        }]),
    ])
    .await;
    let adapter = private_adapter(base_url);

    adapter
        .place_order(place_order_request())
        .await
        .expect("place order");
    adapter
        .cancel_order(cancel_request("latoken_created_order_id_001"))
        .await
        .expect("cancel order");
    adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
        })
        .await
        .expect("cancel all");
    adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("latoken_created_order_id_001".to_string()),
        })
        .await
        .expect("query");
    assert_eq!(
        adapter
            .get_open_orders(OpenOrdersRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("open"),
                exchange: exchange_id(),
                market_type: Some(MarketType::Spot),
                symbol: Some(symbol_scope()),
                page: None,
            })
            .await
            .expect("open orders")
            .orders
            .len(),
        1
    );
    assert_eq!(
        adapter
            .get_recent_fills(RecentFillsRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("fills"),
                exchange: exchange_id(),
                market_type: Some(MarketType::Spot),
                symbol: Some(symbol_scope()),
                client_order_id: None,
                exchange_order_id: Some("latoken_created_order_id_001".to_string()),
                from_trade_id: None,
                start_time: None,
                end_time: None,
                limit: Some(100),
                page: None,
            })
            .await
            .expect("fills")
            .fills
            .len(),
        1
    );

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, LATOKEN_PLACE_ORDER_PATH);
    assert_eq!(requests[0].header("x-la-apikey"), Some("test-key"));
    assert_eq!(requests[1].path, LATOKEN_CANCEL_ORDER_PATH);
    assert_eq!(requests[2].path, LATOKEN_CANCEL_ALL_PATH);
    assert_eq!(
        requests[3].path,
        "/v2/auth/order/getOrder/latoken_created_order_id_001"
    );
    assert_eq!(requests[4].path, LATOKEN_OPEN_ORDERS_PATH);
    assert_eq!(requests[5].path, LATOKEN_RECENT_FILLS_PATH);
}

#[tokio::test]
async fn latoken_streams_should_deliver_public_book_frame_but_keep_private_runtime_unsupported() {
    let adapter = LatokenGatewayAdapter::default_public().expect("adapter");
    let public_frame = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-ws"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::OrderBookSnapshot,
        })
        .await
        .expect("public ws frame");
    assert_eq!(
        public_frame,
        "SUBSCRIBE\nid:public-ws\ndestination:/v1/book/BTC/USDT\n\n\u{0}"
    );

    let private_error = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-ws"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            account_id: AccountId::new("account").expect("account"),
            kind: PrivateStreamKind::Orders,
        })
        .await
        .expect_err("private ws unsupported");
    assert!(matches!(
        private_error,
        ExchangeApiError::Unsupported {
            operation: "latoken.private_streams_user_id_auth_unverified"
        }
    ));

    let destination = public_book_destination(&symbol_scope()).expect("destination");
    assert_eq!(destination, "/v1/book/BTC/USDT");
    assert!(stomp_subscribe_frame("sub-1", &destination).starts_with("SUBSCRIBE\nid:sub-1"));
    assert_eq!(
        latoken_public_book_subscribe_frame(
            &PublicStreamSubscription {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("public-ws"),
                symbol: symbol_scope(),
                kind: PublicStreamKind::OrderBookDelta,
            },
            "sub-2"
        )
        .expect("subscribe frame"),
        "SUBSCRIBE\nid:sub-2\ndestination:/v1/book/BTC/USDT\n\n\u{0}"
    );
    assert_eq!(
        stomp_unsubscribe_frame("sub-1"),
        "UNSUBSCRIBE\nid:sub-1\n\n\u{0}"
    );
    let policy = latoken_public_order_book_ws_policy();
    assert_eq!(policy.destination_template, "/v1/book/{base}/{quote}");
    assert_eq!(policy.interval_ms, None);
    assert_eq!(policy.depth, None);
    assert_eq!(policy.sequence_field, "nonce");
    assert_eq!(policy.checksum, None);
    assert!(heartbeat_policy().contains("nonce sequence gaps"));
    assert!(rest_reconciliation_fallback().contains("/v2/auth/order/active"));
}

#[test]
fn latoken_signing_vectors_should_match_fixtures() {
    let vector = fixture("signing_vectors/rest_place_order_hmac_sha256.json");
    let signature = rest_signature(
        vector["api_secret"].as_str().unwrap(),
        vector["canonical_payload"].as_str().unwrap(),
        LatokenDigest::Sha256,
    )
    .expect("signature");
    assert_eq!(signature, vector["expected_signature"].as_str().unwrap());

    let ws_vector = fixture("signing_vectors/ws_auth_hmac_sha256.json");
    let headers = ws_auth_headers(
        ws_vector["api_key"].as_str().unwrap(),
        ws_vector["api_secret"].as_str().unwrap(),
        ws_vector["timestamp_ms"].as_str().unwrap(),
    )
    .expect("ws headers");
    assert_eq!(
        headers["X-LA-SIGNATURE"].as_str().unwrap(),
        ws_vector["expected_signature"].as_str().unwrap()
    );

    for fixture_name in [
        "signing_vectors/rest_get_fees_hmac_sha256.json",
        "signing_vectors/rest_batch_place_hmac_sha256.json",
        "signing_vectors/rest_batch_cancel_hmac_sha256.json",
    ] {
        let vector = fixture(fixture_name);
        let signature = rest_signature(
            vector["api_secret"].as_str().unwrap(),
            vector["canonical_payload"].as_str().unwrap(),
            LatokenDigest::Sha256,
        )
        .expect("batch signature");
        assert_eq!(signature, vector["expected_signature"].as_str().unwrap());
    }
}

#[test]
fn latoken_private_request_specs_should_be_sanitized_and_request_spec_only() {
    let place_fixture = fixture("request_specs/place_order_limit_buy.json");
    assert_eq!(place_fixture["operation"], "latoken.place_order");
    assert_eq!(place_fixture["method"], "POST");
    assert_eq!(place_fixture["path"], LATOKEN_PLACE_ORDER_PATH);
    assert_eq!(place_fixture["trade_enabled"], false);
    assert_eq!(
        place_fixture["headers"]["X-LA-APIKEY"],
        "<redacted:api-key>"
    );

    let request = place_order_request();
    let (body, body_params) = request_spec_body_from_order(&request).expect("body");
    assert_eq!(body["baseCurrency"], "BTC");
    assert_eq!(body["quoteCurrency"], "USDT");
    let spec = build_signed_private_request_spec(
        "POST",
        LATOKEN_PLACE_ORDER_PATH,
        Vec::new(),
        body_params,
        Some(body),
        "latoken_public_test_key",
        "latoken_private_test_secret",
    )
    .expect("request spec");
    assert_eq!(
        spec.canonical_payload,
        place_fixture["canonical_payload"].as_str().unwrap()
    );
    assert_eq!(
        spec.signature,
        fixture("signing_vectors/rest_place_order_hmac_sha256.json")["expected_signature"]
            .as_str()
            .unwrap()
    );

    let balances = fixture("request_specs/get_balances.json");
    assert_eq!(balances["operation"], "latoken.get_balances");
    assert_eq!(balances["path"], LATOKEN_BALANCES_PATH);
    let balances_spec = build_signed_private_request_spec(
        "GET",
        LATOKEN_BALANCES_PATH,
        vec![("zeros".to_string(), "false".to_string())],
        Vec::new(),
        None,
        "latoken_public_test_key",
        "latoken_private_test_secret",
    )
    .expect("balances request spec");
    assert_eq!(
        balances_spec.canonical_payload,
        balances["canonical_payload"].as_str().unwrap()
    );

    let fees = fixture("request_specs/get_fees.json");
    assert_eq!(fees["operation"], "latoken.get_fees");
    assert_eq!(
        fees["path"],
        format!("{LATOKEN_FEE_PATH_PREFIX}/{{currency}}/{{quote}}")
    );
    assert_eq!(fees["runtime_status"], "credential_gated_runtime");
    let fees_spec = build_signed_private_request_spec(
        "GET",
        "/v2/auth/trade/fee/BTC/USDT",
        Vec::new(),
        Vec::new(),
        None,
        "latoken_public_test_key",
        "latoken_private_test_secret",
    )
    .expect("fees request spec");
    assert_eq!(
        fees_spec.canonical_payload,
        fixture("signing_vectors/rest_get_fees_hmac_sha256.json")["canonical_payload"]
            .as_str()
            .unwrap()
    );
    assert_eq!(
        fees_spec.signature,
        fixture("signing_vectors/rest_get_fees_hmac_sha256.json")["expected_signature"]
            .as_str()
            .unwrap()
    );

    let parsed_fee =
        parse_fee_snapshot(&symbol_scope(), &fixture("parser/fees_ack.json")).expect("fee parser");
    assert_eq!(parsed_fee.maker_rate, "0.001");
    assert_eq!(parsed_fee.taker_rate, "0.002");

    let (cancel_body, cancel_params) = cancel_order_body("latoken_test_order_id_001");
    assert_eq!(cancel_body["id"], "latoken_test_order_id_001");
    assert_eq!(cancel_params[0].1, "latoken_test_order_id_001");
}

#[test]
fn latoken_batch_private_request_specs_should_match_signing_vectors() {
    let batch_place_fixture = fixture("request_specs/batch_place_orders.json");
    let batch_place_request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place"),
        exchange: exchange_id(),
        orders: vec![btc_batch_place_order_request(), eth_place_order_request()],
    };
    let (body, body_params) = batch_place_body(&batch_place_request).expect("batch body");
    assert_eq!(body, batch_place_fixture["body"]);
    let spec = build_signed_private_request_spec(
        "POST",
        LATOKEN_BATCH_PLACE_PATH,
        Vec::new(),
        body_params,
        Some(body),
        "latoken_public_test_key",
        "latoken_private_test_secret",
    )
    .expect("batch place spec");
    assert_eq!(
        spec.canonical_payload,
        fixture("signing_vectors/rest_batch_place_hmac_sha256.json")["canonical_payload"]
            .as_str()
            .unwrap()
    );
    assert_eq!(
        spec.signature,
        fixture("signing_vectors/rest_batch_place_hmac_sha256.json")["expected_signature"]
            .as_str()
            .unwrap()
    );

    let batch_cancel_fixture = fixture("request_specs/batch_cancel_orders.json");
    let batch_cancel_request = BatchCancelOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-cancel"),
        exchange: exchange_id(),
        cancels: vec![
            cancel_request("latoken_test_order_id_001"),
            cancel_request("latoken_test_order_id_002"),
        ],
    };
    let (body, body_params) = batch_cancel_body(&batch_cancel_request).expect("batch cancel body");
    assert_eq!(body, batch_cancel_fixture["body"]);
    let spec = build_signed_private_request_spec(
        "POST",
        LATOKEN_BATCH_CANCEL_PATH,
        Vec::new(),
        body_params,
        Some(body),
        "latoken_public_test_key",
        "latoken_private_test_secret",
    )
    .expect("batch cancel spec");
    assert_eq!(
        spec.canonical_payload,
        batch_cancel_fixture["canonical_payload"].as_str().unwrap()
    );
    assert_eq!(
        spec.signature,
        fixture("signing_vectors/rest_batch_cancel_hmac_sha256.json")["expected_signature"]
            .as_str()
            .unwrap()
    );
}

#[test]
fn latoken_batch_private_parsers_should_parse_partial_fixtures() {
    let balances = parse_balances_ack(
        &exchange_id(),
        context("balances").tenant_id.unwrap(),
        context("balances").account_id.unwrap(),
        &["BTC".to_string()],
        &fixture("parser/balances_ack.json"),
    )
    .expect("balances");
    assert_eq!(balances[0].balances.len(), 1);
    assert_eq!(balances[0].balances[0].locked, 0.002);

    let place_request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-place"),
        exchange: exchange_id(),
        orders: vec![btc_batch_place_order_request(), eth_place_order_request()],
    };
    let place_response = parse_batch_place_orders_ack(
        &exchange_id(),
        &place_request,
        &fixture("parser/batch_place_orders_ack.json"),
    )
    .expect("batch place parser");
    assert_eq!(place_response.orders.len(), 1);
    assert_eq!(
        place_response.orders[0].exchange_order_id.as_deref(),
        Some("latoken_created_order_id_001")
    );
    let place_report = place_response.report.expect("place report");
    assert_eq!(place_report.total_items, 2);
    assert_eq!(place_report.succeeded_count(), 1);
    assert_eq!(place_report.failed_count(), 1);

    let cancel_request = BatchCancelOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-cancel"),
        exchange: exchange_id(),
        cancels: vec![
            cancel_request("latoken_test_order_id_001"),
            cancel_request("latoken_test_order_id_002"),
        ],
    };
    let cancel_response = parse_batch_cancel_orders_ack(
        &exchange_id(),
        &cancel_request,
        &fixture("parser/batch_cancel_orders_ack.json"),
    )
    .expect("batch cancel parser");
    assert_eq!(cancel_response.cancelled_count, 1);
    assert_eq!(cancel_response.orders[0].status, OrderStatus::Cancelled);
    let cancel_report = cancel_response.report.expect("cancel report");
    assert_eq!(cancel_report.succeeded_count(), 1);
    assert_eq!(cancel_report.failed_count(), 1);
}

#[test]
fn latoken_ws_public_book_fixture_should_parse_order_book_payload() {
    let value = fixture("ws/public_orderbook_frame.json");
    let order_book =
        parse_public_book_message(&exchange_id(), symbol_scope(), &value).expect("book");
    assert_eq!(order_book.bids.len(), 1);
    assert_eq!(order_book.asks.len(), 1);
    assert_eq!(order_book.bids[0].price, 62679.15);
    assert_eq!(order_book.sequence, Some(42));
    assert_eq!(public_book_nonce(&value), Some(42));
}

#[test]
fn latoken_nonce_continuity_should_request_reconnect_on_gap_or_regression() {
    assert_eq!(
        latoken_check_nonce_continuity(None, 42),
        LatokenNonceContinuity::First
    );
    assert_eq!(
        latoken_check_nonce_continuity(Some(42), 43),
        LatokenNonceContinuity::Continuous
    );
    assert_eq!(
        latoken_check_nonce_continuity(Some(43), 45),
        LatokenNonceContinuity::Gap {
            expected: 44,
            actual: 45
        }
    );
    assert!(latoken_check_nonce_continuity(Some(43), 45).requires_reconnect());
    assert_eq!(
        latoken_check_nonce_continuity(Some(43), 43),
        LatokenNonceContinuity::NotIncreasing {
            previous: 43,
            actual: 43
        }
    );
    assert!(latoken_check_nonce_continuity(Some(43), 43).requires_reconnect());
}

fn place_order_request() -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place-order"),
        symbol: symbol_scope(),
        client_order_id: Some("latoken_test_client_order_001".to_string()),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("64000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}

fn eth_place_order_request() -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place-order-eth"),
        symbol: eth_symbol_scope(),
        client_order_id: Some("latoken_batch_client_order_002".to_string()),
        side: OrderSide::Sell,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.1".to_string(),
        price: Some("3500".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}

fn btc_batch_place_order_request() -> PlaceOrderRequest {
    PlaceOrderRequest {
        client_order_id: Some("latoken_batch_client_order_001".to_string()),
        ..place_order_request()
    }
}

fn cancel_request(order_id: &str) -> CancelOrderRequest {
    CancelOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("cancel-order"),
        symbol: symbol_scope(),
        client_order_id: None,
        exchange_order_id: Some(order_id.to_string()),
    }
}

fn eth_symbol_scope() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("ETH", "USDT").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "ETH_USDT")
            .expect("symbol"),
    }
}
