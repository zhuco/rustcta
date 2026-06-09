use rustcta_exchange_api::{
    BalancesRequest, BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, OpenOrdersRequest,
    OrderBookRequest, OrderBookStrictness, PlaceOrderRequest, PrivateStreamKind,
    PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription, QueryOrderRequest,
    RecentFillsRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::json;

use super::private::{
    build_private_request_spec, p2b_supports_order_type, request_spec_body_from_order,
    P2B_BALANCE_PATH, P2B_CANCEL_ORDER_PATH, P2B_OPEN_ORDERS_PATH, P2B_PLACE_ORDER_PATH,
    P2B_QUERY_ORDER_PATH, P2B_RECENT_FILLS_PATH,
};
use super::signing::build_private_headers;
use super::streams::{
    p2b_ping_payload, p2b_public_depth_subscribe_payload, p2b_public_depth_unsubscribe_payload,
    p2b_public_order_book_rebuild_strategy, p2b_public_order_book_ws_policy,
    p2b_rest_reconciliation_fallback, parse_p2b_depth_update,
};
use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope};
use super::{P2bGatewayAdapter, P2bGatewayConfig};
use crate::adapters::AdapterBackedGateway;

fn fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "symbol_rules_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/p2b/symbol_rules_success.json")
        }
        "order_book_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/p2b/order_book_success.json")
        }
        "order_book_asks_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/p2b/order_book_asks_success.json")
        }
        "order_history_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/p2b/order_history_success.json")
        }
        "open_orders_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/p2b/open_orders_success.json")
        }
        "recent_fills_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/p2b/recent_fills_success.json")
        }
        "request_specs/place_order_limit_buy.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/p2b/request_specs/place_order_limit_buy.json"
        ),
        "request_specs/get_balances.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/p2b/request_specs/get_balances.json"
        ),
        "request_specs/cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/p2b/request_specs/cancel_order.json"
        ),
        "request_specs/get_open_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/p2b/request_specs/get_open_orders.json"
        ),
        "request_specs/get_recent_fills.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/p2b/request_specs/get_recent_fills.json"
        ),
        "request_specs/query_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/p2b/request_specs/query_order.json"
        ),
        "signing_vectors/private_headers.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/p2b/signing_vectors/private_headers.json"
        ),
        "unsupported_boundary.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/p2b/unsupported_boundary.json")
        }
        "ws/public_streams_unsupported.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/p2b/ws/public_streams_unsupported.json"
        ),
        "ws/depth_update_full.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/p2b/ws/depth_update_full.json")
        }
        "ws/depth_update_incremental.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/p2b/ws/depth_update_incremental.json"
        ),
        _ => panic!("unknown p2b fixture {name}"),
    };
    serde_json::from_str(text).expect("p2b fixture")
}

#[test]
fn p2b_capabilities_should_expose_public_rest_and_guarded_private_readbacks() {
    let adapter = P2bGatewayAdapter::default_public().expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
    assert!(capabilities.supports_query_order);
    assert!(capabilities.supports_open_orders);
    assert!(capabilities.supports_recent_fills);
    assert!(capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(capabilities.capabilities_v2.public_rest.is_supported());
    assert!(capabilities.capabilities_v2.public_streams.is_supported());
    assert!(capabilities.capabilities_v2.private_rest.is_supported());
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .supports_public_subscribe
    );
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .resync
            .order_book
    );
    assert_eq!(capabilities.max_order_book_depth, Some(100));
    assert_eq!(
        capabilities.order_book.strictness,
        OrderBookStrictness::BestEffortDelta
    );
    assert!(!capabilities.order_book.supports_sequence);
    assert!(!capabilities.order_book.supports_checksum);
    assert_eq!(capabilities.supports_order_types, vec![OrderType::Limit]);

    let boundary = fixture("unsupported_boundary.json");
    assert_eq!(boundary["task"], "A-32");
    assert_eq!(boundary["scan_only"], true);
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["private_rest_default_enabled"], false);
}

#[test]
fn p2b_named_registration_should_accept_aliases() {
    AdapterBackedGateway::with_named_adapters("p2b-test", ["p2b"]).expect("p2b");
    AdapterBackedGateway::with_named_adapters("p2b-alias-test", ["p2pb2b"]).expect("p2pb2b alias");
}

#[tokio::test]
async fn p2b_should_load_symbol_rules_from_public_markets() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("symbol_rules_success.json")]).await;
    let adapter = P2bGatewayAdapter::new(P2bGatewayConfig {
        rest_base_url: base_url,
        ..P2bGatewayConfig::default()
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
    assert_eq!(rule.price_increment.as_deref(), Some("0.01"));
    assert_eq!(rule.quantity_increment.as_deref(), Some("0.000001"));
    assert_eq!(rule.min_quantity.as_deref(), Some("0.00001"));
    assert_eq!(rule.min_notional.as_deref(), Some("5"));
    assert!(!rule.supports_market_orders);
    assert!(rule.supports_limit_orders);
    assert_eq!(seen.lock().unwrap()[0].path, "/api/v2/public/markets");
}

#[tokio::test]
async fn p2b_should_load_order_book_snapshot() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture("order_book_success.json"),
        fixture("order_book_asks_success.json"),
    ])
    .await;
    let adapter = P2bGatewayAdapter::new(P2bGatewayConfig {
        rest_base_url: base_url,
        ..P2bGatewayConfig::default()
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
    assert_eq!(response.order_book.bids[0].price, 64250.5);
    assert_eq!(response.order_book.asks[0].quantity, 0.21);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[0].path, "/api/v2/public/book");
    assert_eq!(
        requests[0].query.get("market").map(String::as_str),
        Some("BTC_USDT")
    );
    assert_eq!(
        requests[0].query.get("side").map(String::as_str),
        Some("buy")
    );
    assert_eq!(
        requests[0].query.get("limit").map(String::as_str),
        Some("2")
    );
    assert_eq!(requests[1].path, "/api/v2/public/book");
    assert_eq!(
        requests[1].query.get("side").map(String::as_str),
        Some("sell")
    );
}

#[tokio::test]
async fn p2b_private_write_and_account_operations_should_stay_unsupported_even_with_credentials() {
    let adapter = P2bGatewayAdapter::new(P2bGatewayConfig {
        api_key: "p2b_public_test_key".to_string(),
        api_secret: "p2b_private_test_secret".to_string(),
        enabled_private_rest: true,
        ..P2bGatewayConfig::default()
    })
    .expect("adapter");

    assert!(adapter.config.private_rest_configured());
    assert!(adapter.capabilities().supports_private_rest);
    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect_err("private reads disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "p2b.balances_request_spec_only"
        }
    ));
}

#[tokio::test]
async fn p2b_readbacks_should_fail_closed_without_private_rest_guard() {
    let adapter = P2bGatewayAdapter::default_public().expect("adapter");
    let error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-disabled"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("12345".to_string()),
        })
        .await
        .expect_err("disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "p2b.query_order"
        }
    ));
}

#[tokio::test]
async fn p2b_should_run_guarded_read_only_private_rest() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture("order_history_success.json"),
        fixture("open_orders_success.json"),
        fixture("recent_fills_success.json"),
    ])
    .await;
    let adapter = P2bGatewayAdapter::new(P2bGatewayConfig {
        rest_base_url: base_url,
        api_key: "p2b_public_test_key".to_string(),
        api_secret: "p2b_private_test_secret".to_string(),
        enabled_private_rest: true,
        ..P2bGatewayConfig::default()
    })
    .expect("adapter");

    let order = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("12345".to_string()),
        })
        .await
        .expect("query")
        .order
        .expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("12345"));
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    assert_eq!(order.filled_quantity, "0.01");

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            page: None,
        })
        .await
        .expect("open");
    assert_eq!(open.orders.len(), 1);
    assert_eq!(open.orders[0].status, OrderStatus::Open);

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            client_order_id: None,
            exchange_order_id: Some("12345".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(50),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills.len(), 1);
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("fill-001"));
    assert_eq!(fills.fills[0].order_id.as_deref(), Some("12345"));
    assert_eq!(fills.fills[0].price, 64000.0);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, P2B_QUERY_ORDER_PATH);
    assert_eq!(
        requests[0].headers.get("x-txc-apikey").map(String::as_str),
        Some("p2b_public_test_key")
    );
    assert!(requests[0].headers.contains_key("x-txc-payload"));
    assert!(requests[0].headers.contains_key("x-txc-signature"));
    assert_eq!(
        requests[0].body.as_ref().unwrap()["request"],
        P2B_QUERY_ORDER_PATH
    );
    assert_eq!(requests[0].body.as_ref().unwrap()["market"], "BTC_USDT");
    assert_eq!(requests[0].body.as_ref().unwrap()["orderId"], "12345");
    assert_eq!(requests[1].path, P2B_OPEN_ORDERS_PATH);
    assert_eq!(requests[1].body.as_ref().unwrap()["limit"], "100");
    assert_eq!(requests[2].path, P2B_RECENT_FILLS_PATH);
    assert_eq!(requests[2].body.as_ref().unwrap()["limit"], "50");
}

#[tokio::test]
async fn p2b_batch_place_should_be_explicitly_unsupported() {
    let adapter = P2bGatewayAdapter::default_public().expect("adapter");
    let request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch"),
        exchange: exchange_id(),
        orders: vec![place_order_request()],
    };

    let error = adapter
        .batch_place_orders(request)
        .await
        .expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "p2b.batch_place_orders_unverified"
        }
    ));
}

#[tokio::test]
async fn p2b_streams_should_support_public_depth_and_keep_private_boundary() {
    let adapter = P2bGatewayAdapter::default_public().expect("adapter");
    let public_subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let public_descriptor = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-ws"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::OrderBookDelta,
        })
        .await
        .expect("public depth ws descriptor");
    assert_eq!(
        public_descriptor,
        "p2b:wss://apiws.p2pb2b.com/:depth.subscribe:BTC_USDT"
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
            operation: "p2b.private_streams_unverified"
        }
    ));

    let subscribe =
        p2b_public_depth_subscribe_payload(&public_subscription, Some(250), 7).expect("subscribe");
    assert_eq!(
        subscribe,
        json!({
            "method": "depth.subscribe",
            "params": ["BTC_USDT", 100, "0"],
            "id": 7
        })
    );
    let min_limit =
        p2b_public_depth_subscribe_payload(&public_subscription, Some(0), 8).expect("subscribe");
    assert_eq!(min_limit["params"][1], 1);
    assert_eq!(
        p2b_public_depth_unsubscribe_payload(9),
        json!({"method": "depth.unsubscribe", "params": [], "id": 9})
    );
    assert_eq!(
        p2b_ping_payload(10),
        json!({"method": "server.ping", "params": [], "id": 10})
    );

    let policy = p2b_public_order_book_ws_policy();
    assert_eq!(policy.channel, "depth.subscribe");
    assert_eq!(policy.default_limit, 100);
    assert_eq!(policy.interval_ms, 1_000);
    assert_eq!(policy.full_refresh_interval_ms, 60_000);
    assert_eq!(policy.sequence, None);
    assert_eq!(policy.checksum, None);
    assert!(policy.resync.contains("REST /api/v2/public/book"));

    let full = parse_p2b_depth_update(&exchange_id(), &fixture("ws/depth_update_full.json"))
        .expect("full");
    assert!(full.is_full());
    assert_eq!(full.symbol, "BTC_USDT");
    assert_eq!(full.bids[0].price, "64250.50");
    assert_eq!(full.asks[0].quantity, "0.2100");
    assert!(full.deleted_bid_prices().is_empty());

    let incremental =
        parse_p2b_depth_update(&exchange_id(), &fixture("ws/depth_update_incremental.json"))
            .expect("incremental");
    assert!(!incremental.is_full());
    assert_eq!(incremental.bids[0].quantity, "0");
    assert_eq!(incremental.asks[0].quantity, "0.333");
    assert_eq!(incremental.deleted_bid_prices(), vec!["64250.50"]);

    let ws_boundary = fixture("ws/public_streams_unsupported.json");
    assert_eq!(ws_boundary["public_streams"], "native_depth_subscribe");
    assert_eq!(
        ws_boundary["order_book"]["sequence"],
        "unsupported_no_official_sequence"
    );
    assert_eq!(ws_boundary["order_book"]["checksum"], "unsupported");
    assert_eq!(ws_boundary["private_streams"], "unsupported_unverified");
    assert!(p2b_public_order_book_rebuild_strategy().contains("no sequence/checksum"));
    assert_eq!(
        ws_boundary["rest_reconciliation_fallback"][0],
        "/api/v2/orders"
    );
    assert!(p2b_rest_reconciliation_fallback().contains("market_deals"));
}

#[test]
fn p2b_private_header_signing_vector_should_match_fixture() {
    let vector = fixture("signing_vectors/private_headers.json");
    let headers = build_private_headers(
        vector["api_key"].as_str().unwrap(),
        vector["api_secret"].as_str().unwrap(),
        &vector["body"],
    )
    .expect("private headers");
    assert_eq!(
        headers.api_key,
        vector["expected_headers"]["X-TXC-APIKEY"].as_str().unwrap()
    );
    assert_eq!(
        headers.payload,
        vector["expected_headers"]["X-TXC-PAYLOAD"]
            .as_str()
            .unwrap()
    );
    assert_eq!(
        headers.signature,
        vector["expected_headers"]["X-TXC-SIGNATURE"]
            .as_str()
            .unwrap()
    );
}

#[test]
fn p2b_private_request_specs_should_be_sanitized_and_request_spec_only() {
    let place = fixture("request_specs/place_order_limit_buy.json");
    assert_eq!(place["operation"], "p2b.place_order");
    assert_eq!(place["method"], "POST");
    assert_eq!(place["path"], P2B_PLACE_ORDER_PATH);
    assert_eq!(place["trade_enabled"], false);
    assert_eq!(place["body"]["market"], "BTC_USDT");
    assert_eq!(
        place["headers"]["X-TXC-SIGNATURE"],
        "<redacted:hmac-sha512-hex>"
    );

    let balances = fixture("request_specs/get_balances.json");
    assert_eq!(balances["operation"], "p2b.get_balances");
    assert_eq!(balances["method"], "POST");
    assert_eq!(balances["path"], P2B_BALANCE_PATH);
    assert_eq!(balances["request_spec_only"], true);

    let cancel = fixture("request_specs/cancel_order.json");
    assert_eq!(cancel["operation"], "p2b.cancel_order");
    assert_eq!(cancel["method"], "POST");
    assert_eq!(cancel["path"], P2B_CANCEL_ORDER_PATH);
    assert_eq!(cancel["trade_enabled"], false);
    assert_eq!(
        cancel["headers"]["X-TXC-PAYLOAD"],
        "<redacted:base64-json-payload>"
    );

    let open_orders = fixture("request_specs/get_open_orders.json");
    assert_eq!(open_orders["operation"], "p2b.get_open_orders");
    assert_eq!(open_orders["method"], "POST");
    assert_eq!(open_orders["path"], P2B_OPEN_ORDERS_PATH);
    assert_eq!(open_orders["request_spec_only"], false);
    assert_eq!(open_orders["runtime_status"], "guarded_native");
    assert_eq!(open_orders["credential_scope"], "read_only");

    let recent_fills = fixture("request_specs/get_recent_fills.json");
    assert_eq!(recent_fills["operation"], "p2b.get_recent_fills");
    assert_eq!(recent_fills["method"], "POST");
    assert_eq!(recent_fills["path"], P2B_RECENT_FILLS_PATH);
    assert_eq!(recent_fills["request_spec_only"], false);
    assert_eq!(recent_fills["runtime_status"], "guarded_native");
    assert_eq!(recent_fills["credential_scope"], "read_only");

    let query_order = fixture("request_specs/query_order.json");
    assert_eq!(query_order["operation"], "p2b.query_order");
    assert_eq!(query_order["method"], "POST");
    assert_eq!(query_order["path"], P2B_QUERY_ORDER_PATH);
    assert_eq!(query_order["request_spec_only"], false);
    assert_eq!(query_order["runtime_status"], "guarded_native");

    let spec = build_private_request_spec(
        P2B_BALANCE_PATH,
        json!({}),
        1698508469,
        "p2b_public_test_key",
        "p2b_private_test_secret",
    )
    .expect("request spec");
    assert_eq!(spec.method, "POST");
    assert_eq!(spec.body["request"], P2B_BALANCE_PATH);
    assert_eq!(spec.headers.api_key, "p2b_public_test_key");
    assert!(!spec.headers.payload.is_empty());
    assert_eq!(spec.headers.signature.len(), 128);
}

#[test]
fn p2b_private_order_request_body_should_match_official_shape() {
    let request = place_order_request();
    let body = request_spec_body_from_order(&request);
    assert_eq!(
        body,
        json!({
            "market": "BTC_USDT",
            "side": "buy",
            "amount": "0.01",
            "price": "64000"
        })
    );
    assert!(p2b_supports_order_type(OrderType::Limit));
    assert!(!p2b_supports_order_type(OrderType::Market));
}

fn place_order_request() -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol_scope(),
        client_order_id: None,
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("64000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}
