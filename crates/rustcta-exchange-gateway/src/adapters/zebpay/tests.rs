use rustcta_exchange_api::{
    BalancesRequest, BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, OpenOrdersRequest,
    PlaceOrderRequest, PositionsRequest, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, QueryOrderRequest, RecentFillsRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, ExchangeSymbol, MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::private::{
    build_private_get_request_spec, build_private_request_spec, open_orders_query,
    query_order_query, recent_fills_path, request_spec_body_from_order, zebpay_supports_order_type,
    ZEBPAY_BALANCE_PATH, ZEBPAY_CANCEL_ALL_ORDERS_PATH, ZEBPAY_CANCEL_ORDER_PATH,
    ZEBPAY_OPEN_ORDERS_PATH, ZEBPAY_PLACE_ORDER_PATH, ZEBPAY_QUERY_ORDER_PATH,
    ZEBPAY_RECENT_FILLS_PATH,
};
use super::public::{market_query, order_book_endpoint, order_book_query};
use super::signing::build_bearer_private_headers;
use super::streams::zebpay_rest_reconciliation_fallback;
use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope};
use super::{ZebpayGatewayAdapter, ZebpayGatewayConfig};
use crate::adapters::AdapterBackedGateway;

fn fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "symbol_rules_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/symbol_rules_success.json"
        ),
        "order_book_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/order_book_success.json"
        ),
        "order_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/zebpay/order_success.json")
        }
        "open_orders_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/open_orders_success.json"
        ),
        "recent_fills_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/recent_fills_success.json"
        ),
        "request_specs/place_order_limit_buy.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/request_specs/place_order_limit_buy.json"
        ),
        "request_specs/get_balances.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/request_specs/get_balances.json"
        ),
        "request_specs/cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/request_specs/cancel_order.json"
        ),
        "request_specs/query_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/request_specs/query_order.json"
        ),
        "request_specs/get_open_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/request_specs/get_open_orders.json"
        ),
        "request_specs/get_recent_fills.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/request_specs/get_recent_fills.json"
        ),
        "request_specs/product_line_source_boundary.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/request_specs/product_line_source_boundary.json"
        ),
        "signing_vectors/bearer_auth.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/signing_vectors/bearer_auth.json"
        ),
        "unsupported_boundary.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/unsupported_boundary.json"
        ),
        "ws/public_streams_unsupported.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/zebpay/ws/public_streams_unsupported.json"
        ),
        _ => panic!("unknown zebpay fixture {name}"),
    };
    serde_json::from_str(text).expect("zebpay fixture")
}

#[test]
fn zebpay_capabilities_should_expose_scan_only_spot_public_rest() {
    let adapter = ZebpayGatewayAdapter::default_public().expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_cancel_order);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(capabilities.capabilities_v2.public_rest.is_supported());
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());
    assert!(!capabilities.supports_query_order);

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

    let boundary = fixture("unsupported_boundary.json");
    assert_eq!(boundary["scan_only"], true);
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["private_rest_default_enabled"], false);
    assert_eq!(
        boundary["region_kyc_boundary"],
        "requires_zebpay_api_onboarding"
    );
}

#[test]
fn zebpay_product_line_boundary_should_pin_futures_and_perpetual_gap() {
    let boundary = fixture("request_specs/product_line_source_boundary.json");
    assert_eq!(boundary["exchange"], "zebpay");
    assert_eq!(boundary["status"], "project_unimplemented");
    assert_eq!(boundary["current_adapter_scope"][0], "spot");
    assert_eq!(boundary["official_product_lines"][0], "futures");
    assert_eq!(boundary["official_product_lines"][1], "perpetual");
    assert_eq!(
        boundary["runtime_isolation"],
        "ZebPay futures/perpetual API access requires onboarding, product scope and bearer-token validation separate from the scan-only spot adapter."
    );
    assert_eq!(
        boundary["required_endpoints"]["market_metadata"][0],
        "futures/perpetual instruments"
    );
    assert_eq!(
        boundary["required_endpoints"]["private_order_lifecycle"][0],
        "futures/perpetual place order"
    );
    assert_eq!(
        boundary["unsafe_to_promote_without"][0],
        "ZebPay API onboarding approval for futures/perpetual scope"
    );
}

#[test]
fn zebpay_named_registration_should_accept_adapter_id() {
    AdapterBackedGateway::with_named_adapters("zebpay-test", ["zebpay"]).expect("zebpay");
}

#[test]
fn zebpay_should_parse_symbol_rules_from_public_market_fixture() {
    let rules =
        parse_symbol_rules(&exchange_id(), &fixture("symbol_rules_success.json")).expect("rules");

    assert_eq!(rules.len(), 2);
    let rule = rules
        .iter()
        .find(|rule| rule.symbol.exchange_symbol.symbol == "BTC-INR")
        .expect("BTC-INR rule");
    assert_eq!(rule.base_asset, "BTC");
    assert_eq!(rule.quote_asset, "INR");
    assert_eq!(rule.symbol.exchange_symbol.symbol, "BTC-INR");
    assert_eq!(rule.price_increment.as_deref(), Some("0.01"));
    assert_eq!(rule.quantity_increment.as_deref(), Some("0.000001"));
    assert_eq!(rule.min_notional.as_deref(), Some("100"));

    let query = market_query("singapore");
    assert_eq!(query.get("group").map(String::as_str), Some("singapore"));
}

#[test]
fn zebpay_should_parse_order_book_snapshot_fixture() {
    let snapshot = parse_orderbook_snapshot(
        &exchange_id(),
        symbol_scope(),
        2,
        &fixture("order_book_success.json"),
    )
    .expect("book");

    assert_eq!(snapshot.bids.len(), 2);
    assert_eq!(snapshot.asks.len(), 2);
    assert_eq!(snapshot.bids[0].price, 7_000_000.0);
    assert_eq!(snapshot.asks[0].quantity, 0.12);
    assert_eq!(order_book_endpoint("BTC-INR", 2), "/market/BTC-INR/book");
    assert_eq!(
        order_book_endpoint("BTC-INR", 50),
        "/market/BTC-INR/book_long"
    );
    let query = order_book_query("singapore");
    assert_eq!(query.get("converted").map(String::as_str), Some("0"));
}

#[tokio::test]
async fn zebpay_private_operations_should_stay_unsupported_even_with_credentials() {
    let adapter = ZebpayGatewayAdapter::new(ZebpayGatewayConfig {
        client_id: "zebpay-client-id".to_string(),
        client_secret: "zebpay-client-secret".to_string(),
        access_token: "zebpay-access-token".to_string(),
        enabled_private_rest: true,
        ..ZebpayGatewayConfig::default()
    })
    .expect("adapter");

    assert!(adapter.config.private_rest_configured());
    assert!(adapter.capabilities().supports_query_order);
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
            operation: "zebpay.balances_request_spec_only"
        }
    ));
}

#[tokio::test]
async fn zebpay_readbacks_should_fail_closed_without_private_rest_guard() {
    let adapter = ZebpayGatewayAdapter::new(ZebpayGatewayConfig {
        client_id: "zebpay-client-id".to_string(),
        client_secret: "zebpay-client-secret".to_string(),
        access_token: "zebpay-access-token".to_string(),
        enabled_private_rest: false,
        ..ZebpayGatewayConfig::default()
    })
    .expect("adapter");

    let error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-disabled"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("12345".to_string()),
        })
        .await
        .expect_err("disabled private readback");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "zebpay.query_order"
        }
    ));
}

#[tokio::test]
async fn zebpay_should_query_order_over_guarded_bearer_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("order_success.json")]).await;
    let adapter = ZebpayGatewayAdapter::new(ZebpayGatewayConfig {
        rest_base_url: base_url,
        client_id: "zebpay-client-id".to_string(),
        client_secret: "zebpay-client-secret".to_string(),
        access_token: "zebpay-access-token".to_string(),
        enabled_private_rest: true,
        ..ZebpayGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("12345".to_string()),
        })
        .await
        .expect("query order");

    let order = response.order.expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("12345"));
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, ZEBPAY_QUERY_ORDER_PATH);
    assert_eq!(
        request.headers.get("client_id").map(String::as_str),
        Some("zebpay-client-id")
    );
    assert_eq!(
        request.headers.get("authorization").map(String::as_str),
        Some("Bearer zebpay-access-token")
    );
    assert!(request.headers.get("timestamp").is_some());
    assert!(request.headers.get("requestid").is_some());
    assert_eq!(
        request.query.get("trade_pair").map(String::as_str),
        Some("BTC-INR")
    );
    assert_eq!(
        request.query.get("orderid").map(String::as_str),
        Some("12345")
    );
    assert!(request.body.is_empty());
}

#[tokio::test]
async fn zebpay_should_fetch_open_orders_over_guarded_bearer_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("open_orders_success.json")]).await;
    let adapter = ZebpayGatewayAdapter::new(ZebpayGatewayConfig {
        rest_base_url: base_url,
        client_id: "zebpay-client-id".to_string(),
        client_secret: "zebpay-client-secret".to_string(),
        access_token: "zebpay-access-token".to_string(),
        enabled_private_rest: true,
        ..ZebpayGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open-orders"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            page: None,
        })
        .await
        .expect("open orders");

    assert_eq!(response.orders.len(), 2);
    assert_eq!(response.orders[0].status, OrderStatus::PartiallyFilled);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, ZEBPAY_OPEN_ORDERS_PATH);
    assert_eq!(
        request.query.get("status").map(String::as_str),
        Some("pending")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("500"));
}

#[tokio::test]
async fn zebpay_should_fetch_recent_fills_over_guarded_bearer_rest() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("recent_fills_success.json")]).await;
    let adapter = ZebpayGatewayAdapter::new(ZebpayGatewayConfig {
        rest_base_url: base_url,
        client_id: "zebpay-client-id".to_string(),
        client_secret: "zebpay-client-secret".to_string(),
        access_token: "zebpay-access-token".to_string(),
        enabled_private_rest: true,
        ..ZebpayGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
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
            limit: None,
            page: None,
        })
        .await
        .expect("recent fills");

    assert_eq!(response.fills.len(), 2);
    assert_eq!(response.fills[0].fill_id.as_deref(), Some("zebpay-fill-1"));
    assert_eq!(response.fills[0].order_id.as_deref(), Some("12345"));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/orders/12345/fills");
    assert!(request.query.is_empty());
}

#[tokio::test]
async fn zebpay_futures_positions_should_stop_at_product_line_boundary() {
    let adapter = ZebpayGatewayAdapter::default_public().expect("adapter");
    let error = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("futures-positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![perpetual_symbol()],
        })
        .await
        .expect_err("perpetual product line is not wired");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "zebpay.non_spot_market_type"
        }
    ));
}

#[tokio::test]
async fn zebpay_batch_place_should_be_explicitly_unsupported() {
    let adapter = ZebpayGatewayAdapter::default_public().expect("adapter");
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
            operation: "zebpay.batch_place_orders_unverified"
        }
    ));
}

#[tokio::test]
async fn zebpay_streams_should_keep_rest_reconciliation_boundary() {
    let adapter = ZebpayGatewayAdapter::default_public().expect("adapter");
    let public_error = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-ws"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::OrderBookSnapshot,
        })
        .await
        .expect_err("public ws unsupported");
    assert!(matches!(
        public_error,
        ExchangeApiError::Unsupported {
            operation: "zebpay.public_streams_unverified"
        }
    ));

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
            operation: "zebpay.private_streams_unverified"
        }
    ));
    let ws_boundary = fixture("ws/public_streams_unsupported.json");
    assert_eq!(ws_boundary["public_streams"], "unsupported_unverified");
    assert_eq!(ws_boundary["private_streams"], "unsupported_unverified");
    assert_eq!(
        ws_boundary["rest_reconciliation_fallback"][0],
        "/orders?trade_pair=BTC-INR&status=pending&orderid=0&page=1&limit=500"
    );
    assert!(zebpay_rest_reconciliation_fallback().contains("/orders?status=pending"));
}

#[test]
fn zebpay_bearer_auth_vector_should_match_fixture() {
    let vector = fixture("signing_vectors/bearer_auth.json");
    let headers = build_bearer_private_headers(
        vector["client_id"].as_str().unwrap(),
        vector["access_token"].as_str().unwrap(),
        vector["timestamp"].as_str().unwrap(),
        vector["request_id"].as_str().unwrap(),
    )
    .expect("bearer headers");
    assert_eq!(
        headers.client_id,
        vector["expected_headers"]["client_id"].as_str().unwrap()
    );
    assert_eq!(
        headers.timestamp,
        vector["expected_headers"]["timestamp"].as_str().unwrap()
    );
    assert_eq!(
        headers.request_id,
        vector["expected_headers"]["RequestId"].as_str().unwrap()
    );
    assert_eq!(
        headers.authorization,
        vector["expected_headers"]["Authorization"]
            .as_str()
            .unwrap()
    );
}

#[test]
fn zebpay_private_request_specs_should_be_sanitized_and_request_spec_only() {
    let place = fixture("request_specs/place_order_limit_buy.json");
    assert_eq!(place["operation"], "zebpay.place_order");
    assert_eq!(place["method"], "POST");
    assert_eq!(place["path"], ZEBPAY_PLACE_ORDER_PATH);
    assert_eq!(place["trade_enabled"], false);
    assert_eq!(place["body"]["trade_pair"], "BTC-INR");
    assert_eq!(place["headers"]["Authorization"], "<redacted:bearer-token>");

    let balances = fixture("request_specs/get_balances.json");
    assert_eq!(balances["operation"], "zebpay.get_balances");
    assert_eq!(balances["method"], "GET");
    assert_eq!(balances["path"], ZEBPAY_BALANCE_PATH);
    assert_eq!(balances["request_spec_only"], true);

    let cancel = fixture("request_specs/cancel_order.json");
    assert_eq!(cancel["operation"], "zebpay.cancel_order");
    assert_eq!(cancel["method"], "DELETE");
    assert_eq!(cancel["path"], ZEBPAY_CANCEL_ORDER_PATH);
    assert_eq!(cancel["trade_enabled"], false);

    let open_orders = fixture("request_specs/get_open_orders.json");
    assert_eq!(open_orders["operation"], "zebpay.get_open_orders");
    assert_eq!(open_orders["method"], "GET");
    assert_eq!(open_orders["path"], ZEBPAY_OPEN_ORDERS_PATH);
    assert_eq!(open_orders["query"]["limit"], 500);
    assert_eq!(open_orders["request_spec_only"], false);
    assert_eq!(open_orders["runtime"], "guarded_private_rest");
    assert_eq!(open_orders["credential_scope"], "read_only");

    let recent_fills = fixture("request_specs/get_recent_fills.json");
    assert_eq!(recent_fills["operation"], "zebpay.get_recent_fills");
    assert_eq!(recent_fills["method"], "GET");
    assert_eq!(recent_fills["path"], ZEBPAY_RECENT_FILLS_PATH);
    assert_eq!(recent_fills["request_spec_only"], false);
    assert_eq!(recent_fills["runtime"], "guarded_private_rest");
    assert_eq!(recent_fills["credential_scope"], "read_only");

    let query_order = fixture("request_specs/query_order.json");
    assert_eq!(query_order["operation"], "zebpay.query_order");
    assert_eq!(query_order["method"], "GET");
    assert_eq!(query_order["path"], ZEBPAY_QUERY_ORDER_PATH);
    assert_eq!(query_order["request_spec_only"], false);

    let spec = build_private_request_spec(
        "GET",
        "/wallet/balance?trade_pair=BTC-INR",
        None,
        "zebpay-client-id",
        "zebpay-access-token",
        "1717200000000",
        "00000000-0000-4000-8000-000000000001",
    )
    .expect("request spec");
    assert_eq!(spec.method, "GET");
    assert_eq!(spec.headers.client_id, "zebpay-client-id");
    assert_eq!(spec.headers.authorization, "Bearer zebpay-access-token");
    assert_eq!(ZEBPAY_CANCEL_ALL_ORDERS_PATH, "/orders/CancelAll");

    let read_spec = build_private_get_request_spec(
        ZEBPAY_QUERY_ORDER_PATH,
        query_order_query("BTC-INR", "12345"),
        "zebpay-client-id",
        "zebpay-access-token",
        "1717200000000",
        "00000000-0000-4000-8000-000000000001",
    )
    .expect("read request spec");
    assert_eq!(read_spec.query.len(), 4);
    assert_eq!(open_orders_query("BTC-INR", Some(999))[4].1, "500");
    assert_eq!(recent_fills_path("12345"), "/orders/12345/fills");
}

#[test]
fn zebpay_private_order_request_body_should_match_official_shape() {
    let request = place_order_request();
    let body = request_spec_body_from_order(&request);
    assert_eq!(
        body,
        json!({
            "trade_pair": "BTC-INR",
            "side": "bid",
            "size": "0.01",
            "price": "7000000",
            "tradeType": 1,
            "platform": "API_Trading"
        })
    );
    assert!(zebpay_supports_order_type(OrderType::Limit));
    assert!(!zebpay_supports_order_type(OrderType::Market));
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
        price: Some("7000000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}

fn perpetual_symbol() -> ExchangeSymbol {
    ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "BTC-USDT-PERP").expect("symbol")
}
