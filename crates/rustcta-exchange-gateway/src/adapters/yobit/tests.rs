use rustcta_exchange_api::{
    BalancesRequest, BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, OpenOrdersRequest,
    OrderBookRequest, PlaceOrderRequest, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, QueryOrderRequest, RecentFillsRequest,
    SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType, OrderSide, OrderStatus, OrderType};

use super::parser::{parse_open_orders, parse_order_state, parse_recent_fills};
use super::private::{
    balance_params, build_private_request_spec, open_orders_params, query_order_params,
    recent_fills_params, request_spec_params_from_order, YOBIT_CANCEL_ORDER_METHOD,
    YOBIT_OPEN_ORDERS_METHOD, YOBIT_PLACE_ORDER_METHOD, YOBIT_RECENT_FILLS_METHOD, YOBIT_TAPI_PATH,
};
use super::signing::hmac_sha512_hex;
use super::streams::yobit_rest_reconciliation_fallback;
use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope};
use super::{YobitGatewayAdapter, YobitGatewayConfig};
use crate::adapters::AdapterBackedGateway;

fn fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "info_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/yobit/info_success.json")
        }
        "depth_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/yobit/depth_success.json")
        }
        "order_info_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/yobit/order_info_success.json")
        }
        "open_orders_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/yobit/open_orders_success.json")
        }
        "trade_history_success.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/yobit/trade_history_success.json"
        ),
        "request_specs/place_order_limit_buy.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/yobit/request_specs/place_order_limit_buy.json"
        ),
        "request_specs/get_balances.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/yobit/request_specs/get_balances.json"
        ),
        "request_specs/cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/yobit/request_specs/cancel_order.json"
        ),
        "request_specs/get_open_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/yobit/request_specs/get_open_orders.json"
        ),
        "request_specs/get_recent_fills.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/yobit/request_specs/get_recent_fills.json"
        ),
        "signing_vectors/rest_tapi_hmac_sha512.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/yobit/signing_vectors/rest_tapi_hmac_sha512.json"
        ),
        "unsupported_boundary.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/yobit/unsupported_boundary.json"
        ),
        "ws/public_streams_unsupported.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/yobit/ws/public_streams_unsupported.json"
        ),
        _ => panic!("unknown yobit fixture {name}"),
    };
    serde_json::from_str(text).expect("yobit fixture")
}

#[test]
fn yobit_capabilities_should_expose_scan_only_spot_public_rest() {
    let adapter = YobitGatewayAdapter::default_public().expect("adapter");
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
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(capabilities.capabilities_v2.public_rest.is_supported());
    assert_eq!(capabilities.max_order_book_depth, Some(2000));

    let boundary = fixture("unsupported_boundary.json");
    assert_eq!(boundary["scan_only"], true);
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["withdrawals"], false);
    assert_eq!(boundary["yobicode"], false);
}

#[test]
fn yobit_named_registration_should_accept_aliases() {
    AdapterBackedGateway::with_named_adapters("yobit-test", ["yobit"]).expect("yobit");
    AdapterBackedGateway::with_named_adapters("yobit-alias-test", ["yobit.net"])
        .expect("yobit.net alias");
}

#[tokio::test]
async fn yobit_should_load_symbol_rules_from_public_info() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("info_success.json")]).await;
    let adapter = YobitGatewayAdapter::new(YobitGatewayConfig {
        rest_base_url: base_url,
        ..YobitGatewayConfig::default()
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
    assert_eq!(rule.symbol.exchange_symbol.symbol, "btc_usdt");
    assert_eq!(rule.price_increment.as_deref(), Some("0.01"));
    assert_eq!(rule.min_price.as_deref(), Some("0.00000001"));
    assert_eq!(rule.max_price.as_deref(), Some("1000000"));
    assert_eq!(rule.min_quantity.as_deref(), Some("0.0001"));
    assert!(!rule.supports_market_orders);
    assert!(rule.supports_limit_orders);
    assert_eq!(seen.lock().unwrap()[0].path, "/api/3/info");
}

#[tokio::test]
async fn yobit_should_load_order_book_snapshot() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("depth_success.json")]).await;
    let adapter = YobitGatewayAdapter::new(YobitGatewayConfig {
        rest_base_url: base_url,
        ..YobitGatewayConfig::default()
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
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/api/3/depth/btc_usdt");
    assert_eq!(
        request.query.get("ignore_invalid").map(String::as_str),
        Some("1")
    );
    assert_eq!(request.query.get("limit").map(String::as_str), Some("2"));
}

#[tokio::test]
async fn yobit_private_operations_should_stay_unsupported_even_with_credentials() {
    let adapter = YobitGatewayAdapter::new(YobitGatewayConfig {
        api_key: "yobit_public_fixture_key".to_string(),
        api_secret: "yobit_secret_fixture".to_string(),
        enabled_private_rest: true,
        ..YobitGatewayConfig::default()
    })
    .expect("adapter");

    assert!(adapter.config.private_rest_configured());
    assert!(adapter.capabilities().supports_private_rest);
    assert!(!adapter.capabilities().supports_place_order);
    assert!(!adapter.capabilities().supports_cancel_order);
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
            operation: "yobit.balances_request_spec_only"
        }
    ));
}

#[tokio::test]
async fn yobit_readbacks_should_fail_closed_without_private_rest_guard() {
    let adapter = YobitGatewayAdapter::default_public().expect("adapter");
    let error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-disabled"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("100025362".to_string()),
        })
        .await
        .expect_err("private readbacks disabled");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "yobit.query_order"
        }
    ));
}

#[test]
fn yobit_private_readback_parsers_should_normalize_tapi_payloads() {
    let order = parse_order_state(
        &exchange_id(),
        Some(&symbol_scope()),
        &fixture("order_info_success.json"),
    )
    .expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("100025362"));
    assert_eq!(order.status, OrderStatus::PartiallyFilled);
    assert_eq!(order.quantity, "0.01000000");
    assert_eq!(order.filled_quantity, "0.006");

    let open_orders = parse_open_orders(
        &exchange_id(),
        Some(&symbol_scope()),
        &fixture("open_orders_success.json"),
    )
    .expect("open orders");
    assert_eq!(open_orders.len(), 2);
    assert_eq!(open_orders[1].side, OrderSide::Sell);
    assert_eq!(open_orders[1].status, OrderStatus::Open);

    let fills = parse_recent_fills(
        &exchange_id(),
        context("fills-parser").tenant_id.unwrap(),
        context("fills-parser").account_id.unwrap(),
        &symbol_scope(),
        &fixture("trade_history_success.json"),
    )
    .expect("fills");
    assert_eq!(fills.len(), 2);
    assert_eq!(fills[0].fill_id.as_deref(), Some("200000001"));
    assert_eq!(fills[0].order_id.as_deref(), Some("100025362"));
    assert_eq!(fills[0].quantity, 0.006);
}

#[tokio::test]
async fn yobit_should_query_order_over_guarded_tapi() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("order_info_success.json")]).await;
    let adapter = YobitGatewayAdapter::new(YobitGatewayConfig {
        rest_base_url: base_url,
        api_key: "yobit_public_fixture_key".to_string(),
        api_secret: "yobit_secret_fixture".to_string(),
        enabled_private_rest: true,
        ..YobitGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("100025362".to_string()),
        })
        .await
        .expect("query");

    assert_eq!(
        response
            .order
            .as_ref()
            .and_then(|order| order.exchange_order_id.as_deref()),
        Some("100025362")
    );
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, YOBIT_TAPI_PATH);
    assert_eq!(
        request.headers.get("key").map(String::as_str),
        Some("yobit_public_fixture_key")
    );
    assert!(request
        .headers
        .get("sign")
        .is_some_and(|sign| sign.len() == 128));
    assert_eq!(
        request.form.get("method").map(String::as_str),
        Some("OrderInfo")
    );
    assert_eq!(
        request.form.get("order_id").map(String::as_str),
        Some("100025362")
    );
    assert!(request.form.get("nonce").is_some());
    assert!(request.body_text.contains("method=OrderInfo"));
}

#[tokio::test]
async fn yobit_should_fetch_open_orders_over_guarded_tapi() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("open_orders_success.json")]).await;
    let adapter = YobitGatewayAdapter::new(YobitGatewayConfig {
        rest_base_url: base_url,
        api_key: "yobit_public_fixture_key".to_string(),
        api_secret: "yobit_secret_fixture".to_string(),
        enabled_private_rest: true,
        ..YobitGatewayConfig::default()
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
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(
        request.form.get("method").map(String::as_str),
        Some("ActiveOrders")
    );
    assert_eq!(
        request.form.get("pair").map(String::as_str),
        Some("btc_usdt")
    );
    assert!(request.form.get("nonce").is_some());
}

#[tokio::test]
async fn yobit_should_fetch_recent_fills_over_guarded_tapi() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("trade_history_success.json")]).await;
    let adapter = YobitGatewayAdapter::new(YobitGatewayConfig {
        rest_base_url: base_url,
        api_key: "yobit_public_fixture_key".to_string(),
        api_secret: "yobit_secret_fixture".to_string(),
        enabled_private_rest: true,
        ..YobitGatewayConfig::default()
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
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(2),
            page: None,
        })
        .await
        .expect("recent fills");

    assert_eq!(response.fills.len(), 2);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(
        request.form.get("method").map(String::as_str),
        Some("TradeHistory")
    );
    assert_eq!(
        request.form.get("pair").map(String::as_str),
        Some("btc_usdt")
    );
    assert_eq!(request.form.get("count").map(String::as_str), Some("2"));
    assert!(request.form.get("nonce").is_some());
}

#[tokio::test]
async fn yobit_batch_place_should_be_explicitly_unsupported() {
    let adapter = YobitGatewayAdapter::default_public().expect("adapter");
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
            operation: "yobit.batch_place_orders_unverified"
        }
    ));
}

#[tokio::test]
async fn yobit_streams_should_keep_rest_reconciliation_boundary() {
    let adapter = YobitGatewayAdapter::default_public().expect("adapter");
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
            operation: "yobit.public_streams_unverified"
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
            operation: "yobit.private_streams_unverified"
        }
    ));
    let ws_boundary = fixture("ws/public_streams_unsupported.json");
    assert_eq!(ws_boundary["public_streams"], "unsupported_unverified");
    assert_eq!(ws_boundary["private_streams"], "unsupported_unverified");
    assert_eq!(
        ws_boundary["rest_reconciliation_fallback"][0],
        "ActiveOrders"
    );
    assert!(yobit_rest_reconciliation_fallback().contains("ActiveOrders"));
}

#[test]
fn yobit_hmac_sha512_signing_vector_should_match_fixture() {
    let vector = fixture("signing_vectors/rest_tapi_hmac_sha512.json");
    let signature = hmac_sha512_hex(
        vector["api_secret"].as_str().unwrap(),
        vector["form_body"].as_str().unwrap(),
    )
    .expect("signature");
    assert_eq!(signature, vector["expected_sign"].as_str().unwrap());

    let spec = build_private_request_spec(
        &balance_params(),
        1_700_000_000,
        vector["api_key"].as_str().unwrap(),
        vector["api_secret"].as_str().unwrap(),
    )
    .expect("request spec");
    assert_eq!(spec.path, YOBIT_TAPI_PATH);
    assert_eq!(spec.body, vector["form_body"].as_str().unwrap());
    assert_eq!(spec.headers.api_key, vector["api_key"].as_str().unwrap());
    assert_eq!(
        spec.headers.signature,
        vector["expected_sign"].as_str().unwrap()
    );
}

#[test]
fn yobit_private_request_specs_should_be_sanitized_and_request_spec_only() {
    let place = fixture("request_specs/place_order_limit_buy.json");
    assert_eq!(place["operation"], "yobit.place_order");
    assert_eq!(place["method"], "POST");
    assert_eq!(place["path"], YOBIT_TAPI_PATH);
    assert_eq!(place["trade_enabled"], false);
    assert_eq!(place["body"]["method"], YOBIT_PLACE_ORDER_METHOD);
    assert_eq!(place["headers"]["Key"], "<redacted:api-key>");
    assert_eq!(place["headers"]["Sign"], "<redacted:hmac-sha512>");

    let balances = fixture("request_specs/get_balances.json");
    assert_eq!(balances["operation"], "yobit.get_balances");
    assert_eq!(balances["method"], "POST");
    assert_eq!(balances["path"], YOBIT_TAPI_PATH);
    assert_eq!(balances["request_spec_only"], true);

    let cancel = fixture("request_specs/cancel_order.json");
    assert_eq!(cancel["operation"], "yobit.cancel_order");
    assert_eq!(cancel["body"]["method"], YOBIT_CANCEL_ORDER_METHOD);
    assert_eq!(cancel["trade_enabled"], false);

    let open_orders = fixture("request_specs/get_open_orders.json");
    assert_eq!(open_orders["operation"], "yobit.get_open_orders");
    assert_eq!(open_orders["body"]["method"], YOBIT_OPEN_ORDERS_METHOD);
    assert_eq!(open_orders["credential_scope"], "read_only");

    let recent_fills = fixture("request_specs/get_recent_fills.json");
    assert_eq!(recent_fills["operation"], "yobit.get_recent_fills");
    assert_eq!(recent_fills["body"]["method"], YOBIT_RECENT_FILLS_METHOD);
    assert_eq!(recent_fills["credential_scope"], "read_only");

    let open_spec = build_private_request_spec(
        &open_orders_params("btc_usdt"),
        1_700_000_001,
        "yobit_public_fixture_key",
        "yobit_secret_fixture",
    )
    .expect("open orders spec");
    assert_eq!(
        open_spec.body,
        "method=ActiveOrders&pair=btc_usdt&nonce=1700000001"
    );

    let fills_spec = build_private_request_spec(
        &recent_fills_params("btc_usdt"),
        1_700_000_002,
        "yobit_public_fixture_key",
        "yobit_secret_fixture",
    )
    .expect("fills spec");
    assert_eq!(
        fills_spec.body,
        "method=TradeHistory&pair=btc_usdt&count=100&nonce=1700000002"
    );

    let query_request = QueryOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("query-spec"),
        symbol: symbol_scope(),
        client_order_id: None,
        exchange_order_id: Some("100025362".to_string()),
    };
    let query_spec = build_private_request_spec(
        &query_order_params(&query_request).expect("query params"),
        1_700_000_005,
        "yobit_public_fixture_key",
        "yobit_secret_fixture",
    )
    .expect("query spec");
    assert_eq!(
        query_spec.body,
        "method=OrderInfo&order_id=100025362&nonce=1700000005"
    );
}

#[test]
fn yobit_private_order_request_body_should_match_tapi_shape() {
    let request = place_order_request();
    let spec = build_private_request_spec(
        &request_spec_params_from_order(&request),
        1_700_000_003,
        "yobit_public_fixture_key",
        "yobit_secret_fixture",
    )
    .expect("place spec");
    assert_eq!(
        spec.body,
        "method=Trade&pair=btc_usdt&type=buy&rate=64000&amount=0.01&nonce=1700000003"
    );
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
