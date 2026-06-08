use rustcta_exchange_api::{
    BalancesRequest, CancelAllOrdersRequest, CancelOrderRequest, ExchangeApiError, ExchangeClient,
    OpenOrdersRequest, OrderBookRequest, OrderSide, OrderType, PlaceOrderRequest, PositionsRequest,
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    QueryOrderRequest, RecentFillsRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::streams::{
    alpaca_private_stream_boundary, alpaca_public_auth_payload, alpaca_public_subscribe_payload,
    alpaca_public_unsubscribe_payload, parse_alpaca_public_ws_events,
};
use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope, MockResponse};
use super::{private::alpaca_order_body, AlpacaGatewayAdapter, AlpacaGatewayConfig};
use crate::request_spec::RequestSpec;

#[test]
fn alpaca_capabilities_should_track_credentials_and_private_flag() {
    let adapter = AlpacaGatewayAdapter::new(AlpacaGatewayConfig {
        api_key: String::new(),
        api_secret: String::new(),
        enabled_private_rest: true,
        ..AlpacaGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(!capabilities.supports_public_rest);
    assert!(!capabilities.supports_private_rest);

    let adapter = AlpacaGatewayAdapter::new(AlpacaGatewayConfig {
        api_key: "test-key".to_string(),
        api_secret: "test-secret".to_string(),
        enabled_private_rest: true,
        ..AlpacaGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(
        capabilities.market_types,
        vec![rustcta_types::MarketType::Spot]
    );
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_private_rest);
    assert!(capabilities.supports_place_order);
    assert!(capabilities.supports_quote_market_order);
    assert!(!capabilities.supports_recent_fills);
    assert!(!capabilities.supports_private_streams);
}

#[tokio::test]
async fn alpaca_adapter_should_load_crypto_symbol_rules_from_broker_assets() {
    let (base_url, seen) = spawn_rest_server(vec![MockResponse::json(fixture("assets"))]).await;
    let adapter = AlpacaGatewayAdapter::new(AlpacaGatewayConfig {
        broker_rest_base_url: base_url,
        api_key: "test-key".to_string(),
        api_secret: "test-secret".to_string(),
        enabled_private_rest: false,
        ..AlpacaGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(rustcta_exchange_api::SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(response.rules[0].quote_asset, "USD");
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.0001")
    );
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/v1/assets");
    assert_eq!(request.header("apca-api-key-id"), Some("test-key"));
    assert_eq!(request.header("apca-api-secret-key"), Some("test-secret"));
    load_request_spec("get_symbol_rules.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
}

#[tokio::test]
async fn alpaca_adapter_should_load_latest_orderbook_from_market_data() {
    let (base_url, seen) = spawn_rest_server(vec![MockResponse::json(fixture("orderbook"))]).await;
    let adapter = AlpacaGatewayAdapter::new(AlpacaGatewayConfig {
        market_data_rest_base_url: base_url,
        api_key: "test-key".to_string(),
        api_secret: "test-secret".to_string(),
        enabled_private_rest: false,
        ..AlpacaGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(10),
        })
        .await
        .expect("orderbook");

    assert_eq!(response.order_book.bids[0].price, 65986.962);
    assert_eq!(response.order_book.asks[0].price, 66051.621);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/v1beta3/crypto/us/latest/orderbooks");
    assert_eq!(request.query.get("symbols"), Some(&"BTC%2FUSD".to_string()));
    load_request_spec("get_order_book.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
}

#[tokio::test]
async fn alpaca_private_rest_should_remain_disabled_without_flag() {
    let adapter = AlpacaGatewayAdapter::new(AlpacaGatewayConfig {
        api_key: "test-key".to_string(),
        api_secret: "test-secret".to_string(),
        enabled_private_rest: false,
        ..AlpacaGatewayConfig::default()
    })
    .expect("adapter");
    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            assets: vec![],
        })
        .await
        .expect_err("private disabled");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

#[tokio::test]
async fn alpaca_adapter_should_load_broker_account_balance() {
    let (base_url, seen) = spawn_rest_server(vec![MockResponse::json(fixture("account"))]).await;
    let adapter = private_adapter(base_url, None);

    let response = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            assets: vec!["USD".to_string()],
        })
        .await
        .expect("balances");

    assert_eq!(response.balances[0].balances[0].asset, "USD");
    assert_eq!(response.balances[0].balances[0].available, 1000.0);
    let request = seen.lock().unwrap()[0].clone();
    load_request_spec("get_balances.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
}

#[tokio::test]
async fn alpaca_adapter_should_load_crypto_positions() {
    let (base_url, seen) = spawn_rest_server(vec![MockResponse::json(fixture("positions"))]).await;
    let adapter = private_adapter(base_url, None);

    let response = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            symbols: vec![],
        })
        .await
        .expect("positions");

    assert_eq!(response.positions.len(), 1);
    assert_eq!(
        response.positions[0].canonical_symbol.to_string(),
        "BTC/USD"
    );
    assert_eq!(response.positions[0].quantity, 0.25);
    assert_eq!(
        seen.lock().unwrap()[0].path,
        "/v1/trading/accounts/acct-123/positions"
    );
    let request = seen.lock().unwrap()[0].clone();
    load_request_spec("get_positions.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
}

#[tokio::test]
async fn alpaca_adapter_should_build_limit_order_body() {
    let (base_url, seen) = spawn_rest_server(vec![MockResponse::json(fixture("order_ack"))]).await;
    let adapter = private_adapter(base_url, None);

    let response = adapter
        .place_order(limit_order())
        .await
        .expect("place order");

    assert_eq!(response.order.exchange_order_id.as_deref(), Some("order-1"));
    let request = seen.lock().unwrap()[0].clone();
    load_request_spec("place_order.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
    assert_eq!(request.body.as_ref().expect("body")["symbol"], "BTC/USD");
    assert_eq!(request.body.as_ref().expect("body")["limit_price"], "50000");
}

#[tokio::test]
async fn alpaca_adapter_should_place_quote_sized_market_order() {
    let (base_url, seen) =
        spawn_rest_server(vec![MockResponse::json(fixture("market_order_ack"))]).await;
    let adapter = private_adapter(base_url, None);

    adapter
        .place_quote_market_order(rustcta_exchange_api::QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote-order"),
            symbol: symbol_scope(),
            client_order_id: Some("client-quote".to_string()),
            side: OrderSide::Buy,
            quote_quantity: "25".to_string(),
        })
        .await
        .expect("quote order");

    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.body.as_ref().expect("body")["type"], "market");
    assert_eq!(request.body.as_ref().expect("body")["notional"], "25");
    assert!(request.body.as_ref().expect("body").get("qty").is_none());
    load_request_spec("place_quote_market_order.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
}

#[tokio::test]
async fn alpaca_adapter_should_cancel_order_by_exchange_order_id() {
    let (base_url, seen) = spawn_rest_server(vec![MockResponse::no_content()]).await;
    let adapter = private_adapter(base_url, None);

    let response = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("order-1".to_string()),
        })
        .await
        .expect("cancel");

    assert!(response.cancelled);
    assert_eq!(response.order.exchange_order_id.as_deref(), Some("order-1"));
    let request = seen.lock().unwrap()[0].clone();
    load_request_spec("cancel_order.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
}

#[tokio::test]
async fn alpaca_adapter_should_query_order_by_client_order_id() {
    let (base_url, seen) = spawn_rest_server(vec![MockResponse::json(fixture("order_ack"))]).await;
    let adapter = private_adapter(base_url, None);

    let response = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol_scope(),
            client_order_id: Some("client-1".to_string()),
            exchange_order_id: None,
        })
        .await
        .expect("query");

    assert_eq!(
        response.order.unwrap().client_order_id.as_deref(),
        Some("client-1")
    );
    let request = seen.lock().unwrap()[0].clone();
    load_request_spec("query_order_by_client_id.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
}

#[tokio::test]
async fn alpaca_adapter_should_load_open_orders_for_symbol() {
    let (base_url, seen) =
        spawn_rest_server(vec![MockResponse::json(json!([fixture("order_ack")]))]).await;
    let adapter = private_adapter(base_url, None);

    let response = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            symbol: Some(symbol_scope()),
            page: None,
        })
        .await
        .expect("open orders");

    assert_eq!(response.orders.len(), 1);
    let request = seen.lock().unwrap()[0].clone();
    load_request_spec("get_open_orders.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
}

#[tokio::test]
async fn alpaca_adapter_should_cancel_all_account_orders_only() {
    let (base_url, seen) = spawn_rest_server(vec![MockResponse::json(json!([
        {"id": "order-1", "status": 200}
    ]))])
    .await;
    let adapter = private_adapter(base_url, None);

    let response = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            symbol: None,
        })
        .await
        .expect("cancel all");

    assert_eq!(response.cancelled_count, 1);
    let request = seen.lock().unwrap()[0].clone();
    load_request_spec("cancel_all_orders.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
}

#[tokio::test]
async fn alpaca_symbol_scoped_cancel_all_and_recent_fills_should_be_unsupported() {
    let adapter = private_adapter("http://127.0.0.1:9".to_string(), None);
    let symbol_cancel = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("symbol-cancel"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            symbol: Some(symbol_scope()),
        })
        .await
        .expect_err("symbol cancel-all unsupported");
    assert!(matches!(
        symbol_cancel,
        ExchangeApiError::Unsupported { .. }
    ));

    let recent_fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            symbol: Some(symbol_scope()),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(10),
            page: None,
        })
        .await
        .expect_err("fills unsupported");
    assert!(matches!(recent_fills, ExchangeApiError::Unsupported { .. }));
}

#[test]
fn alpaca_order_body_should_reject_post_only_and_reduce_only() {
    let mut request = limit_order();
    request.post_only = true;
    assert!(matches!(
        alpaca_order_body(&request),
        Err(ExchangeApiError::Unsupported { .. })
    ));
    request.post_only = false;
    request.reduce_only = true;
    assert!(matches!(
        alpaca_order_body(&request),
        Err(ExchangeApiError::Unsupported { .. })
    ));
}

#[test]
fn alpaca_public_ws_payloads_and_parser_should_cover_orderbooks() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let auth = alpaca_public_auth_payload("test-key", "test-secret");
    assert_eq!(auth["action"], "auth");
    assert_eq!(auth["key"], "test-key");

    let subscribe = alpaca_public_subscribe_payload(&subscription).expect("subscribe");
    assert_eq!(subscribe["action"], "subscribe");
    assert_eq!(subscribe["orderbooks"][0], "BTC/USD");

    let unsubscribe = alpaca_public_unsubscribe_payload(&subscription).expect("unsubscribe");
    assert_eq!(unsubscribe["action"], "unsubscribe");
    assert_eq!(unsubscribe["orderbooks"][0], "BTC/USD");

    let events = parse_alpaca_public_ws_events(&exchange_id(), &fixture_text("ws/orderbook"))
        .expect("ws event");
    assert!(matches!(
        events.first(),
        Some(rustcta_exchange_api::ExchangeStreamEvent::OrderBookSnapshot(book))
            if book.order_book.bids[0].price == 65986.962
    ));
    assert_eq!(
        alpaca_private_stream_boundary()["support"].as_str(),
        Some("unsupported")
    );
}

#[tokio::test]
async fn alpaca_private_stream_should_remain_unsupported_for_broker_sse() {
    let adapter = private_adapter("http://127.0.0.1:9".to_string(), None);
    let error = adapter
        .subscribe_private_stream(PrivateStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("private-ws"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            account_id: rustcta_types::AccountId::new("acct-123").expect("account"),
            kind: PrivateStreamKind::Orders,
        })
        .await
        .expect_err("private stream unsupported");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

fn private_adapter(
    broker_rest_base_url: String,
    market_data_rest_base_url: Option<String>,
) -> AlpacaGatewayAdapter {
    AlpacaGatewayAdapter::new(AlpacaGatewayConfig {
        broker_rest_base_url,
        market_data_rest_base_url: market_data_rest_base_url
            .unwrap_or_else(|| "http://127.0.0.1:9".to_string()),
        api_key: "test-key".to_string(),
        api_secret: "test-secret".to_string(),
        enabled_private_rest: true,
        ..AlpacaGatewayConfig::default()
    })
    .expect("adapter")
}

fn limit_order() -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol_scope(),
        client_order_id: Some("client-1".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("50000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}

fn fixture(name: &str) -> Value {
    serde_json::from_str(&fixture_text(name)).expect("fixture json")
}

fn fixture_text(name: &str) -> String {
    match name {
        "assets" => include_str!("../../../../../tests/fixtures/exchanges/alpaca/assets.json"),
        "orderbook" => {
            include_str!("../../../../../tests/fixtures/exchanges/alpaca/orderbook.json")
        }
        "account" => include_str!("../../../../../tests/fixtures/exchanges/alpaca/account.json"),
        "positions" => {
            include_str!("../../../../../tests/fixtures/exchanges/alpaca/positions.json")
        }
        "order_ack" => {
            include_str!("../../../../../tests/fixtures/exchanges/alpaca/order_ack.json")
        }
        "market_order_ack" => {
            include_str!("../../../../../tests/fixtures/exchanges/alpaca/market_order_ack.json")
        }
        "ws/orderbook" => {
            include_str!("../../../../../tests/fixtures/exchanges/alpaca/ws/orderbook.json")
        }
        _ => unreachable!("unknown fixture"),
    }
    .to_string()
}

fn load_request_spec(path: &str) -> RequestSpec {
    let text = match path {
        "get_symbol_rules.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/alpaca/request_specs/get_symbol_rules.json"
        ),
        "get_order_book.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/alpaca/request_specs/get_order_book.json"
        ),
        "get_balances.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/alpaca/request_specs/get_balances.json"
        ),
        "get_positions.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/alpaca/request_specs/get_positions.json"
        ),
        "place_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/alpaca/request_specs/place_order.json"
        ),
        "place_quote_market_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/alpaca/request_specs/place_quote_market_order.json"
        ),
        "cancel_order.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/alpaca/request_specs/cancel_order.json"
        ),
        "query_order_by_client_id.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/alpaca/request_specs/query_order_by_client_id.json"
        ),
        "get_open_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/alpaca/request_specs/get_open_orders.json"
        ),
        "cancel_all_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/alpaca/request_specs/cancel_all_orders.json"
        ),
        _ => unreachable!("unknown request spec"),
    };
    serde_json::from_str(text).expect("request spec")
}
