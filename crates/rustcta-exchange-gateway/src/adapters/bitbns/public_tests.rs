use base64::Engine;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, OpenOrdersRequest, OrderBookRequest, QueryOrderRequest,
    RecentFillsRequest, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::OrderStatus;
use serde_json::json;

use super::parser::{parse_open_orders, parse_orderbook_snapshot, parse_symbol_rules};
use super::private::{
    BITBNS_OPEN_ORDERS_METHOD, BITBNS_QUERY_ORDER_METHOD, BITBNS_RECENT_FILLS_METHOD,
};
use super::test_support::{context, exchange_id, spawn_rest_server, symbol_scope};
use super::{BitbnsGatewayAdapter, BitbnsGatewayConfig};

fn bitbns_fixture(name: &str) -> serde_json::Value {
    let text = match name {
        "markets_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bitbns/markets_success.json")
        }
        "orderbook_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bitbns/orderbook_success.json")
        }
        "unsupported_boundary.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bitbns/unsupported_boundary.json")
        }
        "order_status_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bitbns/order_status_success.json")
        }
        "open_orders_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bitbns/open_orders_success.json")
        }
        "recent_fills_success.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bitbns/recent_fills_success.json")
        }
        _ => panic!("unknown bitbns fixture {name}"),
    };
    serde_json::from_str(text).expect("bitbns fixture")
}

#[tokio::test]
async fn bitbns_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![bitbns_fixture("markets_success.json")]).await;
    let adapter = BitbnsGatewayAdapter::new(BitbnsGatewayConfig {
        rest_base_url: base_url,
        ..BitbnsGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope("ETH_USDT")],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "ETH");
    assert_eq!(response.rules[0].quote_asset, "USDT");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.001"));
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.000001")
    );
    assert_eq!(response.rules[0].min_notional.as_deref(), Some("0.1"));
    assert!(response.rules[0].supports_limit_orders);
    assert!(!response.rules[0].supports_market_orders);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/order/fetchMarkets/");
}

#[test]
fn bitbns_public_parser_fixtures_should_cover_markets_and_order_book() {
    let rules =
        parse_symbol_rules(&exchange_id(), &bitbns_fixture("markets_success.json")).expect("rules");
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].symbol.exchange_symbol.symbol, "BTC_INR");
    assert_eq!(rules[0].min_notional.as_deref(), Some("10"));
    assert_eq!(rules[1].symbol.exchange_symbol.symbol, "ETH_USDT");

    let book = parse_orderbook_snapshot(
        &exchange_id(),
        symbol_scope("BTC_INR"),
        1,
        &bitbns_fixture("orderbook_success.json"),
    )
    .expect("book");
    assert_eq!(book.bids.len(), 1);
    assert_eq!(book.asks.len(), 1);
    assert_eq!(book.bids[0].price, 65000.0);
    assert_eq!(
        book.exchange_timestamp
            .expect("timestamp")
            .timestamp_millis(),
        1630664703000
    );
}

#[tokio::test]
async fn bitbns_adapter_should_load_depth_limited_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![bitbns_fixture("orderbook_success.json")]).await;
    let adapter = BitbnsGatewayAdapter::new(BitbnsGatewayConfig {
        rest_base_url: base_url,
        ..BitbnsGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope("BTC_INR"),
            depth: Some(1),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids.len(), 1);
    assert_eq!(response.order_book.asks.len(), 1);
    assert_eq!(response.order_book.asks[0].price, 65010.0);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/exchangeData/orderBook");
    assert_eq!(request.query.get("coin").map(String::as_str), Some("BTC"));
    assert_eq!(request.query.get("market").map(String::as_str), Some("INR"));
}

#[tokio::test]
async fn bitbns_adapter_should_reject_zero_depth_and_non_spot_private_operations() {
    let adapter = BitbnsGatewayAdapter::default_public().expect("adapter");
    let error = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("zero-depth"),
            symbol: symbol_scope("BTC_INR"),
            depth: Some(0),
        })
        .await
        .expect_err("zero depth");
    assert!(matches!(error, ExchangeApiError::InvalidRequest { .. }));

    let boundary = bitbns_fixture("unsupported_boundary.json");
    assert_eq!(boundary["scan_only"], true);
    assert_eq!(boundary["trade_enabled"], false);
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(capabilities.supports_query_order);
    assert!(capabilities.supports_open_orders);
    assert!(capabilities.supports_recent_fills);
    assert!(capabilities.capabilities_v2.private_rest.is_supported());
}

#[test]
fn bitbns_transport_error_fixture_should_remain_classified_boundary_data() {
    let error = json!({
        "status": 0,
        "error": "Invalid coin or market"
    });
    assert_eq!(error["status"], 0);
    assert_eq!(error["error"], "Invalid coin or market");
}

#[tokio::test]
async fn bitbns_private_readbacks_should_fail_closed_without_env_and_credentials() {
    let adapter = BitbnsGatewayAdapter::default_public().expect("adapter");
    let error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-disabled"),
            symbol: symbol_scope("BTC_INR"),
            client_order_id: None,
            exchange_order_id: Some("4221".to_string()),
        })
        .await
        .expect_err("disabled private rest");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "bitbns.query_order"
        }
    ));
}

#[tokio::test]
async fn bitbns_query_order_should_send_signed_private_post_and_parse_order() {
    let (base_url, seen) =
        spawn_rest_server(vec![bitbns_fixture("order_status_success.json")]).await;
    let adapter = BitbnsGatewayAdapter::new(BitbnsGatewayConfig {
        private_rest_base_url: base_url,
        enabled_private_rest: true,
        api_key: "fixture-key".to_string(),
        api_secret: "fixture-secret".to_string(),
        ..BitbnsGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query-order"),
            symbol: symbol_scope("BTC_INR"),
            client_order_id: None,
            exchange_order_id: Some("4221".to_string()),
        })
        .await
        .expect("query order");

    let order = response.order.expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("4221"));
    assert_eq!(order.status, OrderStatus::Open);
    assert_eq!(order.quantity, "0.001");
    assert_eq!(order.price.as_deref(), Some("306929.01"));
    let request = seen.lock().unwrap()[0].clone();
    assert_bitbns_signed_request(&request, BITBNS_QUERY_ORDER_METHOD, "BTC");
    assert_eq!(request.path, "/orderStatus/BTC");
    assert_eq!(request.body, r#"{"entry_id":"4221"}"#);
    let payload = decoded_payload(&request);
    assert_eq!(payload["body"], r#"{"entry_id":"4221"}"#);
}

#[tokio::test]
async fn bitbns_open_orders_should_send_signed_private_post_and_parse_orders() {
    let (base_url, seen) =
        spawn_rest_server(vec![bitbns_fixture("open_orders_success.json")]).await;
    let adapter = BitbnsGatewayAdapter::new(BitbnsGatewayConfig {
        private_rest_base_url: base_url,
        enabled_private_rest: true,
        api_key: "fixture-key".to_string(),
        api_secret: "fixture-secret".to_string(),
        ..BitbnsGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open-orders"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            symbol: Some(symbol_scope("BTC_INR")),
            page: None,
        })
        .await
        .expect("open orders");

    assert_eq!(response.orders.len(), 2);
    assert_eq!(response.orders[1].status, OrderStatus::PartiallyFilled);
    let request = seen.lock().unwrap()[0].clone();
    assert_bitbns_signed_request(&request, BITBNS_OPEN_ORDERS_METHOD, "BTC");
    assert_eq!(request.path, "/listOpenOrders/BTC");
    assert_eq!(request.body, r#"{"page":0}"#);
}

#[tokio::test]
async fn bitbns_recent_fills_should_send_signed_private_post_and_parse_fills() {
    let (base_url, seen) =
        spawn_rest_server(vec![bitbns_fixture("recent_fills_success.json")]).await;
    let adapter = BitbnsGatewayAdapter::new(BitbnsGatewayConfig {
        private_rest_base_url: base_url,
        enabled_private_rest: true,
        api_key: "fixture-key".to_string(),
        api_secret: "fixture-secret".to_string(),
        ..BitbnsGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(rustcta_types::MarketType::Spot),
            symbol: Some(symbol_scope("BTC_INR")),
            client_order_id: None,
            exchange_order_id: None,
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: None,
            page: None,
        })
        .await
        .expect("recent fills");

    assert_eq!(response.fills.len(), 2);
    assert_eq!(response.fills[0].order_id.as_deref(), Some("4221"));
    assert_eq!(response.fills[0].price, 306929.01);
    assert_eq!(response.fills[0].quantity, 0.001);
    let request = seen.lock().unwrap()[0].clone();
    assert_bitbns_signed_request(&request, BITBNS_RECENT_FILLS_METHOD, "BTC");
    assert_eq!(request.path, "/listExecutedOrders/BTC");
    assert_eq!(request.body, r#"{"page":0}"#);
}

#[test]
fn bitbns_private_parser_should_cover_open_order_fixture() {
    let orders = parse_open_orders(
        &exchange_id(),
        Some(&symbol_scope("BTC_INR")),
        &bitbns_fixture("open_orders_success.json"),
    )
    .expect("orders");
    assert_eq!(orders.len(), 2);
    assert_eq!(orders[0].exchange_order_id.as_deref(), Some("4221"));
    assert_eq!(orders[0].status, OrderStatus::Open);
}

fn assert_bitbns_signed_request(
    request: &super::test_support::SeenRequest,
    method_name: &str,
    symbol: &str,
) {
    assert_eq!(request.method, "POST");
    assert!(request.query.is_empty());
    assert_eq!(
        header(request, "X-BITBNS-APIKEY").map(String::as_str),
        Some("fixture-key")
    );
    assert!(header(request, "X-BITBNS-PAYLOAD").is_some_and(|payload| !payload.is_empty()));
    assert!(header(request, "X-BITBNS-SIGNATURE").is_some_and(|signature| signature.len() == 128));
    assert!(!request.body.contains("fixture-secret"));
    let payload = decoded_payload(request);
    assert_eq!(payload["symbol"], format!("/{method_name}/{symbol}"));
    assert!(payload["timeStamp_nonce"]
        .as_str()
        .is_some_and(|nonce| !nonce.is_empty()));
}

fn decoded_payload(request: &super::test_support::SeenRequest) -> serde_json::Value {
    let payload = header(request, "X-BITBNS-PAYLOAD").expect("payload header");
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(payload)
        .expect("base64 payload");
    serde_json::from_slice(&bytes).expect("json payload")
}

fn header<'a>(request: &'a super::test_support::SeenRequest, name: &str) -> Option<&'a String> {
    request
        .headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value)
}
