use rustcta_exchange_api::{
    CapabilitySupport, ExchangeClient, OrderBookRequest, PublicStreamKind,
    PublicStreamSubscription, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::MarketType;
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::streams::{okx_public_subscribe_payload, parse_okx_public_orderbook_message};
use super::test_support::{
    context, futures_symbol_scope, option_symbol_scope, perpetual_symbol_scope, spawn_rest_server,
    symbol_scope,
};
use super::{OkxGatewayAdapter, OkxGatewayConfig};

fn fixture(path: &str) -> serde_json::Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/okx/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("fixture");
    serde_json::from_str(&text).expect("fixture json")
}

fn public_subscription(kind: PublicStreamKind, perpetual: bool) -> PublicStreamSubscription {
    PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: if perpetual {
            perpetual_symbol_scope()
        } else {
            symbol_scope()
        },
        kind,
    }
}

#[tokio::test]
async fn okx_adapter_should_declare_task9_toolchain_capabilities() {
    let adapter = OkxGatewayAdapter::new(OkxGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..OkxGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();

    assert!(matches!(
        capabilities.capabilities_v2.public_rest,
        CapabilitySupport::Native
    ));
    assert!(matches!(
        capabilities.capabilities_v2.private_rest,
        CapabilitySupport::Native
    ));
    assert!(matches!(
        capabilities.capabilities_v2.public_streams,
        CapabilitySupport::RestFallback { .. }
    ));
    assert!(matches!(
        capabilities.capabilities_v2.private_streams,
        CapabilitySupport::Unsupported { .. }
    ));
    assert!(capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(capabilities.market_types.contains(&MarketType::Futures));
    assert!(capabilities.market_types.contains(&MarketType::Option));
    assert!(capabilities.supports_positions);
    assert!(capabilities.supports_batch_place_order);
    assert!(capabilities.supports_batch_cancel_order);
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.max_items,
        Some(20)
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_cancel_orders.max_items,
        Some(20)
    );
    assert_eq!(
        capabilities.capabilities_v2.fills_history.max_limit,
        Some(100)
    );
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "place_order"
            && endpoint.path.as_deref() == Some("/api/v5/trade/order")));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_recent_fills"
            && endpoint.path.as_deref() == Some("/api/v5/trade/fills-history")));
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_positions"
            && endpoint.market_types.contains(&MarketType::Futures)
            && endpoint.path.as_deref() == Some("/api/v5/account/positions")));
}

#[tokio::test]
async fn okx_public_ws_should_build_books5_books_and_bbo_subscriptions() {
    let adapter = OkxGatewayAdapter::new(OkxGatewayConfig::default()).expect("adapter");
    let books5 = okx_public_subscribe_payload(&public_subscription(
        PublicStreamKind::OrderBookSnapshot,
        false,
    ))
    .expect("books5");
    assert_eq!(books5["op"], "subscribe");
    assert_eq!(books5["args"][0]["channel"], "books5");
    assert_eq!(books5["args"][0]["instId"], "BTC-USDT");

    let books =
        okx_public_subscribe_payload(&public_subscription(PublicStreamKind::OrderBookDelta, true))
            .expect("books");
    assert_eq!(books["args"][0]["channel"], "books");
    assert_eq!(books["args"][0]["instId"], "BTC-USDT-SWAP");

    let futures_books = okx_public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws-futures"),
        symbol: futures_symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    })
    .expect("futures books");
    assert_eq!(futures_books["args"][0]["channel"], "books");
    assert_eq!(futures_books["args"][0]["instId"], "BTC-USD-240329");

    let option_books = okx_public_subscribe_payload(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws-option"),
        symbol: option_symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    })
    .expect("option books");
    assert_eq!(option_books["args"][0]["channel"], "books");
    assert_eq!(option_books["args"][0]["instId"], "BTC-USD-240329-65000-C");

    let session: serde_json::Value = serde_json::from_str(
        &adapter
            .subscribe_public_stream(public_subscription(PublicStreamKind::Ticker, false))
            .await
            .expect("subscribe"),
    )
    .expect("session json");
    assert_eq!(session["url"], "wss://ws.okx.com:8443/ws/v5/public");
    assert_eq!(session["payload"]["args"][0]["channel"], "bbo-tbt");
    assert_eq!(session["heartbeat"]["client_ping"], "ping");
}

#[test]
fn okx_public_ws_fixture_should_parse_books5_sequence() {
    let exchange = super::test_support::exchange_id();
    let book = parse_okx_public_orderbook_message(
        &exchange,
        symbol_scope(),
        &fixture("ws/public_books5_snapshot.json"),
    )
    .expect("book");
    assert_eq!(book.sequence, Some(12345));
    assert_eq!(book.bids[0].quantity, 256.0);
    assert!(book.exchange_timestamp.is_some());
}

#[test]
fn okx_parser_fixtures_should_cover_success_empty_and_error_shapes() {
    let exchange = super::test_support::exchange_id();
    let rules = parse_symbol_rules(
        &exchange,
        MarketType::Spot,
        &fixture("parser/instruments_success.json"),
    )
    .expect("rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].base_asset, "BTC");

    let empty = parse_symbol_rules(
        &exchange,
        MarketType::Spot,
        &fixture("parser/instruments_empty.json"),
    )
    .expect("empty");
    assert!(empty.is_empty());
    assert!(parse_symbol_rules(
        &exchange,
        MarketType::Spot,
        &fixture("parser/instruments_missing_field.json")
    )
    .is_err());
    assert!(parse_orderbook_snapshot(
        &exchange,
        symbol_scope(),
        &fixture("parser/orderbook_error.json")
    )
    .is_err());
}

#[tokio::test]
async fn okx_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "0",
        "msg": "",
        "data": [{
            "instType": "SPOT",
            "instId": "BTC-USDT",
            "baseCcy": "BTC",
            "quoteCcy": "USDT",
            "tickSz": "0.01",
            "lotSz": "0.00000001",
            "minSz": "0.00001",
            "minLmtAmt": "5",
            "maxLmtSz": "100",
            "state": "live"
        }]
    })])
    .await;
    let adapter = OkxGatewayAdapter::new(OkxGatewayConfig {
        rest_base_url: base_url,
        ..OkxGatewayConfig::default()
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
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(response.rules[0].quote_asset, "USDT");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.00000001")
    );
    assert_eq!(response.rules[0].min_notional.as_deref(), Some("5"));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v5/public/instruments");
    assert_eq!(
        request.query.get("instType").map(String::as_str),
        Some("SPOT")
    );
}

#[tokio::test]
async fn okx_adapter_should_load_perpetual_symbol_rules_from_swap_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "0",
        "msg": "",
        "data": [{
            "instType": "SWAP",
            "instId": "BTC-USDT-SWAP",
            "ctValCcy": "BTC",
            "settleCcy": "USDT",
            "tickSz": "0.1",
            "lotSz": "0.01",
            "minSz": "0.01",
            "state": "live"
        }]
    })])
    .await;
    let adapter = OkxGatewayAdapter::new(OkxGatewayConfig {
        rest_base_url: base_url,
        ..OkxGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-rules"),
            symbols: vec![perpetual_symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].symbol.market_type, MarketType::Perpetual);
    assert!(response.rules[0].supports_reduce_only);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(
        request.query.get("instType").map(String::as_str),
        Some("SWAP")
    );
}

#[tokio::test]
async fn okx_adapter_should_load_futures_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "0",
        "msg": "",
        "data": [{
            "instType": "FUTURES",
            "instId": "BTC-USD-240329",
            "ctValCcy": "BTC",
            "settleCcy": "USD",
            "tickSz": "0.1",
            "lotSz": "1",
            "minSz": "1",
            "state": "live"
        }]
    })])
    .await;
    let adapter = OkxGatewayAdapter::new(OkxGatewayConfig {
        rest_base_url: base_url,
        ..OkxGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("futures-rules"),
            symbols: vec![futures_symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].symbol.market_type, MarketType::Futures);
    assert_eq!(response.rules[0].quote_asset, "USD");
    assert!(response.rules[0].supports_reduce_only);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(
        request.query.get("instType").map(String::as_str),
        Some("FUTURES")
    );
}

#[tokio::test]
async fn okx_adapter_should_load_option_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "0",
        "msg": "",
        "data": [{
            "instType": "OPTION",
            "instId": "BTC-USD-240329-65000-C",
            "ctValCcy": "BTC",
            "settleCcy": "USD",
            "tickSz": "0.0005",
            "lotSz": "0.01",
            "minSz": "0.01",
            "state": "live"
        }]
    })])
    .await;
    let adapter = OkxGatewayAdapter::new(OkxGatewayConfig {
        rest_base_url: base_url,
        ..OkxGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("option-rules"),
            symbols: vec![option_symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].symbol.market_type, MarketType::Option);
    assert_eq!(response.rules[0].quote_asset, "USD");
    assert!(response.rules[0].supports_reduce_only);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(
        request.query.get("instType").map(String::as_str),
        Some("OPTION")
    );
}

#[tokio::test]
async fn okx_adapter_should_load_depth_limited_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "0",
        "msg": "",
        "data": [{
            "bids": [["99.5", "1.25", "0", "2"], ["99.0", "2", "0", "1"]],
            "asks": [["100.5", "1.5", "0", "3"], ["101.0", "2", "0", "1"]],
            "ts": "1710000000123"
        }]
    })])
    .await;
    let adapter = OkxGatewayAdapter::new(OkxGatewayConfig {
        rest_base_url: base_url,
        ..OkxGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(7),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids[0].price, 99.5);
    assert_eq!(response.order_book.asks[1].quantity, 2.0);
    assert!(response.order_book.exchange_timestamp.is_some());
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v5/market/books");
    assert_eq!(
        request.query.get("instId").map(String::as_str),
        Some("BTC-USDT")
    );
    assert_eq!(request.query.get("sz").map(String::as_str), Some("10"));
}
