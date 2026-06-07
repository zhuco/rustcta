use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, ExchangeStreamEvent, OrderBookRequest, PublicStreamKind,
    PublicStreamSubscription, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::parser::parse_symbol_rules;
use super::streams::{
    whitebit_public_subscribe_payload, WhiteBitPublicStreamMessage, WhiteBitWsSessionEvent,
};
use super::test_support::{context, perp_symbol_scope, spawn_rest_server, symbol_scope};
use super::{WhiteBitGatewayAdapter, WhiteBitGatewayConfig};

fn fixture(path: &str) -> serde_json::Value {
    serde_json::from_str(match path {
        "success" => include_str!(
            "../../../../../tests/fixtures/exchanges/whitebit/parser/symbol_rules_success.json"
        ),
        "empty" => include_str!(
            "../../../../../tests/fixtures/exchanges/whitebit/parser/symbol_rules_empty.json"
        ),
        "error" => include_str!(
            "../../../../../tests/fixtures/exchanges/whitebit/parser/symbol_rules_error.json"
        ),
        "missing_symbol" => include_str!(
            "../../../../../tests/fixtures/exchanges/whitebit/parser/symbol_rules_missing_symbol.json"
        ),
        _ => unreachable!("unknown fixture"),
    })
    .expect("fixture json")
}

#[test]
fn whitebit_parser_should_read_symbol_rule_fixtures() {
    let exchange = super::test_support::exchange_id();
    let success = parse_symbol_rules(&exchange, &fixture("success")).expect("success fixture");
    assert_eq!(success.len(), 1);
    assert_eq!(success[0].symbol.exchange_symbol.symbol, "BTC_USDT");

    let empty = parse_symbol_rules(&exchange, &fixture("empty")).expect("empty fixture");
    assert!(empty.is_empty());

    let error = parse_symbol_rules(&exchange, &fixture("error")).expect_err("error fixture");
    assert!(error
        .to_string()
        .contains("market response is not an array"));

    let missing = parse_symbol_rules(&exchange, &fixture("missing_symbol"))
        .expect_err("missing symbol should fail");
    assert!(missing.to_string().contains("missing"));
}

#[tokio::test]
async fn whitebit_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": [{
            "name": "BTC_USDT",
            "stock": "BTC",
            "money": "USDT",
            "moneyPrec": 2,
            "stockPrec": 6,
            "minAmount": "0.0001",
            "minTotal": "1",
            "maxTotal": "100000",
            "tradesEnabled": true,
            "type": "spot"
        }]
    })])
    .await;
    let adapter = WhiteBitGatewayAdapter::new(WhiteBitGatewayConfig {
        rest_base_url: base_url,
        ..WhiteBitGatewayConfig::default()
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
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.000001")
    );
    assert_eq!(response.rules[0].min_quantity.as_deref(), Some("0.0001"));
    assert_eq!(response.rules[0].min_notional.as_deref(), Some("1"));
    assert_eq!(
        seen.lock().unwrap()[0].path,
        "/api/v4/public/markets".to_string()
    );
}

#[tokio::test]
async fn whitebit_adapter_should_load_perpetual_symbol_rules_from_public_futures_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": [{
            "ticker_id": "BTC_PERP",
            "product_type": "Perpetual",
            "stock": "BTC",
            "money": "USDT",
            "moneyPrec": 1,
            "stockPrec": 4,
            "minAmount": "0.0001",
            "minTotal": "5",
            "maxTotal": "1000000",
            "tradesEnabled": true
        }]
    })])
    .await;
    let adapter = WhiteBitGatewayAdapter::new(WhiteBitGatewayConfig {
        rest_base_url: base_url,
        ..WhiteBitGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-rules"),
            symbols: vec![perp_symbol_scope()],
        })
        .await
        .expect("perp rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(
        response.rules[0].symbol.market_type,
        rustcta_types::MarketType::Perpetual
    );
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(response.rules[0].quote_asset, "USDT");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.1"));
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.0001")
    );
    assert!(response.rules[0].supports_reduce_only);
    assert_eq!(
        seen.lock().unwrap()[0].path,
        "/api/v4/public/futures".to_string()
    );
}

#[tokio::test]
async fn whitebit_adapter_should_load_depth_limited_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "data": {
            "last": 100,
            "updated_at": 1743054548123_i64,
            "depth": {
                "bids": [["99.5", "1.25"], ["99.0", "2"]],
                "asks": [["100.5", "1.5"], ["101.0", "2"]]
            }
        }
    })])
    .await;
    let adapter = WhiteBitGatewayAdapter::new(WhiteBitGatewayConfig {
        rest_base_url: base_url,
        ..WhiteBitGatewayConfig::default()
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

    assert_eq!(response.order_book.sequence, Some(100));
    assert_eq!(response.order_book.bids[0].price, 99.5);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v4/public/orderbook/BTC_USDT");
    assert_eq!(request.query.get("limit").map(String::as_str), Some("7"));
    assert_eq!(request.query.get("level").map(String::as_str), Some("0"));
}

#[tokio::test]
async fn whitebit_adapter_should_build_public_websocket_subscription_specs() {
    let adapter = WhiteBitGatewayAdapter::default_public().expect("adapter");

    let depth = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("depth-ws"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let payload = whitebit_public_subscribe_payload(&depth, 42).expect("depth payload");
    assert_eq!(payload["id"], 42);
    assert_eq!(payload["method"], "depth_subscribe");
    assert_eq!(payload["params"], json!(["BTC_USDT", 100, "0", true]));

    let subscription_id = adapter
        .subscribe_public_stream(depth)
        .await
        .expect("public stream subscription");
    assert_eq!(
        subscription_id,
        "whitebit:wss://api.whitebit.com/ws:depth_subscribe:BTC_USDT"
    );

    let ticker = whitebit_public_subscribe_payload(
        &PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("ticker-ws"),
            symbol: perp_symbol_scope(),
            kind: PublicStreamKind::Ticker,
        },
        7,
    )
    .expect("ticker payload");
    assert_eq!(ticker["method"], "bookTicker_subscribe");
    assert_eq!(ticker["params"], json!(["BTC_PERP"]));

    let candles = whitebit_public_subscribe_payload(
        &PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("candles-ws"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::Candles {
                interval: "1m".to_string(),
            },
        },
        8,
    )
    .expect("candles payload");
    assert_eq!(candles["method"], "candles_subscribe");
    assert_eq!(candles["params"], json!(["BTC_USDT", 60]));

    let unsupported = whitebit_public_subscribe_payload(
        &PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("bad-candle"),
            symbol: symbol_scope(),
            kind: PublicStreamKind::Candles {
                interval: "2m".to_string(),
            },
        },
        9,
    )
    .expect_err("unsupported candle interval");
    assert!(matches!(unsupported, ExchangeApiError::Unsupported { .. }));
}

#[test]
fn whitebit_public_websocket_session_should_subscribe_heartbeat_and_parse_order_book() {
    let adapter = WhiteBitGatewayAdapter::default_public().expect("adapter");
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-session"),
        symbol: symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let mut session = adapter
        .public_ws_session(subscription)
        .expect("public session");
    assert_eq!(session.url, "wss://api.whitebit.com/ws");
    assert_eq!(session.initial_requests()[0]["method"], "depth_subscribe");

    let ping = session.heartbeat_request(77, chrono::Utc::now());
    assert_eq!(ping["method"], "ping");
    let pong = session
        .handle_text_message(r#"{"id":77,"result":"pong"}"#)
        .expect("pong");
    assert!(pong.iter().any(|event| {
        matches!(
            event,
            WhiteBitWsSessionEvent::Stream(items)
                if matches!(items.first(), Some(ExchangeStreamEvent::Heartbeat { .. }))
        )
    }));

    let events = session
        .handle_text_message(
            r#"{"method":"depth_update","params":[{"bids":[["65000","0.2"]],"asks":[["65010","0.3"]],"last":101,"timestamp":1743054548000}]}"#,
        )
        .expect("depth update");
    assert!(events.iter().any(|event| {
        matches!(
            event,
            WhiteBitWsSessionEvent::Public(WhiteBitPublicStreamMessage::OrderBook(book))
                if book.sequence == Some(101)
                    && book.bids[0].price == 65000.0
                    && book.asks[0].quantity == 0.3
        )
    }));
    assert!(events.iter().any(|event| {
        matches!(
            event,
            WhiteBitWsSessionEvent::Stream(items)
                if matches!(items.first(), Some(ExchangeStreamEvent::OrderBookSnapshot(snapshot)) if snapshot.order_book.sequence == Some(101))
        )
    }));
}
