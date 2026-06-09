use chrono::Utc;
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, PublicStreamKind, PublicStreamSubscription, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType};
use serde_json::json;

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::private::bitvavo_order_body;
use super::signing::{sign_request, sign_ws_auth};
use super::streams::{
    parse_public_stream_message, public_subscribe_payload_for_subscription, validate_book_nonce,
    BitvavoPublicStreamMessage,
};
use super::{BitvavoGatewayAdapter, BitvavoGatewayConfig};

fn bitvavo_exchange() -> ExchangeId {
    ExchangeId::new("bitvavo").expect("exchange")
}

fn bitvavo_symbol_scope() -> SymbolScope {
    SymbolScope {
        exchange: bitvavo_exchange(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "EUR").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(bitvavo_exchange(), MarketType::Spot, "BTC-EUR")
            .expect("symbol"),
    }
}

#[test]
fn bitvavo_signing_should_cover_rest_query_and_ws_auth() {
    let rest = sign_request(
        "test-secret",
        "1548172481125",
        "GET",
        "/order?market=BTC-EUR&orderId=abc",
        "",
    );
    assert_eq!(rest.len(), 64);
    let ws = sign_ws_auth("test-secret", "1548175200641");
    assert_eq!(ws.len(), 64);
    assert_ne!(rest, ws);
}

#[test]
fn bitvavo_parser_should_normalize_markets_and_book() {
    let exchange = bitvavo_exchange();
    let rules = parse_symbol_rules(
        &exchange,
        &json!([
            {
                "market": "BTC-EUR",
                "status": "trading",
                "base": "BTC",
                "quote": "EUR",
                "pricePrecision": "0.01",
                "amountPrecision": "0.00000001",
                "minOrderInQuoteAsset": "5"
            }
        ]),
    )
    .expect("rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].quote_asset, "EUR");

    let snapshot = parse_orderbook_snapshot(
        &exchange,
        rules[0].symbol.clone(),
        &json!({
            "market": "BTC-EUR",
            "nonce": 42,
            "bids": [["30000.00", "0.5"]],
            "asks": [["30010.00", "0.4"]]
        }),
    )
    .expect("book");
    assert_eq!(snapshot.sequence, Some(42));
    assert_eq!(snapshot.bids[0].price, 30000.0);
}

#[test]
fn bitvavo_order_body_should_map_limit_order() {
    let symbol = rustcta_exchange_api::SymbolScope {
        exchange: bitvavo_exchange(),
        market_type: MarketType::Spot,
        canonical_symbol: None,
        exchange_symbol: rustcta_types::ExchangeSymbol::new(
            bitvavo_exchange(),
            MarketType::Spot,
            "BTC-EUR",
        )
        .expect("symbol"),
    };
    let body = bitvavo_order_body(&rustcta_exchange_api::PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: rustcta_exchange_api::RequestContext::new(Utc::now()),
        symbol,
        client_order_id: Some("123e4567-e89b-12d3-a456-426614174000".to_string()),
        side: rustcta_types::OrderSide::Buy,
        position_side: None,
        order_type: rustcta_types::OrderType::Limit,
        time_in_force: Some(rustcta_types::TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("30000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: true,
    })
    .expect("body");
    assert_eq!(body["market"], "BTC-EUR");
    assert_eq!(body["postOnly"], true);
}

#[test]
fn bitvavo_public_stream_should_map_channels_and_parse_book_nonce() {
    let book_payload = public_subscribe_payload_for_subscription(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: rustcta_exchange_api::RequestContext::new(Utc::now()),
        symbol: bitvavo_symbol_scope(),
        kind: PublicStreamKind::OrderBookDelta,
    })
    .expect("book payload");
    assert_eq!(book_payload["channels"][0]["name"], "book");
    assert_eq!(book_payload["channels"][0]["markets"][0], "BTC-EUR");

    let trades_payload = public_subscribe_payload_for_subscription(&PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: rustcta_exchange_api::RequestContext::new(Utc::now()),
        symbol: bitvavo_symbol_scope(),
        kind: PublicStreamKind::Trades,
    })
    .expect("trades payload");
    assert_eq!(trades_payload["channels"][0]["name"], "trades");

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitvavo/ws/public_book.json"
    ))
    .expect("public book fixture");
    let message =
        parse_public_stream_message(&bitvavo_exchange(), bitvavo_symbol_scope(), &fixture)
            .expect("public book");
    match message {
        BitvavoPublicStreamMessage::OrderBook(book) => {
            assert_eq!(book.order_book.sequence, Some(43));
            assert_eq!(book.order_book.bids[0].price, 30000.0);
        }
        other => panic!("unexpected bitvavo public stream message {other:?}"),
    }
    validate_book_nonce(Some(42), 43).expect("contiguous nonce");
    let gap = validate_book_nonce(Some(42), 44).expect_err("nonce gap");
    assert!(matches!(gap, ExchangeApiError::InvalidRequest { .. }));
}

#[test]
fn bitvavo_unsupported_boundary_should_be_explicit() {
    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitvavo/unsupported_boundary.json"
    ))
    .expect("boundary fixture");
    assert_eq!(boundary["funding_enabled"], false);
    assert!(boundary["unsupported_operations"]
        .as_array()
        .expect("operations")
        .iter()
        .any(|operation| operation == "withdrawals"));

    let adapter = BitvavoGatewayAdapter::new(BitvavoGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_batch_cancel_order);
    assert!(!capabilities.supports_positions);
    assert!(!capabilities.supports_reduce_only);
    assert!(capabilities.order_book.supports_sequence);

    let symbol = rustcta_exchange_api::SymbolScope {
        exchange: bitvavo_exchange(),
        market_type: MarketType::Spot,
        canonical_symbol: None,
        exchange_symbol: rustcta_types::ExchangeSymbol::new(
            bitvavo_exchange(),
            MarketType::Spot,
            "BTC-EUR",
        )
        .expect("symbol"),
    };
    let error = bitvavo_order_body(&rustcta_exchange_api::PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: rustcta_exchange_api::RequestContext::new(Utc::now()),
        symbol,
        client_order_id: None,
        side: rustcta_types::OrderSide::Sell,
        position_side: None,
        order_type: rustcta_types::OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("30000".to_string()),
        quote_quantity: None,
        reduce_only: true,
        post_only: false,
    })
    .expect_err("reduce_only unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "bitvavo.reduce_only_unsupported_spot"
        }
    ));
}
