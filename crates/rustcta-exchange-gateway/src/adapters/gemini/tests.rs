use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeClient, EXCHANGE_API_SCHEMA_VERSION};
use rustcta_types::{ExchangeId, MarketType};
use serde_json::{json, Map};

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::private::gemini_order_payload;
use super::signing::{private_payload, sign_private_request};
use super::{GeminiGatewayAdapter, GeminiGatewayConfig};

fn gemini_exchange() -> ExchangeId {
    ExchangeId::new("gemini").expect("exchange")
}

#[test]
fn gemini_signing_should_build_payload_and_signature() {
    let payload = private_payload("/v1/order/new", "1710000000000000", Map::new());
    assert!(payload.contains("\"request\":\"/v1/order/new\""));
    let signed = sign_private_request(
        "test-secret",
        "/v1/order/new",
        "1710000000000000",
        Map::new(),
    );
    assert!(!signed.payload_base64.is_empty());
    assert_eq!(signed.signature_hex.len(), 96);
}

#[test]
fn gemini_parser_should_normalize_symbol_details_and_book() {
    let exchange = gemini_exchange();
    let rules = parse_symbol_rules(
        &exchange,
        &json!([
            {
                "symbol": "btcusd",
                "base_currency": "BTC",
                "quote_currency": "USD",
                "tick_size": "0.00000001",
                "quote_increment": "0.01",
                "min_order_size": "0.00001"
            }
        ]),
    )
    .expect("rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].base_asset, "BTC");
    let snapshot = parse_orderbook_snapshot(
        &exchange,
        rules[0].symbol.clone(),
        &json!({
            "bids": [{"price": "30000.00", "amount": "0.5"}],
            "asks": [{"price": "30010.00", "amount": "0.4"}],
            "timestampms": 1710000000000i64
        }),
    )
    .expect("book");
    assert_eq!(snapshot.bids[0].quantity, 0.5);
}

#[test]
fn gemini_order_payload_should_map_limit_order_options() {
    let symbol = rustcta_exchange_api::SymbolScope {
        exchange: gemini_exchange(),
        market_type: MarketType::Spot,
        canonical_symbol: None,
        exchange_symbol: rustcta_types::ExchangeSymbol::new(
            gemini_exchange(),
            MarketType::Spot,
            "btcusd",
        )
        .expect("symbol"),
    };
    let payload = gemini_order_payload(&rustcta_exchange_api::PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: rustcta_exchange_api::RequestContext::new(Utc::now()),
        symbol,
        client_order_id: Some("client-1".to_string()),
        side: rustcta_types::OrderSide::Buy,
        position_side: None,
        order_type: rustcta_types::OrderType::Limit,
        time_in_force: Some(rustcta_types::TimeInForce::IOC),
        quantity: "0.01".to_string(),
        price: Some("30000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: true,
    })
    .expect("payload");
    assert_eq!(payload["symbol"], "btcusd");
    assert!(payload["options"].as_array().expect("options").len() >= 2);
}

#[test]
fn gemini_unsupported_boundary_should_reject_market_surfaces() {
    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/gemini/unsupported_boundary.json"
    ))
    .expect("boundary fixture");
    assert_eq!(boundary["funding_enabled"], false);
    assert!(boundary["unsupported_operations"]
        .as_array()
        .expect("operations")
        .iter()
        .any(|operation| operation == "travel_rule"));

    let adapter = GeminiGatewayAdapter::new(GeminiGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(!capabilities.supports_fees);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_batch_cancel_order);
    assert!(!capabilities.supports_quote_market_order);
    assert!(!capabilities
        .supports_order_types
        .contains(&rustcta_types::OrderType::Market));

    let symbol = rustcta_exchange_api::SymbolScope {
        exchange: gemini_exchange(),
        market_type: MarketType::Spot,
        canonical_symbol: None,
        exchange_symbol: rustcta_types::ExchangeSymbol::new(
            gemini_exchange(),
            MarketType::Spot,
            "btcusd",
        )
        .expect("symbol"),
    };
    let error = gemini_order_payload(&rustcta_exchange_api::PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: rustcta_exchange_api::RequestContext::new(Utc::now()),
        symbol,
        client_order_id: None,
        side: rustcta_types::OrderSide::Buy,
        position_side: None,
        order_type: rustcta_types::OrderType::Market,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: None,
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    })
    .expect_err("market unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "gemini.market_order_unsupported"
        }
    ));
}
