use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, RequestContext, SymbolScope, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};
use serde_json::Value;

use super::parser::{ndax_canonical_pair, ndax_symbol, parse_order_book_shape, parse_symbol_rules};
use super::private::{
    cancel_order_body, cancel_order_request_spec, ndax_limit_order_body, place_order_request_spec,
};
use super::public::{instruments_request_spec, l2_snapshot_request_spec};
use super::signing::{
    ndax_hmac_sha256_base64, ndax_redacted_signed_headers, ndax_signature_payload,
    ndax_signed_headers,
};
use super::streams::{
    ndax_private_auth_payload, ndax_public_subscribe_payload, ndax_reconnect_policy_ms,
    ndax_unsubscribe_payload, parse_ws_function_name, parse_ws_level2_shape,
};
use super::transport::decode_gateway_payload;
use super::{NdaxGatewayAdapter, NdaxGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("ndax").expect("exchange")
}

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(TenantId::new("tenant").expect("tenant")),
        account_id: Some(AccountId::new("account").expect("account")),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: chrono::Utc::now(),
    }
}

fn symbol(symbol: &str) -> SymbolScope {
    let (base, quote) = ndax_canonical_pair(symbol).expect("canonical pair");
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new(base, quote).expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, symbol)
            .expect("symbol"),
    }
}

#[test]
fn symbol_normalization_should_cover_cad_markets() {
    assert_eq!(ndax_symbol("btc/cad"), "BTCCAD");
    assert_eq!(ndax_symbol("ETH-CAD"), "ETHCAD");
    assert_eq!(
        ndax_canonical_pair("BTCCAD").expect("cad"),
        ("BTC".to_string(), "CAD".to_string())
    );
    assert_eq!(
        ndax_canonical_pair("ETH_CAD").expect("cad separator"),
        ("ETH".to_string(), "CAD".to_string())
    );
}

#[test]
fn public_gateway_specs_and_fixtures_should_parse() {
    let instruments_spec = instruments_request_spec(1);
    assert_eq!(instruments_spec["body"]["n"], "GetInstruments");
    assert_eq!(instruments_spec["body"]["o"], "{\"OMSId\":1}");

    let book_spec = l2_snapshot_request_spec(1, "btc/cad", 50);
    assert_eq!(book_spec["body"]["n"], "GetL2Snapshot");
    assert!(book_spec["body"]["o"]
        .as_str()
        .expect("payload")
        .contains("BTCCAD"));

    let instruments: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/ndax/instruments_success.json"
    ))
    .expect("instruments");
    let rules = parse_symbol_rules(&exchange_id(), &[], &instruments).expect("rules");
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "CAD");
    assert_eq!(rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.00000001"));

    let order_book: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/ndax/orderbook_btc_cad.json"
    ))
    .expect("order book");
    assert_eq!(parse_order_book_shape(&order_book).expect("shape"), (1, 1));

    let envelope: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/ndax/gateway_instruments_envelope.json"
    ))
    .expect("envelope");
    let decoded = decode_gateway_payload(&envelope).expect("decoded");
    assert!(decoded.as_array().expect("array").len() >= 2);

    let boundary: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/ndax/unsupported_boundary.json"
    ))
    .expect("boundary");
    assert_eq!(boundary["private_write_mode"], "offline_request_spec_only");
    assert_eq!(boundary["fiat_ledger_runtime"], "unsupported");
}

#[test]
fn request_spec_and_signing_should_stay_secret_free() {
    let body = "{\"accountId\":1001,\"instrumentId\":1,\"limitPrice\":\"65000\",\"orderType\":2,\"quantity\":\"0.01\",\"side\":0,\"timeInForce\":1}";
    let payload = ndax_signature_payload("1700000000", "POST", "/orders", body);
    assert_eq!(payload, format!("1700000000POST/orders{body}"));
    let signature = ndax_hmac_sha256_base64(b"fixture-secret", &payload).expect("signature");
    assert_eq!(signature, "Gzj+heNRZlaUpsntUhFPdFjxfTEJiUlDUSuGp6hhhJU=");

    let headers = ndax_signed_headers(
        "fixture-key",
        "Zml4dHVyZS1zZWNyZXQ=",
        Some("fixture-passphrase"),
        "1700000000",
        "POST",
        "/orders",
        body,
    )
    .expect("headers");
    assert_eq!(
        headers.get("X-NDAX-SIGNATURE").map(String::as_str),
        Some(signature.as_str())
    );
    assert_eq!(
        ndax_redacted_signed_headers()["X-NDAX-SIGNATURE"],
        "<redacted:signature>"
    );

    let vector: crate::signing_spec::SigningVector = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/ndax/signing_vectors/rest_order_signature.json"
    ))
    .expect("signing vector");
    vector.verify().expect("standard signing vector verifies");

    for raw in [
        include_str!(
            "../../../../../tests/fixtures/exchanges/ndax/request_specs/get_balances.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/ndax/request_specs/place_order_limit.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/ndax/request_specs/cancel_order.json"
        ),
    ] {
        let spec: crate::request_spec::RequestSpec =
            serde_json::from_str(raw).expect("request spec");
        assert_eq!(spec.exchange, "ndax");
        assert!(spec
            .forbid_header_values_containing
            .contains(&"fixture-secret".to_string()));
    }
}

#[test]
fn private_request_builders_should_map_offline_order_fields() {
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol("BTCCAD"),
        client_order_id: Some("100200300".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let body = ndax_limit_order_body(&request, 1001, 1).expect("body");
    assert_eq!(body["AccountId"], 1001);
    assert_eq!(body["InstrumentId"], 1);
    assert_eq!(body["Side"], 0);
    assert_eq!(body["OrderType"], 2);
    assert_eq!(body["LimitPrice"], "65000");

    let place = place_order_request_spec(body);
    assert_eq!(place["method"], "POST");
    assert_eq!(place["path"], "/orders");
    assert_eq!(place["auth"], "ndax_hmac_sha256_base64");

    let cancel_body = cancel_order_body(1001, "offline-order-1");
    assert_eq!(cancel_body["OrderId"], "offline-order-1");
    let cancel = cancel_order_request_spec("offline-order-1");
    assert_eq!(cancel["method"], "DELETE");
    assert_eq!(cancel["path"], "/orders/offline-order-1");
}

#[test]
fn websocket_helpers_should_emit_payloads_and_parse_l2_fixtures() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol("BTCCAD"),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let payload = ndax_public_subscribe_payload(2, 1, &subscription, 50);
    assert_eq!(payload["n"], "SubscribeLevel2");
    assert!(payload["o"].as_str().expect("payload").contains("BTCCAD"));
    assert_eq!(
        ndax_unsubscribe_payload(3, "UnsubscribeLevel2", 1, "BTCCAD")["n"],
        "UnsubscribeLevel2"
    );
    let auth = ndax_private_auth_payload(
        4,
        "<redacted:api_key>",
        "<redacted:user_id>",
        "<nonce>",
        "<redacted:signature>",
    );
    assert_eq!(auth["n"], "AuthenticateUser");
    assert!(auth["o"]
        .as_str()
        .expect("auth")
        .contains("<redacted:signature>"));
    assert_eq!(ndax_reconnect_policy_ms(), (30_000, 45_000, 60_000));

    let ws_book: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/ndax/ws/level2_snapshot.json"
    ))
    .expect("ws book");
    assert_eq!(
        parse_ws_function_name(&ws_book).as_deref(),
        Some("Level2UpdateEvent")
    );
    assert_eq!(parse_ws_level2_shape(&ws_book).expect("shape"), (1, 1));
}

#[tokio::test]
async fn live_trading_surfaces_should_remain_explicitly_unsupported() {
    let adapter = NdaxGatewayAdapter::new(NdaxGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_positions);

    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol("BTCCAD"),
        client_order_id: Some("100200300".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "ndax.place_order_offline_request_spec_only"
        }
    ));
}
