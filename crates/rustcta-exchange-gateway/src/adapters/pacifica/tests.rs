use base64::{engine::general_purpose, Engine as _};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};
use serde_json::json;

use super::parser::{
    parse_book_payload, parse_order_book_snapshot, parse_positions_payload,
    parse_symbol_rules_response,
};
use super::private_parser::parse_order_response;
use super::signing::{pacifica_signing_payload, sign_pacifica_payload};
use super::streams::{
    pacifica_ping_payload, pacifica_private_subscribe_payload, pacifica_public_subscribe_payload,
    pacifica_reconnect_policy_ms,
};
use super::{PacificaGatewayAdapter, PacificaGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("pacifica").expect("exchange")
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

fn symbol() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDC").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "BTC")
            .expect("symbol"),
    }
}

#[test]
fn capabilities_should_enable_public_rest_and_disable_private_writes() {
    let adapter = PacificaGatewayAdapter::new(PacificaGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Perpetual]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/pacifica/unsupported_boundary.json"
    ))
    .expect("fixture");
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["testnet_verified"], true);
}

#[test]
fn signing_payload_should_match_official_compact_json_shape() {
    let operation_data = json!({
        "symbol": "BTC",
        "price": "100000",
        "amount": "0.1",
        "side": "bid",
        "tif": "GTC",
        "reduce_only": false,
        "client_order_id": "12345678-1234-1234-1234-123456789abc"
    });
    let payload =
        pacifica_signing_payload("create_order", 1_748_970_123_456, 5_000, operation_data)
            .expect("payload");
    assert_eq!(
        payload,
        r#"{"data":{"amount":"0.1","client_order_id":"12345678-1234-1234-1234-123456789abc","price":"100000","reduce_only":false,"side":"bid","symbol":"BTC","tif":"GTC"},"expiry_window":5000,"timestamp":1748970123456,"type":"create_order"}"#
    );
    let secret = general_purpose::STANDARD.encode([7_u8; 32]);
    let signature = sign_pacifica_payload(&secret, &payload).expect("signature");
    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/pacifica/signing_vectors/create_order_ed25519.json"
    ))
    .expect("vector");
    assert_eq!(vector["payload"], payload);
    assert_eq!(vector["signature"], signature);
}

#[test]
fn parser_fixtures_should_cover_public_rest_and_private_response() {
    let rules = parse_symbol_rules_response(
        exchange_id(),
        include_str!("../../../../../tests/fixtures/exchanges/pacifica/market_info.json"),
    )
    .expect("rules");
    assert_eq!(rules.rules[0].symbol.exchange_symbol.symbol, "BTC");
    assert_eq!(rules.rules[0].price_increment.as_deref(), Some("1"));

    let book = parse_book_payload(include_str!(
        "../../../../../tests/fixtures/exchanges/pacifica/order_book.json"
    ))
    .expect("book");
    assert_eq!(book.symbol, "BTC");
    assert_eq!(book.bids[0], ("106504".to_string(), "0.26203".to_string()));
    let snapshot = parse_order_book_snapshot(
        exchange_id(),
        MarketType::Perpetual,
        include_str!("../../../../../tests/fixtures/exchanges/pacifica/order_book.json"),
    )
    .expect("snapshot");
    assert_eq!(snapshot.bids.len(), 2);

    let order_id = parse_order_response(include_str!(
        "../../../../../tests/fixtures/exchanges/pacifica/order_response.json"
    ))
    .expect("order response");
    assert_eq!(order_id, Some(123498765));

    let positions = parse_positions_payload(include_str!(
        "../../../../../tests/fixtures/exchanges/pacifica/positions.json"
    ))
    .expect("positions");
    assert_eq!(positions[0].symbol, "BTC");
    assert_eq!(positions[0].amount, "0.25");
    let ws_positions = parse_positions_payload(include_str!(
        "../../../../../tests/fixtures/exchanges/pacifica/ws/account_positions.json"
    ))
    .expect("ws positions");
    assert_eq!(ws_positions[0].symbol, "BTC");
    assert_eq!(ws_positions[0].leverage.as_deref(), Some("5"));
}

#[test]
fn websocket_helpers_should_build_payloads_and_resync_policy() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let payload = pacifica_public_subscribe_payload(&subscription);
    assert_eq!(payload["method"], "subscribe");
    assert_eq!(payload["params"]["source"], "book");
    assert_eq!(payload["params"]["symbol"], "BTC");
    assert_eq!(
        pacifica_private_subscribe_payload("account_positions", "42trU9A5...")["params"]["account"],
        "42trU9A5..."
    );
    assert_eq!(pacifica_ping_payload()["method"], "ping");
    assert_eq!(
        pacifica_reconnect_policy_ms(),
        (30_000, 60_000, 60_000, 86_400_000)
    );
}

#[tokio::test]
async fn private_place_order_should_remain_request_spec_only() {
    let adapter = PacificaGatewayAdapter::new(PacificaGatewayConfig::default()).expect("adapter");
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("order"),
        symbol: symbol(),
        client_order_id: None,
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "1".to_string(),
        price: Some("64000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "pacifica.place_order_request_spec_only"
        }
    ));
}
