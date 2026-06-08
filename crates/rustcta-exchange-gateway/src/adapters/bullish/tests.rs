use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};

use super::parser::{parse_orderbook_snapshot, parse_symbol_rules};
use super::private::create_order_request_spec_fixture;
use super::signing::{bullish_hmac_payload, bullish_hmac_signature};
use super::streams::{
    bullish_keepalive_payload, bullish_reconnect_policy_ms, bullish_subscribe_payload,
};
use super::{BullishGatewayAdapter, BullishGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("bullish").expect("exchange")
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

fn symbol(market_type: MarketType) -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDC").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), market_type, "BTCUSDC")
            .expect("symbol"),
    }
}

#[test]
fn capabilities_should_keep_live_rest_disabled_while_documenting_product_scope() {
    let adapter = BullishGatewayAdapter::new(BullishGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(
        capabilities.market_types,
        vec![MarketType::Spot, MarketType::Perpetual, MarketType::Futures]
    );
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_batch_place_order);

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bullish/unsupported_boundary.json"
    ))
    .expect("fixture");
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["private_write_mode"], "offline_request_spec_only");
}

#[test]
fn parser_fixtures_should_decode_markets_and_order_book() {
    let markets: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bullish/markets.json"
    ))
    .expect("markets fixture");
    let rules = parse_symbol_rules(&exchange_id(), &[], &markets).expect("symbol rules");
    assert_eq!(rules.len(), 2);
    assert!(rules
        .iter()
        .any(|rule| rule.symbol.market_type == MarketType::Spot));
    assert!(rules
        .iter()
        .any(|rule| rule.symbol.market_type == MarketType::Perpetual));

    let depth: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bullish/orderbook_hybrid.json"
    ))
    .expect("depth fixture");
    let book = parse_orderbook_snapshot(&exchange_id(), symbol(MarketType::Spot), &depth)
        .expect("order book");
    assert_eq!(book.bids[0].price, 64999.5);
    assert_eq!(book.asks[0].price, 65000.0);
    assert_eq!(book.sequence, Some(123456789));
}

#[test]
fn request_spec_and_signing_fixture_should_match_official_hmac_shape() {
    let spec = create_order_request_spec_fixture();
    assert_eq!(spec["path"], "/trading-api/v2/orders");
    assert_eq!(spec["auth"], "bearer_jwt_plus_bx_signature");

    let payload = bullish_hmac_payload(
        1_700_000_000_000,
        1_700_000_000_001,
        "POST",
        "/trading-api/v2/orders",
        r#"{"commandType":"V3CreateOrder","symbol":"BTCUSDC"}"#,
    );
    let signature = bullish_hmac_signature("test-secret", &payload).expect("signature");
    assert_eq!(signature.len(), 64);

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bullish/signing_vectors/hmac_create_order.json"
    ))
    .expect("fixture");
    assert_eq!(fixture["payload"], payload);
    assert_eq!(fixture["expected_signature"], signature);
}

#[tokio::test]
async fn place_order_should_be_explicitly_unsupported_until_transport_is_promoted() {
    let adapter = BullishGatewayAdapter::new(BullishGatewayConfig::default()).expect("adapter");
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(MarketType::Spot),
        client_order_id: Some("offline-fixture".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
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
            operation: "bullish.place_order_offline_request_spec_only"
        }
    ));
}

#[test]
fn websocket_helpers_should_build_json_rpc_payloads() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol(MarketType::Perpetual),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let payload = bullish_subscribe_payload(
        "l2Orderbook",
        Some(&subscription.symbol.exchange_symbol.symbol),
        "1",
    );
    assert_eq!(payload["jsonrpc"], "2.0");
    assert_eq!(payload["method"], "subscribe");
    assert_eq!(payload["params"]["symbol"], "BTCUSDC");
    assert_eq!(bullish_keepalive_payload("2")["method"], "keepalivePing");
    assert_eq!(bullish_reconnect_policy_ms(), (240_000, 30_000, 300_000));
}
