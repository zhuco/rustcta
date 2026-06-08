use base64::{engine::general_purpose, Engine as _};
use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};

use super::parser::{btcturk_symbol, parse_btcturk_order_book, parse_btcturk_symbol_rules};
use super::private::place_order_request_spec_fixture;
use super::signing::btcturk_signature;
use super::streams::{btcturk_subscribe_payload, btcturk_ws_auth_payload};
use super::{BtcTurkGatewayAdapter, BtcTurkGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("btcturk").expect("exchange")
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
        market_type: MarketType::Spot,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "TRY").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTCTRY")
            .expect("symbol"),
    }
}

#[test]
fn parser_should_normalize_try_markets_and_orderbook() {
    let exchange_info: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcturk/exchange_info.json"
    ))
    .expect("fixture");
    let rules = parse_btcturk_symbol_rules(exchange_id(), &[], &exchange_info).expect("rules");
    assert!(rules
        .iter()
        .any(|rule| rule.base_asset == "BTC" && rule.quote_asset == "TRY"));
    assert!(rules
        .iter()
        .any(|rule| rule.base_asset == "ETH" && rule.quote_asset == "TRY"));

    let orderbook: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcturk/orderbook.json"
    ))
    .expect("fixture");
    let snapshot = parse_btcturk_order_book(&symbol(), &orderbook).expect("orderbook");
    assert_eq!(snapshot.bids[0].price, 2_499_000.0);
    assert_eq!(btcturk_symbol(&symbol()), "BTCTRY");
}

#[test]
fn request_spec_and_signing_should_match_hmac_boundary() {
    let spec = place_order_request_spec_fixture();
    assert_eq!(spec["path"], "/api/v1/order");
    assert_eq!(spec["auth"], "btcturk_hmac_sha256_base64");
    assert_eq!(spec["body"]["pairSymbol"], "BTCTRY");

    let secret = general_purpose::STANDARD.encode(b"test-secret");
    let signature = btcturk_signature("public-key", &secret, 1_700_000_000_000).expect("signature");
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/btcturk/signing_vectors/rest_hmac_sha256.json"
    ))
    .expect("fixture");
    assert_eq!(fixture["signature"], signature);
}

#[tokio::test]
async fn private_write_should_be_offline_request_spec_only() {
    let adapter = BtcTurkGatewayAdapter::new(BtcTurkGatewayConfig::default()).expect("adapter");
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(),
        client_order_id: Some("offline-fixture".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.01".to_string(),
        price: Some("2500000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };

    let error = adapter.place_order(request).await.expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "btcturk.place_order_offline_request_spec_only"
        }
    ));
}

#[test]
fn websocket_helpers_should_cover_public_and_private_payload_shapes() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol(),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let payload =
        btcturk_subscribe_payload("orderbook", &subscription.symbol.exchange_symbol.symbol);
    assert_eq!(payload[0], 151);
    assert_eq!(payload[1]["event"], "BTCTRY");

    let auth = btcturk_ws_auth_payload("public-key", 1_700_000_000_000, 3000, "sig");
    assert_eq!(auth[0], 114);
    assert_eq!(auth[1]["publicKey"], "public-key");
}
