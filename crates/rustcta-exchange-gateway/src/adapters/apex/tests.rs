use rustcta_exchange_api::{
    ExchangeApiError, ExchangeClient, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};

use super::signing::{apex_hmac_signature, apex_signing_payload, apex_sorted_form_body};
use super::streams::{
    apex_ping_payload, apex_private_login_payload, apex_public_subscribe_payload,
    apex_reconnect_policy_ms,
};
use super::{ApexGatewayAdapter, ApexGatewayConfig};
use crate::adapters::apex::parser::{parse_orderbook_snapshot, parse_symbol_rules};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("apex").expect("exchange")
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
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), market_type, "BTCUSDT")
            .expect("symbol"),
    }
}

#[test]
fn capabilities_should_scope_apex_to_perpetual_and_keep_private_writes_disabled() {
    let adapter = ApexGatewayAdapter::new(ApexGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.market_types, vec![MarketType::Perpetual]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.supports_batch_place_order);

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/apex/unsupported_boundary.json"
    ))
    .expect("fixture");
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["private_write_blocker"], "zklink_l2_signature");
}

#[test]
fn parser_fixtures_should_decode_symbols_and_order_book() {
    let symbols: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/apex/symbols_perpetual.json"
    ))
    .expect("symbols fixture");
    let rules = parse_symbol_rules(&exchange_id(), &[], &symbols).expect("symbol rules");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].quote_asset, "USDT");
    assert_eq!(rules[0].quantity_increment.as_deref(), Some("0.001"));

    let depth: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/apex/depth_btcusdt.json"
    ))
    .expect("depth fixture");
    let book = parse_orderbook_snapshot(&exchange_id(), symbol(MarketType::Perpetual), &depth)
        .expect("order book");
    assert_eq!(book.bids[0].price, 64999.5);
    assert_eq!(book.asks[0].price, 65000.0);
    assert_eq!(book.sequence, Some(123456789));
}

#[test]
fn signing_fixture_should_cover_private_read_hmac_shape() {
    let body = apex_sorted_form_body(&[
        ("limit", Some("100")),
        ("symbol", Some("BTC-USDT")),
        ("page", Some("1")),
    ]);
    assert_eq!(body, "limit=100&page=1&symbol=BTC-USDT");
    let payload = apex_signing_payload(
        "2026-06-08T00:00:00.000Z",
        "GET",
        "/api/v3/open-orders?limit=100&page=1&symbol=BTC-USDT",
        "",
    );
    let signature = apex_hmac_signature("test-secret", &payload).expect("signature");
    assert!(!signature.is_empty());

    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/apex/signing_vectors/private_get_open_orders.json"
    ))
    .expect("fixture");
    assert_eq!(fixture["payload"], payload);
    assert_eq!(fixture["expected_signature"], signature);
}

#[tokio::test]
async fn place_order_should_require_zklink_l2_signature() {
    let adapter = ApexGatewayAdapter::new(ApexGatewayConfig::default()).expect("adapter");
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol(MarketType::Perpetual),
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
            operation: "apex.place_order_requires_zklink_l2_signature"
        }
    ));
}

#[test]
fn websocket_helpers_should_build_public_and_private_payloads() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("ws"),
        symbol: symbol(MarketType::Perpetual),
        kind: PublicStreamKind::OrderBookDelta,
    };
    let topic = format!(
        "orderBook200.H.{}",
        subscription.symbol.exchange_symbol.symbol
    );
    assert_eq!(
        apex_public_subscribe_payload(&topic)["args"][0],
        "orderBook200.H.BTCUSDT"
    );
    assert_eq!(apex_ping_payload(1_700_000_000_000)["op"], "ping");
    let login = apex_private_login_payload("key", "pass", "ts", "sig");
    assert_eq!(login["op"], "login");
    assert!(login["args"][0]
        .as_str()
        .expect("login string")
        .contains("ws_zk_accounts_v3"));
    assert_eq!(apex_reconnect_policy_ms(), (15_000, 15_000, 45_000));
}
