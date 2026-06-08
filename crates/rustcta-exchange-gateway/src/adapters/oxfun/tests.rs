use rustcta_exchange_api::{
    BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};

use super::parser::{parse_depth_event, parse_market_specs};
use super::private_parser::parse_order_ack;
use super::signing::{sign_rest_request, sign_ws_login};
use super::streams::{
    oxfun_ping_payload, oxfun_private_login_payload, oxfun_public_subscribe_payload,
    oxfun_public_unsubscribe_payload, oxfun_reconnect_policy_ms,
};
use super::{OxfunGatewayAdapter, OxfunGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("oxfun").expect("exchange")
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
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").expect("canonical")),
        exchange_symbol: ExchangeSymbol::new(exchange_id(), market_type, "BTC-USD-SWAP-LIN")
            .expect("symbol"),
    }
}

#[test]
fn capabilities_should_expose_audit_scope_without_live_trading() {
    let adapter = OxfunGatewayAdapter::new(OxfunGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(
        capabilities.market_types,
        vec![MarketType::Perpetual, MarketType::Option]
    );
    assert!(!capabilities.supports_public_rest);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/oxfun/unsupported_boundary.json"
    ))
    .expect("fixture");
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["api_maturity"], "audit_shell");
}

#[test]
fn signing_vectors_should_match_oxfun_docs_shape() {
    let signature = sign_rest_request(
        "secret",
        "2026-06-08T00:00:00Z",
        "nonce-1",
        "GET",
        "api.ox.fun",
        "/v3/account",
        "asset=OX",
    )
    .expect("signature");
    assert_eq!(signature, "8N+Ov1G0potibfVQkbwA5ZaOGPASA7I3xnxqR+F+DTw=");
    assert_eq!(
        sign_ws_login("secret", "1700000000000").expect("ws signature"),
        "qNyCEuPA4GPpD3gYnapr/2/0OOe1C+GUn5QWMIV8GoY="
    );

    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/oxfun/signing_vectors/rest_hmac.json"
    ))
    .expect("vector");
    assert_eq!(vector["expected_signature"], signature);
}

#[test]
fn websocket_helpers_should_build_login_subscription_and_heartbeat_payloads() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol(MarketType::Perpetual),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let payload = oxfun_public_subscribe_payload(&subscription);
    assert_eq!(payload["op"], "subscribe");
    assert_eq!(payload["args"][0], "depthUpdate:BTC-USD-SWAP-LIN");
    let login = oxfun_private_login_payload("key", "secret", "1700000000000").expect("login");
    assert_eq!(login["op"], "login");
    assert_eq!(
        login["data"]["signature"],
        "qNyCEuPA4GPpD3gYnapr/2/0OOe1C+GUn5QWMIV8GoY="
    );
    let unsubscribe = oxfun_public_unsubscribe_payload(&subscription);
    assert_eq!(unsubscribe["op"], "unsubscribe");
    assert_eq!(unsubscribe["args"][0], "depthUpdate:BTC-USD-SWAP-LIN");
    assert_eq!(
        oxfun_ping_payload(),
        serde_json::Value::String("ping".to_string())
    );
    assert_eq!(oxfun_reconnect_policy_ms(), (20_000, 45_000, 60_000));
}

#[test]
fn parser_fixtures_should_cover_market_depth_and_private_ack() {
    let markets = parse_market_specs(include_str!(
        "../../../../../tests/fixtures/exchanges/oxfun/ws/market_snapshot.json"
    ))
    .expect("markets");
    assert_eq!(markets[0].market_code, "BTC-USD-SWAP-LIN");
    assert_eq!(markets[0].tick_size.as_deref(), Some("0.5"));

    let depth = parse_depth_event(include_str!(
        "../../../../../tests/fixtures/exchanges/oxfun/ws/depth_update_snapshot.json"
    ))
    .expect("depth");
    assert_eq!(depth.sequence, Some(101));
    assert_eq!(depth.action.as_deref(), Some("partial"));
    assert_eq!(depth.checksum, Some(7654321));
    assert_eq!(depth.bids[0], ("64000".to_string(), "1.25".to_string()));

    let diff = parse_depth_event(include_str!(
        "../../../../../tests/fixtures/exchanges/oxfun/ws/depth_update_diff.json"
    ))
    .expect("diff");
    assert_eq!(diff.action.as_deref(), Some("increment"));
    assert_eq!(diff.sequence, Some(102));
    assert_eq!(diff.asks[0], ("64001".to_string(), "0".to_string()));

    let (submitted, order_id) = parse_order_ack(include_str!(
        "../../../../../tests/fixtures/exchanges/oxfun/ws/private_order_ack.json"
    ))
    .expect("ack");
    assert!(submitted);
    assert_eq!(order_id.as_deref(), Some("1000000700008"));
}

#[tokio::test]
async fn batch_place_should_be_explicitly_spec_only() {
    let adapter = OxfunGatewayAdapter::new(OxfunGatewayConfig::default()).expect("adapter");
    let request = BatchPlaceOrdersRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch"),
        exchange: exchange_id(),
        orders: vec![PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("order"),
            symbol: symbol(MarketType::Perpetual),
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
        }],
    };

    let error = adapter
        .batch_place_orders(request)
        .await
        .expect_err("unsupported");
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "oxfun.batch_place_orders_ws_request_spec_only"
        }
    ));
}
