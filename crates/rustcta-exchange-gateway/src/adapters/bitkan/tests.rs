use rustcta_exchange_api::{
    BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};

use super::streams::{
    bitkan_ping_payload, bitkan_public_subscribe_payload, bitkan_reconnect_policy_ms,
};
use super::{BitkanGatewayAdapter, BitkanGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("bitkan").expect("exchange")
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
        exchange_symbol: ExchangeSymbol::new(exchange_id(), market_type, "BTC-USDT")
            .expect("symbol"),
    }
}

#[test]
fn capabilities_should_expose_product_scope_but_not_unverified_api_surfaces() {
    let adapter = BitkanGatewayAdapter::new(BitkanGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(
        capabilities.market_types,
        vec![MarketType::Spot, MarketType::Perpetual]
    );
    assert!(!capabilities.supports_public_rest);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_batch_cancel_order);
    assert!(!capabilities
        .capabilities_v2
        .batch_place_orders
        .support
        .is_supported());
    assert!(!capabilities.capabilities_v2.public_rest.is_supported());
    assert!(!capabilities
        .capabilities_v2
        .stream_runtime
        .public
        .is_supported());

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitkan/unsupported_boundary.json"
    ))
    .expect("fixture");
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["scan_only"], false);
}

#[test]
fn unsupported_fixtures_should_not_open_scan_or_trade_boundaries() {
    let empty: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitkan/empty_response.json"
    ))
    .expect("empty fixture");
    let error: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitkan/error_response.json"
    ))
    .expect("error fixture");
    let missing: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitkan/missing_required_fields.json"
    ))
    .expect("missing fixture");
    let signing: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitkan/signing_vectors/unsupported_private_rest.json"
    ))
    .expect("unsupported signing fixture");

    assert!(empty["data"].as_array().is_some_and(Vec::is_empty));
    assert_eq!(error["code"], "UNVERIFIED_ENDPOINT");
    assert!(missing.get("symbol").is_some());
    assert_eq!(signing["supported"], false);
    assert_eq!(signing["expected_error"], "bitkan.private_rest_unverified");

    let adapter = BitkanGatewayAdapter::new(BitkanGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(!capabilities.capabilities_v2.public_rest.is_supported());
    assert!(!capabilities.capabilities_v2.private_rest.is_supported());
}

#[tokio::test]
async fn batch_place_should_be_explicitly_unsupported() {
    let adapter = BitkanGatewayAdapter::new(BitkanGatewayConfig::default()).expect("adapter");
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
            operation: "bitkan.batch_place_orders_unverified"
        }
    ));
}

#[test]
fn websocket_helpers_should_build_subscription_and_heartbeat_payloads() {
    let subscription = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-ws"),
        symbol: symbol(MarketType::Spot),
        kind: PublicStreamKind::OrderBookDelta,
    };

    let payload = bitkan_public_subscribe_payload(&subscription);
    assert_eq!(payload["op"], "subscribe");
    assert_eq!(payload["channel"], "depth:BTC-USDT");
    assert_eq!(bitkan_ping_payload()["op"], "ping");
    assert_eq!(bitkan_reconnect_policy_ms(), (30_000, 45_000, 60_000));
}
