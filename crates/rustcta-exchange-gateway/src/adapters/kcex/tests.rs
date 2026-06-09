use rustcta_exchange_api::{
    BatchPlaceOrdersRequest, ExchangeApiError, ExchangeClient, PlaceOrderRequest, PublicStreamKind,
    PublicStreamSubscription, RequestContext, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};

use super::{KcexGatewayAdapter, KcexGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("kcex").expect("exchange")
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
        exchange_symbol: ExchangeSymbol::new(exchange_id(), market_type, "BTC_USDT")
            .expect("symbol"),
    }
}

#[test]
fn capabilities_should_expose_product_scope_without_promoting_unverified_api() {
    let adapter = KcexGatewayAdapter::new(KcexGatewayConfig::default()).expect("adapter");
    let capabilities = adapter.capabilities();

    assert_eq!(
        capabilities.market_types,
        vec![MarketType::Spot, MarketType::Perpetual]
    );
    assert!(!capabilities.supports_public_rest);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_public_streams);
    assert!(!capabilities.supports_private_streams);
    assert!(!capabilities.supports_place_order);
    assert!(!capabilities.capabilities_v2.public_rest.is_supported());
    assert!(!capabilities
        .capabilities_v2
        .stream_runtime
        .public
        .is_supported());

    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/kcex/unsupported_boundary.json"
    ))
    .expect("fixture");
    assert_eq!(boundary["trade_enabled"], false);
    assert_eq!(boundary["official_openapi_verified"], false);
}

#[tokio::test]
async fn batch_place_should_be_explicitly_unsupported() {
    let adapter = KcexGatewayAdapter::new(KcexGatewayConfig::default()).expect("adapter");
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
            operation: "kcex.batch_place_orders_unverified_openapi"
        }
    ));
}

#[tokio::test]
async fn public_stream_should_remain_unsupported_until_openapi_is_verified() {
    let adapter = KcexGatewayAdapter::new(KcexGatewayConfig::default()).expect("adapter");
    let error = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("public-ws"),
            symbol: symbol(MarketType::Perpetual),
            kind: PublicStreamKind::OrderBookDelta,
        })
        .await
        .expect_err("unsupported");

    assert!(matches!(
        error,
        ExchangeApiError::Unsupported {
            operation: "kcex.public_streams_unverified_openapi"
        }
    ));
}
