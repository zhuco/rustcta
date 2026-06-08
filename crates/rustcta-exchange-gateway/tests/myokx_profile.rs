use std::sync::Arc;

use rustcta_exchange_api::{RequestContext, EXCHANGE_API_SCHEMA_VERSION};
use rustcta_exchange_gateway::{
    AdapterBackedGateway, GatewayClient, GetCapabilitiesRequest, InProcessGatewayClient,
};
use rustcta_types::{AccountId, ExchangeId, MarketType, TenantId};

#[tokio::test]
async fn myokx_profile_should_register_as_spot_public_only() {
    let gateway =
        Arc::new(AdapterBackedGateway::with_named_adapters("myokx-test", ["myokx"]).unwrap());
    let client = InProcessGatewayClient::new(gateway);
    let response = client
        .get_capabilities(
            "myokx-capabilities".to_string(),
            TenantId::new("tenant").unwrap(),
            Some(AccountId::new("account").unwrap()),
            GetCapabilitiesRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: RequestContext {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    tenant_id: Some(TenantId::new("tenant").unwrap()),
                    account_id: Some(AccountId::new("account").unwrap()),
                    run_id: None,
                    request_id: Some("myokx-capabilities".to_string()),
                    requested_at: chrono::Utc::now(),
                },
                exchanges: vec![ExchangeId::new("myokx").unwrap()],
            },
        )
        .await
        .unwrap();

    let capabilities = response.capabilities.first().unwrap();
    assert_eq!(capabilities.exchange, ExchangeId::new("myokx").unwrap());
    assert_eq!(capabilities.market_types, vec![MarketType::Spot]);
    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_symbol_rules);
    assert!(capabilities.supports_order_book_snapshot);
    assert!(!capabilities.supports_private_rest);
    assert!(!capabilities.supports_place_order);
    assert!(capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .any(|endpoint| endpoint.operation == "get_balances"
            && endpoint.path.as_deref() == Some("/unsupported/myokx/get_balances")));
}

#[test]
fn myokx_boundary_fixture_should_stay_secret_free() {
    let boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/exchanges/myokx/unsupported_boundary.json"
    ))
    .unwrap();
    assert_eq!(boundary["exchange"], "myokx");
    assert_eq!(boundary["profile_of"], "okx");
    assert_eq!(boundary["private_rest"]["trade_enabled"], false);
    assert!(boundary["credential_values"]
        .as_object()
        .unwrap()
        .is_empty());
}
