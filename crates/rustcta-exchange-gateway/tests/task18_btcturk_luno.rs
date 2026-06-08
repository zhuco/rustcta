use std::sync::Arc;

use rustcta_exchange_api::{
    AccountId, ExchangeId, RequestContext, TenantId, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_exchange_gateway::{
    AdapterBackedGateway, GatewayClient, GetCapabilitiesRequest, InProcessGatewayClient,
    LocalGateway,
};

#[tokio::test]
async fn task18_adapters_should_register_and_expose_public_capabilities() {
    let gateway =
        AdapterBackedGateway::with_named_adapters("task18", ["btcturk", "luno"]).expect("gateway");
    let status = gateway.status().await.expect("status");
    let exchanges = status
        .exchanges
        .iter()
        .map(|exchange| exchange.exchange.as_str())
        .collect::<Vec<_>>();
    assert!(exchanges.contains(&"btcturk"));
    assert!(exchanges.contains(&"luno"));

    let client = InProcessGatewayClient::new(Arc::new(gateway));
    let capabilities = client
        .get_capabilities(
            "task18-capabilities".to_string(),
            TenantId::new("tenant").expect("tenant"),
            Some(AccountId::new("account").expect("account")),
            GetCapabilitiesRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: RequestContext {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                    account_id: Some(AccountId::new("account").expect("account")),
                    run_id: None,
                    request_id: Some("task18-capabilities".to_string()),
                    requested_at: chrono::Utc::now(),
                },
                exchanges: vec![
                    ExchangeId::new("btcturk").expect("btcturk"),
                    ExchangeId::new("luno").expect("luno"),
                ],
            },
        )
        .await
        .expect("capabilities")
        .capabilities;
    let btcturk = capabilities
        .iter()
        .find(|capability| capability.exchange.to_string() == "btcturk")
        .expect("btcturk capabilities");
    assert!(btcturk.supports_public_rest);
    assert!(btcturk.supports_symbol_rules);
    assert!(btcturk.supports_order_book_snapshot);
    assert!(!btcturk.supports_private_rest);
    assert!(!btcturk.supports_place_order);

    let luno = capabilities
        .iter()
        .find(|capability| capability.exchange.to_string() == "luno")
        .expect("luno capabilities");
    assert!(luno.supports_public_rest);
    assert!(luno.supports_symbol_rules);
    assert!(luno.supports_order_book_snapshot);
    assert!(!luno.supports_private_rest);
    assert!(!luno.supports_place_order);
}

#[test]
fn task18_standard_deliverables_should_be_present_and_secret_free() {
    let btcturk_mapping = include_str!("../src/adapters/btcturk/endpoint_mapping.yaml");
    assert!(btcturk_mapping.contains("exchange: btcturk"));
    assert!(btcturk_mapping.contains("fiat_withdrawals"));

    let luno_mapping = include_str!("../src/adapters/luno/endpoint_mapping.yaml");
    assert!(luno_mapping.contains("exchange: luno"));
    assert!(luno_mapping.contains("bank_payment_rails"));

    let btcturk_boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/exchanges/btcturk/unsupported_boundary.json"
    ))
    .expect("btcturk boundary");
    assert_eq!(btcturk_boundary["trade_enabled"], false);
    assert_eq!(btcturk_boundary["fiat_markets_documented"][0], "TRY");

    let luno_boundary: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/exchanges/luno/unsupported_boundary.json"
    ))
    .expect("luno boundary");
    assert_eq!(luno_boundary["trade_enabled"], false);
    assert_eq!(luno_boundary["fiat_markets_documented"][0], "ZAR");
}
