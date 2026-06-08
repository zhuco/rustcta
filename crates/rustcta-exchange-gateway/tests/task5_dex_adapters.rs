use std::sync::Arc;

use chrono::Utc;
use rustcta_exchange_api::{RequestContext, EXCHANGE_API_SCHEMA_VERSION};
use rustcta_exchange_gateway::{
    AdapterBackedGateway, DydxGatewayConfig, GatewayClient, HyperliquidGatewayConfig,
    InProcessGatewayClient, LocalGateway, GATEWAY_PROTOCOL_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, ExchangeId, MarketType, TenantId};

#[tokio::test]
async fn task5_dex_adapters_should_register_named_public_profiles() {
    let gateway =
        AdapterBackedGateway::with_named_adapters("task5-dex-public", ["hyperliquid", "dydx_v4"])
            .expect("task5 named adapters");

    let status = gateway.status().await.expect("gateway status");
    let exchanges = status
        .exchanges
        .iter()
        .map(|status| status.exchange.as_str())
        .collect::<Vec<_>>();
    assert!(exchanges.contains(&"hyperliquid"));
    assert!(exchanges.contains(&"dydx"));
}

#[tokio::test]
async fn task5_dex_private_capabilities_should_match_signing_boundaries() {
    let gateway = task5_private_gateway();
    let client = InProcessGatewayClient::new(Arc::new(gateway));
    let response = task5_capabilities(&client).await;
    let hyperliquid_capabilities = response
        .capabilities
        .iter()
        .find(|capabilities| capabilities.exchange.as_str() == "hyperliquid")
        .expect("hyperliquid capabilities");
    assert_eq!(
        hyperliquid_capabilities.market_types,
        vec![MarketType::Perpetual]
    );
    assert!(hyperliquid_capabilities.supports_private_rest);
    assert!(hyperliquid_capabilities.supports_place_order);
    assert!(hyperliquid_capabilities.supports_positions);
    assert!(hyperliquid_capabilities.supports_private_streams);

    let dydx_capabilities = response
        .capabilities
        .iter()
        .find(|capabilities| capabilities.exchange.as_str() == "dydx")
        .expect("dydx capabilities");
    assert_eq!(dydx_capabilities.market_types, vec![MarketType::Perpetual]);
    assert!(dydx_capabilities.supports_private_rest);
    assert!(dydx_capabilities.supports_positions);
    assert!(dydx_capabilities.supports_private_streams);
    assert!(!dydx_capabilities.supports_place_order);
    assert!(!dydx_capabilities.supports_cancel_order);
}

#[tokio::test]
async fn task5_dex_adapters_should_report_gateway_capabilities() {
    let gateway = task5_private_gateway();
    let client = InProcessGatewayClient::new(Arc::new(gateway));
    let response = task5_capabilities(&client).await;

    assert_eq!(response.capabilities.len(), 2);
    let hyperliquid = response
        .capabilities
        .iter()
        .find(|capabilities| capabilities.exchange.as_str() == "hyperliquid")
        .expect("hyperliquid capabilities");
    assert!(hyperliquid.supports_place_order);
    assert!(hyperliquid.supports_private_rest);
    assert!(hyperliquid.supports_private_streams);

    let dydx = response
        .capabilities
        .iter()
        .find(|capabilities| capabilities.exchange.as_str() == "dydx")
        .expect("dydx capabilities");
    assert!(dydx.supports_private_rest);
    assert!(dydx.supports_private_streams);
    assert!(dydx.supports_positions);
    assert!(!dydx.supports_place_order);
}

fn task5_private_gateway() -> AdapterBackedGateway {
    let gateway = AdapterBackedGateway::new("task5-dex-gateway");
    gateway
        .register_hyperliquid_adapter(HyperliquidGatewayConfig {
            account_address: Some("0x1111111111111111111111111111111111111111".to_string()),
            signing_private_key: Some(
                "0000000000000000000000000000000000000000000000000000000000000001".to_string(),
            ),
            enabled_private_rest: true,
            enabled_private_streams: true,
            ..HyperliquidGatewayConfig::default()
        })
        .expect("register hyperliquid");
    gateway
        .register_dydx_adapter(DydxGatewayConfig {
            wallet_address: Some("dydx1fixturewalletaddress000000000000000000".to_string()),
            subaccount_number: 0,
            enabled_private_indexer_rest: true,
            enabled_private_streams: true,
            enabled_node_private_write: false,
            ..DydxGatewayConfig::default()
        })
        .expect("register dydx");
    gateway
}

async fn task5_capabilities<C: GatewayClient>(
    client: &C,
) -> rustcta_exchange_gateway::GetCapabilitiesResponse {
    client
        .get_capabilities(
            "task5-capabilities".to_string(),
            TenantId::new("tenant").expect("tenant"),
            Some(AccountId::new("account").expect("account")),
            rustcta_exchange_gateway::GetCapabilitiesRequest {
                schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                context: RequestContext {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    tenant_id: Some(TenantId::new("tenant").expect("tenant")),
                    account_id: Some(AccountId::new("account").expect("account")),
                    run_id: None,
                    request_id: Some("task5-capabilities".to_string()),
                    requested_at: Utc::now(),
                },
                exchanges: vec![
                    ExchangeId::new("hyperliquid").expect("hyperliquid"),
                    ExchangeId::new("dydx").expect("dydx"),
                ],
            },
        )
        .await
        .expect("task5 capabilities")
}
