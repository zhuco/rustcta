#![allow(clippy::all)]
use rustcta_exchange_gateway::{AdapterBackedGateway, CoinExGatewayConfig};

#[test]
fn live_readonly_coinex_should_use_gateway_adapter_boundary() {
    let config = CoinExGatewayConfig {
        enabled_private_rest: false,
        ..CoinExGatewayConfig::default()
    };

    assert!(config.enabled);
    assert!(!config.private_rest_enabled());
    assert!(config.rest_base_url.contains("coinex"));

    AdapterBackedGateway::with_named_adapters("live-readonly-coinex", ["coinex"])
        .expect("coinex gateway adapter should register");
}
