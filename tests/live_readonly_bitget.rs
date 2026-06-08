#![allow(clippy::all)]
use rustcta_exchange_gateway::{AdapterBackedGateway, BitgetGatewayConfig};

#[test]
fn live_readonly_bitget_should_use_gateway_adapter_boundary() {
    let config = BitgetGatewayConfig {
        enabled_private_rest: false,
        ..BitgetGatewayConfig::default()
    };

    assert!(config.enabled);
    assert!(!config.private_rest_available());
    assert!(config.rest_base_url.contains("bitget"));

    AdapterBackedGateway::with_named_adapters("live-readonly-bitget", ["bitget"])
        .expect("bitget gateway adapter should register");
}
