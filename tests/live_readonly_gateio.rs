#![allow(clippy::all)]
use rustcta_exchange_gateway::{AdapterBackedGateway, GateIoGatewayConfig};

#[test]
fn live_readonly_gateio_should_use_gateway_adapter_boundary() {
    let config = GateIoGatewayConfig {
        enabled_private_rest: false,
        ..GateIoGatewayConfig::default()
    };

    assert!(config.enabled);
    assert!(!config.private_rest_enabled());
    assert!(config.rest_base_url.contains("gateio"));

    AdapterBackedGateway::with_named_adapters("live-readonly-gateio", ["gateio"])
        .expect("gateio gateway adapter should register");
}
