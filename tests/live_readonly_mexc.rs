#![allow(clippy::all)]
use rustcta_exchange_gateway::{AdapterBackedGateway, MexcGatewayConfig};

#[test]
fn live_readonly_mexc_should_use_gateway_adapter_boundary() {
    let config = MexcGatewayConfig {
        enabled_private_rest: false,
        ..MexcGatewayConfig::default()
    };

    assert!(config.enabled);
    assert!(!config.private_rest_enabled());
    assert!(config.rest_base_url.contains("mexc"));

    AdapterBackedGateway::with_named_adapters("live-readonly-mexc", ["mexc"])
        .expect("mexc gateway adapter should register");
}
