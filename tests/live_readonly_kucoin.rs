#![allow(clippy::all)]
use rustcta_exchange_gateway::{AdapterBackedGateway, KuCoinGatewayConfig};

#[test]
fn live_readonly_kucoin_should_use_gateway_adapter_boundary() {
    let config = KuCoinGatewayConfig {
        enabled_private_rest: false,
        ..KuCoinGatewayConfig::default()
    };

    assert!(config.enabled);
    assert!(!config.private_rest_enabled());
    assert!(config.rest_base_url.contains("kucoin"));

    AdapterBackedGateway::with_named_adapters("live-readonly-kucoin", ["kucoin"])
        .expect("kucoin gateway adapter should register");
}
