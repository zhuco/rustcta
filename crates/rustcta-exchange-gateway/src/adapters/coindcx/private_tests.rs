use rustcta_exchange_api::{CapabilitySupport, ExchangeClient};

use super::{CoinDcxGatewayAdapter, CoinDcxGatewayConfig};

#[test]
fn coindcx_private_tests_should_declare_private_rest_and_socketio_runtime() {
    let adapter = CoinDcxGatewayAdapter::new(CoinDcxGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        ..CoinDcxGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();

    assert!(capabilities.supports_private_rest);
    assert!(capabilities.supports_private_streams);
    assert!(capabilities.supports_batch_place_order);
    assert!(matches!(
        capabilities.capabilities_v2.private_streams,
        CapabilitySupport::WsOnly { .. }
    ));
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .supports_private_subscribe
    );
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .reconnect_requires_login
    );
}
