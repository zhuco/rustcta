use rustcta_exchange_api::{CapabilitySupport, ExchangeClient};

use super::CoinDcxGatewayAdapter;

#[test]
fn coindcx_public_tests_should_declare_socketio_public_runtime() {
    let adapter = CoinDcxGatewayAdapter::default_public().expect("adapter");
    let capabilities = adapter.capabilities();

    assert!(capabilities.supports_public_rest);
    assert!(capabilities.supports_public_streams);
    assert!(matches!(
        capabilities.capabilities_v2.public_streams,
        CapabilitySupport::WsOnly { .. }
    ));
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .supports_public_subscribe
    );
}
