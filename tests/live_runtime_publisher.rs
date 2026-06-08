#![allow(clippy::all)]
use rustcta_exchange_gateway::AdapterBackedGateway;

#[test]
fn live_runtime_publisher_should_depend_on_gateway_read_sources() {
    AdapterBackedGateway::with_named_adapters("live-runtime-publisher", ["gateio", "bitget"])
        .expect("runtime publisher source adapters should register");
}
