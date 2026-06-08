#![allow(clippy::all)]
use rustcta_exchange_gateway::AdapterBackedGateway;

#[test]
fn live_websocket_books_should_use_gateway_stream_adapter_boundary() {
    AdapterBackedGateway::with_named_adapters("live-websocket-books", ["mexc", "coinex"])
        .expect("mexc and coinex gateway stream adapters should register");
}
