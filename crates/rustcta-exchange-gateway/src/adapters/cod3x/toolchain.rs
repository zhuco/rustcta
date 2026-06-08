use rustcta_exchange_api::{
    BatchCapability, CapabilitySupport, EndpointAuth, EndpointCapability, EndpointTransport,
    ExchangeClientCapabilities, HeartbeatCapability, HeartbeatPolicy, HistoryCapability,
    ReconnectCapability, StreamHeartbeatDirection, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

pub(super) fn apply_toolchain_capabilities(capabilities: &mut ExchangeClientCapabilities) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::unsupported(
        "Cod3x is treated as an AI trading terminal/routing layer; no standalone public market REST contract is verified",
    );
    capabilities.capabilities_v2.private_rest = CapabilitySupport::unsupported(
        "Cod3x account reads route through downstream venue accounts and are not a native exchange API",
    );
    capabilities.capabilities_v2.public_streams =
        CapabilitySupport::unsupported("Cod3x public WS runtime is not verified");
    capabilities.capabilities_v2.private_streams =
        CapabilitySupport::unsupported("Cod3x private WS runtime is not verified");
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::unsupported("cod3x.public_stream_unverified"),
        private: CapabilitySupport::unsupported("cod3x.private_stream_unverified"),
        supports_subscribe: false,
        supports_unsubscribe: false,
        heartbeat: HeartbeatCapability {
            supported: false,
            required: false,
            direction: StreamHeartbeatDirection::ClientPing,
            interval_ms: None,
            timeout_ms: None,
        },
        reconnect: ReconnectCapability {
            supported: true,
            requires_resubscribe: true,
            preserves_session: false,
            max_reconnect_attempts: None,
        },
        heartbeat_policy: HeartbeatPolicy::disabled(),
        ..StreamRuntimeCapability::default()
    };
    capabilities.capabilities_v2.batch_place_orders =
        BatchCapability::unsupported("cod3x.batch_place_order_routing_unverified");
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported("cod3x.batch_cancel_order_routing_unverified");
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported("cod3x.cancel_all_orders_routing_unverified");
    capabilities.capabilities_v2.order_history =
        HistoryCapability::unsupported("cod3x.order_history_unverified");
    capabilities.capabilities_v2.fills_history =
        HistoryCapability::unsupported("cod3x.fill_history_unverified");
    capabilities.capabilities_v2.credential_scopes = Vec::new();
    capabilities.capabilities_v2.endpoints = audited_endpoints();
    capabilities.apply_v2_to_legacy_flags();
}

fn audited_endpoints() -> Vec<EndpointCapability> {
    [
        ("get_symbol_rules", "/unsupported/cod3x/markets"),
        ("get_order_book", "/unsupported/cod3x/orderbook"),
        ("get_fees", "/unsupported/cod3x/downstream-fees"),
        ("get_positions", "/unsupported/cod3x/positions"),
        ("get_balances", "/unsupported/cod3x/balances"),
        ("get_open_orders", "/unsupported/cod3x/open-orders"),
        ("query_order", "/unsupported/cod3x/order"),
        ("get_recent_fills", "/unsupported/cod3x/fills"),
        ("place_order", "/unsupported/cod3x/place-order"),
        ("cancel_order", "/unsupported/cod3x/cancel-order"),
        ("batch_place_orders", "/unsupported/cod3x/batch-place"),
        ("batch_cancel_orders", "/unsupported/cod3x/batch-cancel"),
        ("cancel_all_orders", "/unsupported/cod3x/cancel-all"),
    ]
    .into_iter()
    .map(|(operation, path)| EndpointCapability {
        operation: operation.to_string(),
        support: CapabilitySupport::unsupported(format!("{operation} is unsupported_unverified")),
        market_types: vec![MarketType::Perpetual],
        transport: EndpointTransport::Rest,
        method: Some("GET".to_string()),
        path: Some(path.to_string()),
        auth: EndpointAuth::None,
        credential_scopes: Vec::new(),
        rate_limit_bucket: Some("cod3x_unverified".to_string()),
        weight: Some(0),
        supports_testnet: false,
    })
    .collect()
}
