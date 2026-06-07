use rustcta_exchange_api::{
    BatchCapability, CapabilitySupport, EndpointAuth, EndpointCapability, EndpointTransport,
    ExchangeClientCapabilities, HeartbeatCapability, HeartbeatPolicy, HistoryCapability,
    ReconnectCapability, StreamHeartbeatDirection, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

pub(super) fn apply_toolchain_capabilities(capabilities: &mut ExchangeClientCapabilities) {
    capabilities.capabilities_v2.public_rest =
        CapabilitySupport::unsupported("BitKan public REST endpoints are unverified");
    capabilities.capabilities_v2.private_rest =
        CapabilitySupport::unsupported("BitKan private REST signing and endpoints are unverified");
    capabilities.capabilities_v2.public_streams =
        CapabilitySupport::unsupported("BitKan public WS helper is payload-only and disabled");
    capabilities.capabilities_v2.private_streams =
        CapabilitySupport::unsupported("BitKan private WS auth/channel spec is unverified");
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::unsupported("BitKan public WS live channel is unverified"),
        private: CapabilitySupport::unsupported("BitKan private WS is unverified"),
        supports_subscribe: false,
        supports_unsubscribe: false,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: false,
            direction: StreamHeartbeatDirection::None,
            interval_ms: Some(30_000),
            timeout_ms: Some(45_000),
        },
        reconnect: ReconnectCapability {
            supported: false,
            requires_resubscribe: true,
            preserves_session: false,
            max_reconnect_attempts: None,
        },
        heartbeat_policy: HeartbeatPolicy::disabled(),
        ..StreamRuntimeCapability::default()
    };
    capabilities.capabilities_v2.batch_place_orders =
        BatchCapability::unsupported("bitkan.batch_place_orders_unverified");
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported("bitkan.batch_cancel_orders_unverified");
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported("bitkan.cancel_all_orders_unverified");
    capabilities.capabilities_v2.order_history =
        HistoryCapability::unsupported("bitkan.open_orders_unverified");
    capabilities.capabilities_v2.fills_history =
        HistoryCapability::unsupported("bitkan.recent_fills_unverified");
    capabilities.capabilities_v2.credential_scopes = Vec::new();
    capabilities.capabilities_v2.endpoints = unsupported_endpoints();
    capabilities.apply_v2_to_legacy_flags();
}

fn unsupported_endpoints() -> Vec<EndpointCapability> {
    [
        "get_symbol_rules",
        "get_order_book",
        "place_order",
        "cancel_order",
        "batch_place_orders",
        "batch_cancel_orders",
        "cancel_all_orders",
        "query_order",
        "get_open_orders",
        "get_recent_fills",
    ]
    .into_iter()
    .map(|operation| EndpointCapability {
        operation: operation.to_string(),
        support: CapabilitySupport::unsupported(format!("bitkan.{operation}_unverified")),
        market_types: vec![MarketType::Spot, MarketType::Perpetual],
        transport: EndpointTransport::Rest,
        method: None,
        path: None,
        auth: EndpointAuth::None,
        credential_scopes: Vec::new(),
        rate_limit_bucket: Some("bitkan_unsupported".to_string()),
        weight: Some(0),
        supports_testnet: false,
    })
    .collect()
}
