use rustcta_exchange_api::{
    BatchCapability, CapabilitySupport, EndpointAuth, EndpointCapability, EndpointTransport,
    ExchangeClientCapabilities, HeartbeatCapability, HeartbeatPolicy, HistoryCapability,
    ReconnectCapability, StreamHeartbeatDirection, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

pub(super) fn apply_toolchain_capabilities(capabilities: &mut ExchangeClientCapabilities) {
    capabilities.capabilities_v2.public_rest =
        CapabilitySupport::unsupported("KCEX official OpenAPI REST surface is not verified");
    capabilities.capabilities_v2.private_rest = CapabilitySupport::unsupported(
        "KCEX private REST signing and endpoint surface are not verified",
    );
    capabilities.capabilities_v2.public_streams =
        CapabilitySupport::unsupported("KCEX public WS order-book API is not verified");
    capabilities.capabilities_v2.private_streams =
        CapabilitySupport::unsupported("KCEX private WS auth/channel API is not verified");
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::unsupported("kcex.public_streams_unverified_openapi"),
        private: CapabilitySupport::unsupported("kcex.private_streams_unverified_openapi"),
        supports_subscribe: false,
        supports_unsubscribe: false,
        supports_public_subscribe: false,
        supports_public_unsubscribe: false,
        supports_private_subscribe: false,
        supports_private_unsubscribe: false,
        heartbeat: HeartbeatCapability {
            supported: false,
            required: false,
            direction: StreamHeartbeatDirection::None,
            interval_ms: None,
            timeout_ms: None,
        },
        reconnect: ReconnectCapability {
            supported: false,
            requires_resubscribe: true,
            preserves_session: false,
            max_reconnect_attempts: None,
        },
        heartbeat_policy: HeartbeatPolicy::disabled(),
        reconnect_requires_login: false,
        reconnect_requires_resubscribe: true,
        orderbook_requires_snapshot_after_reconnect: true,
        ..StreamRuntimeCapability::default()
    };
    capabilities.capabilities_v2.batch_place_orders =
        BatchCapability::unsupported("kcex.batch_place_orders_unverified_openapi");
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported("kcex.batch_cancel_orders_unverified_openapi");
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported("kcex.cancel_all_orders_unverified_openapi");
    capabilities.capabilities_v2.order_history =
        HistoryCapability::unsupported("kcex.open_orders_unverified_openapi");
    capabilities.capabilities_v2.fills_history =
        HistoryCapability::unsupported("kcex.recent_fills_unverified_openapi");
    capabilities.capabilities_v2.credential_scopes = Vec::new();
    capabilities.capabilities_v2.endpoints = unsupported_endpoints();
    capabilities.apply_v2_to_legacy_flags();
}

fn unsupported_endpoints() -> Vec<EndpointCapability> {
    [
        "get_symbol_rules",
        "get_order_book",
        "get_balances",
        "get_positions",
        "get_fees",
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
        support: CapabilitySupport::unsupported(format!("kcex.{operation}_unverified_openapi")),
        market_types: vec![MarketType::Spot, MarketType::Perpetual],
        transport: EndpointTransport::Rest,
        method: None,
        path: None,
        auth: EndpointAuth::None,
        credential_scopes: Vec::new(),
        rate_limit_bucket: Some("kcex_unsupported".to_string()),
        weight: Some(0),
        supports_testnet: false,
    })
    .collect()
}
