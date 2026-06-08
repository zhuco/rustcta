use rustcta_exchange_api::{
    BatchCapability, CapabilitySupport, EndpointAuth, EndpointCapability, EndpointTransport,
    ExchangeClientCapabilities, HeartbeatCapability, HeartbeatPolicy, HistoryCapability,
    ReconnectCapability, StreamHeartbeatDirection, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

pub(super) fn apply_toolchain_capabilities(capabilities: &mut ExchangeClientCapabilities) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = CapabilitySupport::unsupported(
        "Pacifica private writes are request-spec/signing only until agent-wallet live dry-run is verified",
    );
    capabilities.capabilities_v2.public_streams =
        CapabilitySupport::unsupported("Pacifica public WS payload/parser fixtures are spec-only");
    capabilities.capabilities_v2.private_streams = CapabilitySupport::unsupported(
        "Pacifica private WS account streams require signed live validation",
    );
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::unsupported(
            "Pacifica WS runtime is not wired into the gateway supervisor",
        ),
        private: CapabilitySupport::unsupported("Pacifica private WS runtime is not enabled"),
        supports_subscribe: false,
        supports_unsubscribe: false,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::ClientPing,
            interval_ms: Some(30_000),
            timeout_ms: Some(60_000),
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
        BatchCapability::unsupported("pacifica.batch_orders_request_spec_only");
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported("pacifica.batch_cancel_orders_request_spec_only");
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported("pacifica.cancel_all_orders_request_spec_only");
    capabilities.capabilities_v2.order_history =
        HistoryCapability::unsupported("pacifica.open_orders_request_spec_only");
    capabilities.capabilities_v2.fills_history =
        HistoryCapability::unsupported("pacifica.recent_fills_request_spec_only");
    capabilities.capabilities_v2.credential_scopes = Vec::new();
    capabilities.capabilities_v2.endpoints = endpoint_capabilities();
    capabilities.apply_v2_to_legacy_flags();
}

fn endpoint_capabilities() -> Vec<EndpointCapability> {
    vec![
        endpoint(
            "symbol_rules",
            CapabilitySupport::native(),
            EndpointAuth::None,
            Some("GET"),
            Some("/api/v1/info"),
            1,
        ),
        endpoint(
            "order_book",
            CapabilitySupport::native(),
            EndpointAuth::None,
            Some("GET"),
            Some("/api/v1/book"),
            1,
        ),
        endpoint(
            "get_open_orders",
            CapabilitySupport::unsupported("pacifica.open_orders_request_spec_only"),
            EndpointAuth::None,
            Some("GET"),
            Some("/api/v1/orders"),
            1,
        ),
        endpoint(
            "get_positions",
            CapabilitySupport::unsupported("pacifica.positions_request_spec_only"),
            EndpointAuth::None,
            Some("GET"),
            Some("/api/v1/positions"),
            1,
        ),
        endpoint(
            "place_order",
            CapabilitySupport::unsupported("pacifica.place_order_request_spec_only"),
            EndpointAuth::None,
            Some("POST"),
            Some("/api/v1/orders/create"),
            1,
        ),
        endpoint(
            "cancel_order",
            CapabilitySupport::unsupported("pacifica.cancel_order_request_spec_only"),
            EndpointAuth::None,
            Some("POST"),
            Some("/api/v1/orders/cancel"),
            1,
        ),
        endpoint(
            "cancel_all_orders",
            CapabilitySupport::unsupported("pacifica.cancel_all_orders_request_spec_only"),
            EndpointAuth::None,
            Some("POST"),
            Some("/api/v1/orders/cancel_all"),
            1,
        ),
    ]
}

fn endpoint(
    operation: &str,
    support: CapabilitySupport,
    auth: EndpointAuth,
    method: Option<&str>,
    path: Option<&str>,
    weight: u32,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support,
        market_types: vec![MarketType::Perpetual],
        transport: EndpointTransport::Rest,
        method: method.map(str::to_string),
        path: path.map(str::to_string),
        auth,
        credential_scopes: Vec::new(),
        rate_limit_bucket: Some("pacifica_credit".to_string()),
        weight: Some(weight),
        supports_testnet: true,
    }
}
