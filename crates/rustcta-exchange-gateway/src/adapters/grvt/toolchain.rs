use rustcta_exchange_api::{
    BatchCapability, CapabilitySupport, EndpointAuth, EndpointCapability, EndpointTransport,
    ExchangeClientCapabilities, HeartbeatCapability, HeartbeatPolicy, HistoryCapability,
    ReconnectCapability, StreamHeartbeatDirection, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

pub(super) fn apply_toolchain_capabilities(capabilities: &mut ExchangeClientCapabilities) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::unsupported(
        "GRVT REST is audited but not enabled until parser coverage is promoted beyond fixture-only",
    );
    capabilities.capabilities_v2.private_rest = CapabilitySupport::unsupported(
        "GRVT private REST depends on session-cookie/API-key login and is disabled without request-spec coverage",
    );
    capabilities.capabilities_v2.public_streams =
        CapabilitySupport::unsupported("GRVT public WS is session-spec/parser-fixture only");
    capabilities.capabilities_v2.private_streams =
        CapabilitySupport::unsupported("GRVT private WS requires authenticated session cookie");
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::unsupported("GRVT WS runtime is not opened by this task"),
        private: CapabilitySupport::unsupported("GRVT authenticated WS runtime is not opened"),
        supports_subscribe: false,
        supports_unsubscribe: false,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: false,
            direction: StreamHeartbeatDirection::ClientPing,
            interval_ms: Some(30_000),
            timeout_ms: Some(45_000),
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
        BatchCapability::unsupported("grvt.bulk_orders_session_spec_only");
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported("grvt.bulk_orders_session_spec_only");
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported("grvt.cancel_all_orders_session_spec_only");
    capabilities.capabilities_v2.order_history =
        HistoryCapability::unsupported("grvt.order_history_session_spec_only");
    capabilities.capabilities_v2.fills_history =
        HistoryCapability::unsupported("grvt.fill_history_session_spec_only");
    capabilities.capabilities_v2.credential_scopes = Vec::new();
    capabilities.capabilities_v2.endpoints = audited_endpoints();
    capabilities.apply_v2_to_legacy_flags();
}

fn audited_endpoints() -> Vec<EndpointCapability> {
    [
        (
            "symbol_rules",
            EndpointTransport::Rest,
            Some("POST"),
            Some("/v1/all_instruments"),
        ),
        (
            "order_book",
            EndpointTransport::Rest,
            Some("POST"),
            Some("/v1/book"),
        ),
        (
            "place_order",
            EndpointTransport::Rest,
            Some("POST"),
            Some("/v1/create_order"),
        ),
        (
            "cancel_order",
            EndpointTransport::Rest,
            Some("POST"),
            Some("/v1/cancel_order"),
        ),
        (
            "batch_orders",
            EndpointTransport::Rest,
            Some("POST"),
            Some("/v2/bulk_orders"),
        ),
        (
            "open_orders",
            EndpointTransport::Rest,
            Some("POST"),
            Some("/v1/open_orders"),
        ),
        (
            "positions",
            EndpointTransport::Rest,
            Some("POST"),
            Some("/v1/positions"),
        ),
        (
            "public_book_ws",
            EndpointTransport::WebSocket,
            None,
            Some("v1.book.s"),
        ),
        (
            "private_orders_ws",
            EndpointTransport::WebSocket,
            None,
            Some("v1/order"),
        ),
    ]
    .into_iter()
    .map(|(operation, transport, method, path)| EndpointCapability {
        operation: operation.to_string(),
        support: CapabilitySupport::unsupported(format!("grvt.{operation}_session_spec_only")),
        market_types: vec![MarketType::Perpetual, MarketType::Option],
        transport,
        method: method.map(ToString::to_string),
        path: path.map(ToString::to_string),
        auth: EndpointAuth::None,
        credential_scopes: Vec::new(),
        rate_limit_bucket: Some("grvt_account_pair".to_string()),
        weight: Some(0),
        supports_testnet: true,
    })
    .collect()
}
