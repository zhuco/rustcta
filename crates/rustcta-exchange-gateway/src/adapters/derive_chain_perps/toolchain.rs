use rustcta_exchange_api::{
    BatchCapability, CapabilitySupport, EndpointAuth, EndpointCapability, EndpointTransport,
    ExchangeClientCapabilities, HeartbeatCapability, HeartbeatPolicy, HistoryCapability,
    ReconnectCapability, StreamHeartbeatDirection, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

pub(super) fn apply_toolchain_capabilities(capabilities: &mut ExchangeClientCapabilities) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::unsupported(
        "derive_chain_perps is a Derive Chain settlement profile; use derive for verified exchange API runtime",
    );
    capabilities.capabilities_v2.private_rest = CapabilitySupport::unsupported(
        "Derive Chain account reads require verified wallet/indexer semantics before runtime support",
    );
    capabilities.capabilities_v2.public_streams =
        CapabilitySupport::unsupported("derive_chain_perps public WS runtime is not verified");
    capabilities.capabilities_v2.private_streams =
        CapabilitySupport::unsupported("derive_chain_perps private WS runtime is not verified");
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::unsupported("derive_chain_perps.public_stream_unverified"),
        private: CapabilitySupport::unsupported("derive_chain_perps.private_stream_unverified"),
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
        BatchCapability::unsupported("derive_chain_perps.batch_place_order_signing_unverified");
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported("derive_chain_perps.batch_cancel_order_signing_unverified");
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported("derive_chain_perps.cancel_all_orders_signing_unverified");
    capabilities.capabilities_v2.order_history =
        HistoryCapability::unsupported("derive_chain_perps.order_history_unverified");
    capabilities.capabilities_v2.fills_history =
        HistoryCapability::unsupported("derive_chain_perps.fill_history_unverified");
    capabilities.capabilities_v2.credential_scopes = Vec::new();
    capabilities.capabilities_v2.endpoints = audited_endpoints();
    capabilities.apply_v2_to_legacy_flags();
}

fn audited_endpoints() -> Vec<EndpointCapability> {
    [
        (
            "get_symbol_rules",
            "/unsupported/derive_chain_perps/markets",
        ),
        (
            "get_order_book",
            "/unsupported/derive_chain_perps/orderbook",
        ),
        ("get_fees", "/unsupported/derive_chain_perps/risk"),
        ("get_positions", "/unsupported/derive_chain_perps/positions"),
        ("get_balances", "/unsupported/derive_chain_perps/balances"),
        (
            "get_open_orders",
            "/unsupported/derive_chain_perps/open-orders",
        ),
        ("query_order", "/unsupported/derive_chain_perps/order"),
        ("get_recent_fills", "/unsupported/derive_chain_perps/fills"),
        ("place_order", "/unsupported/derive_chain_perps/place-order"),
        (
            "cancel_order",
            "/unsupported/derive_chain_perps/cancel-order",
        ),
        (
            "batch_place_orders",
            "/unsupported/derive_chain_perps/batch-place",
        ),
        (
            "batch_cancel_orders",
            "/unsupported/derive_chain_perps/batch-cancel",
        ),
        (
            "cancel_all_orders",
            "/unsupported/derive_chain_perps/cancel-all",
        ),
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
        rate_limit_bucket: Some("derive_chain_perps_unverified".to_string()),
        weight: Some(0),
        supports_testnet: false,
    })
    .collect()
}
