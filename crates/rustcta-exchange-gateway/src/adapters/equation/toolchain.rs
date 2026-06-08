use rustcta_exchange_api::{
    BatchCapability, CapabilitySupport, EndpointAuth, EndpointCapability, EndpointTransport,
    ExchangeClientCapabilities, HeartbeatCapability, HeartbeatPolicy, HistoryCapability,
    ReconnectCapability, StreamHeartbeatDirection, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

pub(super) fn apply_toolchain_capabilities(capabilities: &mut ExchangeClientCapabilities) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::unsupported(
        "Equation public market/risk API has not been verified against a stable REST contract",
    );
    capabilities.capabilities_v2.private_rest = CapabilitySupport::unsupported(
        "Equation positions/account reads require verified Arbitrum protocol API or indexer coverage",
    );
    capabilities.capabilities_v2.public_streams =
        CapabilitySupport::unsupported("Equation public WS runtime is not verified");
    capabilities.capabilities_v2.private_streams =
        CapabilitySupport::unsupported("Equation private WS runtime is not verified");
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::unsupported("equation.public_stream_unverified"),
        private: CapabilitySupport::unsupported("equation.private_stream_unverified"),
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
        BatchCapability::unsupported("equation.batch_place_order_signing_unverified");
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported("equation.batch_cancel_order_signing_unverified");
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported("equation.cancel_all_orders_signing_unverified");
    capabilities.capabilities_v2.order_history =
        HistoryCapability::unsupported("equation.order_history_unverified");
    capabilities.capabilities_v2.fills_history =
        HistoryCapability::unsupported("equation.fill_history_unverified");
    capabilities.capabilities_v2.credential_scopes = Vec::new();
    capabilities.capabilities_v2.endpoints = audited_endpoints();
    capabilities.apply_v2_to_legacy_flags();
}

fn audited_endpoints() -> Vec<EndpointCapability> {
    [
        ("get_symbol_rules", "/unsupported/equation/markets"),
        ("get_order_book", "/unsupported/equation/orderbook"),
        ("get_fees", "/unsupported/equation/risk"),
        ("get_positions", "/unsupported/equation/positions"),
        ("get_balances", "/unsupported/equation/balances"),
        ("get_open_orders", "/unsupported/equation/open-orders"),
        ("query_order", "/unsupported/equation/order"),
        ("get_recent_fills", "/unsupported/equation/fills"),
        ("place_order", "/unsupported/equation/place-order"),
        ("cancel_order", "/unsupported/equation/cancel-order"),
        ("batch_place_orders", "/unsupported/equation/batch-place"),
        ("batch_cancel_orders", "/unsupported/equation/batch-cancel"),
        ("cancel_all_orders", "/unsupported/equation/cancel-all"),
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
        rate_limit_bucket: Some("equation_unverified".to_string()),
        weight: Some(0),
        supports_testnet: false,
    })
    .collect()
}
