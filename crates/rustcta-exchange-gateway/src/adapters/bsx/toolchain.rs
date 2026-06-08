use rustcta_exchange_api::{
    BatchCapability, CapabilitySupport, CredentialScope, EndpointAuth, EndpointCapability,
    EndpointTransport, ExchangeClientCapabilities, HeartbeatCapability, HeartbeatPolicy,
    HistoryCapability, ReconnectCapability, StreamHeartbeatDirection, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

use super::private;

pub(super) fn apply_toolchain_capabilities(capabilities: &mut ExchangeClientCapabilities) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = CapabilitySupport::unsupported(
        "BSX private REST requires account-scoped API headers and EIP-712 order signature audit",
    );
    capabilities.capabilities_v2.public_streams = CapabilitySupport::unsupported(
        "BSX public WS payloads are fixture-only until resync is verified",
    );
    capabilities.capabilities_v2.private_streams =
        CapabilitySupport::unsupported("BSX private WS auth lifecycle is not mapped");
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::unsupported("BSX public WS runtime is spec-only"),
        private: CapabilitySupport::unsupported("BSX private WS auth is not enabled"),
        supports_subscribe: false,
        supports_unsubscribe: false,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::ClientPing,
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
        BatchCapability::unsupported(private::BATCH_PLACE_UNSUPPORTED);
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported(private::BATCH_CANCEL_UNSUPPORTED);
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported(private::CANCEL_ALL_UNSUPPORTED);
    capabilities.capabilities_v2.order_history =
        HistoryCapability::unsupported(private::OPEN_ORDERS_UNSUPPORTED);
    capabilities.capabilities_v2.fills_history =
        HistoryCapability::unsupported(private::RECENT_FILLS_UNSUPPORTED);
    capabilities.capabilities_v2.credential_scopes = vec![CredentialScope::ReadOnly];
    capabilities.capabilities_v2.endpoints = endpoint_capabilities();
    capabilities.apply_v2_to_legacy_flags();
}

fn endpoint_capabilities() -> Vec<EndpointCapability> {
    let mut endpoints = vec![
        EndpointCapability {
            operation: "get_symbol_rules".to_string(),
            support: CapabilitySupport::native(),
            market_types: vec![MarketType::Perpetual],
            transport: EndpointTransport::Rest,
            method: Some("GET".to_string()),
            path: Some("/products".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("bsx_public".to_string()),
            weight: Some(1),
            supports_testnet: true,
        },
        EndpointCapability {
            operation: "get_order_book".to_string(),
            support: CapabilitySupport::native(),
            market_types: vec![MarketType::Perpetual],
            transport: EndpointTransport::Rest,
            method: Some("GET".to_string()),
            path: Some("/products/{product_id}/book".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("bsx_public".to_string()),
            weight: Some(1),
            supports_testnet: true,
        },
    ];

    endpoints.extend(
        [
            ("get_balances", private::BALANCES_UNSUPPORTED),
            ("get_positions", private::POSITIONS_UNSUPPORTED),
            ("get_fees", private::FEES_UNSUPPORTED),
            ("place_order", private::PLACE_ORDER_UNSUPPORTED),
            ("cancel_order", private::CANCEL_ORDER_UNSUPPORTED),
            ("batch_place_orders", private::BATCH_PLACE_UNSUPPORTED),
            ("batch_cancel_orders", private::BATCH_CANCEL_UNSUPPORTED),
            ("cancel_all_orders", private::CANCEL_ALL_UNSUPPORTED),
            ("query_order", private::QUERY_ORDER_UNSUPPORTED),
            ("get_open_orders", private::OPEN_ORDERS_UNSUPPORTED),
            ("get_recent_fills", private::RECENT_FILLS_UNSUPPORTED),
        ]
        .into_iter()
        .map(|(operation, reason)| EndpointCapability {
            operation: operation.to_string(),
            support: CapabilitySupport::unsupported(reason),
            market_types: vec![MarketType::Perpetual],
            transport: EndpointTransport::Rest,
            method: None,
            path: None,
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("bsx_unsupported".to_string()),
            weight: Some(0),
            supports_testnet: false,
        }),
    );
    endpoints
}
