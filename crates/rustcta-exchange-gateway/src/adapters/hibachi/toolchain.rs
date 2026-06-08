use rustcta_exchange_api::{
    BatchCapability, CapabilitySupport, EndpointAuth, EndpointCapability, EndpointTransport,
    ExchangeClientCapabilities, HeartbeatCapability, HeartbeatPolicy, HistoryCapability,
    ReconnectCapability, StreamHeartbeatDirection, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

use super::private;

pub(super) fn apply_toolchain_capabilities(capabilities: &mut ExchangeClientCapabilities) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = CapabilitySupport::unsupported(
        "Hibachi private REST is audited but runtime remains disabled by default",
    );
    capabilities.capabilities_v2.public_streams =
        CapabilitySupport::unsupported("Hibachi public WS is payload/parser-only");
    capabilities.capabilities_v2.private_streams =
        CapabilitySupport::unsupported("Hibachi private WS requires account-scoped API key auth");
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::unsupported("hibachi.public_stream_runtime_spec_only"),
        private: CapabilitySupport::unsupported("hibachi.private_stream_runtime_spec_only"),
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
            supported: true,
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
        HistoryCapability::unsupported(private::PRIVATE_REST_DISABLED);
    capabilities.capabilities_v2.fills_history =
        HistoryCapability::unsupported(private::PRIVATE_REST_DISABLED);
    capabilities.capabilities_v2.credential_scopes = Vec::new();
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
            path: Some("/market/exchange-info".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("hibachi_public_rest".to_string()),
            weight: Some(1),
            supports_testnet: false,
        },
        EndpointCapability {
            operation: "get_order_book".to_string(),
            support: CapabilitySupport::native(),
            market_types: vec![MarketType::Perpetual],
            transport: EndpointTransport::Rest,
            method: Some("GET".to_string()),
            path: Some("/market/data/orderbook".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("hibachi_public_rest".to_string()),
            weight: Some(1),
            supports_testnet: false,
        },
        EndpointCapability {
            operation: "get_fees".to_string(),
            support: CapabilitySupport::native(),
            market_types: vec![MarketType::Perpetual],
            transport: EndpointTransport::Rest,
            method: Some("GET".to_string()),
            path: Some("/market/exchange-info".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("hibachi_public_rest".to_string()),
            weight: Some(1),
            supports_testnet: false,
        },
    ];

    endpoints.extend(
        [
            ("place_order", private::PLACE_ORDER_UNSUPPORTED),
            ("cancel_order", private::CANCEL_ORDER_UNSUPPORTED),
            ("amend_order", private::AMEND_ORDER_UNSUPPORTED),
            ("batch_place_orders", private::BATCH_PLACE_UNSUPPORTED),
            ("batch_cancel_orders", private::BATCH_CANCEL_UNSUPPORTED),
            ("cancel_all_orders", private::CANCEL_ALL_UNSUPPORTED),
        ]
        .into_iter()
        .map(|(operation, reason)| EndpointCapability {
            operation: operation.to_string(),
            support: CapabilitySupport::unsupported(reason),
            market_types: vec![MarketType::Perpetual],
            transport: EndpointTransport::Rest,
            method: Some("POST".to_string()),
            path: Some("/trade/order".to_string()),
            auth: EndpointAuth::Hmac,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("hibachi_private_trade".to_string()),
            weight: Some(1),
            supports_testnet: false,
        }),
    );
    endpoints
}
