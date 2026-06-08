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
        "ApolloX DEX V1 private REST is request-spec only until account and region boundaries are validated",
    );
    capabilities.capabilities_v2.public_streams = CapabilitySupport::unsupported(
        "ApolloX DEX public WS payloads are fixture-only until runtime resync is verified",
    );
    capabilities.capabilities_v2.private_streams =
        CapabilitySupport::unsupported("ApolloX DEX private WS listen-key runtime is not enabled");
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::unsupported(
            "ApolloX DEX public WS is payload/parser-only in this adapter",
        ),
        private: CapabilitySupport::unsupported(
            "ApolloX DEX private listen-key stream requires REST reconciliation audit",
        ),
        supports_subscribe: false,
        supports_unsubscribe: false,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::ServerPing,
            interval_ms: Some(180_000),
            timeout_ms: Some(600_000),
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
            path: Some("/fapi/v1/exchangeInfo".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("apollox_dex_public_ip".to_string()),
            weight: Some(1),
            supports_testnet: false,
        },
        EndpointCapability {
            operation: "get_order_book".to_string(),
            support: CapabilitySupport::native(),
            market_types: vec![MarketType::Perpetual],
            transport: EndpointTransport::Rest,
            method: Some("GET".to_string()),
            path: Some("/fapi/v1/depth".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("apollox_dex_public_ip".to_string()),
            weight: Some(5),
            supports_testnet: false,
        },
    ];
    endpoints.extend(
        [
            (
                "get_balances",
                private::BALANCES_UNSUPPORTED,
                "/fapi/v2/account",
            ),
            (
                "get_positions",
                private::POSITIONS_UNSUPPORTED,
                "/fapi/v2/positionRisk",
            ),
            (
                "get_fees",
                private::FEES_UNSUPPORTED,
                "/fapi/v1/commissionRate",
            ),
            (
                "place_order",
                private::PLACE_ORDER_UNSUPPORTED,
                "/fapi/v1/order",
            ),
            (
                "cancel_order",
                private::CANCEL_ORDER_UNSUPPORTED,
                "/fapi/v1/order",
            ),
            (
                "cancel_all_orders",
                private::CANCEL_ALL_UNSUPPORTED,
                "/fapi/v1/allOpenOrders",
            ),
            (
                "query_order",
                private::QUERY_ORDER_UNSUPPORTED,
                "/fapi/v1/order",
            ),
            (
                "get_open_orders",
                private::OPEN_ORDERS_UNSUPPORTED,
                "/fapi/v1/openOrders",
            ),
            (
                "get_recent_fills",
                private::RECENT_FILLS_UNSUPPORTED,
                "/fapi/v1/userTrades",
            ),
        ]
        .into_iter()
        .map(|(operation, reason, path)| EndpointCapability {
            operation: operation.to_string(),
            support: CapabilitySupport::unsupported(reason),
            market_types: vec![MarketType::Perpetual],
            transport: EndpointTransport::Rest,
            method: None,
            path: Some(path.to_string()),
            auth: EndpointAuth::Hmac,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("apollox_dex_signed_uid".to_string()),
            weight: Some(1),
            supports_testnet: false,
        }),
    );
    endpoints
}
