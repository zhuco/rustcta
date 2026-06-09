use rustcta_exchange_api::{
    BatchAtomicity, BatchCapability, BatchExecutionMode, CapabilitySupport, CredentialScope,
    EndpointAuth, EndpointCapability, EndpointTransport, ExchangeClientCapabilities,
    HeartbeatCapability, HeartbeatPolicy, HistoryCapability, ReconnectCapability,
    StreamHeartbeatDirection, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

use super::private;

pub(super) fn apply_toolchain_capabilities(
    capabilities: &mut ExchangeClientCapabilities,
    private_rest: bool,
) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = if private_rest {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(
            "ApolloX DEX V1 private REST requires enabled_private_rest plus API key/secret",
        )
    };
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
    capabilities.capabilities_v2.batch_place_orders = BatchCapability {
        support: if private_rest {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(private::BATCH_PLACE_UNSUPPORTED)
        },
        mode: BatchExecutionMode::Native,
        atomicity: BatchAtomicity::Partial,
        max_items: Some(5),
        same_symbol_required: false,
        same_market_type_required: true,
        supports_client_order_id: true,
        supports_partial_failure: true,
    };
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported(private::BATCH_CANCEL_UNSUPPORTED);
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported(private::CANCEL_ALL_UNSUPPORTED);
    capabilities.capabilities_v2.order_history = if private_rest {
        HistoryCapability {
            support: CapabilitySupport::native(),
            supports_since: false,
            supports_until: false,
            supports_limit: false,
            supports_cursor: false,
            supports_from_id: false,
            max_limit: None,
            max_window_ms: None,
        }
    } else {
        HistoryCapability::unsupported(private::OPEN_ORDERS_UNSUPPORTED)
    };
    capabilities.capabilities_v2.fills_history = if private_rest {
        HistoryCapability {
            support: CapabilitySupport::native(),
            supports_since: true,
            supports_until: true,
            supports_limit: true,
            supports_cursor: false,
            supports_from_id: true,
            max_limit: Some(1000),
            max_window_ms: None,
        }
    } else {
        HistoryCapability::unsupported(private::RECENT_FILLS_UNSUPPORTED)
    };
    capabilities.capabilities_v2.credential_scopes = if private_rest {
        vec![CredentialScope::ReadOnly]
    } else {
        Vec::new()
    };
    capabilities.capabilities_v2.endpoints = endpoint_capabilities(private_rest);
    capabilities.apply_v2_to_legacy_flags();
}

fn endpoint_capabilities(private_rest: bool) -> Vec<EndpointCapability> {
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
                false,
            ),
            (
                "get_positions",
                private::POSITIONS_UNSUPPORTED,
                "/fapi/v2/positionRisk",
                false,
            ),
            (
                "get_fees",
                private::FEES_UNSUPPORTED,
                "/fapi/v1/commissionRate",
                false,
            ),
            (
                "place_order",
                private::PLACE_ORDER_UNSUPPORTED,
                "/fapi/v1/order",
                false,
            ),
            (
                "cancel_order",
                private::CANCEL_ORDER_UNSUPPORTED,
                "/fapi/v1/order",
                false,
            ),
            (
                "cancel_all_orders",
                private::CANCEL_ALL_UNSUPPORTED,
                "/fapi/v1/allOpenOrders",
                false,
            ),
            (
                "query_order",
                private::QUERY_ORDER_UNSUPPORTED,
                "/fapi/v1/order",
                true,
            ),
            (
                "get_open_orders",
                private::OPEN_ORDERS_UNSUPPORTED,
                "/fapi/v1/openOrders",
                true,
            ),
            (
                "get_recent_fills",
                private::RECENT_FILLS_UNSUPPORTED,
                "/fapi/v1/userTrades",
                true,
            ),
        ]
        .into_iter()
        .map(
            |(operation, reason, path, readback_runtime)| EndpointCapability {
                operation: operation.to_string(),
                support: if readback_runtime && private_rest {
                    CapabilitySupport::native()
                } else {
                    CapabilitySupport::unsupported(reason)
                },
                market_types: vec![MarketType::Perpetual],
                transport: EndpointTransport::Rest,
                method: if readback_runtime && private_rest {
                    Some("GET".to_string())
                } else {
                    None
                },
                path: Some(path.to_string()),
                auth: EndpointAuth::Hmac,
                credential_scopes: if readback_runtime && private_rest {
                    vec![CredentialScope::ReadOnly]
                } else {
                    Vec::new()
                },
                rate_limit_bucket: Some("apollox_dex_signed_uid".to_string()),
                weight: Some(1),
                supports_testnet: false,
            },
        ),
    );
    endpoints.extend([
        EndpointCapability {
            operation: "batch_place_orders".to_string(),
            support: if private_rest {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported(private::BATCH_PLACE_UNSUPPORTED)
            },
            market_types: vec![MarketType::Perpetual],
            transport: EndpointTransport::Rest,
            method: Some("POST".to_string()),
            path: Some("/fapi/v1/batchOrders".to_string()),
            auth: EndpointAuth::Hmac,
            credential_scopes: vec![CredentialScope::Trade],
            rate_limit_bucket: Some("apollox_dex_order_count".to_string()),
            weight: Some(5),
            supports_testnet: false,
        },
        EndpointCapability {
            operation: "batch_cancel_orders".to_string(),
            support: CapabilitySupport::unsupported(private::BATCH_CANCEL_UNSUPPORTED),
            market_types: vec![MarketType::Perpetual],
            transport: EndpointTransport::Rest,
            method: None,
            path: Some("/unsupported/apollox_dex/batch_cancel_orders".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: None,
            weight: None,
            supports_testnet: false,
        },
        EndpointCapability {
            operation: "amend_order".to_string(),
            support: CapabilitySupport::unsupported(private::AMEND_ORDER_UNSUPPORTED),
            market_types: vec![MarketType::Perpetual],
            transport: EndpointTransport::Rest,
            method: None,
            path: Some("/unsupported/apollox_dex/amend_order".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: None,
            weight: None,
            supports_testnet: false,
        },
        EndpointCapability {
            operation: "place_order_list".to_string(),
            support: CapabilitySupport::unsupported(private::ORDER_LIST_UNSUPPORTED),
            market_types: vec![MarketType::Perpetual],
            transport: EndpointTransport::Rest,
            method: None,
            path: Some("/unsupported/apollox_dex/order_list".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: None,
            weight: None,
            supports_testnet: false,
        },
    ]);
    endpoints
}
