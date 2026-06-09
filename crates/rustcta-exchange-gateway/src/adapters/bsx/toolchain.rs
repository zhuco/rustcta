use rustcta_exchange_api::{
    BatchAtomicity, BatchCapability, BatchExecutionMode, CapabilitySupport, CredentialScope,
    EndpointAuth, EndpointCapability, EndpointTransport, ExchangeClientCapabilities,
    HeartbeatCapability, HeartbeatPolicy, HistoryCapability, ReconnectCapability,
    StreamHeartbeatDirection, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

use super::private;

pub(super) fn apply_toolchain_capabilities(capabilities: &mut ExchangeClientCapabilities) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest =
        CapabilitySupport::unsupported(private::PRIVATE_REST_DISABLED);
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
    capabilities.capabilities_v2.batch_cancel_orders = BatchCapability {
        support: CapabilitySupport::unsupported(private::BATCH_CANCEL_UNSUPPORTED),
        mode: BatchExecutionMode::Native,
        atomicity: BatchAtomicity::Partial,
        max_items: None,
        same_symbol_required: false,
        same_market_type_required: true,
        supports_client_order_id: true,
        supports_partial_failure: true,
    };
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported(private::CANCEL_ALL_UNSUPPORTED);
    capabilities.capabilities_v2.order_history = HistoryCapability {
        support: CapabilitySupport::native(),
        supports_limit: true,
        ..HistoryCapability::default()
    };
    capabilities.capabilities_v2.fills_history = HistoryCapability {
        support: CapabilitySupport::native(),
        supports_limit: true,
        ..HistoryCapability::default()
    };
    capabilities.capabilities_v2.credential_scopes =
        vec![CredentialScope::ReadOnly, CredentialScope::Trade];
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
            ("place_order_list", private::ORDER_LIST_UNSUPPORTED),
            ("batch_place_orders", private::BATCH_PLACE_UNSUPPORTED),
            ("cancel_all_orders", private::CANCEL_ALL_UNSUPPORTED),
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
    endpoints.extend(
        [
            ("query_order", "GET", "/orders/{order_id}"),
            ("get_open_orders", "GET", "/orders"),
            ("get_recent_fills", "GET", "/trades"),
        ]
        .into_iter()
        .map(|(operation, method, path)| EndpointCapability {
            operation: operation.to_string(),
            support: CapabilitySupport::native(),
            market_types: vec![MarketType::Perpetual],
            transport: EndpointTransport::Rest,
            method: Some(method.to_string()),
            path: Some(path.to_string()),
            auth: EndpointAuth::Hmac,
            credential_scopes: vec![CredentialScope::ReadOnly],
            rate_limit_bucket: Some("bsx_private".to_string()),
            weight: Some(1),
            supports_testnet: true,
        }),
    );
    endpoints.push(EndpointCapability {
        operation: "amend_order".to_string(),
        support: CapabilitySupport::unsupported(private::AMEND_ORDER_UNSUPPORTED),
        market_types: vec![MarketType::Perpetual],
        transport: EndpointTransport::Rest,
        method: Some("PATCH".to_string()),
        path: Some("/source/bsx/amend-order-boundary".to_string()),
        auth: EndpointAuth::Hmac,
        credential_scopes: vec![CredentialScope::Trade],
        rate_limit_bucket: Some("bsx_private".to_string()),
        weight: Some(0),
        supports_testnet: true,
    });
    endpoints.push(EndpointCapability {
        operation: "batch_cancel_orders".to_string(),
        support: CapabilitySupport::unsupported(private::BATCH_CANCEL_UNSUPPORTED),
        market_types: vec![MarketType::Perpetual],
        transport: EndpointTransport::Rest,
        method: Some("DELETE".to_string()),
        path: Some("/source/bsx/batch-cancel-orders-boundary".to_string()),
        auth: EndpointAuth::Hmac,
        credential_scopes: vec![CredentialScope::Trade],
        rate_limit_bucket: Some("bsx_private".to_string()),
        weight: Some(0),
        supports_testnet: true,
    });
    endpoints
}
