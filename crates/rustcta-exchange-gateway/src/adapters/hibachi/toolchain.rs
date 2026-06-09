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
    capabilities.capabilities_v2.batch_place_orders = BatchCapability {
        support: CapabilitySupport::unsupported(private::BATCH_PLACE_UNSUPPORTED),
        mode: BatchExecutionMode::Native,
        atomicity: BatchAtomicity::Partial,
        max_items: None,
        same_symbol_required: false,
        same_market_type_required: true,
        supports_client_order_id: false,
        supports_partial_failure: true,
    };
    capabilities.capabilities_v2.batch_cancel_orders = BatchCapability {
        support: CapabilitySupport::unsupported(private::BATCH_CANCEL_UNSUPPORTED),
        mode: BatchExecutionMode::Native,
        atomicity: BatchAtomicity::Partial,
        max_items: None,
        same_symbol_required: false,
        same_market_type_required: true,
        supports_client_order_id: false,
        supports_partial_failure: true,
    };
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
        EndpointCapability {
            operation: "get_balances".to_string(),
            support: CapabilitySupport::unsupported(private::PRIVATE_REST_DISABLED),
            market_types: vec![MarketType::Perpetual],
            transport: EndpointTransport::Rest,
            method: Some("GET".to_string()),
            path: Some("/trade/account/info".to_string()),
            auth: EndpointAuth::ApiKey,
            credential_scopes: vec![CredentialScope::ReadOnly],
            rate_limit_bucket: Some("hibachi_private_rest".to_string()),
            weight: Some(1),
            supports_testnet: false,
        },
        EndpointCapability {
            operation: "get_positions".to_string(),
            support: CapabilitySupport::unsupported(private::PRIVATE_REST_DISABLED),
            market_types: vec![MarketType::Perpetual],
            transport: EndpointTransport::Rest,
            method: Some("GET".to_string()),
            path: Some("/trade/account/info".to_string()),
            auth: EndpointAuth::ApiKey,
            credential_scopes: vec![CredentialScope::ReadOnly],
            rate_limit_bucket: Some("hibachi_private_rest".to_string()),
            weight: Some(1),
            supports_testnet: false,
        },
        private_read_endpoint("query_order", "/trade/order"),
        private_read_endpoint("get_open_orders", "/trade/orders"),
        private_read_endpoint("get_recent_fills", "/trade/account/trades"),
    ];

    endpoints.extend([
        signed_write_endpoint(
            "place_order",
            "POST",
            "/trade/order",
            private::PLACE_ORDER_UNSUPPORTED,
        ),
        signed_write_endpoint(
            "cancel_order",
            "DELETE",
            "/trade/order",
            private::CANCEL_ORDER_UNSUPPORTED,
        ),
        signed_write_endpoint(
            "amend_order",
            "PATCH",
            "/trade/order",
            private::AMEND_ORDER_UNSUPPORTED,
        ),
        signed_write_endpoint(
            "batch_place_orders",
            "POST",
            "/trade/orders",
            private::BATCH_PLACE_UNSUPPORTED,
        ),
        signed_write_endpoint(
            "batch_cancel_orders",
            "DELETE",
            "/trade/orders",
            private::BATCH_CANCEL_UNSUPPORTED,
        ),
        signed_write_endpoint(
            "cancel_all_orders",
            "DELETE",
            "/trade/orders",
            private::CANCEL_ALL_UNSUPPORTED,
        ),
        unsupported_endpoint("place_order_list", private::ORDER_LIST_UNSUPPORTED),
    ]);
    endpoints
}

fn private_read_endpoint(operation: &str, path: &str) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support: CapabilitySupport::unsupported(private::PRIVATE_REST_DISABLED),
        market_types: vec![MarketType::Perpetual],
        transport: EndpointTransport::Rest,
        method: Some("GET".to_string()),
        path: Some(path.to_string()),
        auth: EndpointAuth::ApiKey,
        credential_scopes: vec![CredentialScope::ReadOnly],
        rate_limit_bucket: Some("hibachi_private_rest".to_string()),
        weight: Some(1),
        supports_testnet: false,
    }
}

fn signed_write_endpoint(
    operation: &str,
    method: &str,
    path: &str,
    reason: &'static str,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support: CapabilitySupport::unsupported(reason),
        market_types: vec![MarketType::Perpetual],
        transport: EndpointTransport::Rest,
        method: Some(method.to_string()),
        path: Some(path.to_string()),
        auth: EndpointAuth::Hmac,
        credential_scopes: vec![CredentialScope::Trade],
        rate_limit_bucket: Some("hibachi_private_trade".to_string()),
        weight: Some(1),
        supports_testnet: false,
    }
}

fn unsupported_endpoint(operation: &str, reason: &'static str) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support: CapabilitySupport::unsupported(reason),
        market_types: vec![MarketType::Perpetual],
        transport: EndpointTransport::Rest,
        method: None,
        path: None,
        auth: EndpointAuth::None,
        credential_scopes: Vec::new(),
        rate_limit_bucket: Some("hibachi_unsupported".to_string()),
        weight: Some(0),
        supports_testnet: false,
    }
}
