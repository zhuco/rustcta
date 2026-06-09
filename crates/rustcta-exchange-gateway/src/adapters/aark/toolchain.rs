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
    capabilities.capabilities_v2.private_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.public_streams = CapabilitySupport::unsupported(
        "Aark public WS payloads are fixture-only until runtime resync is verified",
    );
    capabilities.capabilities_v2.private_streams =
        CapabilitySupport::unsupported("Aark private streams require Orderly account auth audit");
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::unsupported(
            "Aark public WS is payload/parser-only in this adapter",
        ),
        private: CapabilitySupport::unsupported("Aark private WS auth is not mapped"),
        supports_subscribe: false,
        supports_unsubscribe: false,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: false,
            direction: StreamHeartbeatDirection::None,
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
    capabilities.capabilities_v2.batch_place_orders = BatchCapability {
        support: CapabilitySupport::unsupported(private::BATCH_PLACE_UNSUPPORTED),
        mode: BatchExecutionMode::Native,
        atomicity: BatchAtomicity::Partial,
        max_items: None,
        same_symbol_required: false,
        same_market_type_required: true,
        supports_client_order_id: true,
        supports_partial_failure: true,
    };
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported(private::BATCH_CANCEL_UNSUPPORTED);
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported(private::CANCEL_ALL_UNSUPPORTED);
    capabilities.capabilities_v2.order_history = HistoryCapability {
        support: CapabilitySupport::native(),
        supports_limit: true,
        supports_cursor: true,
        max_limit: Some(500),
        ..HistoryCapability::default()
    };
    capabilities.capabilities_v2.fills_history = HistoryCapability {
        support: CapabilitySupport::native(),
        supports_since: true,
        supports_until: true,
        supports_limit: true,
        supports_cursor: true,
        max_limit: Some(500),
        ..HistoryCapability::default()
    };
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
            path: Some("/v1/public/info".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("aark_public".to_string()),
            weight: Some(1),
            supports_testnet: true,
        },
        EndpointCapability {
            operation: "get_order_book".to_string(),
            support: CapabilitySupport::unsupported(private::ORDER_BOOK_UNSUPPORTED),
            market_types: vec![MarketType::Perpetual],
            transport: EndpointTransport::Rest,
            method: Some("GET".to_string()),
            path: Some("/v1/orderbook/{symbol}".to_string()),
            auth: EndpointAuth::ApiKey,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("aark_orderly_signed_read".to_string()),
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
        ]
        .into_iter()
        .map(unsupported_endpoint),
    );
    endpoints.extend([
        signed_read_endpoint("query_order", "GET", "/v1/order/{order_id}"),
        signed_read_endpoint("get_open_orders", "GET", "/v1/orders"),
        signed_read_endpoint("get_recent_fills", "GET", "/v1/trades"),
    ]);
    endpoints.extend([
        signed_spec_endpoint(
            "amend_order",
            "PUT",
            "/v1/order",
            private::AMEND_ORDER_UNSUPPORTED,
        ),
        signed_spec_endpoint(
            "batch_place_orders",
            "POST",
            "/v1/batch-order",
            private::BATCH_PLACE_UNSUPPORTED,
        ),
        unsupported_endpoint(("place_order_list", private::ORDER_LIST_UNSUPPORTED)),
        unsupported_endpoint(("batch_cancel_orders", private::BATCH_CANCEL_UNSUPPORTED)),
        unsupported_endpoint(("cancel_all_orders", private::CANCEL_ALL_UNSUPPORTED)),
    ]);
    endpoints
}

fn signed_read_endpoint(operation: &str, method: &str, path: &str) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support: CapabilitySupport::native(),
        market_types: vec![MarketType::Perpetual],
        transport: EndpointTransport::Rest,
        method: Some(method.to_string()),
        path: Some(path.to_string()),
        auth: EndpointAuth::ApiKey,
        credential_scopes: vec![CredentialScope::ReadOnly],
        rate_limit_bucket: Some("aark_orderly_signed_read".to_string()),
        weight: Some(1),
        supports_testnet: true,
    }
}

fn signed_spec_endpoint(
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
        auth: EndpointAuth::ApiKey,
        credential_scopes: vec![CredentialScope::Trade],
        rate_limit_bucket: Some("aark_orderly_signed_write".to_string()),
        weight: Some(1),
        supports_testnet: true,
    }
}

fn unsupported_endpoint((operation, reason): (&str, &'static str)) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support: CapabilitySupport::unsupported(reason),
        market_types: vec![MarketType::Perpetual],
        transport: EndpointTransport::Rest,
        method: None,
        path: None,
        auth: EndpointAuth::None,
        credential_scopes: Vec::new(),
        rate_limit_bucket: Some("aark_unsupported".to_string()),
        weight: Some(0),
        supports_testnet: false,
    }
}
