use rustcta_exchange_api::{
    BatchAtomicity, BatchCapability, CapabilitySupport, CredentialScope, EndpointAuth,
    EndpointCapability, EndpointTransport, ExchangeClientCapabilities, HeartbeatCapability,
    HeartbeatPolicy, HistoryCapability, ReconnectCapability, StreamHeartbeatDirection,
    StreamRuntimeCapability,
};
use rustcta_types::MarketType;

use super::private;

pub(super) fn apply_toolchain_capabilities(capabilities: &mut ExchangeClientCapabilities) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.public_streams = CapabilitySupport::unsupported(
        "WOOFi Pro public WS payloads are fixture-only until runtime resync is verified",
    );
    capabilities.capabilities_v2.private_streams =
        CapabilitySupport::unsupported("WOOFi Pro private streams require Orderly account audit");
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::unsupported(
            "WOOFi Pro public WS is payload/parser-only in this adapter",
        ),
        private: CapabilitySupport::unsupported("WOOFi Pro private WS auth is not mapped"),
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
    capabilities.capabilities_v2.batch_place_orders =
        BatchCapability::native(BatchAtomicity::Partial, Some(10));
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::native(BatchAtomicity::Partial, Some(10));
    capabilities.capabilities_v2.cancel_all_orders = CapabilitySupport::native();
    capabilities.capabilities_v2.order_history = HistoryCapability {
        support: CapabilitySupport::native(),
        max_limit: Some(500),
        ..HistoryCapability::default()
    };
    capabilities.capabilities_v2.fills_history = HistoryCapability {
        support: CapabilitySupport::native(),
        max_limit: Some(500),
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
            path: Some("/v1/public/info".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("woofipro_public".to_string()),
            weight: Some(1),
            supports_testnet: true,
        },
        signed_endpoint(
            "get_order_book",
            "GET",
            "/v1/orderbook/{symbol}",
            "woofipro_orderly_signed_read",
        ),
        signed_endpoint(
            "get_balances",
            "GET",
            "/v1/client/holding",
            "woofipro_orderly_private",
        ),
        signed_endpoint(
            "get_positions",
            "GET",
            "/v1/position/{symbol}",
            "woofipro_orderly_private",
        ),
        signed_endpoint(
            "place_order",
            "POST",
            "/v1/order",
            "woofipro_orderly_orders",
        ),
        signed_endpoint(
            "cancel_order",
            "DELETE",
            "/v1/order",
            "woofipro_orderly_orders",
        ),
        signed_endpoint("amend_order", "PUT", "/v1/order", "woofipro_orderly_orders"),
        signed_endpoint(
            "batch_place_orders",
            "POST",
            "/v1/batch-order",
            "woofipro_orderly_batch",
        ),
        signed_endpoint(
            "batch_cancel_orders",
            "DELETE",
            "/v1/batch-order",
            "woofipro_orderly_batch",
        ),
        signed_endpoint(
            "cancel_all_orders",
            "DELETE",
            "/v1/orders",
            "woofipro_orderly_orders",
        ),
        signed_endpoint(
            "query_order",
            "GET",
            "/v1/order/{order_id}",
            "woofipro_orderly_private",
        ),
        signed_endpoint(
            "get_open_orders",
            "GET",
            "/v1/orders",
            "woofipro_orderly_private",
        ),
        signed_endpoint(
            "get_recent_fills",
            "GET",
            "/v1/trades",
            "woofipro_orderly_private",
        ),
    ];

    endpoints.extend(
        [
            ("get_fees", private::FEES_UNSUPPORTED),
            ("place_order_list", private::ORDER_LIST_UNSUPPORTED),
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
            rate_limit_bucket: Some("woofipro_unsupported".to_string()),
            weight: Some(0),
            supports_testnet: false,
        }),
    );
    endpoints
}

fn signed_endpoint(operation: &str, method: &str, path: &str, bucket: &str) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support: CapabilitySupport::native(),
        market_types: vec![MarketType::Perpetual],
        transport: EndpointTransport::Rest,
        method: Some(method.to_string()),
        path: Some(path.to_string()),
        auth: EndpointAuth::ApiKey,
        credential_scopes: vec![CredentialScope::ReadOnly, CredentialScope::Trade],
        rate_limit_bucket: Some(bucket.to_string()),
        weight: Some(1),
        supports_testnet: true,
    }
}
