use rustcta_exchange_api::{
    BatchAtomicity, BatchCapability, BatchExecutionMode, CapabilitySupport, CredentialScope,
    EndpointAuth, EndpointCapability, EndpointTransport, ExchangeClientCapabilities,
    HeartbeatCapability, HeartbeatPolicy, HistoryCapability, ReconnectCapability,
    StreamHeartbeatDirection, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

pub(super) fn apply_toolchain_capabilities(capabilities: &mut ExchangeClientCapabilities) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::unsupported(
        "OX.FUN public REST market-data paths were not verified; WS parser fixtures are provided",
    );
    capabilities.capabilities_v2.private_rest = CapabilitySupport::unsupported(
        "OX.FUN private REST signing is fixture-only until account/trading endpoints are verified",
    );
    capabilities.capabilities_v2.public_streams = CapabilitySupport::unsupported(
        "OX.FUN public WS is spec-only until live resync is verified",
    );
    capabilities.capabilities_v2.private_streams =
        CapabilitySupport::unsupported("OX.FUN private WS auth/orders are request-spec only");
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::unsupported(
            "OX.FUN public WS runtime requires resync validation",
        ),
        private: CapabilitySupport::unsupported("OX.FUN private WS runtime is not enabled"),
        supports_subscribe: false,
        supports_unsubscribe: false,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::ClientPing,
            interval_ms: Some(20_000),
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
        support: CapabilitySupport::unsupported(
            "oxfun.batch_place_orders_ws_private_write_runtime_blocked",
        ),
        mode: BatchExecutionMode::Native,
        atomicity: BatchAtomicity::Partial,
        max_items: None,
        same_symbol_required: false,
        same_market_type_required: true,
        supports_client_order_id: true,
        supports_partial_failure: true,
    };
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported("oxfun.batch_cancel_orders_ws_request_spec_only");
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported("oxfun.cancel_all_orders_unverified");
    capabilities.capabilities_v2.order_history =
        HistoryCapability::unsupported("oxfun.open_orders_unverified");
    capabilities.capabilities_v2.fills_history =
        HistoryCapability::unsupported("oxfun.recent_fills_unverified");
    capabilities.capabilities_v2.credential_scopes = Vec::new();
    capabilities.capabilities_v2.endpoints = endpoint_capabilities();
    capabilities.apply_v2_to_legacy_flags();
}

fn endpoint_capabilities() -> Vec<EndpointCapability> {
    let mut endpoints: Vec<_> = [
        (
            "symbol_rules",
            "oxfun.symbol_rules_rest_unverified",
            EndpointAuth::None,
            true,
        ),
        (
            "order_book",
            "oxfun.order_book_ws_resync_required",
            EndpointAuth::None,
            true,
        ),
        (
            "positions",
            "oxfun.positions_unverified",
            EndpointAuth::Hmac,
            false,
        ),
        (
            "funding",
            "oxfun.funding_unverified",
            EndpointAuth::None,
            false,
        ),
        (
            "place_order",
            "oxfun.place_order_ws_request_spec_only",
            EndpointAuth::Hmac,
            true,
        ),
        (
            "cancel_order",
            "oxfun.cancel_order_ws_request_spec_only",
            EndpointAuth::Hmac,
            true,
        ),
        (
            "open_orders",
            "oxfun.open_orders_unverified",
            EndpointAuth::Hmac,
            false,
        ),
    ]
    .into_iter()
    .map(
        |(operation, reason, auth, supports_testnet)| EndpointCapability {
            operation: operation.to_string(),
            support: CapabilitySupport::unsupported(reason),
            market_types: vec![MarketType::Perpetual, MarketType::Option],
            transport: EndpointTransport::Rest,
            method: None,
            path: None,
            auth,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("oxfun_unverified".to_string()),
            weight: Some(0),
            supports_testnet,
        },
    )
    .collect();
    endpoints.extend([
        ws_trade_endpoint(
            "batch_place_orders",
            "placeorders",
            "oxfun.batch_place_orders_ws_private_write_runtime_blocked",
        ),
        ws_trade_endpoint(
            "batch_cancel_orders",
            "cancelorders",
            "oxfun.batch_cancel_orders_ws_request_spec_only",
        ),
        unsupported_endpoint("amend_order", "oxfun.amend_order_unsupported"),
        unsupported_endpoint("place_order_list", "oxfun.order_list_unsupported"),
    ]);
    endpoints
}

fn ws_trade_endpoint(operation: &str, method: &str, reason: &'static str) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support: CapabilitySupport::unsupported(reason),
        market_types: vec![MarketType::Perpetual, MarketType::Option],
        transport: EndpointTransport::WebSocket,
        method: Some(method.to_string()),
        path: Some("/v2/websocket".to_string()),
        auth: EndpointAuth::Hmac,
        credential_scopes: vec![CredentialScope::Trade],
        rate_limit_bucket: Some("oxfun_ws_orders".to_string()),
        weight: Some(1),
        supports_testnet: true,
    }
}

fn unsupported_endpoint(operation: &str, reason: &'static str) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support: CapabilitySupport::unsupported(reason),
        market_types: vec![MarketType::Perpetual, MarketType::Option],
        transport: EndpointTransport::WebSocket,
        method: None,
        path: None,
        auth: EndpointAuth::None,
        credential_scopes: Vec::new(),
        rate_limit_bucket: Some("oxfun_unsupported".to_string()),
        weight: Some(0),
        supports_testnet: false,
    }
}
