use rustcta_exchange_api::{
    BatchCapability, CapabilitySupport, CredentialScope, EndpointAuth, EndpointCapability,
    EndpointTransport, ExchangeClientCapabilities, HeartbeatCapability, HeartbeatPolicy,
    HistoryCapability, ReconnectCapability, StreamHeartbeatDirection, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

pub(super) fn apply_toolchain_capabilities(capabilities: &mut ExchangeClientCapabilities) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = CapabilitySupport::unsupported(
        "LATOKEN private REST is request-spec-only until signing vectors and read/write semantics are promoted by a separate validation task",
    );
    capabilities.capabilities_v2.public_streams = CapabilitySupport::unsupported(
        "LATOKEN STOMP public WS is documented, but this adapter only delivers payload specs and parser fixtures",
    );
    capabilities.capabilities_v2.private_streams = CapabilitySupport::unsupported(
        "LATOKEN private WS requires user-id channels and API-key STOMP auth; runtime is not verified",
    );
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::unsupported("LATOKEN public STOMP runtime not implemented"),
        private: CapabilitySupport::unsupported(
            "LATOKEN private STOMP runtime not implemented; use REST reconciliation after private REST promotion",
        ),
        supports_subscribe: false,
        supports_unsubscribe: false,
        heartbeat: HeartbeatCapability {
            supported: false,
            required: false,
            direction: StreamHeartbeatDirection::None,
            interval_ms: None,
            timeout_ms: None,
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
        BatchCapability::unsupported("latoken.batch_place_orders_request_spec_only");
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported("latoken.batch_cancel_orders_request_spec_only");
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported("latoken.cancel_all_orders_request_spec_only");
    capabilities.capabilities_v2.order_history =
        HistoryCapability::unsupported("latoken.private_order_history_request_spec_only");
    capabilities.capabilities_v2.fills_history =
        HistoryCapability::unsupported("latoken.private_fills_history_request_spec_only");
    capabilities.capabilities_v2.credential_scopes =
        vec![CredentialScope::ReadOnly, CredentialScope::Trade];
    capabilities.capabilities_v2.endpoints = latoken_endpoints();
    capabilities.apply_v2_to_legacy_flags();
}

fn latoken_endpoints() -> Vec<EndpointCapability> {
    vec![
        EndpointCapability {
            operation: "get_symbol_rules".to_string(),
            support: CapabilitySupport::native(),
            market_types: vec![MarketType::Spot],
            transport: EndpointTransport::Rest,
            method: Some("GET".to_string()),
            path: Some("/v2/pair + /v2/currency".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("latoken_public".to_string()),
            weight: Some(2),
            supports_testnet: false,
        },
        EndpointCapability {
            operation: "get_order_book".to_string(),
            support: CapabilitySupport::native(),
            market_types: vec![MarketType::Spot],
            transport: EndpointTransport::Rest,
            method: Some("GET".to_string()),
            path: Some("/v2/book/{currency}/{quote}".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("latoken_public".to_string()),
            weight: Some(1),
            supports_testnet: false,
        },
        private_endpoint(
            "get_balances",
            "/v2/auth/account",
            "GET",
            CredentialScope::ReadOnly,
        ),
        private_endpoint(
            "place_order",
            "/v2/auth/order/place",
            "POST",
            CredentialScope::Trade,
        ),
        private_endpoint(
            "cancel_order",
            "/v2/auth/order/cancel",
            "POST",
            CredentialScope::Trade,
        ),
        private_endpoint(
            "cancel_all_orders",
            "/v2/auth/order/cancelAll",
            "POST",
            CredentialScope::Trade,
        ),
        private_endpoint(
            "get_open_orders",
            "/v2/auth/order/active",
            "GET",
            CredentialScope::ReadOnly,
        ),
        private_endpoint(
            "get_recent_fills",
            "/v2/auth/trade",
            "GET",
            CredentialScope::ReadOnly,
        ),
    ]
}

fn private_endpoint(
    operation: &str,
    path: &str,
    method: &str,
    scope: CredentialScope,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support: CapabilitySupport::unsupported(format!("latoken.{operation}_request_spec_only")),
        market_types: vec![MarketType::Spot],
        transport: EndpointTransport::Rest,
        method: Some(method.to_string()),
        path: Some(path.to_string()),
        auth: EndpointAuth::Hmac,
        credential_scopes: vec![scope],
        rate_limit_bucket: Some("latoken_private".to_string()),
        weight: Some(1),
        supports_testnet: false,
    }
}
