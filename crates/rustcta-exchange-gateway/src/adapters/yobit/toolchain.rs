use rustcta_exchange_api::{
    BatchCapability, CapabilitySupport, CredentialScope, EndpointAuth, EndpointCapability,
    EndpointTransport, ExchangeClientCapabilities, HeartbeatCapability, HeartbeatPolicy,
    HistoryCapability, ReconnectCapability, StreamHeartbeatDirection, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

pub(super) fn apply_toolchain_capabilities(capabilities: &mut ExchangeClientCapabilities) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.public_streams =
        CapabilitySupport::unsupported("yobit public WebSocket API was not verified");
    capabilities.capabilities_v2.private_streams =
        CapabilitySupport::unsupported("yobit private WebSocket API was not verified");
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::unsupported("yobit public WS channels are unverified"),
        private: CapabilitySupport::unsupported(
            "yobit private WS channels are unverified; use REST reconciliation after private REST promotion",
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
        BatchCapability::unsupported("yobit.batch_place_orders_unverified");
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported("yobit.batch_cancel_orders_unverified");
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported("yobit.cancel_all_orders_request_spec_only");
    capabilities.capabilities_v2.order_history = HistoryCapability {
        support: CapabilitySupport::native(),
        supports_limit: true,
        max_limit: Some(100),
        ..HistoryCapability::default()
    };
    capabilities.capabilities_v2.fills_history = HistoryCapability {
        support: CapabilitySupport::native(),
        supports_limit: true,
        max_limit: Some(100),
        ..HistoryCapability::default()
    };
    capabilities.capabilities_v2.credential_scopes =
        vec![CredentialScope::ReadOnly, CredentialScope::Trade];
    capabilities.capabilities_v2.endpoints = yobit_endpoints();
    capabilities.apply_v2_to_legacy_flags();
}

fn yobit_endpoints() -> Vec<EndpointCapability> {
    vec![
        EndpointCapability {
            operation: "get_symbol_rules".to_string(),
            support: CapabilitySupport::native(),
            market_types: vec![MarketType::Spot],
            transport: EndpointTransport::Rest,
            method: Some("GET".to_string()),
            path: Some("/api/3/info".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("yobit_public".to_string()),
            weight: Some(1),
            supports_testnet: false,
        },
        EndpointCapability {
            operation: "get_order_book".to_string(),
            support: CapabilitySupport::native(),
            market_types: vec![MarketType::Spot],
            transport: EndpointTransport::Rest,
            method: Some("GET".to_string()),
            path: Some("/api/3/depth/{pair}".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("yobit_public".to_string()),
            weight: Some(1),
            supports_testnet: false,
        },
        private_endpoint(
            "get_balances",
            "getInfo",
            CredentialScope::ReadOnly,
            CapabilitySupport::unsupported("yobit.get_balances_request_spec_only"),
        ),
        private_endpoint(
            "place_order",
            "Trade",
            CredentialScope::Trade,
            CapabilitySupport::unsupported("yobit.place_order_request_spec_only"),
        ),
        private_endpoint(
            "cancel_order",
            "CancelOrder",
            CredentialScope::Trade,
            CapabilitySupport::unsupported("yobit.cancel_order_request_spec_only"),
        ),
        private_endpoint(
            "query_order",
            "OrderInfo",
            CredentialScope::ReadOnly,
            CapabilitySupport::native(),
        ),
        private_endpoint(
            "get_open_orders",
            "ActiveOrders",
            CredentialScope::ReadOnly,
            CapabilitySupport::native(),
        ),
        private_endpoint(
            "get_recent_fills",
            "TradeHistory",
            CredentialScope::ReadOnly,
            CapabilitySupport::native(),
        ),
    ]
}

fn private_endpoint(
    operation: &str,
    path: &str,
    scope: CredentialScope,
    support: CapabilitySupport,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support,
        market_types: vec![MarketType::Spot],
        transport: EndpointTransport::Rest,
        method: Some("POST".to_string()),
        path: Some(format!("/tapi/ method={path}")),
        auth: EndpointAuth::ApiKey,
        credential_scopes: vec![scope],
        rate_limit_bucket: Some("yobit_private".to_string()),
        weight: Some(1),
        supports_testnet: false,
    }
}
