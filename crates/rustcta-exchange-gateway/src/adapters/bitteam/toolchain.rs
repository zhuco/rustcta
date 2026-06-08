use rustcta_exchange_api::{
    BatchCapability, CapabilitySupport, CredentialScope, EndpointAuth, EndpointCapability,
    EndpointTransport, ExchangeClientCapabilities, HeartbeatCapability, HeartbeatPolicy,
    HistoryCapability, ReconnectCapability, StreamHeartbeatDirection, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

pub(super) fn apply_toolchain_capabilities(capabilities: &mut ExchangeClientCapabilities) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = CapabilitySupport::unsupported(
        "bitteam private REST is request-spec-only until read-only and write semantics are verified",
    );
    capabilities.capabilities_v2.public_streams =
        CapabilitySupport::unsupported("bitteam public WebSocket API was not verified");
    capabilities.capabilities_v2.private_streams =
        CapabilitySupport::unsupported("bitteam private WebSocket API was not verified");
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::unsupported("bitteam public WS channels are unverified"),
        private: CapabilitySupport::unsupported(
            "bitteam private WS channels are unverified; use REST reconciliation after private REST promotion",
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
        BatchCapability::unsupported("bitteam.batch_place_orders_unverified");
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported("bitteam.batch_cancel_orders_unverified");
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported("bitteam.cancel_all_orders_request_spec_only");
    capabilities.capabilities_v2.order_history =
        HistoryCapability::unsupported("bitteam.private_order_history_request_spec_only");
    capabilities.capabilities_v2.fills_history =
        HistoryCapability::unsupported("bitteam.private_fills_history_request_spec_only");
    capabilities.capabilities_v2.credential_scopes =
        vec![CredentialScope::ReadOnly, CredentialScope::Trade];
    capabilities.capabilities_v2.endpoints = bitteam_endpoints();
    capabilities.apply_v2_to_legacy_flags();
}

fn bitteam_endpoints() -> Vec<EndpointCapability> {
    vec![
        EndpointCapability {
            operation: "get_symbol_rules".to_string(),
            support: CapabilitySupport::native(),
            market_types: vec![MarketType::Spot],
            transport: EndpointTransport::Rest,
            method: Some("GET".to_string()),
            path: Some("/trade/api/ccxt/pairs".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("bitteam_public".to_string()),
            weight: Some(1),
            supports_testnet: false,
        },
        EndpointCapability {
            operation: "get_order_book".to_string(),
            support: CapabilitySupport::native(),
            market_types: vec![MarketType::Spot],
            transport: EndpointTransport::Rest,
            method: Some("GET".to_string()),
            path: Some("/trade/api/orderbooks/{symbol}".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("bitteam_public".to_string()),
            weight: Some(1),
            supports_testnet: false,
        },
        private_endpoint(
            "get_balances",
            "/trade/api/ccxt/balance",
            "GET",
            CredentialScope::ReadOnly,
        ),
        private_endpoint(
            "place_order",
            "/trade/api/ccxt/ordercreate",
            "POST",
            CredentialScope::Trade,
        ),
        private_endpoint(
            "cancel_order",
            "/trade/api/ccxt/cancelorder",
            "POST",
            CredentialScope::Trade,
        ),
        private_endpoint(
            "get_open_orders",
            "/trade/api/ccxt/ordersOfUser",
            "GET",
            CredentialScope::ReadOnly,
        ),
        private_endpoint(
            "get_recent_fills",
            "/trade/api/ccxt/tradesOfUser",
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
        support: CapabilitySupport::unsupported(format!("bitteam.{operation}_request_spec_only")),
        market_types: vec![MarketType::Spot],
        transport: EndpointTransport::Rest,
        method: Some(method.to_string()),
        path: Some(path.to_string()),
        auth: EndpointAuth::ApiKey,
        credential_scopes: vec![scope],
        rate_limit_bucket: Some("bitteam_private".to_string()),
        weight: Some(1),
        supports_testnet: false,
    }
}
