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
        CapabilitySupport::unsupported("zebpay public WebSocket API was not verified");
    capabilities.capabilities_v2.private_streams =
        CapabilitySupport::unsupported("zebpay private WebSocket API was not verified");
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::unsupported("zebpay public WS channels are unverified"),
        private: CapabilitySupport::unsupported(
            "zebpay private WS channels are unverified; use REST reconciliation after private REST promotion",
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
        BatchCapability::unsupported("zebpay.batch_place_orders_unverified");
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported("zebpay.batch_cancel_orders_unverified");
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported("zebpay.cancel_all_orders_request_spec_only");
    capabilities.capabilities_v2.order_history = HistoryCapability {
        support: CapabilitySupport::native(),
        supports_limit: true,
        max_limit: Some(500),
        ..HistoryCapability::default()
    };
    capabilities.capabilities_v2.fills_history = HistoryCapability {
        support: CapabilitySupport::native(),
        ..HistoryCapability::default()
    };
    capabilities.capabilities_v2.credential_scopes =
        vec![CredentialScope::ReadOnly, CredentialScope::Trade];
    capabilities.capabilities_v2.endpoints = zebpay_endpoints();
    capabilities.apply_v2_to_legacy_flags();
}

fn zebpay_endpoints() -> Vec<EndpointCapability> {
    vec![
        EndpointCapability {
            operation: "get_symbol_rules".to_string(),
            support: CapabilitySupport::native(),
            market_types: vec![MarketType::Spot],
            transport: EndpointTransport::Rest,
            method: Some("GET".to_string()),
            path: Some("/market?group={group}".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("zebpay_public".to_string()),
            weight: Some(1),
            supports_testnet: false,
        },
        EndpointCapability {
            operation: "get_order_book".to_string(),
            support: CapabilitySupport::native(),
            market_types: vec![MarketType::Spot],
            transport: EndpointTransport::Rest,
            method: Some("GET".to_string()),
            path: Some("/market/{trade_pair}/book?group={group}&converted=0".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("zebpay_public".to_string()),
            weight: Some(1),
            supports_testnet: false,
        },
        private_endpoint(
            "get_balances",
            "GET",
            "/wallet/balance?trade_pair={trade_pair}",
            CredentialScope::ReadOnly,
            false,
        ),
        private_endpoint(
            "place_order",
            "POST",
            "/orders",
            CredentialScope::Trade,
            false,
        ),
        private_endpoint(
            "cancel_order",
            "DELETE",
            "/orders/{order_id}",
            CredentialScope::Trade,
            false,
        ),
        private_endpoint(
            "cancel_all_orders",
            "DELETE",
            "/orders/CancelAll?trade_pair={trade_pair}",
            CredentialScope::Trade,
            false,
        ),
        private_endpoint(
            "get_open_orders",
            "GET",
            "/orders?trade_pair={trade_pair}&status=pending&orderid=0&page=1&limit=500",
            CredentialScope::ReadOnly,
            true,
        ),
        private_endpoint(
            "get_recent_fills",
            "GET",
            "/orders/{order_id}/fills",
            CredentialScope::ReadOnly,
            true,
        ),
        private_endpoint(
            "query_order",
            "GET",
            "/orders?trade_pair={trade_pair}&orderid={order_id}&page=1&limit=100",
            CredentialScope::ReadOnly,
            true,
        ),
        product_line_boundary_endpoint("contract_product"),
        product_line_boundary_endpoint("futures_product"),
    ]
}

fn private_endpoint(
    operation: &str,
    method: &str,
    path: &str,
    scope: CredentialScope,
    readback_runtime: bool,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support: if readback_runtime {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(format!("zebpay.{operation}_request_spec_only"))
        },
        market_types: vec![MarketType::Spot],
        transport: EndpointTransport::Rest,
        method: Some(method.to_string()),
        path: Some(path.to_string()),
        auth: EndpointAuth::Bearer,
        credential_scopes: vec![scope],
        rate_limit_bucket: Some("zebpay_private".to_string()),
        weight: Some(1),
        supports_testnet: false,
    }
}

fn product_line_boundary_endpoint(operation: &str) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support: CapabilitySupport::unsupported(format!(
            "zebpay.{operation}_project_unimplemented_product_line"
        )),
        market_types: vec![MarketType::Futures, MarketType::Perpetual],
        transport: EndpointTransport::Rest,
        method: None,
        path: None,
        auth: EndpointAuth::Bearer,
        credential_scopes: vec![CredentialScope::ReadOnly, CredentialScope::Trade],
        rate_limit_bucket: Some("zebpay_private".to_string()),
        weight: None,
        supports_testnet: false,
    }
}
