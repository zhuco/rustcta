use rustcta_exchange_api::{
    BatchAtomicity, BatchCapability, BatchExecutionMode, CapabilitySupport, CredentialScope,
    EndpointAuth, EndpointCapability, EndpointTransport, ExchangeClientCapabilities,
    HeartbeatCapability, HeartbeatPolicy, HistoryCapability, ReconnectCapability,
    StreamHeartbeatDirection, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

pub(super) fn apply_toolchain_capabilities(
    capabilities: &mut ExchangeClientCapabilities,
    private_rest: bool,
) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = if private_rest {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(
            "LATOKEN private REST requires enabled_private_rest plus API key/secret",
        )
    };
    capabilities.capabilities_v2.public_streams = CapabilitySupport::native();
    capabilities.capabilities_v2.private_streams = CapabilitySupport::unsupported(
        "LATOKEN private WS requires user-id channels and API-key STOMP auth; runtime is not verified",
    );
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::native(),
        private: CapabilitySupport::unsupported(
            "LATOKEN private STOMP runtime not implemented; use REST reconciliation after private REST promotion",
        ),
        supports_subscribe: true,
        supports_unsubscribe: true,
        supports_public_subscribe: true,
        supports_public_unsubscribe: true,
        heartbeat: HeartbeatCapability {
            supported: false,
            required: false,
            direction: StreamHeartbeatDirection::None,
            interval_ms: None,
            timeout_ms: None,
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
    capabilities
        .capabilities_v2
        .stream_runtime
        .resync
        .order_book = true;
    capabilities.capabilities_v2.batch_place_orders = BatchCapability {
        support: if private_rest {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "latoken /v2/auth/order/placeBulk requires enabled private REST credentials",
            )
        },
        mode: BatchExecutionMode::Native,
        atomicity: BatchAtomicity::Partial,
        max_items: Some(50),
        same_symbol_required: false,
        same_market_type_required: true,
        supports_client_order_id: true,
        supports_partial_failure: true,
    };
    capabilities.capabilities_v2.batch_cancel_orders = BatchCapability {
        support: if private_rest {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "latoken /v2/auth/order/cancelBulk requires enabled private REST credentials",
            )
        },
        mode: BatchExecutionMode::Native,
        atomicity: BatchAtomicity::Partial,
        max_items: Some(50),
        same_symbol_required: false,
        same_market_type_required: true,
        supports_client_order_id: false,
        supports_partial_failure: true,
    };
    capabilities.capabilities_v2.cancel_all_orders = if private_rest {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(
            "latoken /v2/auth/order/cancelAll requires enabled private REST credentials",
        )
    };
    capabilities.capabilities_v2.order_history = if private_rest {
        HistoryCapability {
            support: CapabilitySupport::native(),
            supports_since: false,
            supports_until: false,
            supports_limit: true,
            supports_cursor: false,
            supports_from_id: false,
            max_limit: Some(1000),
            max_window_ms: None,
        }
    } else {
        HistoryCapability::unsupported(
            "latoken private order history requires enabled private REST credentials",
        )
    };
    capabilities.capabilities_v2.fills_history = if private_rest {
        HistoryCapability {
            support: CapabilitySupport::native(),
            supports_since: false,
            supports_until: false,
            supports_limit: true,
            supports_cursor: false,
            supports_from_id: false,
            max_limit: Some(1000),
            max_window_ms: None,
        }
    } else {
        HistoryCapability::unsupported(
            "latoken private fills history requires enabled private REST credentials",
        )
    };
    capabilities.capabilities_v2.credential_scopes =
        vec![CredentialScope::ReadOnly, CredentialScope::Trade];
    capabilities.capabilities_v2.endpoints = latoken_endpoints(private_rest);
    capabilities.apply_v2_to_legacy_flags();
}

fn latoken_endpoints(private_rest: bool) -> Vec<EndpointCapability> {
    vec![
        EndpointCapability {
            operation: "subscribe_public_order_book".to_string(),
            support: CapabilitySupport::native(),
            market_types: vec![MarketType::Spot],
            transport: EndpointTransport::WebSocket,
            method: Some("SUBSCRIBE".to_string()),
            path: Some("/v1/book/{base}/{quote}".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: None,
            weight: None,
            supports_testnet: false,
        },
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
            private_rest,
        ),
        private_endpoint(
            "place_order",
            "/v2/auth/order/place",
            "POST",
            CredentialScope::Trade,
            private_rest,
        ),
        private_endpoint(
            "cancel_order",
            "/v2/auth/order/cancel",
            "POST",
            CredentialScope::Trade,
            private_rest,
        ),
        private_endpoint(
            "cancel_all_orders",
            "/v2/auth/order/cancelAll",
            "POST",
            CredentialScope::Trade,
            private_rest,
        ),
        private_endpoint(
            "batch_place_orders",
            "/v2/auth/order/placeBulk",
            "POST",
            CredentialScope::Trade,
            private_rest,
        ),
        private_endpoint(
            "batch_cancel_orders",
            "/v2/auth/order/cancelBulk",
            "POST",
            CredentialScope::Trade,
            private_rest,
        ),
        private_endpoint(
            "get_open_orders",
            "/v2/auth/order/active",
            "GET",
            CredentialScope::ReadOnly,
            private_rest,
        ),
        private_endpoint(
            "get_recent_fills",
            "/v2/auth/trade",
            "GET",
            CredentialScope::ReadOnly,
            private_rest,
        ),
        private_endpoint(
            "query_order",
            "/v2/auth/order/getOrder/{id}",
            "GET",
            CredentialScope::ReadOnly,
            private_rest,
        ),
        product_line_boundary_endpoint("contract_product"),
        product_line_boundary_endpoint("futures_product"),
    ]
}

fn private_endpoint(
    operation: &str,
    path: &str,
    method: &str,
    scope: CredentialScope,
    native_batch_runtime: bool,
) -> EndpointCapability {
    let support = if native_batch_runtime {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(format!("latoken.{operation}_request_spec_only"))
    };
    EndpointCapability {
        operation: operation.to_string(),
        support,
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

fn product_line_boundary_endpoint(operation: &str) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support: CapabilitySupport::unsupported(format!(
            "latoken.{operation}_project_unimplemented_product_line"
        )),
        market_types: vec![MarketType::Futures, MarketType::Perpetual],
        transport: EndpointTransport::Rest,
        method: None,
        path: None,
        auth: EndpointAuth::Hmac,
        credential_scopes: vec![CredentialScope::ReadOnly, CredentialScope::Trade],
        rate_limit_bucket: Some("latoken_private".to_string()),
        weight: None,
        supports_testnet: false,
    }
}
