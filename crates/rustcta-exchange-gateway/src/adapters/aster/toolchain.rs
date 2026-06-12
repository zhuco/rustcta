use rustcta_exchange_api::{
    BatchAtomicity, BatchCapability, CapabilitySupport, CredentialScope, EndpointAuth,
    EndpointCapability, EndpointTransport, ExchangeClientCapabilities, HeartbeatCapability,
    HistoryCapability, ReconnectCapability, StreamAuthCapability, StreamHeartbeatDirection,
    StreamResyncCapability, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

pub(super) fn apply_toolchain_capabilities(
    capabilities: &mut ExchangeClientCapabilities,
    private_rest_enabled: bool,
    private_streams_enabled: bool,
) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = if private_rest_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(
            "Aster private REST requires V3 user address, signer address, and signer private key",
        )
    };
    capabilities.capabilities_v2.public_streams = CapabilitySupport::native();
    capabilities.capabilities_v2.private_streams = if private_streams_enabled {
        CapabilitySupport::native()
    } else if private_rest_enabled {
        CapabilitySupport::unsupported("Aster private WS is disabled by configuration")
    } else {
        CapabilitySupport::unsupported("Aster private WS requires V3 API wallet credentials")
    };
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::native(),
        private: capabilities.capabilities_v2.private_streams.clone(),
        supports_subscribe: true,
        supports_unsubscribe: true,
        supports_public_subscribe: true,
        supports_public_unsubscribe: true,
        supports_private_subscribe: private_streams_enabled,
        supports_private_unsubscribe: private_streams_enabled,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::ClientPing,
            interval_ms: Some(30_000),
            timeout_ms: Some(10_000),
        },
        reconnect: ReconnectCapability {
            supported: true,
            requires_resubscribe: true,
            preserves_session: false,
            max_reconnect_attempts: None,
        },
        resync: StreamResyncCapability {
            order_book: true,
            balances: private_streams_enabled,
            positions: private_streams_enabled,
            orders: private_streams_enabled,
        },
        auth: StreamAuthCapability {
            required: private_streams_enabled,
            credential_scopes: vec![CredentialScope::ReadOnly, CredentialScope::Trade],
            renewal_ms: Some(30 * 60 * 1_000),
            uses_listen_key: true,
            requires_relogin_on_reconnect: true,
        },
        ..StreamRuntimeCapability::default()
    };
    capabilities.capabilities_v2.funding_rates = CapabilitySupport::native();
    capabilities.capabilities_v2.batch_place_orders = if private_rest_enabled {
        BatchCapability {
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: true,
            ..BatchCapability::native(BatchAtomicity::Partial, Some(5))
        }
    } else {
        BatchCapability::unsupported("Aster batch place order requires private REST credentials")
    };
    capabilities.capabilities_v2.batch_cancel_orders = if private_rest_enabled {
        BatchCapability {
            same_symbol_required: true,
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: true,
            ..BatchCapability::native(BatchAtomicity::Partial, Some(10))
        }
    } else {
        BatchCapability::unsupported("Aster batch cancel order requires private REST credentials")
    };
    capabilities.capabilities_v2.cancel_all_orders = if private_rest_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("Aster cancel-all requires private REST credentials")
    };
    capabilities.capabilities_v2.order_history = HistoryCapability {
        support: if private_rest_enabled {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("Aster order history requires private REST credentials")
        },
        supports_since: false,
        supports_until: false,
        supports_limit: false,
        supports_cursor: false,
        supports_from_id: false,
        max_limit: None,
        max_window_ms: None,
    };
    capabilities.capabilities_v2.fills_history = HistoryCapability {
        support: if private_rest_enabled {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("Aster userTrades requires private REST credentials")
        },
        supports_since: true,
        supports_until: true,
        supports_limit: true,
        supports_cursor: false,
        supports_from_id: true,
        max_limit: Some(1000),
        max_window_ms: None,
    };
    capabilities.capabilities_v2.credential_scopes = if private_rest_enabled {
        vec![CredentialScope::ReadOnly, CredentialScope::Trade]
    } else {
        Vec::new()
    };
    capabilities.capabilities_v2.endpoints = endpoint_capabilities(private_rest_enabled);
    capabilities.apply_v2_to_legacy_flags();
}

fn endpoint_capabilities(private_rest_enabled: bool) -> Vec<EndpointCapability> {
    let mut endpoints = vec![
        rest_endpoint(
            "get_symbol_rules",
            CapabilitySupport::native(),
            "GET",
            "/fapi/v3/exchangeInfo",
            EndpointAuth::None,
            Vec::new(),
            Some("public_ip"),
            Some(10),
        ),
        rest_endpoint(
            "get_order_book",
            CapabilitySupport::native(),
            "GET",
            "/fapi/v3/depth",
            EndpointAuth::None,
            Vec::new(),
            Some("public_ip"),
            Some(5),
        ),
        rest_endpoint(
            "get_funding_rates",
            CapabilitySupport::native(),
            "GET",
            "/fapi/v3/premiumIndex",
            EndpointAuth::None,
            Vec::new(),
            Some("public_ip"),
            Some(1),
        ),
        rest_endpoint(
            "get_funding_rates_history",
            CapabilitySupport::native(),
            "GET",
            "/fapi/v3/fundingRate",
            EndpointAuth::None,
            Vec::new(),
            Some("public_ip"),
            Some(1),
        ),
        ws_endpoint(
            "subscribe_public_stream",
            CapabilitySupport::native(),
            EndpointAuth::None,
            Vec::new(),
        ),
    ];

    let private_support = if private_rest_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("private REST credentials unavailable")
    };
    for (operation, method, path, credential_scope, weight) in [
        (
            "get_balances",
            "GET",
            "/fapi/v3/balance",
            CredentialScope::ReadOnly,
            20,
        ),
        (
            "get_positions",
            "GET",
            "/fapi/v3/positionRisk",
            CredentialScope::ReadOnly,
            20,
        ),
        (
            "get_fees",
            "GET",
            "/fapi/v3/commissionRate",
            CredentialScope::ReadOnly,
            20,
        ),
        (
            "place_order",
            "POST",
            "/fapi/v3/order",
            CredentialScope::Trade,
            1,
        ),
        (
            "amend_order",
            "PUT",
            "/fapi/v3/order",
            CredentialScope::Trade,
            1,
        ),
        (
            "batch_place_orders",
            "POST",
            "/fapi/v3/batchOrders",
            CredentialScope::Trade,
            5,
        ),
        (
            "batch_cancel_orders",
            "DELETE",
            "/fapi/v3/batchOrders",
            CredentialScope::Trade,
            1,
        ),
        (
            "cancel_order",
            "DELETE",
            "/fapi/v3/order",
            CredentialScope::Trade,
            1,
        ),
        (
            "cancel_all_orders",
            "DELETE",
            "/fapi/v3/allOpenOrders",
            CredentialScope::Trade,
            1,
        ),
        (
            "query_order",
            "GET",
            "/fapi/v3/order",
            CredentialScope::ReadOnly,
            4,
        ),
        (
            "get_open_orders",
            "GET",
            "/fapi/v3/openOrders",
            CredentialScope::ReadOnly,
            6,
        ),
        (
            "get_recent_fills",
            "GET",
            "/fapi/v3/userTrades",
            CredentialScope::ReadOnly,
            20,
        ),
        (
            "set_leverage",
            "POST",
            "/fapi/v3/leverage",
            CredentialScope::Trade,
            1,
        ),
        (
            "get_position_mode",
            "GET",
            "/fapi/v3/positionSide/dual",
            CredentialScope::ReadOnly,
            30,
        ),
        (
            "set_position_mode",
            "POST",
            "/fapi/v3/positionSide/dual",
            CredentialScope::Trade,
            1,
        ),
    ] {
        endpoints.push(rest_endpoint(
            operation,
            private_support.clone(),
            method,
            path,
            EndpointAuth::Bearer,
            vec![credential_scope],
            Some("signed_uid"),
            Some(weight),
        ));
    }
    endpoints.push(ws_endpoint(
        "subscribe_private_stream",
        private_support,
        EndpointAuth::Bearer,
        vec![CredentialScope::ReadOnly, CredentialScope::Trade],
    ));

    endpoints
}

fn rest_endpoint(
    operation: &str,
    support: CapabilitySupport,
    method: &str,
    path: &str,
    auth: EndpointAuth,
    credential_scopes: Vec<CredentialScope>,
    rate_limit_bucket: Option<&str>,
    weight: Option<u32>,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support,
        market_types: vec![MarketType::Perpetual],
        transport: EndpointTransport::Rest,
        method: Some(method.to_string()),
        path: Some(path.to_string()),
        auth,
        credential_scopes,
        rate_limit_bucket: rate_limit_bucket.map(str::to_string),
        weight,
        supports_testnet: true,
    }
}

fn ws_endpoint(
    operation: &str,
    support: CapabilitySupport,
    auth: EndpointAuth,
    credential_scopes: Vec<CredentialScope>,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support,
        market_types: vec![MarketType::Perpetual],
        transport: EndpointTransport::WebSocket,
        method: None,
        path: None,
        auth,
        credential_scopes,
        rate_limit_bucket: None,
        weight: None,
        supports_testnet: true,
    }
}
