use rustcta_exchange_api::{
    BatchAtomicity, BatchCapability, BatchExecutionMode, CapabilitySupport, CredentialScope,
    EndpointAuth, EndpointCapability, EndpointTransport, ExchangeClientCapabilities,
    HeartbeatCapability, HistoryCapability, ReconnectCapability, StreamHeartbeatDirection,
    StreamResyncCapability, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

pub(super) fn apply_toolchain_capabilities(
    capabilities: &mut ExchangeClientCapabilities,
    private_rest_enabled: bool,
) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = if private_rest_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("CoinW private REST requires enabled API key and secret")
    };
    capabilities.capabilities_v2.public_streams = CapabilitySupport::native();
    capabilities.capabilities_v2.private_streams = if private_rest_enabled {
        CapabilitySupport::rest_fallback(
            "CoinW private WS request specs and parsers are present; REST order/balance/position/fill reconciliation remains the source of truth before live validation",
        )
    } else {
        CapabilitySupport::unsupported("CoinW private streams require private credentials")
    };
    capabilities.capabilities_v2.stream_runtime = stream_runtime_capability(private_rest_enabled);
    capabilities.capabilities_v2.batch_place_orders = if private_rest_enabled {
        BatchCapability {
            support: CapabilitySupport::composed(
                "CoinW has no uniform native batch-place endpoint; gateway sends single orders sequentially",
            ),
            mode: BatchExecutionMode::ComposedSequential,
            atomicity: BatchAtomicity::NonAtomic,
            max_items: Some(20),
            same_symbol_required: false,
            same_market_type_required: false,
            supports_client_order_id: true,
            supports_partial_failure: true,
        }
    } else {
        BatchCapability::unsupported("CoinW batch place requires private REST credentials")
    };
    capabilities.capabilities_v2.batch_cancel_orders = if private_rest_enabled {
        BatchCapability {
            support: CapabilitySupport::composed(
                "Spot cancel is sequential; perpetual cancel uses native /v1/perpum/batchOrders per position type",
            ),
            mode: BatchExecutionMode::ComposedSequential,
            atomicity: BatchAtomicity::Partial,
            max_items: Some(20),
            same_symbol_required: false,
            same_market_type_required: false,
            supports_client_order_id: false,
            supports_partial_failure: true,
        }
    } else {
        BatchCapability::unsupported("CoinW batch cancel requires private REST credentials")
    };
    capabilities.capabilities_v2.cancel_all_orders = if private_rest_enabled {
        CapabilitySupport::composed(
            "Spot uses cancelAllOrder; perpetual loads open orders then calls batchOrders",
        )
    } else {
        CapabilitySupport::unsupported("CoinW cancel-all requires private REST credentials")
    };
    capabilities.capabilities_v2.order_history = HistoryCapability {
        support: if private_rest_enabled {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "CoinW order reconciliation requires private REST credentials",
            )
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
            CapabilitySupport::unsupported(
                "CoinW fills reconciliation requires private REST credentials",
            )
        },
        supports_since: false,
        supports_until: false,
        supports_limit: true,
        supports_cursor: false,
        supports_from_id: false,
        max_limit: Some(100),
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

fn stream_runtime_capability(private_rest_enabled: bool) -> StreamRuntimeCapability {
    StreamRuntimeCapability {
        public: CapabilitySupport::native(),
        private: if private_rest_enabled {
            CapabilitySupport::rest_fallback(
                "CoinW private WS parser is adapter-local; REST reconciliation is required after reconnect and before live-dry-run",
            )
        } else {
            CapabilitySupport::unsupported("CoinW private WS requires credentials")
        },
        supports_subscribe: true,
        supports_unsubscribe: false,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::Bidirectional,
            interval_ms: Some(15_000),
            timeout_ms: Some(30_000),
        },
        reconnect: ReconnectCapability {
            supported: true,
            requires_resubscribe: true,
            preserves_session: false,
            max_reconnect_attempts: None,
        },
        resync: StreamResyncCapability {
            order_book: true,
            balances: true,
            positions: true,
            orders: true,
        },
        auth: rustcta_exchange_api::StreamAuthCapability {
            required: private_rest_enabled,
            credential_scopes: if private_rest_enabled {
                vec![CredentialScope::ReadOnly, CredentialScope::Trade]
            } else {
                Vec::new()
            },
            renewal_ms: None,
            uses_listen_key: false,
            requires_relogin_on_reconnect: true,
        },
        ..StreamRuntimeCapability::default()
    }
}

fn endpoint_capabilities(private_rest_enabled: bool) -> Vec<EndpointCapability> {
    let mut endpoints = vec![
        rest_endpoint(
            "get_symbol_rules",
            CapabilitySupport::native(),
            vec![MarketType::Spot],
            "GET",
            "/api/v1/public?command=returnSymbol",
            EndpointAuth::None,
            Vec::new(),
            "coinw_spot_public",
            1,
        ),
        rest_endpoint(
            "get_order_book",
            CapabilitySupport::native(),
            vec![MarketType::Spot],
            "GET",
            "/api/v1/public?command=returnOrderBook",
            EndpointAuth::None,
            Vec::new(),
            "coinw_spot_public",
            1,
        ),
        rest_endpoint(
            "get_symbol_rules",
            CapabilitySupport::native(),
            vec![MarketType::Perpetual],
            "GET",
            "/v1/perpum/instruments",
            EndpointAuth::None,
            Vec::new(),
            "coinw_futures_public",
            1,
        ),
        rest_endpoint(
            "get_order_book",
            CapabilitySupport::native(),
            vec![MarketType::Perpetual],
            "GET",
            "/v1/perpumPublic/depth",
            EndpointAuth::None,
            Vec::new(),
            "coinw_futures_public",
            1,
        ),
    ];

    let private_support = if private_rest_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("private REST credentials unavailable")
    };
    for endpoint in private_rest_endpoints(private_support) {
        endpoints.push(endpoint);
    }
    endpoints
}

fn private_rest_endpoints(private_support: CapabilitySupport) -> Vec<EndpointCapability> {
    let spot = vec![
        (
            "get_balances",
            "POST",
            "/api/v1/private?command=returnCompleteBalances",
            1,
        ),
        ("place_order", "POST", "/api/v1/private?command=doTrade", 1),
        (
            "place_quote_market_order",
            "POST",
            "/api/v1/private?command=doTrade",
            1,
        ),
        (
            "cancel_order",
            "POST",
            "/api/v1/private?command=cancelOrder",
            1,
        ),
        (
            "cancel_all_orders",
            "POST",
            "/api/v1/private?command=cancelAllOrder",
            1,
        ),
        (
            "query_order",
            "POST",
            "/api/v1/private?command=returnOrderStatus",
            1,
        ),
        (
            "get_open_orders",
            "POST",
            "/api/v1/private?command=returnOpenOrders",
            1,
        ),
        (
            "get_recent_fills",
            "POST",
            "/api/v1/private?command=returnUTradeHistory",
            1,
        ),
    ];
    let futures = vec![
        ("get_balances", "GET", "/v1/perpum/account/getUserAssets", 1),
        ("get_positions", "GET", "/v1/perpum/positions", 1),
        ("get_fees", "GET", "/v1/perpum/account/fees", 1),
        ("place_order", "POST", "/v1/perpum/order", 1),
        ("cancel_order", "DELETE", "/v1/perpum/order", 1),
        ("cancel_all_orders", "DELETE", "/v1/perpum/batchOrders", 1),
        ("batch_cancel_orders", "DELETE", "/v1/perpum/batchOrders", 1),
        ("query_order", "GET", "/v1/perpum/order", 1),
        ("get_open_orders", "GET", "/v1/perpum/orders/open", 1),
        ("get_recent_fills", "GET", "/v1/perpum/orders/deals", 1),
    ];
    let mut endpoints = Vec::new();
    for (operation, method, path, weight) in spot {
        endpoints.push(rest_endpoint(
            operation,
            private_support.clone(),
            vec![MarketType::Spot],
            method,
            path,
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly, CredentialScope::Trade],
            "coinw_spot_private",
            weight,
        ));
    }
    for (operation, method, path, weight) in futures {
        endpoints.push(rest_endpoint(
            operation,
            private_support.clone(),
            vec![MarketType::Perpetual],
            method,
            path,
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly, CredentialScope::Trade],
            "coinw_futures_private",
            weight,
        ));
    }
    endpoints
}

fn rest_endpoint(
    operation: &str,
    support: CapabilitySupport,
    market_types: Vec<MarketType>,
    method: &str,
    path: &str,
    auth: EndpointAuth,
    credential_scopes: Vec<CredentialScope>,
    rate_limit_bucket: &str,
    weight: u32,
) -> EndpointCapability {
    EndpointCapability {
        operation: format!("coinw.{operation}"),
        support,
        market_types,
        transport: EndpointTransport::Rest,
        method: Some(method.to_string()),
        path: Some(path.to_string()),
        auth,
        credential_scopes,
        rate_limit_bucket: Some(rate_limit_bucket.to_string()),
        weight: Some(weight),
        supports_testnet: false,
    }
}
