use rustcta_exchange_api::{
    BatchAtomicity, BatchCapability, BatchExecutionMode, CapabilitySupport, CredentialScope,
    EndpointAuth, EndpointCapability, EndpointTransport, ExchangeClientCapabilities,
    HeartbeatCapability, HeartbeatDirection, HeartbeatPolicy, HistoryCapability,
    ReconnectCapability, StreamHeartbeatDirection, StreamResyncCapability, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

pub(super) fn apply_toolchain_capabilities(
    capabilities: &mut ExchangeClientCapabilities,
    private_rest_enabled: bool,
    public_streams_enabled: bool,
) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = if private_rest_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("Binance private REST requires enabled API key and secret")
    };
    capabilities.capabilities_v2.public_streams = if public_streams_enabled {
        CapabilitySupport::rest_fallback(
            "Binance public WS depth/bookTicker subscription is mapped; REST depth snapshot remains the resync source",
        )
    } else {
        CapabilitySupport::unsupported("Binance public streams disabled by config")
    };
    capabilities.capabilities_v2.private_streams = CapabilitySupport::unsupported(
        "Binance private user data stream is not wired; use REST reconciliation through account, open orders, query order, and myTrades",
    );
    capabilities.capabilities_v2.stream_runtime = stream_runtime(public_streams_enabled);
    capabilities.capabilities_v2.batch_place_orders = if private_rest_enabled {
        BatchCapability {
            support: CapabilitySupport::native(),
            mode: BatchExecutionMode::Native,
            atomicity: BatchAtomicity::Partial,
            max_items: Some(5),
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: true,
            ..BatchCapability::default()
        }
    } else {
        BatchCapability::unsupported("Binance USD-M batch place requires private REST credentials")
    };
    capabilities.capabilities_v2.batch_cancel_orders = if private_rest_enabled {
        BatchCapability {
            support: CapabilitySupport::native(),
            mode: BatchExecutionMode::Native,
            atomicity: BatchAtomicity::Partial,
            max_items: Some(10),
            same_symbol_required: true,
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: true,
            ..BatchCapability::default()
        }
    } else {
        BatchCapability::unsupported("Binance USD-M batch cancel requires private REST credentials")
    };
    capabilities.capabilities_v2.cancel_all_orders = if private_rest_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("Binance cancel-all requires private REST credentials")
    };
    capabilities.capabilities_v2.order_history = HistoryCapability {
        support: if private_rest_enabled {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "Binance order history requires private REST credentials",
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
            CapabilitySupport::unsupported("Binance myTrades requires private REST credentials")
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

fn stream_runtime(public_streams_enabled: bool) -> StreamRuntimeCapability {
    StreamRuntimeCapability {
        public: if public_streams_enabled {
            CapabilitySupport::rest_fallback(
                "REST depth snapshot required after reconnect or sequence gap",
            )
        } else {
            CapabilitySupport::unsupported("public streams disabled")
        },
        private: CapabilitySupport::unsupported("private user data stream is not wired"),
        supports_subscribe: public_streams_enabled,
        supports_unsubscribe: public_streams_enabled,
        supports_public_subscribe: public_streams_enabled,
        supports_public_unsubscribe: public_streams_enabled,
        supports_private_subscribe: false,
        supports_private_unsubscribe: false,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::ServerPing,
            interval_ms: Some(20_000),
            timeout_ms: Some(60_000),
        },
        reconnect: ReconnectCapability {
            supported: true,
            requires_resubscribe: true,
            preserves_session: false,
            max_reconnect_attempts: None,
        },
        resync: StreamResyncCapability {
            order_book: true,
            balances: false,
            positions: false,
            orders: false,
        },
        heartbeat_policy: HeartbeatPolicy {
            direction: HeartbeatDirection::ServerPing,
            ping_interval_ms: 20_000,
            pong_timeout_ms: 60_000,
            stale_message_ms: 60_000,
            requires_pong_payload_echo: true,
        },
        reconnect_requires_login: false,
        reconnect_requires_resubscribe: true,
        orderbook_requires_snapshot_after_reconnect: true,
        ..StreamRuntimeCapability::default()
    }
}

fn endpoint_capabilities(private_rest_enabled: bool) -> Vec<EndpointCapability> {
    let private_support = if private_rest_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("private REST credentials unavailable")
    };
    let mut endpoints = vec![
        rest_endpoint(
            "get_symbol_rules",
            CapabilitySupport::native(),
            "GET",
            "/api/v3/exchangeInfo",
            EndpointAuth::None,
            vec![MarketType::Spot],
            Vec::new(),
            Some("public_ip"),
            Some(10),
        ),
        rest_endpoint(
            "get_symbol_rules",
            CapabilitySupport::native(),
            "GET",
            "/fapi/v1/exchangeInfo",
            EndpointAuth::None,
            vec![MarketType::Perpetual],
            Vec::new(),
            Some("public_ip"),
            Some(10),
        ),
        rest_endpoint(
            "get_order_book",
            CapabilitySupport::native(),
            "GET",
            "/api/v3/depth",
            EndpointAuth::None,
            vec![MarketType::Spot],
            Vec::new(),
            Some("public_ip"),
            Some(5),
        ),
        rest_endpoint(
            "get_order_book",
            CapabilitySupport::native(),
            "GET",
            "/fapi/v1/depth",
            EndpointAuth::None,
            vec![MarketType::Perpetual],
            Vec::new(),
            Some("public_ip"),
            Some(5),
        ),
    ];

    for spec in private_endpoint_specs() {
        endpoints.push(rest_endpoint(
            spec.operation,
            private_support.clone(),
            spec.method,
            spec.path,
            EndpointAuth::Hmac,
            spec.market_types,
            vec![spec.credential_scope],
            Some("signed_uid"),
            Some(spec.weight),
        ));
    }

    endpoints
}

struct PrivateEndpointSpec {
    operation: &'static str,
    method: &'static str,
    path: &'static str,
    market_types: Vec<MarketType>,
    credential_scope: CredentialScope,
    weight: u32,
}

fn private_endpoint_specs() -> Vec<PrivateEndpointSpec> {
    use CredentialScope::{ReadOnly, Trade};
    vec![
        private_endpoint(
            "get_balances",
            "GET",
            "/api/v3/account",
            vec![MarketType::Spot],
            ReadOnly,
            20,
        ),
        private_endpoint(
            "get_balances",
            "GET",
            "/fapi/v2/balance",
            vec![MarketType::Perpetual],
            ReadOnly,
            20,
        ),
        private_endpoint(
            "get_positions",
            "GET",
            "/fapi/v2/positionRisk",
            vec![MarketType::Perpetual],
            ReadOnly,
            20,
        ),
        private_endpoint(
            "get_fees",
            "GET",
            "/api/v3/account/commission",
            vec![MarketType::Spot],
            ReadOnly,
            20,
        ),
        private_endpoint(
            "get_fees",
            "GET",
            "/fapi/v1/commissionRate",
            vec![MarketType::Perpetual],
            ReadOnly,
            20,
        ),
        private_endpoint(
            "place_order",
            "POST",
            "/api/v3/order",
            vec![MarketType::Spot],
            Trade,
            1,
        ),
        private_endpoint(
            "place_order",
            "POST",
            "/fapi/v1/order",
            vec![MarketType::Perpetual],
            Trade,
            1,
        ),
        private_endpoint(
            "batch_place_orders",
            "POST",
            "/fapi/v1/batchOrders",
            vec![MarketType::Perpetual],
            Trade,
            5,
        ),
        private_endpoint(
            "place_quote_market_order",
            "POST",
            "/api/v3/order",
            vec![MarketType::Spot],
            Trade,
            1,
        ),
        private_endpoint(
            "cancel_order",
            "DELETE",
            "/api/v3/order",
            vec![MarketType::Spot],
            Trade,
            1,
        ),
        private_endpoint(
            "cancel_order",
            "DELETE",
            "/fapi/v1/order",
            vec![MarketType::Perpetual],
            Trade,
            1,
        ),
        private_endpoint(
            "batch_cancel_orders",
            "DELETE",
            "/fapi/v1/batchOrders",
            vec![MarketType::Perpetual],
            Trade,
            1,
        ),
        private_endpoint(
            "cancel_all_orders",
            "DELETE",
            "/api/v3/openOrders",
            vec![MarketType::Spot],
            Trade,
            1,
        ),
        private_endpoint(
            "cancel_all_orders",
            "DELETE",
            "/fapi/v1/allOpenOrders",
            vec![MarketType::Perpetual],
            Trade,
            1,
        ),
        private_endpoint(
            "amend_order",
            "PUT",
            "/api/v3/order/amend/keepPriority",
            vec![MarketType::Spot],
            Trade,
            4,
        ),
        private_endpoint(
            "place_order_list",
            "POST",
            "/api/v3/orderList/oco",
            vec![MarketType::Spot],
            Trade,
            1,
        ),
        private_endpoint(
            "query_order",
            "GET",
            "/api/v3/order",
            vec![MarketType::Spot],
            ReadOnly,
            4,
        ),
        private_endpoint(
            "query_order",
            "GET",
            "/fapi/v1/order",
            vec![MarketType::Perpetual],
            ReadOnly,
            4,
        ),
        private_endpoint(
            "get_open_orders",
            "GET",
            "/api/v3/openOrders",
            vec![MarketType::Spot],
            ReadOnly,
            6,
        ),
        private_endpoint(
            "get_open_orders",
            "GET",
            "/fapi/v1/openOrders",
            vec![MarketType::Perpetual],
            ReadOnly,
            6,
        ),
        private_endpoint(
            "get_recent_fills",
            "GET",
            "/api/v3/myTrades",
            vec![MarketType::Spot],
            ReadOnly,
            20,
        ),
        private_endpoint(
            "get_recent_fills",
            "GET",
            "/fapi/v1/userTrades",
            vec![MarketType::Perpetual],
            ReadOnly,
            20,
        ),
    ]
}

fn private_endpoint(
    operation: &'static str,
    method: &'static str,
    path: &'static str,
    market_types: Vec<MarketType>,
    credential_scope: CredentialScope,
    weight: u32,
) -> PrivateEndpointSpec {
    PrivateEndpointSpec {
        operation,
        method,
        path,
        market_types,
        credential_scope,
        weight,
    }
}

fn rest_endpoint(
    operation: &str,
    support: CapabilitySupport,
    method: &str,
    path: &str,
    auth: EndpointAuth,
    market_types: Vec<MarketType>,
    credential_scopes: Vec<CredentialScope>,
    rate_limit_bucket: Option<&str>,
    weight: Option<u32>,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support,
        market_types,
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
