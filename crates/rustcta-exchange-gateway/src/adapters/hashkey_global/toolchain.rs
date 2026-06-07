use rustcta_exchange_api::{
    AuthRenewalKind, AuthRenewalPolicy, BatchAtomicity, BatchCapability, BatchExecutionMode,
    CapabilitySupport, CredentialScope, EndpointAuth, EndpointCapability, EndpointTransport,
    ExchangeClientCapabilities, HeartbeatCapability, HeartbeatDirection, HeartbeatPolicy,
    HistoryCapability, ReconnectCapability, StreamHeartbeatDirection, StreamResyncCapability,
    StreamRuntimeCapability,
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
        CapabilitySupport::unsupported(
            "HashKey Global private REST requires enabled API key and secret",
        )
    };
    capabilities.capabilities_v2.public_streams = CapabilitySupport::rest_fallback(
        "HashKey Global WS specs/parsers are adapter-local; REST snapshots remain resync source",
    );
    capabilities.capabilities_v2.private_streams = if private_rest_enabled {
        CapabilitySupport::rest_fallback(
            "HashKey Global listen-key private WS specs/parsers exist; REST reconciliation remains required before live validation",
        )
    } else {
        CapabilitySupport::unsupported("HashKey Global private WS requires private credentials")
    };
    capabilities.capabilities_v2.stream_runtime = stream_runtime(private_rest_enabled);
    capabilities.capabilities_v2.batch_place_orders = composed_batch(
        private_rest_enabled,
        "HashKey Global batch place is gateway-composed from sequential single-order calls",
    );
    capabilities.capabilities_v2.batch_cancel_orders = composed_batch(
        private_rest_enabled,
        "HashKey Global batch cancel is gateway-composed from sequential single-order cancels",
    );
    capabilities.capabilities_v2.cancel_all_orders = if private_rest_enabled {
        CapabilitySupport::composed("loads open orders then cancels each order")
    } else {
        CapabilitySupport::unsupported(
            "HashKey Global cancel-all requires private REST credentials",
        )
    };
    capabilities.capabilities_v2.order_history = history(private_rest_enabled, Some(1000));
    capabilities.capabilities_v2.fills_history = history(private_rest_enabled, Some(1000));
    capabilities.capabilities_v2.credential_scopes = scopes(private_rest_enabled);
    capabilities.capabilities_v2.endpoints = endpoints(private_rest_enabled);
    capabilities.apply_v2_to_legacy_flags();
}

fn stream_runtime(private_rest_enabled: bool) -> StreamRuntimeCapability {
    StreamRuntimeCapability {
        public: CapabilitySupport::rest_fallback(
            "REST order-book snapshot is required after reconnect",
        ),
        private: if private_rest_enabled {
            CapabilitySupport::rest_fallback(
                "REST account/order/fill reconciliation remains source of truth",
            )
        } else {
            CapabilitySupport::unsupported("private stream requires credentials")
        },
        supports_subscribe: true,
        supports_unsubscribe: true,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::Bidirectional,
            interval_ms: Some(30_000),
            timeout_ms: Some(45_000),
        },
        reconnect: ReconnectCapability {
            supported: true,
            requires_resubscribe: true,
            preserves_session: false,
            max_reconnect_attempts: None,
        },
        resync: StreamResyncCapability {
            order_book: true,
            balances: private_rest_enabled,
            positions: private_rest_enabled,
            orders: private_rest_enabled,
        },
        auth: rustcta_exchange_api::StreamAuthCapability {
            required: private_rest_enabled,
            credential_scopes: scopes(private_rest_enabled),
            renewal_ms: if private_rest_enabled {
                Some(1_800_000)
            } else {
                None
            },
            uses_listen_key: private_rest_enabled,
            requires_relogin_on_reconnect: true,
        },
        public_private_separate_connections: true,
        heartbeat_policy: HeartbeatPolicy {
            direction: HeartbeatDirection::ApplicationMessage,
            ping_interval_ms: 30_000,
            pong_timeout_ms: 45_000,
            stale_message_ms: 60_000,
            requires_pong_payload_echo: false,
        },
        auth_renewal_policy: AuthRenewalPolicy {
            kind: if private_rest_enabled {
                AuthRenewalKind::ListenKeyKeepAlive
            } else {
                AuthRenewalKind::None
            },
            renew_before_expiry_ms: 60_000,
            renewal_interval_ms: if private_rest_enabled {
                Some(1_800_000)
            } else {
                None
            },
            reconnect_on_renewal_failure: true,
            resubscribe_after_renewal: true,
        },
        reconnect_requires_login: true,
        reconnect_requires_resubscribe: true,
        orderbook_requires_snapshot_after_reconnect: true,
        ..StreamRuntimeCapability::default()
    }
}

fn composed_batch(private_rest_enabled: bool, reason: &str) -> BatchCapability {
    if private_rest_enabled {
        BatchCapability {
            support: CapabilitySupport::composed(reason),
            mode: BatchExecutionMode::ComposedSequential,
            atomicity: BatchAtomicity::Partial,
            max_items: None,
            same_symbol_required: false,
            same_market_type_required: false,
            supports_client_order_id: true,
            supports_partial_failure: true,
        }
    } else {
        BatchCapability::unsupported(
            "HashKey Global batch operations require private REST credentials",
        )
    }
}

fn history(private_rest_enabled: bool, max_limit: Option<u32>) -> HistoryCapability {
    HistoryCapability {
        support: if private_rest_enabled {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "HashKey Global history requires private REST credentials",
            )
        },
        supports_since: false,
        supports_until: false,
        supports_limit: max_limit.is_some(),
        supports_cursor: false,
        supports_from_id: false,
        max_limit,
        max_window_ms: None,
    }
}

fn scopes(enabled: bool) -> Vec<CredentialScope> {
    if enabled {
        vec![CredentialScope::ReadOnly, CredentialScope::Trade]
    } else {
        Vec::new()
    }
}

fn endpoints(private_rest_enabled: bool) -> Vec<EndpointCapability> {
    let mut endpoints = vec![
        endpoint(
            "get_symbol_rules",
            CapabilitySupport::native(),
            vec![MarketType::Spot],
            "GET",
            "/api/v1/exchangeInfo",
            EndpointAuth::None,
            Vec::new(),
            "hashkey_global_public_rest",
        ),
        endpoint(
            "get_symbol_rules",
            CapabilitySupport::native(),
            vec![MarketType::Perpetual],
            "GET",
            "/api/v1/futures/exchangeInfo",
            EndpointAuth::None,
            Vec::new(),
            "hashkey_global_public_rest",
        ),
        endpoint(
            "get_order_book",
            CapabilitySupport::native(),
            vec![MarketType::Spot],
            "GET",
            "/api/v1/depth",
            EndpointAuth::None,
            Vec::new(),
            "hashkey_global_public_rest",
        ),
        endpoint(
            "get_order_book",
            CapabilitySupport::native(),
            vec![MarketType::Perpetual],
            "GET",
            "/api/v1/futures/depth",
            EndpointAuth::None,
            Vec::new(),
            "hashkey_global_public_rest",
        ),
    ];
    let private_support = if private_rest_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("private REST credentials unavailable")
    };
    for (operation, market, method, path) in [
        ("get_balances", MarketType::Spot, "GET", "/api/v1/account"),
        (
            "get_balances",
            MarketType::Perpetual,
            "GET",
            "/api/v1/futures/account",
        ),
        (
            "get_positions",
            MarketType::Perpetual,
            "GET",
            "/api/v1/futures/account",
        ),
        ("get_fees", MarketType::Spot, "GET", "/api/v1/account"),
        (
            "get_fees",
            MarketType::Perpetual,
            "GET",
            "/api/v1/futures/commissionRate",
        ),
        ("place_order", MarketType::Spot, "POST", "/api/v1/order"),
        (
            "place_order",
            MarketType::Perpetual,
            "POST",
            "/api/v1/futures/order",
        ),
        ("cancel_order", MarketType::Spot, "DELETE", "/api/v1/order"),
        (
            "cancel_order",
            MarketType::Perpetual,
            "DELETE",
            "/api/v1/futures/order",
        ),
        ("query_order", MarketType::Spot, "GET", "/api/v1/order"),
        (
            "query_order",
            MarketType::Perpetual,
            "GET",
            "/api/v1/futures/order",
        ),
        (
            "get_open_orders",
            MarketType::Spot,
            "GET",
            "/api/v1/openOrders",
        ),
        (
            "get_open_orders",
            MarketType::Perpetual,
            "GET",
            "/api/v1/futures/openOrders",
        ),
        (
            "get_recent_fills",
            MarketType::Spot,
            "GET",
            "/api/v2/myTrades",
        ),
        (
            "get_recent_fills",
            MarketType::Perpetual,
            "GET",
            "/api/v1/futures/myTrades",
        ),
    ] {
        endpoints.push(endpoint(
            operation,
            private_support.clone(),
            vec![market],
            method,
            path,
            EndpointAuth::Hmac,
            scopes(private_rest_enabled),
            "hashkey_global_private_rest",
        ));
    }
    endpoints
}

fn endpoint(
    operation: &str,
    support: CapabilitySupport,
    market_types: Vec<MarketType>,
    method: &str,
    path: &str,
    auth: EndpointAuth,
    credential_scopes: Vec<CredentialScope>,
    bucket: &str,
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
        rate_limit_bucket: Some(bucket.to_string()),
        weight: Some(1),
        supports_testnet: false,
    }
}
