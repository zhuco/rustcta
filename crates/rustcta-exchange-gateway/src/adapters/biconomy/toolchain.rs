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
        CapabilitySupport::unsupported("Biconomy private REST requires enabled API key and secret")
    };
    capabilities.capabilities_v2.public_streams = CapabilitySupport::rest_fallback(
        "Biconomy public WS specs/parsers are adapter-local; REST snapshots remain resync source",
    );
    capabilities.capabilities_v2.private_streams = if private_rest_enabled {
        CapabilitySupport::rest_fallback(
            "Biconomy private WS specs/parsers exist; REST account/order/fill reconciliation remains required before live-dry-run",
        )
    } else {
        CapabilitySupport::unsupported("Biconomy private WS requires private credentials")
    };
    capabilities.capabilities_v2.stream_runtime = stream_runtime(private_rest_enabled);
    capabilities.capabilities_v2.batch_place_orders = composed_batch(
        private_rest_enabled,
        "Biconomy batch place is gateway-composed from sequential single-order calls",
    );
    capabilities.capabilities_v2.batch_cancel_orders = composed_batch(
        private_rest_enabled,
        "Biconomy batch cancel is gateway-composed from sequential single-order cancels",
    );
    capabilities.capabilities_v2.cancel_all_orders = if private_rest_enabled {
        CapabilitySupport::composed("queries openOrders then cancels each order")
    } else {
        CapabilitySupport::unsupported("Biconomy cancel-all requires private REST credentials")
    };
    capabilities.capabilities_v2.order_history = history(private_rest_enabled, None);
    capabilities.capabilities_v2.fills_history = history(private_rest_enabled, Some(100));
    capabilities.capabilities_v2.credential_scopes = scopes(private_rest_enabled);
    capabilities.capabilities_v2.endpoints = endpoints(private_rest_enabled);
    capabilities.apply_v2_to_legacy_flags();
}

fn stream_runtime(private_rest_enabled: bool) -> StreamRuntimeCapability {
    StreamRuntimeCapability {
        public: CapabilitySupport::rest_fallback("REST order-book snapshot is resync source"),
        private: if private_rest_enabled {
            CapabilitySupport::rest_fallback(
                "REST reconciliation required for account/order/fill state",
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
            positions: false,
            orders: private_rest_enabled,
        },
        auth: rustcta_exchange_api::StreamAuthCapability {
            required: private_rest_enabled,
            credential_scopes: scopes(private_rest_enabled),
            renewal_ms: None,
            uses_listen_key: false,
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
            kind: AuthRenewalKind::None,
            renew_before_expiry_ms: 60_000,
            renewal_interval_ms: None,
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
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: true,
        }
    } else {
        BatchCapability::unsupported("Biconomy batch operations require private REST credentials")
    }
}

fn history(private_rest_enabled: bool, max_limit: Option<u32>) -> HistoryCapability {
    HistoryCapability {
        support: if private_rest_enabled {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("Biconomy history requires private REST credentials")
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

fn scopes(private_rest_enabled: bool) -> Vec<CredentialScope> {
    if private_rest_enabled {
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
            "GET",
            "/api/v1/exchangeInfo",
            EndpointAuth::None,
            Vec::new(),
            "biconomy_public_rest",
        ),
        endpoint(
            "get_order_book",
            CapabilitySupport::native(),
            "GET",
            "/api/v1/depth",
            EndpointAuth::None,
            Vec::new(),
            "biconomy_public_rest",
        ),
    ];
    let private_support = if private_rest_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("private REST credentials unavailable")
    };
    for (operation, path) in [
        ("get_balances", "/api/v2/private/account"),
        ("get_fees", "/api/v2/private/account"),
        ("place_order", "/api/v2/private/order"),
        ("cancel_order", "/api/v2/private/cancel"),
        ("query_order", "/api/v2/private/orderInfo"),
        ("get_open_orders", "/api/v2/private/openOrders"),
        ("get_recent_fills", "/api/v2/private/myTrades"),
    ] {
        endpoints.push(endpoint(
            operation,
            private_support.clone(),
            "POST",
            path,
            EndpointAuth::Hmac,
            scopes(private_rest_enabled),
            "biconomy_private_rest",
        ));
    }
    endpoints
}

fn endpoint(
    operation: &str,
    support: CapabilitySupport,
    method: &str,
    path: &str,
    auth: EndpointAuth,
    credential_scopes: Vec<CredentialScope>,
    bucket: &str,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support,
        market_types: vec![MarketType::Spot],
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
