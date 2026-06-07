use rustcta_exchange_api::{
    AuthRenewalKind, AuthRenewalPolicy, BatchAtomicity, BatchCapability, BatchExecutionMode,
    CapabilitySupport, CredentialScope, EndpointAuth, EndpointCapability, EndpointTransport,
    ExchangeClientCapabilities, HeartbeatCapability, HeartbeatDirection, HeartbeatPolicy,
    HistoryCapability, ReconnectCapability, StreamAuthCapability, StreamHeartbeatDirection,
    StreamResyncCapability, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

pub(super) const BITRUE_COMPOSED_BATCH_MAX_ITEMS: u32 = 10;
pub(super) const BITRUE_MAX_HISTORY_LIMIT: u32 = 1000;

pub(super) fn apply_bitrue_capabilities_v2(
    capabilities: &mut ExchangeClientCapabilities,
    private_enabled: bool,
) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = if private_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(
            "Bitrue private REST requires API key/secret and explicit enablement",
        )
    };
    capabilities.capabilities_v2.public_streams = CapabilitySupport::ws_only(
        "Bitrue public WS subscribe, heartbeat and parsers are adapter-local; shared supervisor integration is pending",
    );
    capabilities.capabilities_v2.private_streams = if private_enabled {
        CapabilitySupport::rest_fallback(
            "Bitrue private WS listen-key helpers and parsers are adapter-local; REST reconciliation remains source of truth",
        )
    } else {
        CapabilitySupport::unsupported("Bitrue private WS requires private REST credentials")
    };
    capabilities.capabilities_v2.stream_runtime = bitrue_stream_runtime_capability(private_enabled);
    capabilities.capabilities_v2.batch_place_orders = bitrue_batch_capability(
        private_enabled,
        "Bitrue batch place requires private REST credentials",
        "Bitrue has no verified native batch place endpoint; adapter composes validated single-order REST calls sequentially",
    );
    capabilities.capabilities_v2.batch_cancel_orders = bitrue_batch_capability(
        private_enabled,
        "Bitrue batch cancel requires private REST credentials",
        "Bitrue has no verified native batch cancel endpoint; adapter composes validated single-cancel REST calls sequentially",
    );
    capabilities.capabilities_v2.cancel_all_orders = if private_enabled {
        CapabilitySupport::composed(
            "Bitrue cancel-all queries open orders for one symbol and cancels each order sequentially",
        )
    } else {
        CapabilitySupport::unsupported("Bitrue cancel-all requires private REST credentials")
    };
    capabilities.capabilities_v2.order_history = HistoryCapability {
        support: if private_enabled {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "Bitrue order readback requires private REST credentials",
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
        support: if private_enabled {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("Bitrue fills history requires private REST credentials")
        },
        supports_since: true,
        supports_until: true,
        supports_limit: true,
        supports_cursor: false,
        supports_from_id: false,
        max_limit: Some(BITRUE_MAX_HISTORY_LIMIT),
        max_window_ms: None,
    };
    capabilities.capabilities_v2.endpoints = bitrue_endpoint_capabilities(private_enabled);
    capabilities.capabilities_v2.credential_scopes = if private_enabled {
        vec![CredentialScope::ReadOnly, CredentialScope::Trade]
    } else {
        Vec::new()
    };
    capabilities.apply_v2_to_legacy_flags();
}

fn bitrue_batch_capability(
    private_enabled: bool,
    unsupported_reason: &str,
    composed_reason: &str,
) -> BatchCapability {
    if private_enabled {
        BatchCapability {
            support: CapabilitySupport::composed(composed_reason),
            mode: BatchExecutionMode::ComposedSequential,
            atomicity: BatchAtomicity::NonAtomic,
            max_items: Some(BITRUE_COMPOSED_BATCH_MAX_ITEMS),
            same_symbol_required: false,
            same_market_type_required: false,
            supports_client_order_id: true,
            supports_partial_failure: false,
        }
    } else {
        BatchCapability::unsupported(unsupported_reason)
    }
}

fn bitrue_stream_runtime_capability(private_enabled: bool) -> StreamRuntimeCapability {
    let auth_renewal = AuthRenewalPolicy {
        kind: if private_enabled {
            AuthRenewalKind::ListenKeyKeepAlive
        } else {
            AuthRenewalKind::None
        },
        renew_before_expiry_ms: 5 * 60 * 1000,
        renewal_interval_ms: if private_enabled {
            Some(25 * 60 * 1000)
        } else {
            None
        },
        reconnect_on_renewal_failure: true,
        resubscribe_after_renewal: true,
    };
    StreamRuntimeCapability {
        public: CapabilitySupport::ws_only(
            "Bitrue public WS helper builds subscriptions and parses ping/orderbook/trade messages",
        ),
        private: if private_enabled {
            CapabilitySupport::rest_fallback(
                "Bitrue private WS uses listen keys but REST reconciliation is required until shared supervisor is wired",
            )
        } else {
            CapabilitySupport::unsupported("Bitrue private WS requires private REST credentials")
        },
        supports_subscribe: true,
        supports_unsubscribe: false,
        supports_public_subscribe: true,
        supports_public_unsubscribe: false,
        supports_private_subscribe: private_enabled,
        supports_private_unsubscribe: false,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::ServerPing,
            interval_ms: Some(30_000),
            timeout_ms: Some(10_000),
        },
        reconnect: ReconnectCapability {
            supported: true,
            requires_resubscribe: true,
            preserves_session: false,
            max_reconnect_attempts: Some(5),
        },
        resync: StreamResyncCapability {
            order_book: true,
            balances: true,
            positions: true,
            orders: true,
        },
        auth: StreamAuthCapability {
            required: private_enabled,
            credential_scopes: if private_enabled {
                vec![CredentialScope::ReadOnly]
            } else {
                Vec::new()
            },
            renewal_ms: if private_enabled {
                Some(25 * 60 * 1000)
            } else {
                None
            },
            uses_listen_key: true,
            requires_relogin_on_reconnect: true,
        },
        public_private_separate_connections: true,
        heartbeat_policy: HeartbeatPolicy {
            direction: HeartbeatDirection::ApplicationMessage,
            ping_interval_ms: 30_000,
            pong_timeout_ms: 10_000,
            stale_message_ms: 45_000,
            requires_pong_payload_echo: true,
        },
        auth_renewal: auth_renewal.clone(),
        auth_renewal_policy: auth_renewal,
        reconnect_requires_login: true,
        reconnect_requires_resubscribe: true,
        orderbook_requires_snapshot_after_reconnect: true,
    }
}

fn bitrue_endpoint_capabilities(private_enabled: bool) -> Vec<EndpointCapability> {
    let private_support = if private_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("Bitrue private REST credentials unavailable")
    };
    let mut endpoints = Vec::new();
    endpoints.extend([
        rest_endpoint(
            "get_symbol_rules",
            CapabilitySupport::native(),
            vec![MarketType::Spot],
            "GET",
            "/api/v1/exchangeInfo",
            EndpointAuth::None,
            Vec::new(),
            "bitrue.spot.public",
            1,
        ),
        rest_endpoint(
            "get_symbol_rules",
            CapabilitySupport::native(),
            vec![MarketType::Perpetual],
            "GET",
            "/fapi/v1/contracts",
            EndpointAuth::None,
            Vec::new(),
            "bitrue.futures.public",
            1,
        ),
        rest_endpoint(
            "get_order_book",
            CapabilitySupport::native(),
            vec![MarketType::Spot],
            "GET",
            "/api/v1/depth",
            EndpointAuth::None,
            Vec::new(),
            "bitrue.spot.public",
            1,
        ),
        rest_endpoint(
            "get_order_book",
            CapabilitySupport::native(),
            vec![MarketType::Perpetual],
            "GET",
            "/fapi/v1/depth",
            EndpointAuth::None,
            Vec::new(),
            "bitrue.futures.public",
            1,
        ),
    ]);

    for endpoint in [
        (
            "get_balances",
            vec![MarketType::Spot],
            "GET",
            "/api/v1/account",
            "bitrue.spot.signed",
        ),
        (
            "get_balances",
            vec![MarketType::Perpetual],
            "GET",
            "/fapi/v2/account",
            "bitrue.futures.signed",
        ),
        (
            "get_fees",
            vec![MarketType::Spot],
            "GET",
            "/api/v1/account",
            "bitrue.spot.signed",
        ),
        (
            "get_fees",
            vec![MarketType::Perpetual],
            "GET",
            "/fapi/v2/commissionRate",
            "bitrue.futures.signed",
        ),
        (
            "place_order",
            vec![MarketType::Spot],
            "POST",
            "/api/v1/order",
            "bitrue.spot.orders",
        ),
        (
            "place_order",
            vec![MarketType::Perpetual],
            "POST",
            "/fapi/v2/order",
            "bitrue.futures.orders",
        ),
        (
            "cancel_order",
            vec![MarketType::Spot],
            "DELETE",
            "/api/v1/order",
            "bitrue.spot.orders",
        ),
        (
            "cancel_order",
            vec![MarketType::Perpetual],
            "POST",
            "/fapi/v2/cancel",
            "bitrue.futures.orders",
        ),
        (
            "query_order",
            vec![MarketType::Spot],
            "GET",
            "/api/v1/order",
            "bitrue.spot.signed",
        ),
        (
            "query_order",
            vec![MarketType::Perpetual],
            "GET",
            "/fapi/v2/order",
            "bitrue.futures.signed",
        ),
        (
            "get_open_orders",
            vec![MarketType::Spot],
            "GET",
            "/api/v1/openOrders",
            "bitrue.spot.signed",
        ),
        (
            "get_open_orders",
            vec![MarketType::Perpetual],
            "GET",
            "/fapi/v2/openOrders",
            "bitrue.futures.signed",
        ),
        (
            "get_recent_fills",
            vec![MarketType::Spot],
            "GET",
            "/api/v2/myTrades",
            "bitrue.spot.signed",
        ),
        (
            "get_recent_fills",
            vec![MarketType::Perpetual],
            "GET",
            "/fapi/v2/myTrades",
            "bitrue.futures.signed",
        ),
    ] {
        endpoints.push(rest_endpoint(
            endpoint.0,
            private_support.clone(),
            endpoint.1,
            endpoint.2,
            endpoint.3,
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly, CredentialScope::Trade],
            endpoint.4,
            1,
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
        operation: operation.to_string(),
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
