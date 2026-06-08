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
    public_streams_enabled: bool,
    private_streams_enabled: bool,
) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = if private_rest_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(
            "BitMart private REST requires API key, secret and optional memo",
        )
    };
    capabilities.capabilities_v2.public_streams = if public_streams_enabled {
        CapabilitySupport::rest_fallback(
            "BitMart WS specs/parsers are offline validated; REST snapshot is resync source",
        )
    } else {
        CapabilitySupport::unsupported("BitMart public streams disabled by config")
    };
    capabilities.capabilities_v2.private_streams = if private_streams_enabled {
        CapabilitySupport::rest_fallback("BitMart private WS auth/subscription specs exist; REST reconciliation remains source of truth")
    } else {
        CapabilitySupport::unsupported("BitMart private streams require credentials")
    };
    capabilities.capabilities_v2.stream_runtime = stream_runtime(
        private_rest_enabled,
        public_streams_enabled,
        private_streams_enabled,
    );
    capabilities.capabilities_v2.batch_place_orders = BatchCapability::unsupported(
        "BitMart futures submit-plan-order is a trigger/plan order endpoint, not shared regular batch-place semantics",
    );
    capabilities.capabilities_v2.batch_cancel_orders = BatchCapability {
        support: if private_rest_enabled {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("BitMart batch cancel requires private REST")
        },
        mode: if private_rest_enabled {
            BatchExecutionMode::Native
        } else {
            BatchExecutionMode::Unsupported
        },
        atomicity: BatchAtomicity::NonAtomic,
        max_items: None,
        same_symbol_required: false,
        same_market_type_required: true,
        supports_client_order_id: true,
        supports_partial_failure: true,
    };
    capabilities.capabilities_v2.cancel_all_orders = if private_rest_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("BitMart cancel-all requires private REST")
    };
    capabilities.capabilities_v2.order_history = history(private_rest_enabled, Some(100));
    capabilities.capabilities_v2.fills_history = history(private_rest_enabled, Some(100));
    capabilities.capabilities_v2.credential_scopes = if private_rest_enabled {
        vec![CredentialScope::ReadOnly, CredentialScope::Trade]
    } else {
        Vec::new()
    };
    capabilities.capabilities_v2.endpoints = endpoints(private_rest_enabled);
    capabilities.apply_v2_to_legacy_flags();
}

fn stream_runtime(
    private_rest_enabled: bool,
    public_streams_enabled: bool,
    private_streams_enabled: bool,
) -> StreamRuntimeCapability {
    StreamRuntimeCapability {
        public: if public_streams_enabled {
            CapabilitySupport::rest_fallback("REST order-book snapshot required after reconnect")
        } else {
            CapabilitySupport::unsupported("public streams disabled")
        },
        private: if private_streams_enabled {
            CapabilitySupport::rest_fallback(
                "REST account/order/fill reconciliation remains source of truth",
            )
        } else {
            CapabilitySupport::unsupported("private streams disabled")
        },
        supports_subscribe: public_streams_enabled || private_streams_enabled,
        supports_unsubscribe: public_streams_enabled || private_streams_enabled,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::ClientPing,
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
            balances: private_rest_enabled,
            positions: private_rest_enabled,
            orders: private_rest_enabled,
        },
        auth: rustcta_exchange_api::StreamAuthCapability {
            required: private_streams_enabled,
            credential_scopes: if private_streams_enabled {
                vec![CredentialScope::ReadOnly, CredentialScope::Trade]
            } else {
                Vec::new()
            },
            renewal_ms: None,
            uses_listen_key: false,
            requires_relogin_on_reconnect: true,
        },
        public_private_separate_connections: true,
        heartbeat_policy: HeartbeatPolicy {
            direction: HeartbeatDirection::ClientPing,
            ping_interval_ms: 15_000,
            pong_timeout_ms: 30_000,
            stale_message_ms: 45_000,
            requires_pong_payload_echo: false,
        },
        auth_renewal_policy: AuthRenewalPolicy {
            kind: AuthRenewalKind::ReLogin,
            renew_before_expiry_ms: 60_000,
            renewal_interval_ms: None,
            reconnect_on_renewal_failure: true,
            resubscribe_after_renewal: true,
        },
        reconnect_requires_login: private_streams_enabled,
        reconnect_requires_resubscribe: true,
        orderbook_requires_snapshot_after_reconnect: true,
        ..StreamRuntimeCapability::default()
    }
}

fn history(private_rest_enabled: bool, max_limit: Option<u32>) -> HistoryCapability {
    HistoryCapability {
        support: if private_rest_enabled {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("BitMart history requires private REST credentials")
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

fn endpoints(private_rest_enabled: bool) -> Vec<EndpointCapability> {
    let private_support = if private_rest_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("private REST credentials unavailable")
    };
    vec![
        endpoint(
            "get_symbol_rules",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "GET",
            "/spot/v1/symbols/details",
            EndpointAuth::None,
            Vec::new(),
            "bitmart_public_rest",
        ),
        endpoint(
            "get_order_book",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "GET",
            "/spot/v1/symbols/book",
            EndpointAuth::None,
            Vec::new(),
            "bitmart_public_rest",
        ),
        endpoint(
            "get_balances",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "GET",
            "/spot/v1/wallet",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            "bitmart_private_rest",
        ),
        endpoint(
            "get_positions",
            private_support.clone(),
            vec![MarketType::Perpetual],
            "GET",
            "/contract/private/position",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            "bitmart_private_rest",
        ),
        endpoint(
            "place_order",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "POST",
            "/spot/v2/submit_order",
            EndpointAuth::Hmac,
            vec![CredentialScope::Trade],
            "bitmart_private_rest",
        ),
        endpoint(
            "cancel_order",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "POST",
            "/spot/v3/cancel_order",
            EndpointAuth::Hmac,
            vec![CredentialScope::Trade],
            "bitmart_private_rest",
        ),
        endpoint(
            "batch_cancel_orders",
            private_support.clone(),
            vec![MarketType::Perpetual],
            "POST",
            "/contract/private/cancel-orders",
            EndpointAuth::Hmac,
            vec![CredentialScope::Trade],
            "bitmart_private_rest",
        ),
        endpoint(
            "cancel_all_orders",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "POST",
            "/spot/v3/cancel_orders",
            EndpointAuth::Hmac,
            vec![CredentialScope::Trade],
            "bitmart_private_rest",
        ),
        endpoint(
            "query_order",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "GET",
            "/spot/v2/order_detail",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            "bitmart_private_rest",
        ),
        endpoint(
            "get_open_orders",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "GET",
            "/spot/v2/orders",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            "bitmart_private_rest",
        ),
        endpoint(
            "get_recent_fills",
            private_support,
            vec![MarketType::Spot, MarketType::Perpetual],
            "GET",
            "/spot/v2/trades",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            "bitmart_private_rest",
        ),
    ]
}

fn endpoint(
    operation: &str,
    support: CapabilitySupport,
    market_types: Vec<MarketType>,
    method: &str,
    path: &str,
    auth: EndpointAuth,
    credential_scopes: Vec<CredentialScope>,
    rate_limit_bucket: &str,
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
        weight: Some(1),
        supports_testnet: false,
    }
}
