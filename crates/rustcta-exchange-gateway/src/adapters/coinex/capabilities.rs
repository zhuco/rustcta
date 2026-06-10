use rustcta_exchange_api::{
    AuthRenewalKind, AuthRenewalPolicy, BatchAtomicity, BatchCapability, BatchExecutionMode,
    CapabilitySupport, CredentialScope, EndpointAuth, EndpointCapability, EndpointTransport,
    ExchangeClientCapabilities, ExchangeClientCapabilitiesV2, HeartbeatCapability,
    HeartbeatDirection, HeartbeatPolicy, HistoryCapability, RateLimitBucket, RateLimitPlan,
    RateLimitScope, ReconcilePlan, ReconcileTrigger, ReconnectCapability, RetryReconcilePolicy,
    StreamAuthCapability, StreamHeartbeatDirection, StreamResyncCapability,
    StreamRuntimeCapability, UnknownOrderPolicy, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType};

pub const COINEX_SPOT_MAX_PAGE_LIMIT: u32 = 1000;
pub const COINEX_COMPOSED_BATCH_MAX_ITEMS: u32 = 10;

pub fn apply_coinex_capabilities_v2(
    capabilities: &mut ExchangeClientCapabilities,
    private_enabled: bool,
) {
    capabilities.capabilities_v2 = coinex_capabilities_v2(private_enabled);
    capabilities.apply_v2_to_legacy_flags();
}

pub fn coinex_capabilities_v2(private_enabled: bool) -> ExchangeClientCapabilitiesV2 {
    let private_rest = if private_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(
            "CoinEx private REST requires API key/secret and explicit enablement",
        )
    };

    let batch_place_orders = if private_enabled {
        BatchCapability {
            support: CapabilitySupport::composed(
                "CoinEx Spot has verified single-order REST endpoints; batch place is composed sequentially by the adapter",
            ),
            mode: BatchExecutionMode::ComposedSequential,
            atomicity: BatchAtomicity::NonAtomic,
            max_items: Some(COINEX_COMPOSED_BATCH_MAX_ITEMS),
            same_symbol_required: false,
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: true,
        }
    } else {
        BatchCapability::unsupported("CoinEx batch place requires private REST")
    };

    let batch_cancel_orders = if private_enabled {
        BatchCapability {
            support: CapabilitySupport::composed(
                "CoinEx Spot has verified single-cancel REST endpoints; batch cancel is composed sequentially by the adapter",
            ),
            mode: BatchExecutionMode::ComposedSequential,
            atomicity: BatchAtomicity::NonAtomic,
            max_items: Some(COINEX_COMPOSED_BATCH_MAX_ITEMS),
            same_symbol_required: false,
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: true,
        }
    } else {
        BatchCapability::unsupported("CoinEx batch cancel requires private REST")
    };

    ExchangeClientCapabilitiesV2 {
        public_rest: CapabilitySupport::native(),
        private_rest,
        public_streams: CapabilitySupport::ws_only(
            "CoinEx public Spot WebSocket policy and parser helpers are adapter-local; shared supervisor integration is pending",
        ),
        private_streams: CapabilitySupport::rest_fallback(
            "CoinEx private Spot WebSocket auth policy is declared adapter-local; REST reconciliation remains source of truth",
        ),
        stream_runtime: coinex_stream_runtime_capability(),
        batch_place_orders,
        batch_cancel_orders,
        cancel_all_orders: if private_enabled {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("CoinEx cancel-all requires private REST")
        },
        order_history: HistoryCapability {
            support: if private_enabled {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported("CoinEx order history requires private REST")
            },
            supports_since: false,
            supports_until: false,
            supports_limit: true,
            supports_cursor: false,
            supports_from_id: false,
            max_limit: Some(COINEX_SPOT_MAX_PAGE_LIMIT),
            max_window_ms: None,
        },
        fills_history: HistoryCapability {
            support: if private_enabled {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported("CoinEx fill history requires private REST")
            },
            supports_since: true,
            supports_until: true,
            supports_limit: true,
            supports_cursor: false,
            supports_from_id: false,
            max_limit: Some(COINEX_SPOT_MAX_PAGE_LIMIT),
            max_window_ms: None,
        },
        endpoints: coinex_endpoint_capabilities(private_enabled),
        credential_scopes: if private_enabled {
            vec![CredentialScope::ReadOnly, CredentialScope::Trade]
        } else {
            Vec::new()
        },
        ..ExchangeClientCapabilitiesV2::default()
    }
}

pub fn coinex_stream_runtime_capability() -> StreamRuntimeCapability {
    StreamRuntimeCapability {
        public: CapabilitySupport::ws_only(
            "CoinEx public Spot WebSocket policy and parser helpers are adapter-local",
        ),
        private: CapabilitySupport::rest_fallback(
            "CoinEx private Spot WebSocket auth policy is adapter-local; REST reconciliation remains source of truth",
        ),
        supports_subscribe: true,
        supports_unsubscribe: true,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::ClientPing,
            interval_ms: Some(30_000),
            timeout_ms: Some(10_000),
            ..HeartbeatCapability::default()
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
            positions: false,
            orders: true,
        },
        auth: StreamAuthCapability {
            required: true,
            credential_scopes: vec![CredentialScope::ReadOnly, CredentialScope::Trade],
            renewal_ms: Some(30 * 60 * 1000),
            uses_listen_key: false,
            requires_relogin_on_reconnect: true,
        },
        public_private_separate_connections: true,
        heartbeat_policy: coinex_heartbeat_policy(),
        auth_renewal_policy: coinex_auth_renewal_policy(),
        reconnect_requires_login: true,
        reconnect_requires_resubscribe: true,
        orderbook_requires_snapshot_after_reconnect: true,
        ..StreamRuntimeCapability::default()
    }
}

pub fn coinex_heartbeat_policy() -> HeartbeatPolicy {
    HeartbeatPolicy {
        direction: HeartbeatDirection::ClientPing,
        ping_interval_ms: 30_000,
        pong_timeout_ms: 10_000,
        stale_message_ms: 45_000,
        requires_pong_payload_echo: false,
    }
}

pub fn coinex_auth_renewal_policy() -> AuthRenewalPolicy {
    AuthRenewalPolicy {
        kind: AuthRenewalKind::ReLogin,
        renew_before_expiry_ms: 60_000,
        renewal_interval_ms: Some(30 * 60 * 1000),
        reconnect_on_renewal_failure: true,
        resubscribe_after_renewal: true,
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn coinex_rate_limit_plan() -> RateLimitPlan {
    RateLimitPlan::fixed_window([
        RateLimitBucket::new("coinex.rest", RateLimitScope::Rest, 400, 60_000).with_burst(40),
        RateLimitBucket::new("coinex.orders", RateLimitScope::Orders, 120, 60_000).with_burst(20),
        RateLimitBucket::new("coinex.ws", RateLimitScope::WebSocket, 100, 60_000).with_burst(20),
    ])
}

#[cfg_attr(not(test), allow(dead_code))]
pub fn coinex_reconcile_plan(
    exchange: ExchangeId,
    trigger: ReconcileTrigger,
    symbol: Option<rustcta_exchange_api::SymbolScope>,
    reason: impl Into<String>,
) -> ReconcilePlan {
    ReconcilePlan {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        exchange,
        trigger,
        state: rustcta_exchange_api::OrderReconcileState::UnknownNeedsRetry,
        symbol,
        idempotency_key: None,
        unknown_order_policy: UnknownOrderPolicy::QueryByClientOrderId,
        retry_policy: RetryReconcilePolicy::default(),
        requires_query_order: true,
        requires_open_orders: true,
        requires_recent_fills: true,
        allow_order_replay: false,
        reason: reason.into(),
    }
}

pub fn coinex_endpoint_capabilities(private_enabled: bool) -> Vec<EndpointCapability> {
    let private_support = if private_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("CoinEx private REST disabled")
    };
    vec![
        rest_endpoint(
            "get_symbol_rules",
            CapabilitySupport::native(),
            "GET",
            "/spot/market",
            EndpointAuth::None,
            vec![CredentialScope::ReadOnly],
            Some("coinex.rest"),
            1,
        ),
        rest_endpoint(
            "get_order_book",
            CapabilitySupport::native(),
            "GET",
            "/spot/depth",
            EndpointAuth::None,
            vec![CredentialScope::ReadOnly],
            Some("coinex.rest"),
            1,
        ),
        rest_endpoint(
            "get_balances",
            private_support.clone(),
            "GET",
            "/assets/spot/balance",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            Some("coinex.rest"),
            1,
        ),
        rest_endpoint(
            "get_fees",
            private_support.clone(),
            "GET",
            "/spot/market",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            Some("coinex.rest"),
            1,
        ),
        rest_endpoint(
            "place_order",
            private_support.clone(),
            "POST",
            "/spot/order",
            EndpointAuth::Hmac,
            vec![CredentialScope::Trade],
            Some("coinex.orders"),
            1,
        ),
        rest_endpoint(
            "cancel_order",
            private_support.clone(),
            "DELETE",
            "/spot/order",
            EndpointAuth::Hmac,
            vec![CredentialScope::Trade],
            Some("coinex.orders"),
            1,
        ),
        rest_endpoint(
            "cancel_all_orders",
            private_support.clone(),
            "POST",
            "/spot/cancel-all-order",
            EndpointAuth::Hmac,
            vec![CredentialScope::Trade],
            Some("coinex.orders"),
            5,
        ),
        rest_endpoint(
            "amend_order",
            private_support.clone(),
            "POST",
            "/spot/modify-order",
            EndpointAuth::Hmac,
            vec![CredentialScope::Trade],
            Some("coinex.orders"),
            1,
        ),
        rest_endpoint(
            "query_order",
            private_support.clone(),
            "GET",
            "/spot/order-status",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            Some("coinex.rest"),
            1,
        ),
        rest_endpoint(
            "get_open_orders",
            private_support.clone(),
            "GET",
            "/spot/pending-order",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            Some("coinex.rest"),
            1,
        ),
        rest_endpoint(
            "get_recent_fills",
            private_support,
            "GET",
            "/spot/finished-order",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            Some("coinex.rest"),
            1,
        ),
    ]
}

fn rest_endpoint(
    operation: &str,
    support: CapabilitySupport,
    method: &str,
    path: &str,
    auth: EndpointAuth,
    credential_scopes: Vec<CredentialScope>,
    rate_limit_bucket: Option<&str>,
    weight: u32,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support,
        market_types: vec![MarketType::Spot, MarketType::Perpetual],
        transport: EndpointTransport::Rest,
        method: Some(method.to_string()),
        path: Some(path.to_string()),
        auth,
        credential_scopes,
        rate_limit_bucket: rate_limit_bucket.map(str::to_string),
        weight: Some(weight),
        supports_testnet: false,
    }
}
