#![allow(dead_code)]

use rustcta_exchange_api::{
    AuthRenewalKind, AuthRenewalPolicy, BatchAtomicity, BatchCapability, BatchExecutionMode,
    CapabilitySupport, CredentialScope, EndpointAuth, EndpointCapability, EndpointTransport,
    ExchangeClientCapabilitiesV2, HeartbeatDirection, HeartbeatPolicy, HistoryCapability,
    RateLimitBucket, RateLimitPlan, RateLimitScope, ReconcilePlan, ReconcileTrigger,
    RetryReconcilePolicy, StreamRuntimeCapability, UnknownOrderPolicy, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType};

pub const BIGONE_MAX_PAGE_LIMIT: u32 = 200;
pub const BIGONE_SPOT_BATCH_MAX_ITEMS: u32 = 20;
pub const BIGONE_CONTRACT_COMPOSED_BATCH_MAX_ITEMS: u32 = 10;

pub fn bigone_capabilities_v2(private_enabled: bool) -> ExchangeClientCapabilitiesV2 {
    let private_rest = if private_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(
            "BigONE private REST requires API key/secret and explicit enablement",
        )
    };
    ExchangeClientCapabilitiesV2 {
        public_rest: CapabilitySupport::native(),
        private_rest: private_rest.clone(),
        public_streams: CapabilitySupport::ws_only(
            "BigONE public stream session helpers cover subscribe, heartbeat and parsers; shared supervisor integration is adapter-local",
        ),
        private_streams: if private_enabled {
            CapabilitySupport::ws_only(
                "BigONE private streams use JWT login and REST reconciliation remains source of truth",
            )
        } else {
            CapabilitySupport::unsupported("BigONE private WebSocket requires private REST credentials")
        },
        stream_runtime: bigone_stream_runtime_capability(),
        batch_place_orders: if private_enabled {
            BatchCapability {
                support: CapabilitySupport::composed(
                    "BigONE Spot uses a native multi-order endpoint; contract orders are composed sequentially by the adapter",
                ),
                mode: BatchExecutionMode::ComposedSequential,
                atomicity: BatchAtomicity::Partial,
                max_items: Some(BIGONE_SPOT_BATCH_MAX_ITEMS),
                same_symbol_required: false,
                same_market_type_required: true,
                supports_client_order_id: true,
                supports_partial_failure: true,
            }
        } else {
            BatchCapability::unsupported("BigONE batch place requires private REST")
        },
        batch_cancel_orders: if private_enabled {
            BatchCapability {
                support: CapabilitySupport::composed(
                    "BigONE contract has a native multi-cancel endpoint; Spot cancel is composed sequentially by the adapter",
                ),
                mode: BatchExecutionMode::ComposedSequential,
                atomicity: BatchAtomicity::Partial,
                max_items: Some(BIGONE_SPOT_BATCH_MAX_ITEMS),
                same_symbol_required: false,
                same_market_type_required: true,
                supports_client_order_id: true,
                supports_partial_failure: true,
            }
        } else {
            BatchCapability::unsupported("BigONE batch cancel requires private REST")
        },
        cancel_all_orders: if private_enabled {
            CapabilitySupport::composed(
                "BigONE cancel-all is reconciled from open orders then composed through single cancel",
            )
        } else {
            CapabilitySupport::unsupported("BigONE cancel-all requires private REST")
        },
        order_history: HistoryCapability {
            support: private_rest.clone(),
            supports_since: false,
            supports_until: false,
            supports_limit: true,
            supports_cursor: false,
            supports_from_id: false,
            max_limit: Some(BIGONE_MAX_PAGE_LIMIT),
            max_window_ms: None,
        },
        fills_history: HistoryCapability {
            support: private_rest,
            supports_since: false,
            supports_until: false,
            supports_limit: true,
            supports_cursor: false,
            supports_from_id: false,
            max_limit: Some(BIGONE_MAX_PAGE_LIMIT),
            max_window_ms: None,
        },
        endpoints: bigone_endpoint_capabilities(private_enabled),
        credential_scopes: if private_enabled {
            vec![CredentialScope::ReadOnly, CredentialScope::Trade]
        } else {
            Vec::new()
        },
    }
}

pub fn bigone_stream_runtime_capability() -> StreamRuntimeCapability {
    let auth_renewal = bigone_auth_renewal_policy();
    StreamRuntimeCapability {
        public: CapabilitySupport::native(),
        private: CapabilitySupport::native(),
        supports_subscribe: true,
        supports_unsubscribe: false,
        supports_public_subscribe: true,
        supports_public_unsubscribe: false,
        supports_private_subscribe: true,
        supports_private_unsubscribe: false,
        heartbeat: rustcta_exchange_api::HeartbeatCapability {
            supported: true,
            required: true,
            direction: rustcta_exchange_api::StreamHeartbeatDirection::ClientPing,
            interval_ms: Some(15_000),
            timeout_ms: Some(30_000),
        },
        reconnect: rustcta_exchange_api::ReconnectCapability {
            supported: true,
            requires_resubscribe: true,
            preserves_session: false,
            max_reconnect_attempts: None,
        },
        resync: rustcta_exchange_api::StreamResyncCapability {
            order_book: true,
            balances: true,
            positions: true,
            orders: true,
        },
        auth: rustcta_exchange_api::StreamAuthCapability {
            required: true,
            credential_scopes: vec![CredentialScope::ReadOnly, CredentialScope::Trade],
            renewal_ms: Some(25 * 60 * 1000),
            uses_listen_key: false,
            requires_relogin_on_reconnect: true,
        },
        public_private_separate_connections: true,
        heartbeat_policy: bigone_heartbeat_policy(),
        auth_renewal: auth_renewal.clone(),
        auth_renewal_policy: auth_renewal,
        reconnect_requires_login: true,
        reconnect_requires_resubscribe: true,
        orderbook_requires_snapshot_after_reconnect: true,
    }
}

pub fn bigone_heartbeat_policy() -> HeartbeatPolicy {
    HeartbeatPolicy {
        direction: HeartbeatDirection::ClientPing,
        ping_interval_ms: 15_000,
        pong_timeout_ms: 30_000,
        stale_message_ms: 45_000,
        requires_pong_payload_echo: false,
    }
}

pub fn bigone_auth_renewal_policy() -> AuthRenewalPolicy {
    AuthRenewalPolicy {
        kind: AuthRenewalKind::JwtRefresh,
        renew_before_expiry_ms: 60_000,
        renewal_interval_ms: Some(25 * 60 * 1000),
        reconnect_on_renewal_failure: true,
        resubscribe_after_renewal: true,
    }
}

pub fn bigone_rate_limit_plan() -> RateLimitPlan {
    RateLimitPlan::fixed_window([
        RateLimitBucket::new("bigone.rest.public", RateLimitScope::Rest, 120, 60_000)
            .with_burst(20),
        RateLimitBucket::new("bigone.rest.private", RateLimitScope::Account, 60, 60_000)
            .with_burst(10),
        RateLimitBucket::new("bigone.orders", RateLimitScope::Orders, 30, 60_000).with_burst(5),
        RateLimitBucket::new("bigone.ws", RateLimitScope::WebSocket, 60, 60_000).with_burst(10),
    ])
}

pub fn bigone_reconcile_plan(
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

fn bigone_endpoint_capabilities(private_enabled: bool) -> Vec<EndpointCapability> {
    let private_support = if private_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("BigONE private REST disabled")
    };
    vec![
        rest_endpoint(
            "get_symbol_rules",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "GET",
            "/api/v3/asset_pairs|/api/contract/v2/instruments",
            EndpointAuth::None,
            vec![CredentialScope::ReadOnly],
            "bigone.rest.public",
            1,
        ),
        rest_endpoint(
            "get_order_book",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "GET",
            "/api/v3/asset_pairs/{asset_pair_name}/depth|/api/contract/v2/depth",
            EndpointAuth::None,
            vec![CredentialScope::ReadOnly],
            "bigone.rest.public",
            1,
        ),
        rest_endpoint(
            "get_balances",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "GET",
            "/api/v3/viewer/accounts|/api/contract/v2/accounts",
            EndpointAuth::Jwt,
            vec![CredentialScope::ReadOnly],
            "bigone.rest.private",
            1,
        ),
        rest_endpoint(
            "get_positions",
            private_support.clone(),
            vec![MarketType::Perpetual],
            "GET",
            "/api/contract/v2/positions",
            EndpointAuth::Jwt,
            vec![CredentialScope::ReadOnly],
            "bigone.rest.private",
            1,
        ),
        rest_endpoint(
            "place_order",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "POST",
            "/api/v3/viewer/orders|/api/contract/v2/orders",
            EndpointAuth::Jwt,
            vec![CredentialScope::Trade],
            "bigone.orders",
            1,
        ),
        rest_endpoint(
            "cancel_order",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "POST",
            "/api/v3/viewer/orders/{order_id}/cancel|DELETE /api/contract/v2/order",
            EndpointAuth::Jwt,
            vec![CredentialScope::Trade],
            "bigone.orders",
            1,
        ),
        rest_endpoint(
            "batch_place_orders",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "POST",
            "/api/v3/viewer/orders/multi|composed /api/contract/v2/orders",
            EndpointAuth::Jwt,
            vec![CredentialScope::Trade],
            "bigone.orders",
            5,
        ),
        rest_endpoint(
            "batch_cancel_orders",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "POST",
            "composed /api/v3/viewer/orders/{order_id}/cancel|/api/contract/v2/orders/cancel",
            EndpointAuth::Jwt,
            vec![CredentialScope::Trade],
            "bigone.orders",
            5,
        ),
        rest_endpoint(
            "cancel_all_orders",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "COMPOSED",
            "open_orders+cancel_order",
            EndpointAuth::Jwt,
            vec![CredentialScope::Trade],
            "bigone.orders",
            10,
        ),
        rest_endpoint(
            "query_order",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "GET",
            "/api/v3/viewer/order|/api/contract/v2/order",
            EndpointAuth::Jwt,
            vec![CredentialScope::ReadOnly],
            "bigone.rest.private",
            1,
        ),
        rest_endpoint(
            "get_open_orders",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "GET",
            "/api/v3/viewer/orders|/api/contract/v2/orders",
            EndpointAuth::Jwt,
            vec![CredentialScope::ReadOnly],
            "bigone.rest.private",
            1,
        ),
        rest_endpoint(
            "get_recent_fills",
            private_support,
            vec![MarketType::Spot, MarketType::Perpetual],
            "GET",
            "/api/v3/viewer/trades|/api/contract/v2/fills",
            EndpointAuth::Jwt,
            vec![CredentialScope::ReadOnly],
            "bigone.rest.private",
            1,
        ),
    ]
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
