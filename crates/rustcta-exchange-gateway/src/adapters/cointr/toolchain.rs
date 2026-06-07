#![allow(dead_code)]

use rustcta_exchange_api::{
    AuthRenewalKind, AuthRenewalPolicy, BatchAtomicity, BatchCapability, BatchExecutionMode,
    CapabilitySupport, CredentialScope, EndpointAuth, EndpointCapability, EndpointTransport,
    ExchangeClientCapabilities, HeartbeatCapability, HeartbeatDirection, HeartbeatPolicy,
    HistoryCapability, RateLimitBucket, RateLimitPlan, RateLimitScope, ReconcilePlan,
    ReconcileTrigger, ReconnectCapability, RetryReconcilePolicy, StreamAuthCapability,
    StreamHeartbeatDirection, StreamResyncCapability, StreamRuntimeCapability, UnknownOrderPolicy,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType};

pub(super) const COINTR_MAX_BATCH_ITEMS: u32 = 20;
pub(super) const COINTR_MAX_RECENT_FILL_LIMIT: u32 = 100;

pub(super) fn apply_cointr_toolchain_capabilities(
    capabilities: &mut ExchangeClientCapabilities,
    private_enabled: bool,
) {
    capabilities.refresh_v2_from_legacy_flags();
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = if private_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(
            "CoinTR private REST requires API key, secret, passphrase, and explicit enablement",
        )
    };
    capabilities.capabilities_v2.public_streams = CapabilitySupport::ws_only(
        "CoinTR public WebSocket session helpers expose subscribe, heartbeat, and parsers for shared runtime integration",
    );
    capabilities.capabilities_v2.private_streams = if private_enabled {
        CapabilitySupport::rest_fallback(
            "CoinTR private WebSocket login/parser specs are present; REST query/open-orders/fills reconciliation remains the source of truth after reconnect",
        )
    } else {
        CapabilitySupport::unsupported("CoinTR private streams require private credentials")
    };
    capabilities.capabilities_v2.credential_scopes = if private_enabled {
        vec![CredentialScope::ReadOnly, CredentialScope::Trade]
    } else {
        Vec::new()
    };
    capabilities.capabilities_v2.stream_runtime = cointr_stream_runtime_capability(private_enabled);
    capabilities.capabilities_v2.batch_place_orders =
        cointr_batch_place_capability(private_enabled);
    capabilities.capabilities_v2.batch_cancel_orders =
        cointr_batch_cancel_capability(private_enabled);
    capabilities.capabilities_v2.cancel_all_orders = if private_enabled {
        CapabilitySupport::composed(
            "CoinTR cancel-all is a symbol-scoped open-order sweep followed by single cancels",
        )
    } else {
        CapabilitySupport::unsupported("CoinTR cancel-all requires private REST credentials")
    };
    capabilities.capabilities_v2.order_history = HistoryCapability {
        support: if private_enabled {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("CoinTR order history requires private REST credentials")
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
            CapabilitySupport::unsupported("CoinTR fills history requires private REST credentials")
        },
        supports_since: false,
        supports_until: false,
        supports_limit: true,
        supports_cursor: false,
        supports_from_id: false,
        max_limit: Some(COINTR_MAX_RECENT_FILL_LIMIT),
        max_window_ms: None,
    };
    capabilities.capabilities_v2.endpoints = cointr_endpoint_capabilities(private_enabled);
}

pub(super) fn cointr_stream_runtime_capability(private_enabled: bool) -> StreamRuntimeCapability {
    let auth_renewal = cointr_auth_renewal_policy(private_enabled);
    let mut capability = StreamRuntimeCapability::default();
    capability.public = CapabilitySupport::ws_only(
        "CoinTR public WS uses JSON subscribe frames over native WebSocket; Socket.IO is not required for this adapter",
    );
    capability.private = if private_enabled {
        CapabilitySupport::rest_fallback(
            "Private WS supports login and user channels, with REST reconciliation on reconnect",
        )
    } else {
        CapabilitySupport::unsupported("CoinTR private WS requires private credentials")
    };
    capability.supports_subscribe = true;
    capability.supports_unsubscribe = false;
    capability.supports_public_subscribe = true;
    capability.supports_public_unsubscribe = false;
    capability.supports_private_subscribe = private_enabled;
    capability.supports_private_unsubscribe = false;
    capability.heartbeat = HeartbeatCapability {
        supported: true,
        required: true,
        direction: StreamHeartbeatDirection::Bidirectional,
        interval_ms: Some(15_000),
        timeout_ms: Some(30_000),
    };
    capability.reconnect = ReconnectCapability {
        supported: true,
        requires_resubscribe: true,
        preserves_session: false,
        max_reconnect_attempts: None,
    };
    capability.resync = StreamResyncCapability {
        order_book: true,
        balances: private_enabled,
        positions: private_enabled,
        orders: private_enabled,
    };
    capability.auth = StreamAuthCapability {
        required: private_enabled,
        credential_scopes: if private_enabled {
            vec![CredentialScope::ReadOnly, CredentialScope::Trade]
        } else {
            Vec::new()
        },
        renewal_ms: None,
        uses_listen_key: false,
        requires_relogin_on_reconnect: private_enabled,
    };
    capability.public_private_separate_connections = true;
    capability.heartbeat_policy = cointr_heartbeat_policy();
    capability.auth_renewal = auth_renewal.clone();
    capability.auth_renewal_policy = auth_renewal;
    capability.reconnect_requires_login = private_enabled;
    capability.reconnect_requires_resubscribe = true;
    capability.orderbook_requires_snapshot_after_reconnect = true;
    capability
}

pub(super) fn cointr_heartbeat_policy() -> HeartbeatPolicy {
    HeartbeatPolicy {
        direction: HeartbeatDirection::ClientPing,
        ping_interval_ms: 15_000,
        pong_timeout_ms: 30_000,
        stale_message_ms: 30_000,
        requires_pong_payload_echo: false,
    }
}

pub(super) fn cointr_auth_renewal_policy(private_enabled: bool) -> AuthRenewalPolicy {
    AuthRenewalPolicy {
        kind: if private_enabled {
            AuthRenewalKind::ReLogin
        } else {
            AuthRenewalKind::None
        },
        renew_before_expiry_ms: 60_000,
        renewal_interval_ms: None,
        reconnect_on_renewal_failure: true,
        resubscribe_after_renewal: true,
    }
}

pub(super) fn cointr_rate_limit_plan() -> RateLimitPlan {
    RateLimitPlan::fixed_window([
        RateLimitBucket::new("cointr.rest.public", RateLimitScope::Ip, 60, 60_000).with_burst(10),
        RateLimitBucket::new("cointr.rest.private", RateLimitScope::Account, 30, 60_000)
            .with_burst(5),
        RateLimitBucket::new("cointr.orders", RateLimitScope::Orders, 20, 60_000).with_burst(5),
        RateLimitBucket::new("cointr.ws", RateLimitScope::WebSocket, 60, 60_000).with_burst(10),
    ])
}

pub(super) fn cointr_reconcile_plan(
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

fn cointr_batch_place_capability(private_enabled: bool) -> BatchCapability {
    if private_enabled {
        BatchCapability {
            support: CapabilitySupport::native(),
            mode: BatchExecutionMode::Native,
            atomicity: BatchAtomicity::Partial,
            max_items: Some(COINTR_MAX_BATCH_ITEMS),
            same_symbol_required: false,
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: true,
        }
    } else {
        BatchCapability::unsupported("CoinTR batch place requires private REST credentials")
    }
}

fn cointr_batch_cancel_capability(private_enabled: bool) -> BatchCapability {
    if private_enabled {
        BatchCapability {
            support: CapabilitySupport::native(),
            mode: BatchExecutionMode::Native,
            atomicity: BatchAtomicity::Partial,
            max_items: Some(COINTR_MAX_BATCH_ITEMS),
            same_symbol_required: false,
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: true,
        }
    } else {
        BatchCapability::unsupported("CoinTR batch cancel requires private REST credentials")
    }
}

fn cointr_endpoint_capabilities(private_enabled: bool) -> Vec<EndpointCapability> {
    let private_support = if private_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("CoinTR private REST disabled")
    };
    let mut endpoints = vec![
        rest_endpoint(
            "get_symbol_rules",
            CapabilitySupport::native(),
            vec![MarketType::Spot],
            "GET",
            "/api/v2/spot/public/symbols",
            EndpointAuth::None,
            Vec::new(),
            "cointr.rest.public",
            1,
        ),
        rest_endpoint(
            "get_symbol_rules",
            CapabilitySupport::native(),
            vec![MarketType::Perpetual],
            "GET",
            "/api/v2/mix/market/contracts",
            EndpointAuth::None,
            Vec::new(),
            "cointr.rest.public",
            1,
        ),
        rest_endpoint(
            "get_order_book",
            CapabilitySupport::native(),
            vec![MarketType::Spot],
            "GET",
            "/api/v2/spot/market/orderbook",
            EndpointAuth::None,
            Vec::new(),
            "cointr.rest.public",
            1,
        ),
        rest_endpoint(
            "get_order_book",
            CapabilitySupport::native(),
            vec![MarketType::Perpetual],
            "GET",
            "/api/v2/mix/market/orderbook",
            EndpointAuth::None,
            Vec::new(),
            "cointr.rest.public",
            1,
        ),
        ws_endpoint(
            "subscribe_public_stream",
            CapabilitySupport::ws_only("CoinTR public stream session helper"),
            vec![MarketType::Spot, MarketType::Perpetual],
            "wss://ws.cointr.com/v2/ws/public",
            EndpointAuth::None,
            Vec::new(),
            "cointr.ws",
            1,
        ),
    ];
    for endpoint in cointr_private_rest_endpoints(private_support.clone()) {
        endpoints.push(endpoint);
    }
    endpoints.push(ws_endpoint(
        "subscribe_private_stream",
        if private_enabled {
            CapabilitySupport::rest_fallback(
                "CoinTR private stream parser requires REST reconciliation after reconnect",
            )
        } else {
            CapabilitySupport::unsupported("CoinTR private stream requires credentials")
        },
        vec![MarketType::Spot, MarketType::Perpetual],
        "wss://ws.cointr.com/v2/ws/private",
        EndpointAuth::Hmac,
        vec![CredentialScope::ReadOnly, CredentialScope::Trade],
        "cointr.ws",
        1,
    ));
    endpoints
}

fn cointr_private_rest_endpoints(private_support: CapabilitySupport) -> Vec<EndpointCapability> {
    let read_scopes = vec![CredentialScope::ReadOnly];
    let trade_scopes = vec![CredentialScope::Trade];
    vec![
        rest_endpoint(
            "get_balances",
            private_support.clone(),
            vec![MarketType::Spot],
            "GET",
            "/api/v2/spot/account/assets",
            EndpointAuth::Hmac,
            read_scopes.clone(),
            "cointr.rest.private",
            1,
        ),
        rest_endpoint(
            "get_balances",
            private_support.clone(),
            vec![MarketType::Perpetual],
            "GET",
            "/api/v2/mix/account/accounts",
            EndpointAuth::Hmac,
            read_scopes.clone(),
            "cointr.rest.private",
            1,
        ),
        rest_endpoint(
            "get_positions",
            private_support.clone(),
            vec![MarketType::Perpetual],
            "GET",
            "/api/v2/mix/position/all-position",
            EndpointAuth::Hmac,
            read_scopes.clone(),
            "cointr.rest.private",
            1,
        ),
        rest_endpoint(
            "get_fees",
            private_support.clone(),
            vec![MarketType::Spot],
            "GET",
            "/api/v2/spot/account/bills",
            EndpointAuth::Hmac,
            read_scopes.clone(),
            "cointr.rest.private",
            1,
        ),
        rest_endpoint(
            "place_order",
            private_support.clone(),
            vec![MarketType::Spot],
            "POST",
            "/api/v2/spot/trade/place-order",
            EndpointAuth::Hmac,
            trade_scopes.clone(),
            "cointr.orders",
            1,
        ),
        rest_endpoint(
            "place_order",
            private_support.clone(),
            vec![MarketType::Perpetual],
            "POST",
            "/api/v2/mix/order/place-order",
            EndpointAuth::Hmac,
            trade_scopes.clone(),
            "cointr.orders",
            1,
        ),
        rest_endpoint(
            "place_quote_market_order",
            private_support.clone(),
            vec![MarketType::Spot],
            "POST",
            "/api/v2/spot/trade/place-order",
            EndpointAuth::Hmac,
            trade_scopes.clone(),
            "cointr.orders",
            1,
        ),
        rest_endpoint(
            "cancel_order",
            private_support.clone(),
            vec![MarketType::Spot],
            "POST",
            "/api/v2/spot/trade/cancel-order",
            EndpointAuth::Hmac,
            trade_scopes.clone(),
            "cointr.orders",
            1,
        ),
        rest_endpoint(
            "cancel_order",
            private_support.clone(),
            vec![MarketType::Perpetual],
            "POST",
            "/api/v2/mix/order/cancel-order",
            EndpointAuth::Hmac,
            trade_scopes.clone(),
            "cointr.orders",
            1,
        ),
        rest_endpoint(
            "batch_place_orders",
            private_support.clone(),
            vec![MarketType::Spot],
            "POST",
            "/api/v2/spot/trade/batch-orders",
            EndpointAuth::Hmac,
            trade_scopes.clone(),
            "cointr.orders",
            5,
        ),
        rest_endpoint(
            "batch_place_orders",
            private_support.clone(),
            vec![MarketType::Perpetual],
            "POST",
            "/api/v2/mix/order/batch-place-order",
            EndpointAuth::Hmac,
            trade_scopes.clone(),
            "cointr.orders",
            5,
        ),
        rest_endpoint(
            "batch_cancel_orders",
            private_support.clone(),
            vec![MarketType::Spot],
            "POST",
            "/api/v2/spot/trade/batch-cancel-order",
            EndpointAuth::Hmac,
            trade_scopes.clone(),
            "cointr.orders",
            5,
        ),
        rest_endpoint(
            "batch_cancel_orders",
            private_support.clone(),
            vec![MarketType::Perpetual],
            "POST",
            "/api/v2/mix/order/batch-cancel-orders",
            EndpointAuth::Hmac,
            trade_scopes.clone(),
            "cointr.orders",
            5,
        ),
        rest_endpoint(
            "cancel_all_orders",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "COMPOSED",
            "get_open_orders+cancel_order",
            EndpointAuth::Hmac,
            trade_scopes.clone(),
            "cointr.orders",
            10,
        ),
        rest_endpoint(
            "query_order",
            private_support.clone(),
            vec![MarketType::Spot],
            "POST",
            "/api/v2/spot/trade/orderInfo",
            EndpointAuth::Hmac,
            read_scopes.clone(),
            "cointr.rest.private",
            1,
        ),
        rest_endpoint(
            "query_order",
            private_support.clone(),
            vec![MarketType::Perpetual],
            "POST",
            "/api/v2/mix/order/detail",
            EndpointAuth::Hmac,
            read_scopes.clone(),
            "cointr.rest.private",
            1,
        ),
        rest_endpoint(
            "get_open_orders",
            private_support.clone(),
            vec![MarketType::Spot],
            "POST",
            "/api/v2/spot/trade/unfilled-orders",
            EndpointAuth::Hmac,
            read_scopes.clone(),
            "cointr.rest.private",
            1,
        ),
        rest_endpoint(
            "get_open_orders",
            private_support.clone(),
            vec![MarketType::Perpetual],
            "POST",
            "/api/v2/mix/order/orders-pending",
            EndpointAuth::Hmac,
            read_scopes.clone(),
            "cointr.rest.private",
            1,
        ),
        rest_endpoint(
            "get_recent_fills",
            private_support.clone(),
            vec![MarketType::Spot],
            "POST",
            "/api/v2/spot/trade/fills",
            EndpointAuth::Hmac,
            read_scopes.clone(),
            "cointr.rest.private",
            1,
        ),
        rest_endpoint(
            "get_recent_fills",
            private_support,
            vec![MarketType::Perpetual],
            "POST",
            "/api/v2/mix/order/fills",
            EndpointAuth::Hmac,
            read_scopes,
            "cointr.rest.private",
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
        operation: format!("cointr.{operation}"),
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

fn ws_endpoint(
    operation: &str,
    support: CapabilitySupport,
    market_types: Vec<MarketType>,
    path: &str,
    auth: EndpointAuth,
    credential_scopes: Vec<CredentialScope>,
    rate_limit_bucket: &str,
    weight: u32,
) -> EndpointCapability {
    EndpointCapability {
        operation: format!("cointr.{operation}"),
        support,
        market_types,
        transport: EndpointTransport::WebSocket,
        method: Some("SUBSCRIBE".to_string()),
        path: Some(path.to_string()),
        auth,
        credential_scopes,
        rate_limit_bucket: Some(rate_limit_bucket.to_string()),
        weight: Some(weight),
        supports_testnet: false,
    }
}
