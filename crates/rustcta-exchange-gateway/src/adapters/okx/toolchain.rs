use rustcta_exchange_api::{
    BatchAtomicity, BatchCapability, CapabilitySupport, CredentialScope, EndpointAuth,
    EndpointCapability, EndpointTransport, ExchangeClientCapabilities, HeartbeatCapability,
    HeartbeatDirection, HeartbeatPolicy, HistoryCapability, ReconnectCapability,
    StreamHeartbeatDirection, StreamResyncCapability, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

pub(super) fn apply_toolchain_capabilities(
    capabilities: &mut ExchangeClientCapabilities,
    private_rest_enabled: bool,
    exchange_id: &str,
    public_streams_enabled: bool,
) {
    let label = profile_label(exchange_id);
    let regional_profile = exchange_id != "okx";
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = if private_rest_enabled {
        CapabilitySupport::native()
    } else if regional_profile {
        CapabilitySupport::unsupported(format!(
            "{label} private REST is disabled pending regional credential scope audit"
        ))
    } else {
        CapabilitySupport::unsupported(
            "OKX private REST requires enabled API key, secret, and passphrase",
        )
    };
    capabilities.capabilities_v2.public_streams = if public_streams_enabled {
        CapabilitySupport::rest_fallback(format!(
            "{label} public WS books5/books/bbo-tbt subscription is mapped; REST books snapshot remains the resync source"
        ))
    } else {
        CapabilitySupport::unsupported(format!("{label} public streams disabled by config"))
    };
    capabilities.capabilities_v2.private_streams = CapabilitySupport::unsupported(format!(
        "{label} private WS login/order stream is not wired; use REST reconciliation after private enablement"
    ));
    capabilities.capabilities_v2.stream_runtime = stream_runtime(public_streams_enabled);
    capabilities.capabilities_v2.batch_place_orders = if private_rest_enabled {
        let mut capability = BatchCapability::native(BatchAtomicity::Partial, Some(20));
        capability.same_market_type_required = true;
        capability.supports_client_order_id = true;
        capability.supports_partial_failure = true;
        capability
    } else {
        BatchCapability::unsupported(format!("{label} batch place order requires private REST"))
    };
    capabilities.capabilities_v2.batch_cancel_orders = if private_rest_enabled {
        let mut capability = BatchCapability::native(BatchAtomicity::Partial, Some(20));
        capability.same_market_type_required = true;
        capability.supports_client_order_id = true;
        capability.supports_partial_failure = true;
        capability
    } else {
        BatchCapability::unsupported(format!("{label} batch cancel order requires private REST"))
    };
    capabilities.capabilities_v2.cancel_all_orders = if private_rest_enabled {
        CapabilitySupport::composed("loads open orders then calls native cancel-batch-orders")
    } else if regional_profile {
        CapabilitySupport::unsupported(format!(
            "{label} cancel-all is disabled pending regional private REST audit"
        ))
    } else {
        CapabilitySupport::unsupported("OKX cancel-all requires private REST credentials")
    };
    capabilities.capabilities_v2.order_history = HistoryCapability {
        support: if private_rest_enabled {
            CapabilitySupport::native()
        } else if regional_profile {
            CapabilitySupport::unsupported(format!(
                "{label} order queries are disabled pending regional private REST audit"
            ))
        } else {
            CapabilitySupport::unsupported("OKX order queries require private REST credentials")
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
        } else if regional_profile {
            CapabilitySupport::unsupported(format!(
                "{label} fills-history is disabled pending regional private REST audit"
            ))
        } else {
            CapabilitySupport::unsupported("OKX fills-history requires private REST credentials")
        },
        supports_since: true,
        supports_until: true,
        supports_limit: true,
        supports_cursor: false,
        supports_from_id: false,
        max_limit: Some(100),
        max_window_ms: None,
    };
    capabilities.capabilities_v2.funding_rates = if exchange_id == "okx" {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(format!(
            "{label} funding rates require OKX derivatives market access"
        ))
    };
    capabilities.capabilities_v2.credential_scopes = if private_rest_enabled {
        vec![CredentialScope::ReadOnly, CredentialScope::Trade]
    } else {
        Vec::new()
    };
    capabilities.capabilities_v2.endpoints =
        endpoint_capabilities(private_rest_enabled, exchange_id);
    capabilities.apply_v2_to_legacy_flags();
}

fn stream_runtime(public_streams_enabled: bool) -> StreamRuntimeCapability {
    StreamRuntimeCapability {
        public: if public_streams_enabled {
            CapabilitySupport::rest_fallback(
                "REST /api/v5/market/books snapshot required after reconnect or seqId gap",
            )
        } else {
            CapabilitySupport::unsupported("public streams disabled")
        },
        private: CapabilitySupport::unsupported("private WS login/order stream is not wired"),
        supports_subscribe: public_streams_enabled,
        supports_unsubscribe: public_streams_enabled,
        supports_public_subscribe: public_streams_enabled,
        supports_public_unsubscribe: public_streams_enabled,
        supports_private_subscribe: false,
        supports_private_unsubscribe: false,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::ClientPing,
            interval_ms: Some(25_000),
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
            balances: false,
            positions: false,
            orders: false,
        },
        heartbeat_policy: HeartbeatPolicy {
            direction: HeartbeatDirection::ClientPing,
            ping_interval_ms: 25_000,
            pong_timeout_ms: 30_000,
            stale_message_ms: 30_000,
            requires_pong_payload_echo: false,
        },
        reconnect_requires_login: false,
        reconnect_requires_resubscribe: true,
        orderbook_requires_snapshot_after_reconnect: true,
        ..StreamRuntimeCapability::default()
    }
}

fn endpoint_capabilities(private_rest_enabled: bool, exchange_id: &str) -> Vec<EndpointCapability> {
    let regional_profile = exchange_id != "okx";
    let mut endpoints = vec![
        rest_endpoint(
            "get_symbol_rules",
            CapabilitySupport::native(),
            "GET",
            "/api/v5/public/instruments",
            EndpointAuth::None,
            Vec::new(),
            Some("public_ip"),
            Some(1),
        ),
        rest_endpoint(
            "get_order_book",
            CapabilitySupport::native(),
            "GET",
            "/api/v5/market/books",
            EndpointAuth::None,
            Vec::new(),
            Some("public_ip"),
            Some(1),
        ),
    ];

    let private_support = if private_rest_enabled {
        CapabilitySupport::native()
    } else if exchange_id != "okx" {
        CapabilitySupport::unsupported(format!(
            "{} private REST disabled pending regional credential scope audit",
            profile_label(exchange_id)
        ))
    } else {
        CapabilitySupport::unsupported("private REST credentials unavailable")
    };
    for (operation, method, path, credential_scope, weight) in [
        (
            "get_balances",
            "GET",
            "/api/v5/account/balance",
            CredentialScope::ReadOnly,
            1,
        ),
        (
            "get_fees",
            "GET",
            "/api/v5/account/trade-fee",
            CredentialScope::ReadOnly,
            1,
        ),
        (
            "get_positions",
            "GET",
            "/api/v5/account/positions",
            CredentialScope::ReadOnly,
            1,
        ),
        (
            "place_order",
            "POST",
            "/api/v5/trade/order",
            CredentialScope::Trade,
            1,
        ),
        (
            "place_quote_market_order",
            "POST",
            "/api/v5/trade/order",
            CredentialScope::Trade,
            1,
        ),
        (
            "cancel_order",
            "POST",
            "/api/v5/trade/cancel-order",
            CredentialScope::Trade,
            1,
        ),
        (
            "batch_place_orders",
            "POST",
            "/api/v5/trade/batch-orders",
            CredentialScope::Trade,
            1,
        ),
        (
            "batch_cancel_orders",
            "POST",
            "/api/v5/trade/cancel-batch-orders",
            CredentialScope::Trade,
            1,
        ),
        (
            "cancel_all_orders",
            "POST",
            "/api/v5/trade/cancel-batch-orders",
            CredentialScope::Trade,
            1,
        ),
        (
            "amend_order",
            "POST",
            "/api/v5/trade/amend-order",
            CredentialScope::Trade,
            1,
        ),
        (
            "query_order",
            "GET",
            "/api/v5/trade/order",
            CredentialScope::ReadOnly,
            1,
        ),
        (
            "get_open_orders",
            "GET",
            "/api/v5/trade/orders-pending",
            CredentialScope::ReadOnly,
            1,
        ),
        (
            "get_recent_fills",
            "GET",
            "/api/v5/trade/fills-history",
            CredentialScope::ReadOnly,
            1,
        ),
    ] {
        let (method, path, auth, rate_limit_bucket, weight) =
            if !private_rest_enabled && exchange_id != "okx" {
                (
                    "UNSUPPORTED",
                    format!("/unsupported/{exchange_id}/{operation}"),
                    EndpointAuth::None,
                    Some("unsupported"),
                    Some(0),
                )
            } else {
                (
                    method,
                    path.to_string(),
                    EndpointAuth::Hmac,
                    Some("private_uid"),
                    Some(weight),
                )
            };
        let mut endpoint = rest_endpoint(
            operation,
            private_support.clone(),
            method,
            &path,
            auth,
            vec![credential_scope],
            rate_limit_bucket,
            weight,
        );
        if operation == "place_quote_market_order" {
            endpoint.market_types = vec![MarketType::Spot];
        }
        endpoints.push(endpoint);
    }

    if regional_profile {
        for endpoint in &mut endpoints {
            endpoint.market_types = vec![MarketType::Spot];
        }
    }

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
        market_types: vec![
            MarketType::Spot,
            MarketType::Perpetual,
            MarketType::Futures,
            MarketType::Option,
        ],
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

fn profile_label(exchange_id: &str) -> &'static str {
    match exchange_id {
        "myokx" => "MyOKX",
        "okxus" => "OKX US",
        _ => "OKX",
    }
}
