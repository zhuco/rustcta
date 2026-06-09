use rustcta_exchange_api::{
    BatchAtomicity, BatchCapability, BatchExecutionMode, CapabilitySupport, CredentialScope,
    EndpointAuth, EndpointCapability, EndpointTransport, ExchangeClientCapabilities,
    HistoryCapability,
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
            "Binance COIN-M private REST requires enabled API key and secret",
        )
    };
    capabilities.capabilities_v2.public_streams = CapabilitySupport::unsupported(
        "Binance COIN-M public WS runtime is not wired in this adapter yet",
    );
    capabilities.capabilities_v2.private_streams = CapabilitySupport::unsupported(
        "Binance COIN-M private user data stream is not wired; use REST reconciliation through account, open orders, query order, and myTrades",
    );
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
        BatchCapability::unsupported("Binance COIN-M batch place requires private REST credentials")
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
        BatchCapability::unsupported(
            "Binance COIN-M batch cancel requires private REST credentials",
        )
    };
    capabilities.capabilities_v2.cancel_all_orders = if private_rest_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(
            "Binance COIN-M cancel-all requires private REST credentials",
        )
    };
    capabilities.capabilities_v2.order_history = HistoryCapability {
        support: if private_rest_enabled {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "Binance COIN-M order history requires private REST credentials",
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
                "Binance COIN-M myTrades requires private REST credentials",
            )
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

fn endpoint_capabilities(private_rest_enabled: bool) -> Vec<EndpointCapability> {
    let mut endpoints = vec![
        rest_endpoint(
            "get_symbol_rules",
            CapabilitySupport::native(),
            "GET",
            "/dapi/v1/exchangeInfo",
            EndpointAuth::None,
            Vec::new(),
            Some("public_ip"),
            Some(10),
        ),
        rest_endpoint(
            "get_order_book",
            CapabilitySupport::native(),
            "GET",
            "/dapi/v1/depth",
            EndpointAuth::None,
            Vec::new(),
            Some("public_ip"),
            Some(5),
        ),
    ];

    let private_support = if private_rest_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("private REST credentials unavailable")
    };
    for (operation, method, path, credential_scope, weight) in [
        (
            "get_balances",
            "GET",
            "/dapi/v1/balance",
            CredentialScope::ReadOnly,
            20,
        ),
        (
            "get_positions",
            "GET",
            "/dapi/v1/positionRisk",
            CredentialScope::ReadOnly,
            20,
        ),
        (
            "get_fees",
            "GET",
            "/dapi/v1/account/commission",
            CredentialScope::ReadOnly,
            20,
        ),
        (
            "place_order",
            "POST",
            "/dapi/v1/order",
            CredentialScope::Trade,
            1,
        ),
        (
            "cancel_order",
            "DELETE",
            "/dapi/v1/order",
            CredentialScope::Trade,
            1,
        ),
        (
            "batch_place_orders",
            "POST",
            "/dapi/v1/batchOrders",
            CredentialScope::Trade,
            5,
        ),
        (
            "batch_cancel_orders",
            "DELETE",
            "/dapi/v1/batchOrders",
            CredentialScope::Trade,
            1,
        ),
        (
            "cancel_all_orders",
            "DELETE",
            "/dapi/v1/allOpenOrders",
            CredentialScope::Trade,
            1,
        ),
        (
            "amend_order",
            "PUT",
            "/dapi/v1/order",
            CredentialScope::Trade,
            1,
        ),
        (
            "query_order",
            "GET",
            "/dapi/v1/order",
            CredentialScope::ReadOnly,
            4,
        ),
        (
            "get_open_orders",
            "GET",
            "/dapi/v1/openOrders",
            CredentialScope::ReadOnly,
            6,
        ),
        (
            "get_recent_fills",
            "GET",
            "/dapi/v1/myTrades",
            CredentialScope::ReadOnly,
            20,
        ),
    ] {
        endpoints.push(rest_endpoint(
            operation,
            private_support.clone(),
            method,
            path,
            EndpointAuth::Hmac,
            vec![credential_scope],
            Some("signed_uid"),
            Some(weight),
        ));
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
        market_types: vec![MarketType::Futures, MarketType::Perpetual],
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
