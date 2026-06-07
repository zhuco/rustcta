use rustcta_exchange_api::{
    BatchCapability, CapabilitySupport, CredentialScope, EndpointAuth, EndpointCapability,
    EndpointTransport, ExchangeClientCapabilities, HistoryCapability,
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
        CapabilitySupport::unsupported("Binance private REST requires enabled API key and secret")
    };
    capabilities.capabilities_v2.public_streams = CapabilitySupport::unsupported(
        "Binance public WS runtime is not wired in this adapter yet",
    );
    capabilities.capabilities_v2.private_streams = CapabilitySupport::unsupported(
        "Binance private user data stream is not wired; use REST reconciliation through account, open orders, query order, and myTrades",
    );
    capabilities.capabilities_v2.batch_place_orders =
        BatchCapability::unsupported("Binance batch place order is not implemented");
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported("Binance batch cancel order is not implemented");
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

fn endpoint_capabilities(private_rest_enabled: bool) -> Vec<EndpointCapability> {
    let mut endpoints = vec![
        rest_endpoint(
            "get_symbol_rules",
            CapabilitySupport::native(),
            "GET",
            "/api/v3/exchangeInfo",
            EndpointAuth::None,
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
            "/api/v3/account",
            CredentialScope::ReadOnly,
            20,
        ),
        (
            "get_fees",
            "GET",
            "/api/v3/account/commission",
            CredentialScope::ReadOnly,
            20,
        ),
        (
            "place_order",
            "POST",
            "/api/v3/order",
            CredentialScope::Trade,
            1,
        ),
        (
            "place_quote_market_order",
            "POST",
            "/api/v3/order",
            CredentialScope::Trade,
            1,
        ),
        (
            "cancel_order",
            "DELETE",
            "/api/v3/order",
            CredentialScope::Trade,
            1,
        ),
        (
            "cancel_all_orders",
            "DELETE",
            "/api/v3/openOrders",
            CredentialScope::Trade,
            1,
        ),
        (
            "amend_order",
            "PUT",
            "/api/v3/order/amend/keepPriority",
            CredentialScope::Trade,
            4,
        ),
        (
            "place_order_list",
            "POST",
            "/api/v3/orderList/oco",
            CredentialScope::Trade,
            1,
        ),
        (
            "query_order",
            "GET",
            "/api/v3/order",
            CredentialScope::ReadOnly,
            4,
        ),
        (
            "get_open_orders",
            "GET",
            "/api/v3/openOrders",
            CredentialScope::ReadOnly,
            6,
        ),
        (
            "get_recent_fills",
            "GET",
            "/api/v3/myTrades",
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
        market_types: vec![MarketType::Spot],
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
