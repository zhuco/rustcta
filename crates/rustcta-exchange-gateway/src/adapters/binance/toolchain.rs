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
    let private_support = if private_rest_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("private REST credentials unavailable")
    };
    let mut endpoints = vec![
        rest_endpoint(
            "get_symbol_rules",
            CapabilitySupport::native(),
            "GET",
            "/api/v3/exchangeInfo",
            EndpointAuth::None,
            vec![MarketType::Spot],
            Vec::new(),
            Some("public_ip"),
            Some(10),
        ),
        rest_endpoint(
            "get_symbol_rules",
            CapabilitySupport::native(),
            "GET",
            "/fapi/v1/exchangeInfo",
            EndpointAuth::None,
            vec![MarketType::Perpetual],
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
            vec![MarketType::Spot],
            Vec::new(),
            Some("public_ip"),
            Some(5),
        ),
        rest_endpoint(
            "get_order_book",
            CapabilitySupport::native(),
            "GET",
            "/fapi/v1/depth",
            EndpointAuth::None,
            vec![MarketType::Perpetual],
            Vec::new(),
            Some("public_ip"),
            Some(5),
        ),
    ];

    for spec in private_endpoint_specs() {
        endpoints.push(rest_endpoint(
            spec.operation,
            private_support.clone(),
            spec.method,
            spec.path,
            EndpointAuth::Hmac,
            spec.market_types,
            vec![spec.credential_scope],
            Some("signed_uid"),
            Some(spec.weight),
        ));
    }

    endpoints
}

struct PrivateEndpointSpec {
    operation: &'static str,
    method: &'static str,
    path: &'static str,
    market_types: Vec<MarketType>,
    credential_scope: CredentialScope,
    weight: u32,
}

fn private_endpoint_specs() -> Vec<PrivateEndpointSpec> {
    use CredentialScope::{ReadOnly, Trade};
    vec![
        private_endpoint(
            "get_balances",
            "GET",
            "/api/v3/account",
            vec![MarketType::Spot],
            ReadOnly,
            20,
        ),
        private_endpoint(
            "get_balances",
            "GET",
            "/fapi/v2/balance",
            vec![MarketType::Perpetual],
            ReadOnly,
            20,
        ),
        private_endpoint(
            "get_positions",
            "GET",
            "/fapi/v2/positionRisk",
            vec![MarketType::Perpetual],
            ReadOnly,
            20,
        ),
        private_endpoint(
            "get_fees",
            "GET",
            "/api/v3/account/commission",
            vec![MarketType::Spot],
            ReadOnly,
            20,
        ),
        private_endpoint(
            "get_fees",
            "GET",
            "/fapi/v1/commissionRate",
            vec![MarketType::Perpetual],
            ReadOnly,
            20,
        ),
        private_endpoint(
            "place_order",
            "POST",
            "/api/v3/order",
            vec![MarketType::Spot],
            Trade,
            1,
        ),
        private_endpoint(
            "place_order",
            "POST",
            "/fapi/v1/order",
            vec![MarketType::Perpetual],
            Trade,
            1,
        ),
        private_endpoint(
            "place_quote_market_order",
            "POST",
            "/api/v3/order",
            vec![MarketType::Spot],
            Trade,
            1,
        ),
        private_endpoint(
            "cancel_order",
            "DELETE",
            "/api/v3/order",
            vec![MarketType::Spot],
            Trade,
            1,
        ),
        private_endpoint(
            "cancel_order",
            "DELETE",
            "/fapi/v1/order",
            vec![MarketType::Perpetual],
            Trade,
            1,
        ),
        private_endpoint(
            "cancel_all_orders",
            "DELETE",
            "/api/v3/openOrders",
            vec![MarketType::Spot],
            Trade,
            1,
        ),
        private_endpoint(
            "cancel_all_orders",
            "DELETE",
            "/fapi/v1/allOpenOrders",
            vec![MarketType::Perpetual],
            Trade,
            1,
        ),
        private_endpoint(
            "amend_order",
            "PUT",
            "/api/v3/order/amend/keepPriority",
            vec![MarketType::Spot],
            Trade,
            4,
        ),
        private_endpoint(
            "place_order_list",
            "POST",
            "/api/v3/orderList/oco",
            vec![MarketType::Spot],
            Trade,
            1,
        ),
        private_endpoint(
            "query_order",
            "GET",
            "/api/v3/order",
            vec![MarketType::Spot],
            ReadOnly,
            4,
        ),
        private_endpoint(
            "query_order",
            "GET",
            "/fapi/v1/order",
            vec![MarketType::Perpetual],
            ReadOnly,
            4,
        ),
        private_endpoint(
            "get_open_orders",
            "GET",
            "/api/v3/openOrders",
            vec![MarketType::Spot],
            ReadOnly,
            6,
        ),
        private_endpoint(
            "get_open_orders",
            "GET",
            "/fapi/v1/openOrders",
            vec![MarketType::Perpetual],
            ReadOnly,
            6,
        ),
        private_endpoint(
            "get_recent_fills",
            "GET",
            "/api/v3/myTrades",
            vec![MarketType::Spot],
            ReadOnly,
            20,
        ),
        private_endpoint(
            "get_recent_fills",
            "GET",
            "/fapi/v1/userTrades",
            vec![MarketType::Perpetual],
            ReadOnly,
            20,
        ),
    ]
}

fn private_endpoint(
    operation: &'static str,
    method: &'static str,
    path: &'static str,
    market_types: Vec<MarketType>,
    credential_scope: CredentialScope,
    weight: u32,
) -> PrivateEndpointSpec {
    PrivateEndpointSpec {
        operation,
        method,
        path,
        market_types,
        credential_scope,
        weight,
    }
}

fn rest_endpoint(
    operation: &str,
    support: CapabilitySupport,
    method: &str,
    path: &str,
    auth: EndpointAuth,
    market_types: Vec<MarketType>,
    credential_scopes: Vec<CredentialScope>,
    rate_limit_bucket: Option<&str>,
    weight: Option<u32>,
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
        rate_limit_bucket: rate_limit_bucket.map(str::to_string),
        weight,
        supports_testnet: true,
    }
}
