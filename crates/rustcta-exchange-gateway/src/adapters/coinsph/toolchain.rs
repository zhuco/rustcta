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
        CapabilitySupport::unsupported(
            "Coins.ph private REST requires COINSPH_API_KEY and COINSPH_API_SECRET",
        )
    };
    capabilities.capabilities_v2.public_streams = CapabilitySupport::native();
    capabilities.capabilities_v2.private_streams = CapabilitySupport::unsupported(
        "Coins.ph private stream runtime is not wired; use REST account, openOrders, order, and myTrades reconciliation",
    );
    capabilities.capabilities_v2.batch_place_orders =
        BatchCapability::unsupported("Coins.ph batch place order is not implemented");
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported("Coins.ph batch cancel order is not implemented");
    capabilities.capabilities_v2.cancel_all_orders = CapabilitySupport::unsupported(
        "Coins.ph cancel-all is intentionally left unsupported until local partial-cancel semantics are live-audited",
    );
    capabilities.capabilities_v2.order_history = HistoryCapability {
        support: if private_rest_enabled {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "Coins.ph order readback requires private REST credentials",
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
            CapabilitySupport::unsupported("Coins.ph myTrades requires private REST credentials")
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
            "/openapi/v1/exchangeInfo",
            EndpointAuth::None,
            Vec::new(),
            Some("public_ip"),
            Some(10),
        ),
        rest_endpoint(
            "get_order_book",
            CapabilitySupport::native(),
            "GET",
            "/openapi/quote/v1/depth",
            EndpointAuth::None,
            Vec::new(),
            Some("public_ip"),
            Some(5),
        ),
        websocket_endpoint(
            "public_book_ticker",
            "{symbol}@bookTicker",
            Some("public_ws"),
        ),
        websocket_endpoint(
            "public_orderbook_partial_depth",
            "{symbol}@depth5|10|20@100ms, {symbol}@depth200@1000ms",
            Some("public_ws"),
        ),
        websocket_endpoint(
            "public_orderbook_diff_depth",
            "{symbol}@depth@100ms|1000ms",
            Some("public_ws"),
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
            "/openapi/v1/account",
            CredentialScope::ReadOnly,
            20,
        ),
        (
            "get_fees",
            "GET",
            "/openapi/v1/asset/tradeFee",
            CredentialScope::ReadOnly,
            1,
        ),
        (
            "place_order",
            "POST",
            "/openapi/v1/order",
            CredentialScope::Trade,
            1,
        ),
        (
            "cancel_order",
            "DELETE",
            "/openapi/v1/order",
            CredentialScope::Trade,
            1,
        ),
        (
            "query_order",
            "GET",
            "/openapi/v1/order",
            CredentialScope::ReadOnly,
            4,
        ),
        (
            "get_open_orders",
            "GET",
            "/openapi/v1/openOrders",
            CredentialScope::ReadOnly,
            6,
        ),
        (
            "get_recent_fills",
            "GET",
            "/openapi/v1/myTrades",
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

    for operation in [
        "amend_order",
        "place_order_list",
        "batch_place_orders",
        "batch_cancel_orders",
        "cancel_all_orders",
        "payment",
        "wallet",
        "fiat_transfer",
        "withdraw",
        "sub_account_transfer",
    ] {
        let reason = if matches!(
            operation,
            "amend_order"
                | "place_order_list"
                | "batch_place_orders"
                | "batch_cancel_orders"
                | "cancel_all_orders"
        ) {
            "Coins.ph reviewed spot profile does not expose shared amend, OCO/OTO, native batch place/cancel, or safe cancel-all semantics"
        } else {
            "funding, wallet, payment, and fiat-transfer APIs are outside the Coins.ph trading credential boundary"
        };
        endpoints.push(rest_endpoint(
            operation,
            CapabilitySupport::unsupported(reason),
            "N/A",
            "",
            EndpointAuth::Hmac,
            vec![CredentialScope::Transfer, CredentialScope::Withdraw],
            None,
            None,
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
        supports_testnet: false,
    }
}

fn websocket_endpoint(
    operation: &str,
    path: &str,
    rate_limit_bucket: Option<&str>,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support: CapabilitySupport::native(),
        market_types: vec![MarketType::Spot],
        transport: EndpointTransport::WebSocket,
        method: Some("SUBSCRIBE".to_string()),
        path: Some(path.to_string()),
        auth: EndpointAuth::None,
        credential_scopes: Vec::new(),
        rate_limit_bucket: rate_limit_bucket.map(str::to_string),
        weight: None,
        supports_testnet: false,
    }
}
