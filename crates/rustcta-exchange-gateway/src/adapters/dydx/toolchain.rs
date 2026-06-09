use rustcta_exchange_api::{
    BatchAtomicity, BatchCapability, BatchExecutionMode, CapabilitySupport, CredentialScope,
    EndpointAuth, EndpointCapability, EndpointTransport, ExchangeClientCapabilities,
    HeartbeatCapability, HistoryCapability, ReconnectCapability, StreamHeartbeatDirection,
    StreamRuntimeCapability,
};
use rustcta_types::MarketType;

use super::signing::{
    AMEND_PROJECT_UNIMPLEMENTED, BATCH_CANCEL_PROJECT_UNIMPLEMENTED,
    BATCH_PLACE_PROJECT_UNIMPLEMENTED, NODE_WRITE_PROJECT_UNIMPLEMENTED,
};

pub(super) fn apply_capabilities(
    capabilities: &mut ExchangeClientCapabilities,
    private_read: bool,
    public_streams: bool,
    private_streams: bool,
) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = if private_read {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(
            "dydx private Indexer readback requires wallet_address/subaccount configuration",
        )
    };
    capabilities.capabilities_v2.public_streams = if public_streams {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("dydx public streams disabled by config")
    };
    capabilities.capabilities_v2.private_streams = if private_streams {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(
            "dydx private streams require wallet_address/subaccount configuration",
        )
    };
    capabilities.capabilities_v2.stream_runtime =
        stream_runtime_capability(public_streams, private_streams);
    capabilities.capabilities_v2.batch_place_orders =
        chain_batch_capability(BATCH_PLACE_PROJECT_UNIMPLEMENTED);
    capabilities.capabilities_v2.batch_cancel_orders =
        chain_batch_capability(BATCH_CANCEL_PROJECT_UNIMPLEMENTED);
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported(NODE_WRITE_PROJECT_UNIMPLEMENTED);
    capabilities.capabilities_v2.order_history = history_capability(
        private_read,
        "dydx order history requires private Indexer readback configuration",
    );
    capabilities.capabilities_v2.fills_history = history_capability(
        private_read,
        "dydx fills history requires private Indexer readback configuration",
    );
    capabilities.capabilities_v2.credential_scopes =
        vec![CredentialScope::ReadOnly, CredentialScope::Trade];
    capabilities.capabilities_v2.endpoints = endpoint_capabilities(private_read);
}

fn stream_runtime_capability(
    public_streams: bool,
    private_streams: bool,
) -> StreamRuntimeCapability {
    StreamRuntimeCapability {
        public: if public_streams {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("dydx public streams disabled by config")
        },
        private: if private_streams {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "dydx private streams require wallet_address/subaccount configuration",
            )
        },
        supports_subscribe: public_streams || private_streams,
        supports_unsubscribe: false,
        supports_public_subscribe: public_streams,
        supports_private_subscribe: private_streams,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::ServerPing,
            interval_ms: Some(30_000),
            timeout_ms: Some(90_000),
        },
        reconnect: ReconnectCapability {
            supported: true,
            requires_resubscribe: true,
            preserves_session: false,
            max_reconnect_attempts: None,
        },
        ..StreamRuntimeCapability::default()
    }
}

fn chain_batch_capability(reason: &'static str) -> BatchCapability {
    BatchCapability {
        support: CapabilitySupport::unsupported(reason),
        mode: BatchExecutionMode::Native,
        atomicity: BatchAtomicity::Partial,
        max_items: None,
        same_symbol_required: false,
        same_market_type_required: true,
        supports_client_order_id: true,
        supports_partial_failure: true,
    }
}

fn history_capability(private_read: bool, disabled_reason: &'static str) -> HistoryCapability {
    if private_read {
        HistoryCapability {
            support: CapabilitySupport::native(),
            supports_since: false,
            supports_until: false,
            supports_limit: true,
            supports_cursor: true,
            supports_from_id: false,
            max_limit: Some(100),
            max_window_ms: None,
        }
    } else {
        HistoryCapability::unsupported(disabled_reason)
    }
}

fn endpoint_capabilities(private_read: bool) -> Vec<EndpointCapability> {
    let read_support = if private_read {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("dydx private Indexer readback is not configured")
    };
    let mut endpoints = vec![
        endpoint(
            "symbol_rules",
            CapabilitySupport::native(),
            Some("GET"),
            Some("/v4/perpetualMarkets"),
            EndpointAuth::None,
            Vec::new(),
        ),
        endpoint(
            "order_book",
            CapabilitySupport::native(),
            Some("GET"),
            Some("/v4/orderbooks/perpetualMarket/{ticker}"),
            EndpointAuth::None,
            Vec::new(),
        ),
        endpoint(
            "balances",
            read_support.clone(),
            Some("GET"),
            Some("/v4/addresses/{address}/subaccountNumber/{subaccountNumber}"),
            EndpointAuth::None,
            vec![CredentialScope::ReadOnly],
        ),
        endpoint(
            "positions",
            read_support.clone(),
            Some("GET"),
            Some("/v4/perpetualPositions"),
            EndpointAuth::None,
            vec![CredentialScope::ReadOnly],
        ),
        endpoint(
            "query_order",
            read_support.clone(),
            Some("GET"),
            Some("/v4/orders/{orderId}"),
            EndpointAuth::None,
            vec![CredentialScope::ReadOnly],
        ),
        endpoint(
            "open_orders",
            read_support.clone(),
            Some("GET"),
            Some("/v4/orders"),
            EndpointAuth::None,
            vec![CredentialScope::ReadOnly],
        ),
        endpoint(
            "recent_fills",
            read_support,
            Some("GET"),
            Some("/v4/fills"),
            EndpointAuth::None,
            vec![CredentialScope::ReadOnly],
        ),
    ];
    endpoints.extend(advanced_order_endpoints());
    endpoints
}

fn advanced_order_endpoints() -> Vec<EndpointCapability> {
    vec![
        chain_tx_endpoint("place_order", NODE_WRITE_PROJECT_UNIMPLEMENTED),
        chain_tx_endpoint("cancel_order", NODE_WRITE_PROJECT_UNIMPLEMENTED),
        chain_tx_endpoint("amend_order", AMEND_PROJECT_UNIMPLEMENTED),
        chain_tx_endpoint("batch_place_orders", BATCH_PLACE_PROJECT_UNIMPLEMENTED),
        chain_tx_endpoint("batch_cancel_orders", BATCH_CANCEL_PROJECT_UNIMPLEMENTED),
        EndpointCapability {
            operation: "place_order_list".to_string(),
            support: CapabilitySupport::unsupported("dydx.order_list_unsupported"),
            market_types: vec![MarketType::Perpetual],
            transport: EndpointTransport::Rest,
            method: None,
            path: None,
            auth: EndpointAuth::None,
            credential_scopes: vec![CredentialScope::Trade],
            rate_limit_bucket: Some("dydx_node_private".to_string()),
            weight: Some(0),
            supports_testnet: true,
        },
    ]
}

fn chain_tx_endpoint(operation: &'static str, reason: &'static str) -> EndpointCapability {
    endpoint(
        operation,
        CapabilitySupport::unsupported(reason),
        Some("CHAIN_TX"),
        Some(match operation {
            "amend_order" => "node private Replace Order transaction",
            "batch_place_orders" => "node private batch Place Order transaction",
            "batch_cancel_orders" => "node private batch Cancel Order transaction",
            "cancel_order" => "node private Cancel Order transaction",
            _ => "node private Place Order transaction",
        }),
        EndpointAuth::Hmac,
        vec![CredentialScope::Trade],
    )
}

fn endpoint(
    operation: &'static str,
    support: CapabilitySupport,
    method: Option<&'static str>,
    path: Option<&'static str>,
    auth: EndpointAuth,
    credential_scopes: Vec<CredentialScope>,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support,
        market_types: vec![MarketType::Perpetual],
        transport: EndpointTransport::Rest,
        method: method.map(ToString::to_string),
        path: path.map(ToString::to_string),
        auth,
        credential_scopes,
        rate_limit_bucket: Some(
            if method == Some("CHAIN_TX") {
                "dydx_node_private"
            } else {
                "dydx_indexer"
            }
            .to_string(),
        ),
        weight: Some(if method == Some("CHAIN_TX") { 0 } else { 1 }),
        supports_testnet: true,
    }
}
