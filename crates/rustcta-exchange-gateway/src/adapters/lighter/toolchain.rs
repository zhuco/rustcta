use rustcta_exchange_api::{
    BatchAtomicity, BatchCapability, BatchExecutionMode, CapabilitySupport, CredentialScope,
    EndpointAuth, EndpointCapability, EndpointTransport, ExchangeClientCapabilities,
    HeartbeatCapability, HeartbeatPolicy, HistoryCapability, ReconnectCapability,
    StreamHeartbeatDirection, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

use super::private;

pub(super) fn apply_toolchain_capabilities(capabilities: &mut ExchangeClientCapabilities) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::unsupported(
        "Lighter public REST is audited but not enabled until order-book parser fixtures are promoted",
    );
    capabilities.capabilities_v2.private_rest = CapabilitySupport::unsupported(
        "Lighter private REST requires auth-token/read-only boundary and signed tx vectors",
    );
    capabilities.capabilities_v2.public_streams =
        CapabilitySupport::unsupported("Lighter public WS is session-spec/parser-fixture only");
    capabilities.capabilities_v2.private_streams =
        CapabilitySupport::unsupported("Lighter private WS requires bearer auth-token fixtures");
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::unsupported("Lighter low-latency WS runtime is not opened"),
        private: CapabilitySupport::unsupported("Lighter private WS runtime is not opened"),
        supports_subscribe: false,
        supports_unsubscribe: false,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::ClientPing,
            interval_ms: Some(60_000),
            timeout_ms: Some(120_000),
        },
        reconnect: ReconnectCapability {
            supported: true,
            requires_resubscribe: true,
            preserves_session: false,
            max_reconnect_attempts: None,
        },
        heartbeat_policy: HeartbeatPolicy::disabled(),
        ..StreamRuntimeCapability::default()
    };
    capabilities.capabilities_v2.batch_place_orders = BatchCapability {
        support: CapabilitySupport::unsupported(private::BATCH_PLACE_UNSUPPORTED),
        mode: BatchExecutionMode::Native,
        atomicity: BatchAtomicity::Partial,
        max_items: None,
        same_symbol_required: false,
        same_market_type_required: true,
        supports_client_order_id: true,
        supports_partial_failure: true,
    };
    capabilities.capabilities_v2.batch_cancel_orders = BatchCapability {
        support: CapabilitySupport::unsupported(private::BATCH_CANCEL_UNSUPPORTED),
        mode: BatchExecutionMode::Native,
        atomicity: BatchAtomicity::Partial,
        max_items: None,
        same_symbol_required: false,
        same_market_type_required: true,
        supports_client_order_id: true,
        supports_partial_failure: true,
    };
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported(private::CANCEL_ALL_UNSUPPORTED);
    capabilities.capabilities_v2.order_history =
        HistoryCapability::unsupported("lighter.order_history_auth_token_unverified");
    capabilities.capabilities_v2.fills_history =
        HistoryCapability::unsupported("lighter.trades_auth_token_unverified");
    capabilities.capabilities_v2.credential_scopes =
        vec![CredentialScope::ReadOnly, CredentialScope::Trade];
    capabilities.capabilities_v2.endpoints = audited_endpoints();
    capabilities.apply_v2_to_legacy_flags();
}

fn audited_endpoints() -> Vec<EndpointCapability> {
    [
        (
            "symbol_rules",
            EndpointTransport::Rest,
            Some("GET"),
            Some("/orderBooks"),
            EndpointAuth::None,
        ),
        (
            "symbol_rule_details",
            EndpointTransport::Rest,
            Some("GET"),
            Some("/orderBookDetails"),
            EndpointAuth::None,
        ),
        (
            "order_book",
            EndpointTransport::Rest,
            Some("GET"),
            Some("/orderBookOrders"),
            EndpointAuth::None,
        ),
        (
            "query_order",
            EndpointTransport::Rest,
            Some("GET"),
            Some("/accountActiveOrders"),
            EndpointAuth::Bearer,
        ),
        (
            "get_open_orders",
            EndpointTransport::Rest,
            Some("GET"),
            Some("/accountActiveOrders"),
            EndpointAuth::Bearer,
        ),
        (
            "get_recent_fills",
            EndpointTransport::Rest,
            Some("GET"),
            Some("/trades"),
            EndpointAuth::Bearer,
        ),
        (
            "get_positions",
            EndpointTransport::Rest,
            Some("GET"),
            Some("/account"),
            EndpointAuth::Bearer,
        ),
        (
            "public_order_book_ws",
            EndpointTransport::WebSocket,
            None,
            Some("order_book/{market_index}"),
            EndpointAuth::None,
        ),
        (
            "private_account_ws",
            EndpointTransport::WebSocket,
            None,
            Some("account_all/{account_index}"),
            EndpointAuth::Bearer,
        ),
    ]
    .into_iter()
    .map(
        |(operation, transport, method, path, auth)| EndpointCapability {
            operation: operation.to_string(),
            support: if matches!(
                operation,
                "query_order" | "get_open_orders" | "get_recent_fills"
            ) {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported(format!("lighter.{operation}_session_spec_only"))
            },
            market_types: vec![MarketType::Perpetual],
            transport,
            method: method.map(ToString::to_string),
            path: path.map(ToString::to_string),
            auth,
            credential_scopes: if matches!(
                operation,
                "query_order" | "get_open_orders" | "get_recent_fills"
            ) {
                vec![CredentialScope::ReadOnly]
            } else {
                Vec::new()
            },
            rate_limit_bucket: Some("lighter_rest_or_ws".to_string()),
            weight: Some(0),
            supports_testnet: true,
        },
    )
    .chain([
        signed_write_endpoint(
            "place_order",
            "/sendTx",
            "lighter.send_tx_signing_unverified",
        ),
        signed_write_endpoint(
            "cancel_order",
            "/sendTx",
            "lighter.send_tx_cancel_signing_unverified",
        ),
        signed_write_endpoint("amend_order", "/sendTx", private::AMEND_ORDER_UNSUPPORTED),
        signed_write_endpoint("batch_tx", "/sendTxBatch", private::BATCH_PLACE_UNSUPPORTED),
        signed_write_endpoint(
            "batch_place_orders",
            "/sendTxBatch",
            private::BATCH_PLACE_UNSUPPORTED,
        ),
        signed_write_endpoint(
            "batch_cancel",
            "/sendTxBatch",
            private::BATCH_CANCEL_UNSUPPORTED,
        ),
        signed_write_endpoint(
            "batch_cancel_orders",
            "/sendTxBatch",
            private::BATCH_CANCEL_UNSUPPORTED,
        ),
        EndpointCapability {
            operation: "place_order_list".to_string(),
            support: CapabilitySupport::unsupported(private::ORDER_LIST_UNSUPPORTED),
            market_types: vec![MarketType::Perpetual],
            transport: EndpointTransport::Rest,
            method: None,
            path: None,
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("lighter_unsupported".to_string()),
            weight: Some(0),
            supports_testnet: false,
        },
    ])
    .collect()
}

fn signed_write_endpoint(operation: &str, path: &str, reason: &'static str) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support: CapabilitySupport::unsupported(reason),
        market_types: vec![MarketType::Perpetual],
        transport: EndpointTransport::Rest,
        method: Some("POST".to_string()),
        path: Some(path.to_string()),
        auth: EndpointAuth::Bearer,
        credential_scopes: vec![CredentialScope::Trade],
        rate_limit_bucket: Some("lighter_send_tx".to_string()),
        weight: Some(6),
        supports_testnet: true,
    }
}
