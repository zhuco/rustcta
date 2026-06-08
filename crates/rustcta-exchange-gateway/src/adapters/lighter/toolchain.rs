use rustcta_exchange_api::{
    BatchCapability, CapabilitySupport, EndpointAuth, EndpointCapability, EndpointTransport,
    ExchangeClientCapabilities, HeartbeatCapability, HeartbeatPolicy, HistoryCapability,
    ReconnectCapability, StreamHeartbeatDirection, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

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
    capabilities.capabilities_v2.batch_place_orders =
        BatchCapability::unsupported("lighter.send_tx_batch_signing_unverified");
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported("lighter.send_tx_batch_signing_unverified");
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported("lighter.cancel_all_orders_signing_unverified");
    capabilities.capabilities_v2.order_history =
        HistoryCapability::unsupported("lighter.order_history_auth_token_unverified");
    capabilities.capabilities_v2.fills_history =
        HistoryCapability::unsupported("lighter.trades_auth_token_unverified");
    capabilities.capabilities_v2.credential_scopes = Vec::new();
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
            "open_orders",
            EndpointTransport::Rest,
            Some("GET"),
            Some("/accountActiveOrders"),
            EndpointAuth::Bearer,
        ),
        (
            "positions",
            EndpointTransport::Rest,
            Some("GET"),
            Some("/account"),
            EndpointAuth::Bearer,
        ),
        (
            "place_order",
            EndpointTransport::Rest,
            Some("POST"),
            Some("/sendTx"),
            EndpointAuth::Bearer,
        ),
        (
            "batch_tx",
            EndpointTransport::Rest,
            Some("POST"),
            Some("/sendTxBatch"),
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
            support: CapabilitySupport::unsupported(format!(
                "lighter.{operation}_session_spec_only"
            )),
            market_types: vec![MarketType::Perpetual],
            transport,
            method: method.map(ToString::to_string),
            path: path.map(ToString::to_string),
            auth,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("lighter_rest_or_ws".to_string()),
            weight: Some(0),
            supports_testnet: true,
        },
    )
    .collect()
}
