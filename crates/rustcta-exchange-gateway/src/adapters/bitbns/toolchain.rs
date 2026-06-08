use rustcta_exchange_api::{
    BatchCapability, CapabilitySupport, EndpointAuth, EndpointCapability, EndpointTransport,
    ExchangeClientCapabilities, HistoryCapability, StreamRuntimeCapability,
};
use rustcta_types::MarketType;

pub(super) fn apply_toolchain_capabilities(capabilities: &mut ExchangeClientCapabilities) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest =
        CapabilitySupport::unsupported("bitbns private REST is audited but disabled");
    capabilities.capabilities_v2.public_streams =
        CapabilitySupport::unsupported("bitbns Socket.IO helpers are spec-only in this adapter");
    capabilities.capabilities_v2.private_streams =
        CapabilitySupport::unsupported("bitbns private Socket.IO token stream is not wired");
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability::default();
    capabilities.capabilities_v2.batch_place_orders =
        BatchCapability::unsupported("bitbns batch place is unsupported in scan-only adapter");
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported("bitbns batch cancel is unsupported in scan-only adapter");
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported("bitbns cancel-all is unsupported in scan-only adapter");
    capabilities.capabilities_v2.order_history =
        HistoryCapability::unsupported("bitbns order history requires private REST review");
    capabilities.capabilities_v2.fills_history =
        HistoryCapability::unsupported("bitbns fills require private REST review");
    capabilities.capabilities_v2.credential_scopes = Vec::new();
    capabilities.capabilities_v2.endpoints = endpoints();
    capabilities.apply_v2_to_legacy_flags();
    capabilities.supports_public_rest = true;
    capabilities.supports_symbol_rules = true;
    capabilities.supports_order_book_snapshot = true;
}

fn endpoints() -> Vec<EndpointCapability> {
    vec![
        EndpointCapability {
            operation: "get_symbol_rules".to_string(),
            support: CapabilitySupport::native(),
            market_types: vec![MarketType::Spot],
            transport: EndpointTransport::Rest,
            method: Some("GET".to_string()),
            path: Some("/order/fetchMarkets/".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("bitbns_public".to_string()),
            weight: Some(1),
            supports_testnet: false,
        },
        EndpointCapability {
            operation: "get_order_book".to_string(),
            support: CapabilitySupport::native(),
            market_types: vec![MarketType::Spot],
            transport: EndpointTransport::Rest,
            method: Some("GET".to_string()),
            path: Some("/exchangeData/orderBook".to_string()),
            auth: EndpointAuth::None,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("bitbns_public".to_string()),
            weight: Some(1),
            supports_testnet: false,
        },
        EndpointCapability {
            operation: "private_rest".to_string(),
            support: CapabilitySupport::unsupported(
                "bitbns private REST is intentionally not mapped to ExchangeClient",
            ),
            market_types: vec![MarketType::Spot],
            transport: EndpointTransport::Rest,
            method: None,
            path: None,
            auth: EndpointAuth::Hmac,
            credential_scopes: Vec::new(),
            rate_limit_bucket: Some("bitbns_private_disabled".to_string()),
            weight: Some(0),
            supports_testnet: false,
        },
    ]
}
