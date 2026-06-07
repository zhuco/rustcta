use serde::{Deserialize, Serialize};

use crate::{
    ExchangeId, MarketType, OrderType, PrivateStreamCapabilities, StreamRuntimeCapability,
    TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode")]
pub enum CapabilitySupport {
    Native,
    Composed { reason: String },
    RestFallback { reason: String },
    WsOnly { reason: String },
    Unsupported { reason: String },
}

impl Default for CapabilitySupport {
    fn default() -> Self {
        Self::Unsupported {
            reason: "not declared".to_string(),
        }
    }
}

impl CapabilitySupport {
    pub fn native() -> Self {
        Self::Native
    }

    pub fn composed(reason: impl Into<String>) -> Self {
        Self::Composed {
            reason: reason.into(),
        }
    }

    pub fn rest_fallback(reason: impl Into<String>) -> Self {
        Self::RestFallback {
            reason: reason.into(),
        }
    }

    pub fn ws_only(reason: impl Into<String>) -> Self {
        Self::WsOnly {
            reason: reason.into(),
        }
    }

    pub fn unsupported(reason: impl Into<String>) -> Self {
        Self::Unsupported {
            reason: reason.into(),
        }
    }

    pub fn is_supported(&self) -> bool {
        !matches!(self, Self::Unsupported { .. })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialScope {
    ReadOnly,
    Trade,
    Transfer,
    Withdraw,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointTransport {
    Rest,
    WebSocket,
    SocketIo,
    JsonRpc,
}

impl Default for EndpointTransport {
    fn default() -> Self {
        Self::Rest
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EndpointAuth {
    None,
    ApiKey,
    Hmac,
    Jwt,
    Bearer,
    ListenKey,
}

impl Default for EndpointAuth {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointCapability {
    #[serde(default)]
    pub operation: String,
    #[serde(default)]
    pub support: CapabilitySupport,
    #[serde(default)]
    pub market_types: Vec<MarketType>,
    #[serde(default)]
    pub transport: EndpointTransport,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub auth: EndpointAuth,
    #[serde(default)]
    pub credential_scopes: Vec<CredentialScope>,
    #[serde(default)]
    pub rate_limit_bucket: Option<String>,
    #[serde(default)]
    pub weight: Option<u32>,
    #[serde(default)]
    pub supports_testnet: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchExecutionMode {
    Native,
    ComposedSequential,
    ComposedConcurrent,
    Unsupported,
}

impl Default for BatchExecutionMode {
    fn default() -> Self {
        Self::Unsupported
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchAtomicity {
    Atomic,
    NonAtomic,
    Partial,
    Unknown,
}

impl Default for BatchAtomicity {
    fn default() -> Self {
        Self::Unknown
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchCapability {
    #[serde(default)]
    pub support: CapabilitySupport,
    #[serde(default)]
    pub mode: BatchExecutionMode,
    #[serde(default)]
    pub atomicity: BatchAtomicity,
    #[serde(default)]
    pub max_items: Option<u32>,
    #[serde(default)]
    pub same_symbol_required: bool,
    #[serde(default)]
    pub same_market_type_required: bool,
    #[serde(default)]
    pub supports_client_order_id: bool,
    #[serde(default)]
    pub supports_partial_failure: bool,
}

impl Default for BatchCapability {
    fn default() -> Self {
        Self {
            support: CapabilitySupport::default(),
            mode: BatchExecutionMode::default(),
            atomicity: BatchAtomicity::default(),
            max_items: None,
            same_symbol_required: false,
            same_market_type_required: false,
            supports_client_order_id: false,
            supports_partial_failure: false,
        }
    }
}

impl BatchCapability {
    pub fn unsupported(reason: impl Into<String>) -> Self {
        Self {
            support: CapabilitySupport::unsupported(reason),
            ..Self::default()
        }
    }

    pub fn native(atomicity: BatchAtomicity, max_items: Option<u32>) -> Self {
        Self {
            support: CapabilitySupport::native(),
            mode: BatchExecutionMode::Native,
            atomicity,
            max_items,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryCapability {
    #[serde(default)]
    pub support: CapabilitySupport,
    #[serde(default)]
    pub supports_since: bool,
    #[serde(default)]
    pub supports_until: bool,
    #[serde(default)]
    pub supports_limit: bool,
    #[serde(default)]
    pub supports_cursor: bool,
    #[serde(default)]
    pub supports_from_id: bool,
    #[serde(default)]
    pub max_limit: Option<u32>,
    #[serde(default)]
    pub max_window_ms: Option<i64>,
}

impl Default for HistoryCapability {
    fn default() -> Self {
        Self {
            support: CapabilitySupport::default(),
            supports_since: false,
            supports_until: false,
            supports_limit: false,
            supports_cursor: false,
            supports_from_id: false,
            max_limit: None,
            max_window_ms: None,
        }
    }
}

impl HistoryCapability {
    pub fn unsupported(reason: impl Into<String>) -> Self {
        Self {
            support: CapabilitySupport::unsupported(reason),
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ExchangeClientCapabilitiesV2 {
    #[serde(default)]
    pub public_rest: CapabilitySupport,
    #[serde(default)]
    pub private_rest: CapabilitySupport,
    #[serde(default)]
    pub public_streams: CapabilitySupport,
    #[serde(default)]
    pub private_streams: CapabilitySupport,
    #[serde(default)]
    pub stream_runtime: StreamRuntimeCapability,
    #[serde(default)]
    pub batch_place_orders: BatchCapability,
    #[serde(default)]
    pub batch_cancel_orders: BatchCapability,
    #[serde(default)]
    pub cancel_all_orders: CapabilitySupport,
    #[serde(default)]
    pub order_history: HistoryCapability,
    #[serde(default)]
    pub fills_history: HistoryCapability,
    #[serde(default)]
    pub endpoints: Vec<EndpointCapability>,
    #[serde(default)]
    pub credential_scopes: Vec<CredentialScope>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderBookStrictness {
    SnapshotOnly,
    BestEffortDelta,
    StrictDelta,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderBookChecksumMode {
    Disabled,
    TopLevelSum,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderBookCapability {
    pub strictness: OrderBookStrictness,
    pub supports_sequence: bool,
    pub supports_checksum: bool,
    pub checksum_mode: OrderBookChecksumMode,
    pub supports_resync_endpoint: bool,
    pub max_depth: Option<u32>,
}

impl Default for OrderBookCapability {
    fn default() -> Self {
        Self::snapshot_only(None)
    }
}

impl OrderBookCapability {
    pub fn snapshot_only(max_depth: Option<u32>) -> Self {
        Self {
            strictness: OrderBookStrictness::SnapshotOnly,
            supports_sequence: false,
            supports_checksum: false,
            checksum_mode: OrderBookChecksumMode::Disabled,
            supports_resync_endpoint: true,
            max_depth,
        }
    }

    pub fn best_effort_delta(max_depth: Option<u32>) -> Self {
        Self {
            strictness: OrderBookStrictness::BestEffortDelta,
            supports_sequence: false,
            supports_checksum: false,
            checksum_mode: OrderBookChecksumMode::Disabled,
            supports_resync_endpoint: true,
            max_depth,
        }
    }

    pub fn strict_delta(max_depth: Option<u32>) -> Self {
        Self {
            strictness: OrderBookStrictness::StrictDelta,
            supports_sequence: true,
            supports_checksum: false,
            checksum_mode: OrderBookChecksumMode::Disabled,
            supports_resync_endpoint: true,
            max_depth,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeClientCapabilities {
    pub schema_version: u16,
    pub exchange: ExchangeId,
    pub market_types: Vec<MarketType>,
    pub supports_public_rest: bool,
    pub supports_private_rest: bool,
    pub supports_public_streams: bool,
    pub supports_private_streams: bool,
    #[serde(default)]
    pub private_stream_capabilities: Option<PrivateStreamCapabilities>,
    pub supports_symbol_rules: bool,
    pub supports_order_book_snapshot: bool,
    pub supports_balances: bool,
    pub supports_positions: bool,
    pub supports_fees: bool,
    pub supports_place_order: bool,
    pub supports_cancel_order: bool,
    pub supports_query_order: bool,
    pub supports_open_orders: bool,
    pub supports_recent_fills: bool,
    pub supports_batch_place_order: bool,
    pub supports_batch_cancel_order: bool,
    pub supports_cancel_all_orders: bool,
    #[serde(default)]
    pub supports_quote_market_order: bool,
    #[serde(default)]
    pub supports_amend_order: bool,
    #[serde(default)]
    pub supports_order_list: bool,
    pub supports_client_order_id: bool,
    pub supports_reduce_only: bool,
    pub supports_post_only: bool,
    pub supports_time_in_force: Vec<TimeInForce>,
    pub supports_order_types: Vec<OrderType>,
    pub max_order_book_depth: Option<u32>,
    #[serde(default)]
    pub order_book: OrderBookCapability,
    pub max_recent_fill_limit: Option<u32>,
    #[serde(default)]
    pub capabilities_v2: ExchangeClientCapabilitiesV2,
}

impl ExchangeClientCapabilities {
    pub fn new(exchange: ExchangeId) -> Self {
        let mut capabilities = Self {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange,
            market_types: Vec::new(),
            supports_public_rest: false,
            supports_private_rest: false,
            supports_public_streams: false,
            supports_private_streams: false,
            private_stream_capabilities: Some(PrivateStreamCapabilities::unsupported(
                EXCHANGE_API_SCHEMA_VERSION,
            )),
            supports_symbol_rules: false,
            supports_order_book_snapshot: false,
            supports_balances: false,
            supports_positions: false,
            supports_fees: false,
            supports_place_order: false,
            supports_cancel_order: false,
            supports_query_order: false,
            supports_open_orders: false,
            supports_recent_fills: false,
            supports_batch_place_order: false,
            supports_batch_cancel_order: false,
            supports_cancel_all_orders: false,
            supports_quote_market_order: false,
            supports_amend_order: false,
            supports_order_list: false,
            supports_client_order_id: false,
            supports_reduce_only: false,
            supports_post_only: false,
            supports_time_in_force: Vec::new(),
            supports_order_types: Vec::new(),
            max_order_book_depth: None,
            order_book: OrderBookCapability::default(),
            max_recent_fill_limit: None,
            capabilities_v2: ExchangeClientCapabilitiesV2::default(),
        };
        capabilities.refresh_v2_from_legacy_flags();
        capabilities
    }

    pub fn refresh_v2_from_legacy_flags(&mut self) {
        self.capabilities_v2.public_rest = support_from_bool(
            self.supports_public_rest,
            "legacy supports_public_rest=false",
        );
        self.capabilities_v2.private_rest = support_from_bool(
            self.supports_private_rest,
            "legacy supports_private_rest=false",
        );
        self.capabilities_v2.public_streams = support_from_bool(
            self.supports_public_streams,
            "legacy supports_public_streams=false",
        );
        self.capabilities_v2.private_streams = support_from_bool(
            self.supports_private_streams,
            "legacy supports_private_streams=false",
        );
        self.capabilities_v2.batch_place_orders = batch_from_bool(
            self.supports_batch_place_order,
            "legacy supports_batch_place_order=false",
        );
        self.capabilities_v2.batch_cancel_orders = batch_from_bool(
            self.supports_batch_cancel_order,
            "legacy supports_batch_cancel_order=false",
        );
        self.capabilities_v2.cancel_all_orders = support_from_bool(
            self.supports_cancel_all_orders,
            "legacy supports_cancel_all_orders=false",
        );
        self.capabilities_v2.fills_history = history_from_bool(
            self.supports_recent_fills,
            self.max_recent_fill_limit,
            "legacy supports_recent_fills=false",
        );
        self.capabilities_v2.order_history = support_from_open_orders(
            self.supports_open_orders,
            "legacy supports_open_orders=false",
        );
        self.capabilities_v2.credential_scopes = credential_scopes_from_legacy_flags(self);
    }

    pub fn apply_v2_to_legacy_flags(&mut self) {
        self.supports_public_rest = self.capabilities_v2.public_rest.is_supported();
        self.supports_private_rest = self.capabilities_v2.private_rest.is_supported();
        self.supports_public_streams = self.capabilities_v2.public_streams.is_supported();
        self.supports_private_streams = self.capabilities_v2.private_streams.is_supported();
        self.supports_batch_place_order = self
            .capabilities_v2
            .batch_place_orders
            .support
            .is_supported();
        self.supports_batch_cancel_order = self
            .capabilities_v2
            .batch_cancel_orders
            .support
            .is_supported();
        self.supports_cancel_all_orders = self.capabilities_v2.cancel_all_orders.is_supported();
        self.supports_open_orders = self.capabilities_v2.order_history.support.is_supported();
        self.supports_recent_fills = self.capabilities_v2.fills_history.support.is_supported();
        self.max_recent_fill_limit = self.capabilities_v2.fills_history.max_limit;
    }
}

fn support_from_bool(supported: bool, unsupported_reason: &'static str) -> CapabilitySupport {
    if supported {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(unsupported_reason)
    }
}

fn batch_from_bool(supported: bool, unsupported_reason: &'static str) -> BatchCapability {
    if supported {
        BatchCapability {
            support: CapabilitySupport::native(),
            mode: BatchExecutionMode::Native,
            atomicity: BatchAtomicity::Unknown,
            max_items: None,
            same_symbol_required: false,
            same_market_type_required: false,
            supports_client_order_id: false,
            supports_partial_failure: false,
        }
    } else {
        BatchCapability::unsupported(unsupported_reason)
    }
}

fn history_from_bool(
    supported: bool,
    max_limit: Option<u32>,
    unsupported_reason: &'static str,
) -> HistoryCapability {
    if supported {
        HistoryCapability {
            support: CapabilitySupport::native(),
            supports_since: false,
            supports_until: false,
            supports_limit: max_limit.is_some(),
            supports_cursor: false,
            supports_from_id: false,
            max_limit,
            max_window_ms: None,
        }
    } else {
        HistoryCapability::unsupported(unsupported_reason)
    }
}

fn support_from_open_orders(
    supported: bool,
    unsupported_reason: &'static str,
) -> HistoryCapability {
    if supported {
        HistoryCapability {
            support: CapabilitySupport::native(),
            supports_since: false,
            supports_until: false,
            supports_limit: false,
            supports_cursor: false,
            supports_from_id: false,
            max_limit: None,
            max_window_ms: None,
        }
    } else {
        HistoryCapability::unsupported(unsupported_reason)
    }
}

fn credential_scopes_from_legacy_flags(
    capabilities: &ExchangeClientCapabilities,
) -> Vec<CredentialScope> {
    let mut scopes = Vec::new();
    if capabilities.supports_private_rest || capabilities.supports_private_streams {
        scopes.push(CredentialScope::ReadOnly);
    }
    if capabilities.supports_place_order
        || capabilities.supports_cancel_order
        || capabilities.supports_batch_place_order
        || capabilities.supports_batch_cancel_order
        || capabilities.supports_cancel_all_orders
        || capabilities.supports_amend_order
        || capabilities.supports_order_list
    {
        scopes.push(CredentialScope::Trade);
    }
    scopes
}
