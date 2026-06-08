use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use rustcta_types::{
    LiquidityRole, MarketType, OrderBookSnapshot, OrderSide, OrderStatus, OrderType,
    SymbolCapability, SymbolStatus as SharedSymbolStatus, TimeInForce,
};
use serde::{Deserialize, Serialize};

use crate::scanner::{FeeSource, SymbolStatus};

use super::{
    DustPosition, InventoryReadiness, MarketLiquidationPlan, PassiveLiquidationSession,
    RuntimeComponentFreshness, SnapshotComponentStatus,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeDataAuthority {
    ExchangeRest,
    ExchangeWebSocket,
    LocalRegistry,
    LocalReservationManager,
    ReconciliationService,
    ConfigFallback,
    Unsupported,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeComponentMetadata {
    pub component: String,
    pub authority: RuntimeDataAuthority,
    pub fetched_at: DateTime<Utc>,
    #[serde(default)]
    pub exchange_timestamp_optional: Option<DateTime<Utc>>,
    pub age_ms: i64,
    pub freshness_status: RuntimeComponentFreshness,
    #[serde(default)]
    pub error_optional: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderOwnershipClass {
    SpotControl,
    ArbitrageStrategy,
    OtherKnownStrategy,
    ManualOperator,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillOwnershipRecord {
    pub exchange: String,
    pub symbol: String,
    pub fill_id: String,
    #[serde(default)]
    pub order_id: Option<String>,
    #[serde(default)]
    pub client_order_id_optional: Option<String>,
    pub ownership_class: OrderOwnershipClass,
    pub side: OrderSide,
    pub quantity: f64,
    pub price: f64,
    #[serde(default)]
    pub fee: Option<f64>,
    #[serde(default)]
    pub fee_asset: Option<String>,
    pub liquidity_role: LiquidityRole,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotConsistency {
    StrongEnoughForControl,
    SuitableForObservation,
    Stale,
    Inconsistent,
    MissingCriticalData,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotConsistencyReport {
    pub snapshot_id: String,
    pub generated_at: DateTime<Utc>,
    #[serde(default)]
    pub oldest_required_component_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub newest_component_at: Option<DateTime<Utc>>,
    pub maximum_component_age_ms: i64,
    pub component_time_skew_ms: i64,
    pub status: SnapshotConsistency,
    pub warnings: Vec<String>,
    pub critical_errors: Vec<String>,
}

impl SnapshotConsistencyReport {
    pub fn from_snapshot(
        snapshot: &SpotControlRuntimeSnapshotReadModel,
        max_component_time_skew_ms: u64,
    ) -> Self {
        let component_times = snapshot
            .component_statuses
            .iter()
            .filter_map(|status| status.updated_at)
            .collect::<Vec<_>>();
        let oldest = component_times.iter().min().copied();
        let newest = component_times.iter().max().copied();
        let maximum_component_age_ms = component_times
            .iter()
            .map(|at| {
                snapshot
                    .generated_at
                    .signed_duration_since(*at)
                    .num_milliseconds()
                    .max(0)
            })
            .max()
            .unwrap_or_default();
        let component_time_skew_ms = match (oldest, newest) {
            (Some(oldest), Some(newest)) => newest.signed_duration_since(oldest).num_milliseconds(),
            _ => 0,
        };
        let mut warnings = snapshot.warnings.clone();
        let mut critical_errors = snapshot.critical_errors.clone();
        let missing_critical = snapshot
            .component_statuses
            .iter()
            .any(SnapshotComponentStatus::is_critical_failure);
        if component_time_skew_ms > max_component_time_skew_ms as i64 {
            critical_errors.push(format!(
                "component time skew {component_time_skew_ms}ms exceeds policy"
            ));
        }
        let status = if missing_critical {
            SnapshotConsistency::MissingCriticalData
        } else if component_time_skew_ms > max_component_time_skew_ms as i64 {
            SnapshotConsistency::Inconsistent
        } else if !critical_errors.is_empty() {
            SnapshotConsistency::Stale
        } else if !warnings.is_empty() {
            SnapshotConsistency::SuitableForObservation
        } else {
            SnapshotConsistency::StrongEnoughForControl
        };
        if matches!(status, SnapshotConsistency::SuitableForObservation) {
            warnings.push("snapshot is observable but contains non-critical warnings".to_string());
        }
        Self {
            snapshot_id: snapshot.snapshot_id.clone(),
            generated_at: snapshot.generated_at,
            oldest_required_component_at: oldest,
            newest_component_at: newest,
            maximum_component_age_ms,
            component_time_skew_ms,
            status,
            warnings,
            critical_errors,
        }
    }

    pub fn allows_control(&self) -> bool {
        self.status == SnapshotConsistency::StrongEnoughForControl
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeOrderBookLevel {
    pub price: f64,
    pub quantity: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeOrderBookSnapshot {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub bids: Vec<RuntimeOrderBookLevel>,
    pub asks: Vec<RuntimeOrderBookLevel>,
    #[serde(default)]
    pub sequence: Option<u64>,
    #[serde(default)]
    pub exchange_timestamp: Option<DateTime<Utc>>,
    pub received_at: DateTime<Utc>,
    #[serde(default)]
    pub is_stale: bool,
}

impl From<&OrderBookSnapshot> for RuntimeOrderBookSnapshot {
    fn from(snapshot: &OrderBookSnapshot) -> Self {
        Self {
            exchange: snapshot.exchange_id.as_str().to_string(),
            market_type: snapshot.market_type,
            symbol: super::normalize_symbol(snapshot.canonical_symbol.as_str()),
            bids: snapshot
                .bids
                .iter()
                .map(|level| RuntimeOrderBookLevel {
                    price: level.price,
                    quantity: level.quantity,
                })
                .collect(),
            asks: snapshot
                .asks
                .iter()
                .map(|level| RuntimeOrderBookLevel {
                    price: level.price,
                    quantity: level.quantity,
                })
                .collect(),
            sequence: snapshot.sequence,
            exchange_timestamp: snapshot.exchange_timestamp,
            received_at: snapshot.received_at,
            is_stale: snapshot.is_stale,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeSymbolRule {
    pub exchange: String,
    pub market_type: MarketType,
    pub internal_symbol: String,
    pub exchange_symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub price_precision: u32,
    pub quantity_precision: u32,
    pub tick_size: f64,
    pub step_size: f64,
    pub min_quantity: f64,
    pub min_notional: f64,
    #[serde(default)]
    pub max_quantity: Option<f64>,
    #[serde(default)]
    pub status: SymbolStatus,
    #[serde(default)]
    pub supported_order_types: Vec<OrderType>,
    #[serde(default)]
    pub supported_time_in_force: Vec<TimeInForce>,
}

impl TryFrom<&SymbolCapability> for RuntimeSymbolRule {
    type Error = &'static str;

    fn try_from(capability: &SymbolCapability) -> Result<Self, Self::Error> {
        Ok(Self {
            exchange: capability.exchange_id.as_str().to_string(),
            market_type: capability.market_type,
            internal_symbol: super::normalize_symbol(capability.canonical_symbol.as_str()),
            exchange_symbol: capability.exchange_symbol.symbol.clone(),
            base_asset: capability.base_asset.clone(),
            quote_asset: capability.quote_asset.clone(),
            price_precision: capability.price_precision.ok_or("price_precision")?,
            quantity_precision: capability.quantity_precision.ok_or("quantity_precision")?,
            tick_size: capability.tick_size.ok_or("tick_size")?,
            step_size: capability.step_size.ok_or("step_size")?,
            min_quantity: capability.min_quantity.ok_or("min_quantity")?,
            min_notional: capability.min_notional.ok_or("min_notional")?,
            max_quantity: capability.max_quantity,
            status: shared_symbol_status(capability.status),
            supported_order_types: capability.supported_order_types.clone(),
            supported_time_in_force: capability.supported_time_in_force.clone(),
        })
    }
}

fn shared_symbol_status(status: SharedSymbolStatus) -> SymbolStatus {
    match status {
        SharedSymbolStatus::Trading => SymbolStatus::Trading,
        SharedSymbolStatus::Halted => SymbolStatus::Halted,
        SharedSymbolStatus::Suspended => SymbolStatus::Suspended,
        SharedSymbolStatus::ReduceOnly => SymbolStatus::ReduceOnly,
        SharedSymbolStatus::PreOpen => SymbolStatus::PreOpen,
        SharedSymbolStatus::Settling => SymbolStatus::Settling,
        SharedSymbolStatus::Delisted => SymbolStatus::Delisted,
        SharedSymbolStatus::Unknown => SymbolStatus::Unknown,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeFeeView {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub maker_fee_bps: f64,
    pub taker_fee_bps: f64,
    pub source: FeeSource,
    pub updated_at: DateTime<Utc>,
    pub authoritative: bool,
    pub fallback: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeBalanceView {
    pub exchange: String,
    pub market_type: MarketType,
    pub asset: String,
    pub total: f64,
    pub available: f64,
    pub locked_by_exchange: f64,
    pub locally_reserved: f64,
    pub effective_available: f64,
    pub source: String,
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeReservationView {
    pub exchange: String,
    pub asset: String,
    pub locally_reserved: f64,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeUnmanagedPosition {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub asset: String,
    pub quantity: f64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeDisabledState {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub blocked: bool,
    #[serde(default)]
    pub reason: Option<String>,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryOwnership {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub asset: String,
    pub total_balance: f64,
    pub exchange_locked: f64,
    pub locally_reserved: f64,
    pub unmanaged_quantity: f64,
    pub other_strategy_owned_quantity: f64,
    pub spot_control_managed_quantity: f64,
    pub effective_sellable_managed_quantity: f64,
    pub ownership_known: bool,
    pub source: String,
    pub reasons: Vec<String>,
}

impl InventoryOwnership {
    pub fn compute(
        exchange: impl Into<String>,
        symbol: impl Into<String>,
        asset: impl Into<String>,
        total_balance: f64,
        exchange_locked: f64,
        locally_reserved: f64,
        unmanaged_quantity: f64,
        other_strategy_owned_quantity: f64,
        ownership_known: bool,
    ) -> Self {
        let spot_control_managed_quantity = if ownership_known {
            (total_balance - unmanaged_quantity - other_strategy_owned_quantity).max(0.0)
        } else {
            0.0
        };
        let effective_sellable_managed_quantity =
            (spot_control_managed_quantity - exchange_locked - locally_reserved).max(0.0);
        let mut reasons = Vec::new();
        if !ownership_known {
            reasons.push("inventory ownership unknown".to_string());
        }
        if unmanaged_quantity > 0.0 {
            reasons.push("unmanaged inventory excluded".to_string());
        }
        if other_strategy_owned_quantity > 0.0 {
            reasons.push("other strategy inventory excluded".to_string());
        }
        if exchange_locked > 0.0 {
            reasons.push("exchange locked inventory excluded".to_string());
        }
        if locally_reserved > 0.0 {
            reasons.push("local reservation excluded".to_string());
        }
        Self {
            exchange: super::normalize_exchange(&exchange.into()),
            market_type: MarketType::Spot,
            symbol: super::normalize_symbol(&symbol.into()),
            asset: asset.into().to_ascii_uppercase(),
            total_balance,
            exchange_locked,
            locally_reserved,
            unmanaged_quantity,
            other_strategy_owned_quantity,
            spot_control_managed_quantity,
            effective_sellable_managed_quantity,
            ownership_known,
            source: "authoritative_runtime_snapshot".to_string(),
            reasons,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenOrderOwnership {
    pub exchange: String,
    pub symbol: String,
    pub order_id: String,
    #[serde(default)]
    pub client_order_id: Option<String>,
    pub owning_strategy: String,
    #[serde(default)]
    pub lifecycle_command_id_optional: Option<String>,
    pub side: OrderSide,
    pub quantity: f64,
    pub filled_quantity: f64,
    pub remaining_quantity: f64,
    pub status: OrderStatus,
    pub reservation_impact: RuntimeReservationView,
    pub ownership_known: bool,
    #[serde(default = "default_order_ownership_class")]
    pub ownership_class: OrderOwnershipClass,
}

fn default_order_ownership_class() -> OrderOwnershipClass {
    OrderOwnershipClass::Unknown
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DirectionReadinessStatus {
    Ready,
    ObserveOnly,
    BuyInventoryMissing,
    SellInventoryMissing,
    BookStale,
    FeeFallbackWarning,
    Disabled,
    Locked,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectionReadiness {
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub buy_quote_available: f64,
    pub sell_base_available: f64,
    pub buy_book_fresh: bool,
    pub sell_book_fresh: bool,
    pub buy_symbol_tradable: bool,
    pub sell_symbol_tradable: bool,
    pub buy_fee_known: bool,
    pub sell_fee_known: bool,
    pub max_safe_notional: f64,
    pub readiness: DirectionReadinessStatus,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LiquidationPreview {
    pub market_plans: Vec<MarketLiquidationPlan>,
    pub passive_sessions: Vec<PassiveLiquidationSession>,
    pub dust: Vec<DustPosition>,
    pub rejected: bool,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotControlRuntimeSnapshotReadModel {
    pub generated_at: DateTime<Utc>,
    pub snapshot_id: String,
    pub symbol: String,
    pub selected_exchanges: Vec<String>,
    pub symbol_rules: BTreeMap<String, RuntimeSymbolRule>,
    pub books: BTreeMap<String, RuntimeOrderBookSnapshot>,
    pub book_health: Vec<SnapshotComponentStatus>,
    pub exchange_health: Vec<SnapshotComponentStatus>,
    pub fees: BTreeMap<String, RuntimeFeeView>,
    pub balances: Vec<RuntimeBalanceView>,
    pub reservations: Vec<RuntimeReservationView>,
    pub inventory_ownership: Vec<InventoryOwnership>,
    pub unmanaged_positions: Vec<RuntimeUnmanagedPosition>,
    pub disabled_state: Vec<RuntimeDisabledState>,
    pub open_orders: Vec<OpenOrderOwnership>,
    #[serde(default)]
    pub fill_ownership: Vec<FillOwnershipRecord>,
    pub direction_readiness: Vec<DirectionReadiness>,
    pub effective_tradability: Option<serde_json::Value>,
    pub liquidation_preview: LiquidationPreview,
    pub data_sources: Vec<String>,
    #[serde(default = "default_snapshot_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub source_metadata: Vec<RuntimeComponentMetadata>,
    #[serde(default)]
    pub consistency_report: Option<SnapshotConsistencyReport>,
    pub component_statuses: Vec<SnapshotComponentStatus>,
    pub warnings: Vec<String>,
    pub critical_errors: Vec<String>,
}

fn default_snapshot_schema_version() -> u32 {
    1
}

impl SpotControlRuntimeSnapshotReadModel {
    pub fn age_ms(&self) -> i64 {
        Utc::now()
            .signed_duration_since(self.generated_at)
            .num_milliseconds()
            .max(0)
    }

    pub fn has_critical_errors(&self) -> bool {
        !self.critical_errors.is_empty()
    }

    pub fn component(&self, name: &str) -> Option<&SnapshotComponentStatus> {
        self.component_statuses
            .iter()
            .find(|status| status.component == name)
    }
}

pub fn inventory_status_from_direction(readiness: &[DirectionReadiness]) -> InventoryReadiness {
    if readiness.is_empty() {
        return InventoryReadiness::Unknown;
    }
    let buy_ok = readiness.iter().any(|item| item.buy_quote_available > 0.0);
    let sell_ok = readiness.iter().any(|item| item.sell_base_available > 0.0);
    match (buy_ok, sell_ok) {
        (true, true) => InventoryReadiness::ReadyBothDirections,
        (true, false) => InventoryReadiness::ReadyBuyOnly,
        (false, true) => InventoryReadiness::ReadySellOnly,
        (false, false) => InventoryReadiness::Unknown,
    }
}

pub fn supports_post_only(rule: &RuntimeSymbolRule) -> bool {
    rule.supported_order_types.contains(&OrderType::PostOnly)
        || rule.supported_time_in_force.contains(&TimeInForce::GTX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_snapshot() -> SpotControlRuntimeSnapshotReadModel {
        SpotControlRuntimeSnapshotReadModel {
            generated_at: Utc::now(),
            snapshot_id: "snapshot-1".to_string(),
            symbol: "BTCUSDT".to_string(),
            selected_exchanges: Vec::new(),
            symbol_rules: BTreeMap::new(),
            books: BTreeMap::new(),
            book_health: Vec::new(),
            exchange_health: Vec::new(),
            fees: BTreeMap::new(),
            balances: Vec::new(),
            reservations: Vec::new(),
            inventory_ownership: Vec::new(),
            unmanaged_positions: Vec::new(),
            disabled_state: Vec::new(),
            open_orders: Vec::new(),
            fill_ownership: Vec::new(),
            direction_readiness: Vec::new(),
            effective_tradability: None,
            liquidation_preview: LiquidationPreview::default(),
            data_sources: Vec::new(),
            schema_version: 1,
            source_metadata: Vec::new(),
            consistency_report: None,
            component_statuses: Vec::new(),
            warnings: Vec::new(),
            critical_errors: Vec::new(),
        }
    }

    #[test]
    fn snapshot_consistency_should_allow_clean_snapshot() {
        let mut snapshot = empty_snapshot();
        snapshot
            .component_statuses
            .push(SnapshotComponentStatus::fresh(
                "books",
                snapshot.generated_at,
                "test",
            ));

        let report = SnapshotConsistencyReport::from_snapshot(&snapshot, 10_000);

        assert_eq!(report.status, SnapshotConsistency::StrongEnoughForControl);
        assert!(report.allows_control());
    }

    #[test]
    fn inventory_ownership_should_exclude_unmanaged_balances() {
        let ownership = InventoryOwnership::compute(
            "GateIO", "btc_usdt", "btc", 10.0, 1.0, 2.0, 3.0, 1.0, true,
        );

        assert_eq!(ownership.exchange, "gateio");
        assert_eq!(ownership.symbol, "BTCUSDT");
        assert_eq!(ownership.spot_control_managed_quantity, 6.0);
        assert_eq!(ownership.effective_sellable_managed_quantity, 3.0);
    }

    #[test]
    fn runtime_order_book_should_adapt_shared_market_contract() {
        let shared = OrderBookSnapshot::new(
            rustcta_types::ExchangeId::new("GateIO").expect("valid exchange"),
            MarketType::Spot,
            rustcta_types::CanonicalSymbol::new("BTC", "USDT").expect("valid symbol"),
            vec![rustcta_types::OrderBookLevel::new(70_000.0, 0.5).expect("valid bid")],
            vec![rustcta_types::OrderBookLevel::new(70_001.0, 0.4).expect("valid ask")],
            Utc::now(),
        )
        .expect("valid shared order book");

        let runtime = RuntimeOrderBookSnapshot::from(&shared);

        assert_eq!(runtime.exchange, "gateio");
        assert_eq!(runtime.symbol, "BTCUSDT");
        assert_eq!(runtime.bids[0].price, 70_000.0);
    }

    #[test]
    fn runtime_symbol_rule_should_adapt_shared_symbol_capability() {
        let exchange = rustcta_types::ExchangeId::new("Bitget").expect("valid exchange");
        let canonical_symbol =
            rustcta_types::CanonicalSymbol::new("ETH", "USDT").expect("valid symbol");
        let exchange_symbol =
            rustcta_types::ExchangeSymbol::new(exchange.clone(), MarketType::Spot, "ETHUSDT")
                .expect("valid exchange symbol");
        let mut capability = SymbolCapability::new(
            exchange,
            MarketType::Spot,
            canonical_symbol,
            exchange_symbol,
        );
        capability.status = SharedSymbolStatus::Trading;
        capability.price_precision = Some(2);
        capability.quantity_precision = Some(6);
        capability.tick_size = Some(0.01);
        capability.step_size = Some(0.000001);
        capability.min_quantity = Some(0.00001);
        capability.min_notional = Some(5.0);
        capability.supported_order_types = vec![OrderType::Limit, OrderType::PostOnly];
        capability.supported_time_in_force = vec![TimeInForce::GTC, TimeInForce::GTX];

        let rule = RuntimeSymbolRule::try_from(&capability).expect("runtime rule");

        assert_eq!(rule.exchange, "bitget");
        assert_eq!(rule.internal_symbol, "ETHUSDT");
        assert_eq!(rule.status, SymbolStatus::Trading);
        assert!(supports_post_only(&rule));
    }
}
