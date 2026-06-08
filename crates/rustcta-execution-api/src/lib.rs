//! Stable execution command and event protocol for RustCTA.
//!
//! This crate is intentionally free of venue signing, REST, websocket, strategy,
//! and supervisor implementation details. Types here are designed for strategy
//! submission, gateway routing, audit persistence, and deterministic replay.

// Public protocol errors intentionally preserve the shared ExchangeError payload
// so callers and serialized events keep the same schema.
#![allow(clippy::large_enum_variant, clippy::result_large_err)]

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeBalance, ExchangeError, ExchangeId, ExchangeSymbol, Fill,
    LiquidityRole, MarketType, OrderSide, OrderType, PositionSide, RunId, StrategyId, TenantId,
    TimeInForce,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

pub const EXECUTION_API_SCHEMA_VERSION: u16 = 1;

pub type Result<T> = std::result::Result<T, ExecutionApiError>;

#[derive(Debug, Error, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExecutionApiError {
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("unsupported capability: {0}")]
    UnsupportedCapability(String),
    #[error("adapter error: {0}")]
    Adapter(String),
    #[error("exchange error: {0:?}")]
    Exchange(ExchangeError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradingCapabilities {
    pub schema_version: u16,
    pub exchange_id: ExchangeId,
    pub supports_market_orders: bool,
    pub supports_limit_orders: bool,
    pub supports_post_only: bool,
    pub supports_ioc: bool,
    pub supports_fok: bool,
    pub supports_reduce_only: bool,
    pub supports_hedge_mode: bool,
    pub supports_client_order_id: bool,
    pub supports_cancel_by_client_order_id: bool,
    pub supports_cancel_by_exchange_order_id: bool,
    pub supports_cancel_all: bool,
    pub supports_symbol_scoped_cancel_all: bool,
    pub supports_idempotency_key_echo: bool,
    pub max_batch_orders: Option<u32>,
}

impl TradingCapabilities {
    pub fn new(exchange_id: ExchangeId) -> Self {
        Self {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            exchange_id,
            supports_market_orders: true,
            supports_limit_orders: true,
            supports_post_only: true,
            supports_ioc: true,
            supports_fok: false,
            supports_reduce_only: true,
            supports_hedge_mode: true,
            supports_client_order_id: true,
            supports_cancel_by_client_order_id: true,
            supports_cancel_by_exchange_order_id: true,
            supports_cancel_all: false,
            supports_symbol_scoped_cancel_all: false,
            supports_idempotency_key_echo: false,
            max_batch_orders: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationIdentity {
    pub tenant_id: TenantId,
    pub account_id: AccountId,
    pub strategy_id: StrategyId,
    pub run_id: RunId,
    pub idempotency_key: String,
    pub risk_profile_id: String,
    pub requested_at: DateTime<Utc>,
}

impl MutationIdentity {
    pub fn validate(&self) -> Result<()> {
        validate_required_text("idempotency_key", &self.idempotency_key)?;
        validate_required_text("risk_profile_id", &self.risk_profile_id)?;
        Ok(())
    }
}

pub trait HasMutationIdentity {
    fn identity(&self) -> &MutationIdentity;
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderCommand {
    pub schema_version: u16,
    #[serde(flatten)]
    pub identity: MutationIdentity,
    pub command_id: String,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: ExchangeSymbol,
    pub client_order_id: String,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub quantity: f64,
    pub price: Option<f64>,
    pub post_only: bool,
    pub reduce_only: bool,
    pub max_slippage_bps: Option<u32>,
    pub correlation_id: Option<String>,
    pub source_intent_id: Option<String>,
}

impl HasMutationIdentity for OrderCommand {
    fn identity(&self) -> &MutationIdentity {
        &self.identity
    }
}

impl OrderCommand {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identity: MutationIdentity,
        command_id: impl Into<String>,
        exchange_id: ExchangeId,
        market_type: MarketType,
        canonical_symbol: CanonicalSymbol,
        exchange_symbol: ExchangeSymbol,
        client_order_id: impl Into<String>,
        side: OrderSide,
        position_side: PositionSide,
        order_type: OrderType,
        time_in_force: TimeInForce,
        quantity: f64,
        price: Option<f64>,
    ) -> Self {
        Self {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            identity,
            command_id: command_id.into(),
            exchange_id,
            market_type,
            canonical_symbol,
            exchange_symbol,
            client_order_id: client_order_id.into(),
            side,
            position_side,
            order_type,
            time_in_force,
            quantity,
            price,
            post_only: false,
            reduce_only: false,
            max_slippage_bps: None,
            correlation_id: None,
            source_intent_id: None,
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_schema_version(self.schema_version)?;
        self.identity.validate()?;
        validate_required_text("command_id", &self.command_id)?;
        validate_required_text("client_order_id", &self.client_order_id)?;
        validate_positive_f64("quantity", self.quantity)?;

        if self.price.is_some() {
            validate_optional_positive_f64("price", self.price)?;
        } else if order_type_requires_price(self.order_type) || self.post_only {
            return Err(ExecutionApiError::Validation(
                "price is required for priced order types".to_string(),
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderAck {
    pub schema_version: u16,
    pub tenant_id: TenantId,
    pub account_id: AccountId,
    pub strategy_id: StrategyId,
    pub run_id: RunId,
    pub command_id: String,
    pub exchange_id: ExchangeId,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub idempotency_key: String,
    pub accepted: bool,
    pub state: OrderState,
    pub message: Option<String>,
    pub acknowledged_at: DateTime<Utc>,
}

impl OrderAck {
    pub fn rejected(
        command: &OrderCommand,
        message: impl Into<String>,
        acknowledged_at: DateTime<Utc>,
    ) -> Self {
        Self {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            tenant_id: command.identity.tenant_id.clone(),
            account_id: command.identity.account_id.clone(),
            strategy_id: command.identity.strategy_id.clone(),
            run_id: command.identity.run_id.clone(),
            command_id: command.command_id.clone(),
            exchange_id: command.exchange_id.clone(),
            client_order_id: command.client_order_id.clone(),
            exchange_order_id: None,
            idempotency_key: command.identity.idempotency_key.clone(),
            accepted: false,
            state: OrderState::Rejected,
            message: Some(message.into()),
            acknowledged_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancellationIds {
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
}

impl CancellationIds {
    pub fn by_client_order_id(client_order_id: impl Into<String>) -> Self {
        Self {
            client_order_id: Some(client_order_id.into()),
            exchange_order_id: None,
        }
    }

    pub fn by_exchange_order_id(exchange_order_id: impl Into<String>) -> Self {
        Self {
            client_order_id: None,
            exchange_order_id: Some(exchange_order_id.into()),
        }
    }

    pub fn validate(&self) -> Result<()> {
        match (&self.client_order_id, &self.exchange_order_id) {
            (Some(client_order_id), _) if client_order_id.trim().is_empty() => Err(
                ExecutionApiError::Validation("client_order_id cannot be blank".to_string()),
            ),
            (_, Some(exchange_order_id)) if exchange_order_id.trim().is_empty() => Err(
                ExecutionApiError::Validation("exchange_order_id cannot be blank".to_string()),
            ),
            (None, None) => Err(ExecutionApiError::Validation(
                "cancel command requires client_order_id or exchange_order_id".to_string(),
            )),
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelCommand {
    pub schema_version: u16,
    #[serde(flatten)]
    pub identity: MutationIdentity,
    pub command_id: String,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: ExchangeSymbol,
    #[serde(flatten)]
    pub cancellation_ids: CancellationIds,
    pub reason: Option<String>,
    pub correlation_id: Option<String>,
}

impl HasMutationIdentity for CancelCommand {
    fn identity(&self) -> &MutationIdentity {
        &self.identity
    }
}

impl CancelCommand {
    pub fn new(
        identity: MutationIdentity,
        command_id: impl Into<String>,
        exchange_id: ExchangeId,
        market_type: MarketType,
        canonical_symbol: CanonicalSymbol,
        exchange_symbol: ExchangeSymbol,
        cancellation_ids: CancellationIds,
    ) -> Self {
        Self {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            identity,
            command_id: command_id.into(),
            exchange_id,
            market_type,
            canonical_symbol,
            exchange_symbol,
            cancellation_ids,
            reason: None,
            correlation_id: None,
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_schema_version(self.schema_version)?;
        self.identity.validate()?;
        validate_required_text("command_id", &self.command_id)?;
        self.cancellation_ids.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelAck {
    pub schema_version: u16,
    pub tenant_id: TenantId,
    pub account_id: AccountId,
    pub strategy_id: StrategyId,
    pub run_id: RunId,
    pub command_id: String,
    pub exchange_id: ExchangeId,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub idempotency_key: String,
    pub accepted: bool,
    pub state: OrderState,
    pub message: Option<String>,
    pub acknowledged_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelAllCommand {
    pub schema_version: u16,
    #[serde(flatten)]
    pub identity: MutationIdentity,
    pub command_id: String,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: Option<CanonicalSymbol>,
    pub exchange_symbol: Option<ExchangeSymbol>,
    pub cancel_all_id: String,
    pub reason: Option<String>,
    pub correlation_id: Option<String>,
}

impl HasMutationIdentity for CancelAllCommand {
    fn identity(&self) -> &MutationIdentity {
        &self.identity
    }
}

impl CancelAllCommand {
    pub fn new(
        identity: MutationIdentity,
        command_id: impl Into<String>,
        exchange_id: ExchangeId,
        market_type: MarketType,
        cancel_all_id: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            identity,
            command_id: command_id.into(),
            exchange_id,
            market_type,
            canonical_symbol: None,
            exchange_symbol: None,
            cancel_all_id: cancel_all_id.into(),
            reason: None,
            correlation_id: None,
        }
    }

    pub fn for_symbol(
        mut self,
        canonical_symbol: CanonicalSymbol,
        exchange_symbol: ExchangeSymbol,
    ) -> Self {
        self.canonical_symbol = Some(canonical_symbol);
        self.exchange_symbol = Some(exchange_symbol);
        self
    }

    pub fn validate(&self) -> Result<()> {
        validate_schema_version(self.schema_version)?;
        self.identity.validate()?;
        validate_required_text("command_id", &self.command_id)?;
        validate_required_text("cancel_all_id", &self.cancel_all_id)?;

        if self.canonical_symbol.is_some() ^ self.exchange_symbol.is_some() {
            return Err(ExecutionApiError::Validation(
                "canonical_symbol and exchange_symbol must be supplied together".to_string(),
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelAllAck {
    pub schema_version: u16,
    pub tenant_id: TenantId,
    pub account_id: AccountId,
    pub strategy_id: StrategyId,
    pub run_id: RunId,
    pub command_id: String,
    pub exchange_id: ExchangeId,
    pub cancel_all_id: String,
    pub idempotency_key: String,
    pub accepted: bool,
    pub cancelled_orders: u32,
    pub message: Option<String>,
    pub acknowledged_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionFeeRole {
    Maker,
    Taker,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionFeeSource {
    ExchangeApi,
    ConfigDefault,
    SymbolOverride,
    VipOverride,
    PlatformTokenDiscount,
    Fallback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionFeeAssetMode {
    Quote,
    Base,
    PlatformToken,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeRateSnapshot {
    pub schema_version: u16,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: Option<CanonicalSymbol>,
    pub maker_fee_rate: f64,
    pub taker_fee_rate: f64,
    pub fee_asset: Option<String>,
    pub fee_asset_mode: ExecutionFeeAssetMode,
    pub source: ExecutionFeeSource,
    pub observed_at: DateTime<Utc>,
}

impl FeeRateSnapshot {
    pub fn validate(&self) -> Result<()> {
        validate_schema_version(self.schema_version)?;
        validate_finite_f64("maker_fee_rate", self.maker_fee_rate)?;
        validate_finite_f64("taker_fee_rate", self.taker_fee_rate)
    }

    pub fn rate_for_role(&self, role: ExecutionFeeRole) -> f64 {
        match role {
            ExecutionFeeRole::Maker => self.maker_fee_rate,
            ExecutionFeeRole::Taker => self.taker_fee_rate,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeEstimate {
    pub schema_version: u16,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: Option<CanonicalSymbol>,
    pub role: ExecutionFeeRole,
    pub notional: f64,
    pub fee_rate: f64,
    pub fee_asset: Option<String>,
    pub fee_asset_mode: ExecutionFeeAssetMode,
    pub estimated_fee_amount: f64,
    pub source: ExecutionFeeSource,
    pub estimated_at: DateTime<Utc>,
}

impl FeeEstimate {
    pub fn from_rate(
        rate: &FeeRateSnapshot,
        role: ExecutionFeeRole,
        notional: f64,
        estimated_at: DateTime<Utc>,
    ) -> Result<Self> {
        rate.validate()?;
        validate_positive_or_zero_f64("notional", notional)?;
        let fee_rate = rate.rate_for_role(role);
        validate_finite_f64("fee_rate", fee_rate)?;
        Ok(Self {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            exchange_id: rate.exchange_id.clone(),
            market_type: rate.market_type,
            canonical_symbol: rate.canonical_symbol.clone(),
            role,
            notional,
            fee_rate,
            fee_asset: rate.fee_asset.clone(),
            fee_asset_mode: rate.fee_asset_mode,
            estimated_fee_amount: notional.max(0.0) * fee_rate,
            source: rate.source,
            estimated_at,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BundleLegKind {
    Long,
    Short,
    Maker,
    Taker,
    Hedge,
    CloseLong,
    CloseShort,
    EmergencyCloseLong,
    EmergencyCloseShort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BundleSubmissionPath {
    GatewaySequential,
    DryRun,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BundleExecutionStatus {
    Submitted,
    Accepted,
    PartiallyFilled,
    Filled,
    RequiresReconcile,
    OneSidedExposure,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BundleOrderLeg {
    pub leg_id: String,
    pub leg_kind: BundleLegKind,
    pub command: OrderCommand,
}

impl BundleOrderLeg {
    pub fn validate(&self, bundle_id: &str) -> Result<()> {
        validate_required_text("leg_id", &self.leg_id)?;
        self.command.validate()?;
        if self.command.source_intent_id.as_deref() != Some(bundle_id) {
            return Err(ExecutionApiError::Validation(format!(
                "bundle leg {} source_intent_id must match bundle_id",
                self.leg_id
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BundleSubmitCommand {
    pub schema_version: u16,
    #[serde(flatten)]
    pub identity: MutationIdentity,
    pub bundle_id: String,
    pub opportunity_id: Option<String>,
    pub legs: Vec<BundleOrderLeg>,
    pub correlation_id: Option<String>,
    pub requested_at: DateTime<Utc>,
}

impl HasMutationIdentity for BundleSubmitCommand {
    fn identity(&self) -> &MutationIdentity {
        &self.identity
    }
}

impl BundleSubmitCommand {
    pub fn validate(&self) -> Result<()> {
        validate_schema_version(self.schema_version)?;
        self.identity.validate()?;
        validate_required_text("bundle_id", &self.bundle_id)?;
        if self.legs.len() != 2 {
            return Err(ExecutionApiError::Validation(
                "bundle submit requires exactly two legs".to_string(),
            ));
        }
        let mut leg_ids = std::collections::HashSet::new();
        let mut client_order_ids = std::collections::HashSet::new();
        for leg in &self.legs {
            leg.validate(&self.bundle_id)?;
            if !leg_ids.insert(leg.leg_id.clone()) {
                return Err(ExecutionApiError::Validation(format!(
                    "duplicate bundle leg_id {}",
                    leg.leg_id
                )));
            }
            if !client_order_ids.insert(leg.command.client_order_id.clone()) {
                return Err(ExecutionApiError::Validation(format!(
                    "duplicate bundle client_order_id {}",
                    leg.command.client_order_id
                )));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BundleSubmitAck {
    pub schema_version: u16,
    pub tenant_id: TenantId,
    pub account_id: AccountId,
    pub strategy_id: StrategyId,
    pub run_id: RunId,
    pub bundle_id: String,
    pub idempotency_key: String,
    pub submission_path: BundleSubmissionPath,
    pub status: BundleExecutionStatus,
    pub order_acks: Vec<OrderAck>,
    pub requires_reconcile: bool,
    pub message: Option<String>,
    pub acknowledged_at: DateTime<Utc>,
}

impl BundleSubmitAck {
    pub fn from_order_acks(
        command: &BundleSubmitCommand,
        submission_path: BundleSubmissionPath,
        order_acks: Vec<OrderAck>,
        acknowledged_at: DateTime<Utc>,
    ) -> Self {
        let has_reject = order_acks.iter().any(|ack| !ack.accepted);
        let has_partial = order_acks
            .iter()
            .any(|ack| ack.state == OrderState::PartiallyFilled);
        let all_filled =
            !order_acks.is_empty() && order_acks.iter().all(|ack| ack.state == OrderState::Filled);
        let all_accepted = !order_acks.is_empty() && order_acks.iter().all(|ack| ack.accepted);
        let status = if has_reject {
            BundleExecutionStatus::RequiresReconcile
        } else if has_partial {
            BundleExecutionStatus::PartiallyFilled
        } else if all_filled {
            BundleExecutionStatus::Filled
        } else if all_accepted {
            BundleExecutionStatus::Accepted
        } else {
            BundleExecutionStatus::Submitted
        };
        let requires_reconcile = matches!(
            status,
            BundleExecutionStatus::RequiresReconcile
                | BundleExecutionStatus::PartiallyFilled
                | BundleExecutionStatus::OneSidedExposure
        );
        Self {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            tenant_id: command.identity.tenant_id.clone(),
            account_id: command.identity.account_id.clone(),
            strategy_id: command.identity.strategy_id.clone(),
            run_id: command.identity.run_id.clone(),
            bundle_id: command.bundle_id.clone(),
            idempotency_key: command.identity.idempotency_key.clone(),
            submission_path,
            status,
            order_acks,
            requires_reconcile,
            message: requires_reconcile
                .then(|| format!("bundle {} requires reconciliation", command.bundle_id)),
            acknowledged_at,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMutationKind {
    PlaceOrder,
    CancelOrder,
    CancelAllOrders,
    SubmitBundle,
    FeeModel,
    LiveDryRun,
    Reservation,
    Idempotency,
    Reconciliation,
    Rejection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionDecisionOutcome {
    Approved,
    Rejected,
    Recorded,
    Skipped,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeModelDecision {
    pub schema_version: u16,
    #[serde(flatten)]
    pub identity: MutationIdentity,
    pub command_id: String,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: Option<CanonicalSymbol>,
    pub maker_fee_rate: Option<f64>,
    pub taker_fee_rate: Option<f64>,
    pub estimated_fee_asset: Option<String>,
    pub estimated_fee_amount: Option<f64>,
    pub decided_at: DateTime<Utc>,
}

impl FeeModelDecision {
    pub fn validate(&self) -> Result<()> {
        validate_schema_version(self.schema_version)?;
        self.identity.validate()?;
        validate_required_text("command_id", &self.command_id)?;
        validate_optional_finite_f64("maker_fee_rate", self.maker_fee_rate)?;
        validate_optional_finite_f64("taker_fee_rate", self.taker_fee_rate)?;
        validate_optional_positive_or_zero_f64("estimated_fee_amount", self.estimated_fee_amount)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReservationDecision {
    pub schema_version: u16,
    #[serde(flatten)]
    pub identity: MutationIdentity,
    pub command_id: String,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: Option<CanonicalSymbol>,
    pub asset: Option<String>,
    pub requested_quantity: Option<f64>,
    pub reserved_quantity: Option<f64>,
    pub outcome: ExecutionDecisionOutcome,
    pub reason: Option<String>,
    pub decided_at: DateTime<Utc>,
}

impl ReservationDecision {
    pub fn validate(&self) -> Result<()> {
        validate_schema_version(self.schema_version)?;
        self.identity.validate()?;
        validate_required_text("command_id", &self.command_id)?;
        validate_optional_positive_or_zero_f64("requested_quantity", self.requested_quantity)?;
        validate_optional_positive_or_zero_f64("reserved_quantity", self.reserved_quantity)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotencyDecision {
    pub schema_version: u16,
    #[serde(flatten)]
    pub identity: MutationIdentity,
    pub command_id: String,
    pub mutation_kind: ExecutionMutationKind,
    pub outcome: ExecutionDecisionOutcome,
    pub existing_command_id: Option<String>,
    pub reason: Option<String>,
    pub decided_at: DateTime<Utc>,
}

impl IdempotencyDecision {
    pub fn validate(&self) -> Result<()> {
        validate_schema_version(self.schema_version)?;
        self.identity.validate()?;
        validate_required_text("command_id", &self.command_id)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiveDryRunDecision {
    pub schema_version: u16,
    #[serde(flatten)]
    pub identity: MutationIdentity,
    pub command_id: String,
    pub mutation_kind: ExecutionMutationKind,
    pub gateway_mutation_blocked: bool,
    pub message: String,
    pub decided_at: DateTime<Utc>,
}

impl LiveDryRunDecision {
    pub fn validate(&self) -> Result<()> {
        validate_schema_version(self.schema_version)?;
        self.identity.validate()?;
        validate_required_text("command_id", &self.command_id)?;
        validate_required_text("message", &self.message)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RejectionDecision {
    pub schema_version: u16,
    #[serde(flatten)]
    pub identity: MutationIdentity,
    pub command_id: String,
    pub mutation_kind: ExecutionMutationKind,
    pub reason: String,
    pub rejected_at: DateTime<Utc>,
    #[serde(default)]
    pub metadata: Value,
}

impl RejectionDecision {
    pub fn validate(&self) -> Result<()> {
        validate_schema_version(self.schema_version)?;
        self.identity.validate()?;
        validate_required_text("command_id", &self.command_id)?;
        validate_required_text("reason", &self.reason)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderState {
    Planned,
    Submitted,
    Accepted,
    PartiallyFilled,
    Filled,
    CancelRequested,
    Cancelled,
    Rejected,
    Expired,
    Failed,
    Unknown,
}

impl OrderState {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Cancelled | Self::Rejected | Self::Expired | Self::Failed
        )
    }

    pub fn is_open(self) -> bool {
        matches!(
            self,
            Self::Submitted | Self::Accepted | Self::PartiallyFilled
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskDecision {
    Approved {
        risk_profile_id: String,
        decision_id: String,
        decided_at: DateTime<Utc>,
    },
    Rejected {
        risk_profile_id: String,
        decision_id: String,
        reason: String,
        decided_at: DateTime<Utc>,
    },
    Reduced {
        risk_profile_id: String,
        decision_id: String,
        reason: String,
        decided_at: DateTime<Utc>,
    },
}

impl RiskDecision {
    pub fn is_approved(&self) -> bool {
        matches!(self, Self::Approved { .. } | Self::Reduced { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExecutionEvent {
    OrderSubmitted(OrderAck),
    OrderUpdated(OrderStatusEvent),
    OrderRejected(OrderAck),
    CancelSubmitted(CancelAck),
    CancelAllSubmitted(CancelAllAck),
    Fill(FillEvent),
    Reconciliation(ReconciliationEvent),
    RiskDecision(RiskDecisionEvent),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderStatusEvent {
    pub schema_version: u16,
    pub tenant_id: TenantId,
    pub account_id: AccountId,
    pub strategy_id: StrategyId,
    pub run_id: RunId,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: ExchangeSymbol,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub state: OrderState,
    pub filled_quantity: f64,
    pub average_fill_price: Option<f64>,
    pub observed_at: DateTime<Utc>,
}

impl OrderStatusEvent {
    pub fn validate(&self) -> Result<()> {
        validate_schema_version(self.schema_version)?;
        validate_optional_positive_or_zero_f64("filled_quantity", Some(self.filled_quantity))?;
        validate_optional_positive_f64("average_fill_price", self.average_fill_price)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FillEvent {
    pub schema_version: u16,
    pub tenant_id: TenantId,
    pub account_id: AccountId,
    pub strategy_id: StrategyId,
    pub run_id: RunId,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: ExchangeSymbol,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub fill_id: String,
    pub trade_id: Option<String>,
    pub side: OrderSide,
    pub liquidity: LiquidityRole,
    pub price: f64,
    pub quantity: f64,
    pub fee_asset: Option<String>,
    pub fee_amount: Option<f64>,
    pub exchange_fill: Option<Fill>,
    pub filled_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
}

impl FillEvent {
    pub fn validate(&self) -> Result<()> {
        validate_schema_version(self.schema_version)?;
        validate_required_text("fill_id", &self.fill_id)?;
        validate_positive_f64("price", self.price)?;
        validate_positive_f64("quantity", self.quantity)?;
        validate_optional_positive_or_zero_f64("fee_amount", self.fee_amount)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReconciliationEvent {
    pub schema_version: u16,
    pub tenant_id: TenantId,
    pub account_id: AccountId,
    pub strategy_id: Option<StrategyId>,
    pub run_id: Option<RunId>,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: Option<CanonicalSymbol>,
    pub exchange_symbol: Option<ExchangeSymbol>,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub expected_state: Option<OrderState>,
    pub observed_state: Option<OrderState>,
    pub balance_snapshot: Vec<ExchangeBalance>,
    pub discrepancy: Option<String>,
    pub reconciled_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationTrigger {
    PrivateStreamLag,
    PrivateStreamDisconnected,
    UnknownPrivateOrderEvent,
    StartupRecovery,
    Manual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReconciliationSeverity {
    Info,
    Warn,
    Error,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderReconciliationOutcome {
    Filled,
    PartiallyFilled,
    Cancelled,
    Rejected,
    Expired,
    Unknown,
    OrderNotFound,
    Timeout,
    RateLimited,
    InconsistentStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderReconciliationSummary {
    pub schema_version: u16,
    pub tenant_id: TenantId,
    pub account_id: AccountId,
    pub strategy_id: Option<StrategyId>,
    pub run_id: Option<RunId>,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: Option<CanonicalSymbol>,
    pub exchange_symbol: Option<ExchangeSymbol>,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub trigger: ReconciliationTrigger,
    pub outcome: OrderReconciliationOutcome,
    pub severity: ReconciliationSeverity,
    pub attempts: u32,
    pub message: String,
    pub reconciled_at: DateTime<Utc>,
}

impl OrderReconciliationSummary {
    pub fn validate(&self) -> Result<()> {
        validate_schema_version(self.schema_version)?;
        validate_required_text("message", &self.message)?;
        if self.canonical_symbol.is_some() ^ self.exchange_symbol.is_some() {
            return Err(ExecutionApiError::Validation(
                "canonical_symbol and exchange_symbol must be supplied together".to_string(),
            ));
        }
        Ok(())
    }
}

impl ReconciliationEvent {
    pub fn validate(&self) -> Result<()> {
        validate_schema_version(self.schema_version)?;
        if self.canonical_symbol.is_some() ^ self.exchange_symbol.is_some() {
            return Err(ExecutionApiError::Validation(
                "canonical_symbol and exchange_symbol must be supplied together".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NormalizedUserStreamEvent {
    pub schema_version: u16,
    pub tenant_id: TenantId,
    pub account_id: AccountId,
    pub strategy_id: Option<StrategyId>,
    pub run_id: Option<RunId>,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: Option<CanonicalSymbol>,
    pub exchange_symbol: Option<ExchangeSymbol>,
    pub event_id: Option<String>,
    pub exchange_sequence: Option<u64>,
    pub received_at: DateTime<Utc>,
    pub kind: NormalizedUserStreamEventKind,
    #[serde(default)]
    pub raw: Option<Value>,
}

impl NormalizedUserStreamEvent {
    pub fn validate(&self) -> Result<()> {
        validate_schema_version(self.schema_version)?;
        if self.canonical_symbol.is_some() ^ self.exchange_symbol.is_some() {
            return Err(ExecutionApiError::Validation(
                "canonical_symbol and exchange_symbol must be supplied together".to_string(),
            ));
        }
        match &self.kind {
            NormalizedUserStreamEventKind::Order(event) => event.validate(),
            NormalizedUserStreamEventKind::Fill(event) => event.validate(),
            NormalizedUserStreamEventKind::BalanceSnapshot(_) => Ok(()),
            NormalizedUserStreamEventKind::Reconciliation(event) => event.validate(),
            NormalizedUserStreamEventKind::Error { message, .. } => {
                validate_required_text("message", message)
            }
            NormalizedUserStreamEventKind::Heartbeat
            | NormalizedUserStreamEventKind::StreamDisconnected { .. } => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event_kind", content = "event", rename_all = "snake_case")]
pub enum NormalizedUserStreamEventKind {
    Order(OrderStatusEvent),
    Fill(FillEvent),
    BalanceSnapshot(ExchangeBalance),
    Reconciliation(ReconciliationEvent),
    Error {
        code: Option<String>,
        message: String,
        retry_after_ms: Option<u64>,
        occurred_at: DateTime<Utc>,
    },
    Heartbeat,
    StreamDisconnected {
        reason: Option<String>,
        disconnected_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskDecisionEvent {
    pub schema_version: u16,
    pub tenant_id: TenantId,
    pub account_id: AccountId,
    pub strategy_id: StrategyId,
    pub run_id: RunId,
    pub command_id: String,
    pub idempotency_key: String,
    pub decision: RiskDecision,
}

#[async_trait]
pub trait TradingAdapter: Send + Sync {
    fn exchange_id(&self) -> ExchangeId;

    fn capabilities(&self) -> TradingCapabilities;

    async fn place_order(&self, command: OrderCommand) -> Result<OrderAck>;

    async fn cancel_order(&self, command: CancelCommand) -> Result<CancelAck>;

    async fn cancel_all_orders(&self, command: CancelAllCommand) -> Result<CancelAllAck>;
}

pub fn validate_command_identity<C: HasMutationIdentity>(command: &C) -> Result<()> {
    command.identity().validate()
}

pub fn validate_schema_version(schema_version: u16) -> Result<()> {
    if schema_version == EXECUTION_API_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(ExecutionApiError::Validation(format!(
            "unsupported schema_version {schema_version}, expected {EXECUTION_API_SCHEMA_VERSION}"
        )))
    }
}

pub fn validate_required_text(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        Err(ExecutionApiError::Validation(format!(
            "{field} cannot be blank"
        )))
    } else {
        Ok(())
    }
}

pub fn validate_positive_f64(field: &str, value: f64) -> Result<()> {
    if value.is_finite() && value > 0.0 {
        Ok(())
    } else {
        Err(ExecutionApiError::Validation(format!(
            "{field} must be finite and greater than zero"
        )))
    }
}

pub fn validate_positive_or_zero_f64(field: &str, value: f64) -> Result<()> {
    if value.is_finite() && value >= 0.0 {
        Ok(())
    } else {
        Err(ExecutionApiError::Validation(format!(
            "{field} must be finite and greater than or equal to zero"
        )))
    }
}

pub fn validate_finite_f64(field: &str, value: f64) -> Result<()> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(ExecutionApiError::Validation(format!(
            "{field} must be finite"
        )))
    }
}

pub fn validate_optional_positive_f64(field: &str, value: Option<f64>) -> Result<()> {
    match value {
        Some(value) => validate_positive_f64(field, value),
        None => Ok(()),
    }
}

pub fn validate_optional_positive_or_zero_f64(field: &str, value: Option<f64>) -> Result<()> {
    match value {
        Some(value) if value.is_finite() && value >= 0.0 => Ok(()),
        Some(_) => Err(ExecutionApiError::Validation(format!(
            "{field} must be finite and greater than or equal to zero"
        ))),
        None => Ok(()),
    }
}

pub fn validate_optional_finite_f64(field: &str, value: Option<f64>) -> Result<()> {
    match value {
        Some(value) if value.is_finite() => Ok(()),
        Some(_) => Err(ExecutionApiError::Validation(format!(
            "{field} must be finite"
        ))),
        None => Ok(()),
    }
}

pub fn order_type_requires_price(order_type: OrderType) -> bool {
    order_type.requires_limit_price()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn identity() -> MutationIdentity {
        MutationIdentity {
            tenant_id: TenantId::unchecked("tenant-a"),
            account_id: AccountId::unchecked("account-a"),
            strategy_id: StrategyId::unchecked("strategy-a"),
            run_id: RunId::unchecked("run-a"),
            idempotency_key: "idem-1".to_string(),
            risk_profile_id: "risk-conservative".to_string(),
            requested_at: DateTime::parse_from_rfc3339("2026-06-06T00:00:00Z")
                .expect("valid timestamp")
                .with_timezone(&Utc),
        }
    }

    fn exchange_id() -> ExchangeId {
        ExchangeId::unchecked("binance")
    }

    fn canonical_symbol() -> CanonicalSymbol {
        CanonicalSymbol::new("BTC", "USDT").expect("valid canonical symbol")
    }

    fn exchange_symbol() -> ExchangeSymbol {
        ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTCUSDT")
            .expect("valid exchange symbol")
    }

    fn order_command() -> OrderCommand {
        OrderCommand::new(
            identity(),
            "cmd-1",
            exchange_id(),
            MarketType::Spot,
            canonical_symbol(),
            exchange_symbol(),
            "client-1",
            OrderSide::Buy,
            PositionSide::Net,
            OrderType::Limit,
            TimeInForce::GTC,
            0.1,
            Some(50_000.0),
        )
    }

    #[test]
    fn execution_api_should_serialize_order_required_identity_fields() {
        let command = order_command();
        command.validate().expect("valid command");

        let value = serde_json::to_value(&command).expect("serializable order command");

        assert_required_identity_fields(&value);
        assert_eq!(value["client_order_id"], "client-1");
        assert_eq!(value["schema_version"], EXECUTION_API_SCHEMA_VERSION);
    }

    #[test]
    fn execution_api_should_serialize_cancel_required_identity_fields() {
        let command = CancelCommand::new(
            identity(),
            "cancel-1",
            exchange_id(),
            MarketType::Spot,
            canonical_symbol(),
            exchange_symbol(),
            CancellationIds::by_client_order_id("client-1"),
        );
        command.validate().expect("valid cancel command");

        let value = serde_json::to_value(&command).expect("serializable cancel command");

        assert_required_identity_fields(&value);
        assert_eq!(value["client_order_id"], "client-1");
        assert_eq!(value["schema_version"], EXECUTION_API_SCHEMA_VERSION);
    }

    #[test]
    fn execution_api_should_serialize_cancel_all_required_identity_fields() {
        let command = CancelAllCommand::new(
            identity(),
            "cancel-all-1",
            exchange_id(),
            MarketType::Spot,
            "cancel-all-id-1",
        );
        command.validate().expect("valid cancel-all command");

        let value = serde_json::to_value(&command).expect("serializable cancel-all command");

        assert_required_identity_fields(&value);
        assert_eq!(value["cancel_all_id"], "cancel-all-id-1");
        assert_eq!(value["schema_version"], EXECUTION_API_SCHEMA_VERSION);
    }

    #[test]
    fn execution_api_should_reject_cancel_without_cancellation_ids() {
        let command = CancelCommand::new(
            identity(),
            "cancel-1",
            exchange_id(),
            MarketType::Spot,
            canonical_symbol(),
            exchange_symbol(),
            CancellationIds {
                client_order_id: None,
                exchange_order_id: None,
            },
        );

        assert!(matches!(
            command.validate(),
            Err(ExecutionApiError::Validation(message))
                if message.contains("client_order_id or exchange_order_id")
        ));
    }

    #[test]
    fn execution_api_should_reject_non_positive_order_quantity() {
        let mut command = order_command();
        command.quantity = 0.0;

        assert!(matches!(
            command.validate(),
            Err(ExecutionApiError::Validation(message))
                if message.contains("quantity")
        ));
    }

    #[test]
    fn execution_api_should_serialize_router_decision_identity_fields() {
        let decision = IdempotencyDecision {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            identity: identity(),
            command_id: "cmd-1".to_string(),
            mutation_kind: ExecutionMutationKind::PlaceOrder,
            outcome: ExecutionDecisionOutcome::Recorded,
            existing_command_id: None,
            reason: None,
            decided_at: Utc::now(),
        };
        decision.validate().expect("valid decision");

        let value = serde_json::to_value(&decision).expect("serializable decision");

        assert_required_identity_fields(&value);
        assert_eq!(value["mutation_kind"], "place_order");
        assert_eq!(value["outcome"], "recorded");
    }

    #[test]
    fn execution_api_should_reject_invalid_fee_model_decision_values() {
        let decision = FeeModelDecision {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            identity: identity(),
            command_id: "cmd-1".to_string(),
            exchange_id: exchange_id(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(canonical_symbol()),
            maker_fee_rate: Some(f64::NAN),
            taker_fee_rate: Some(0.001),
            estimated_fee_asset: Some("USDT".to_string()),
            estimated_fee_amount: Some(0.1),
            decided_at: Utc::now(),
        };

        assert!(matches!(
            decision.validate(),
            Err(ExecutionApiError::Validation(message)) if message.contains("maker_fee_rate")
        ));
    }

    #[test]
    fn fee_estimate_should_calculate_from_rate_snapshot() {
        let rate = FeeRateSnapshot {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            exchange_id: exchange_id(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(canonical_symbol()),
            maker_fee_rate: 0.001,
            taker_fee_rate: 0.002,
            fee_asset: Some("USDT".to_string()),
            fee_asset_mode: ExecutionFeeAssetMode::Quote,
            source: ExecutionFeeSource::Fallback,
            observed_at: Utc::now(),
        };

        let estimate = FeeEstimate::from_rate(&rate, ExecutionFeeRole::Taker, 100.0, Utc::now())
            .expect("fee estimate");

        assert_eq!(estimate.fee_rate, 0.002);
        assert_eq!(estimate.estimated_fee_amount, 0.2);
        assert_eq!(estimate.fee_asset.as_deref(), Some("USDT"));
    }

    #[test]
    fn bundle_submit_command_should_validate_two_unique_legs() {
        let mut first = order_command();
        first.source_intent_id = Some("bundle-1".to_string());
        let mut second = order_command();
        second.command_id = "cmd-2".to_string();
        second.client_order_id = "client-2".to_string();
        second.identity.idempotency_key = "idem-2".to_string();
        second.source_intent_id = Some("bundle-1".to_string());
        let command = BundleSubmitCommand {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            identity: identity(),
            bundle_id: "bundle-1".to_string(),
            opportunity_id: Some("opportunity-1".to_string()),
            legs: vec![
                BundleOrderLeg {
                    leg_id: "long".to_string(),
                    leg_kind: BundleLegKind::Long,
                    command: first,
                },
                BundleOrderLeg {
                    leg_id: "short".to_string(),
                    leg_kind: BundleLegKind::Short,
                    command: second,
                },
            ],
            correlation_id: None,
            requested_at: Utc::now(),
        };

        command.validate().expect("valid bundle");
        let ack = BundleSubmitAck::from_order_acks(
            &command,
            BundleSubmissionPath::DryRun,
            Vec::new(),
            Utc::now(),
        );
        assert_eq!(ack.bundle_id, "bundle-1");
        assert_eq!(ack.submission_path, BundleSubmissionPath::DryRun);
    }

    #[test]
    fn bundle_submit_command_should_reject_duplicate_client_order_ids() {
        let mut first = order_command();
        first.source_intent_id = Some("bundle-1".to_string());
        let mut second = order_command();
        second.command_id = "cmd-2".to_string();
        second.source_intent_id = Some("bundle-1".to_string());
        let command = BundleSubmitCommand {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            identity: identity(),
            bundle_id: "bundle-1".to_string(),
            opportunity_id: None,
            legs: vec![
                BundleOrderLeg {
                    leg_id: "long".to_string(),
                    leg_kind: BundleLegKind::Long,
                    command: first,
                },
                BundleOrderLeg {
                    leg_id: "short".to_string(),
                    leg_kind: BundleLegKind::Short,
                    command: second,
                },
            ],
            correlation_id: None,
            requested_at: Utc::now(),
        };

        assert!(matches!(
            command.validate(),
            Err(ExecutionApiError::Validation(message))
                if message.contains("duplicate bundle client_order_id")
        ));
    }

    #[test]
    fn normalized_user_stream_event_should_validate_nested_fill() {
        let event = NormalizedUserStreamEvent {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            tenant_id: TenantId::unchecked("tenant-a"),
            account_id: AccountId::unchecked("account-a"),
            strategy_id: Some(StrategyId::unchecked("strategy-a")),
            run_id: Some(RunId::unchecked("run-a")),
            exchange_id: exchange_id(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(canonical_symbol()),
            exchange_symbol: Some(exchange_symbol()),
            event_id: Some("event-1".to_string()),
            exchange_sequence: Some(42),
            received_at: Utc::now(),
            kind: NormalizedUserStreamEventKind::Fill(FillEvent {
                schema_version: EXECUTION_API_SCHEMA_VERSION,
                tenant_id: TenantId::unchecked("tenant-a"),
                account_id: AccountId::unchecked("account-a"),
                strategy_id: StrategyId::unchecked("strategy-a"),
                run_id: RunId::unchecked("run-a"),
                exchange_id: exchange_id(),
                market_type: MarketType::Spot,
                canonical_symbol: canonical_symbol(),
                exchange_symbol: exchange_symbol(),
                client_order_id: Some("client-1".to_string()),
                exchange_order_id: Some("order-1".to_string()),
                fill_id: "fill-1".to_string(),
                trade_id: Some("trade-1".to_string()),
                side: OrderSide::Buy,
                liquidity: LiquidityRole::Taker,
                price: 100.0,
                quantity: 0.1,
                fee_asset: Some("USDT".to_string()),
                fee_amount: Some(0.01),
                exchange_fill: None,
                filled_at: Utc::now(),
                received_at: Utc::now(),
            }),
            raw: None,
        };

        event.validate().expect("valid normalized event");
        let value = serde_json::to_value(&event).expect("serializable event");
        assert_eq!(value["event_id"], "event-1");
        assert_eq!(value["kind"]["event_kind"], "fill");
    }

    #[test]
    fn reconciliation_event_should_require_matching_symbol_scope() {
        let event = ReconciliationEvent {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            tenant_id: TenantId::unchecked("tenant-a"),
            account_id: AccountId::unchecked("account-a"),
            strategy_id: Some(StrategyId::unchecked("strategy-a")),
            run_id: Some(RunId::unchecked("run-a")),
            exchange_id: exchange_id(),
            market_type: MarketType::Spot,
            canonical_symbol: Some(canonical_symbol()),
            exchange_symbol: None,
            client_order_id: None,
            exchange_order_id: None,
            expected_state: Some(OrderState::Accepted),
            observed_state: Some(OrderState::Filled),
            balance_snapshot: Vec::new(),
            discrepancy: Some("missing fill".to_string()),
            reconciled_at: Utc::now(),
        };

        assert!(matches!(
            event.validate(),
            Err(ExecutionApiError::Validation(message))
                if message.contains("canonical_symbol and exchange_symbol")
        ));
    }

    fn assert_required_identity_fields(value: &Value) {
        assert_eq!(value["tenant_id"], "tenant-a");
        assert_eq!(value["account_id"], "account-a");
        assert_eq!(value["strategy_id"], "strategy-a");
        assert_eq!(value["run_id"], "run-a");
        assert_eq!(value["idempotency_key"], "idem-1");
        assert_eq!(value["risk_profile_id"], "risk-conservative");
        assert_eq!(value["requested_at"], "2026-06-06T00:00:00Z");
    }
}
