//! Safe strategy development interface for RustCTA strategy crates.
//!
//! This crate is intentionally adapter-free. Strategies receive a
//! [`StrategyContext`] and submit execution intent through its execution client
//! instead of importing exchange adapters or calling venue REST APIs directly.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// Result type used by SDK-level client abstractions.
pub type SdkResult<T> = Result<T, StrategySdkError>;

/// Error type for SDK-provided abstractions.
#[derive(Debug, Error)]
pub enum StrategySdkError {
    #[error("execution client rejected request: {0}")]
    ExecutionRejected(String),

    #[error("execution client is unavailable: {0}")]
    ExecutionUnavailable(String),

    #[error("invalid strategy command: {0}")]
    InvalidCommand(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// Stable identity for one configured strategy instance.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StrategyInstanceId(pub String);

impl StrategyInstanceId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StrategyInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Runtime contract implemented by independently developed strategy crates.
#[async_trait]
pub trait StrategyRuntime: Send + Sync {
    fn spec(&self) -> StrategySpec;

    async fn start(&mut self, ctx: StrategyContext) -> anyhow::Result<()>;

    async fn stop(&mut self) -> anyhow::Result<()>;

    async fn handle_event(&mut self, event: StrategyEvent) -> anyhow::Result<()>;

    async fn snapshot(&self) -> anyhow::Result<StrategySnapshot>;
}

/// Execution intent client exposed to strategies.
///
/// Implementations may be backed by the execution router, an in-memory test
/// harness, or a remote process client. The trait deliberately avoids concrete
/// exchange adapter types.
#[async_trait]
pub trait StrategyExecutionClient: Send + Sync {
    async fn submit_order(&self, command: ExecutionOrderCommand) -> SdkResult<ExecutionOrderAck>;

    async fn cancel_order(&self, command: ExecutionCancelCommand) -> SdkResult<ExecutionCancelAck>;

    async fn submit_raw_intent(&self, intent: ExecutionIntent) -> SdkResult<ExecutionIntentAck>;
}

/// HTTP-backed implementation for runtimes hosted outside the execution router
/// process. The strategy still emits SDK commands; gateway/risk routing remains
/// owned by the remote platform endpoint.
#[derive(Clone)]
pub struct HttpStrategyExecutionClient {
    client: reqwest::Client,
    base_url: String,
}

impl HttpStrategyExecutionClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: trim_base_url(base_url.into()),
        }
    }

    async fn post_json<T, R>(&self, path: &str, payload: &T) -> SdkResult<R>
    where
        T: Serialize + ?Sized,
        R: for<'de> Deserialize<'de>,
    {
        let url = format!("{}/{}", self.base_url, path.trim_start_matches('/'));
        let response = self
            .client
            .post(url)
            .json(payload)
            .send()
            .await
            .map_err(|error| StrategySdkError::ExecutionUnavailable(error.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(StrategySdkError::ExecutionRejected(format!(
                "remote execution endpoint returned HTTP {status}: {body}"
            )));
        }
        response
            .json::<R>()
            .await
            .map_err(|error| StrategySdkError::ExecutionUnavailable(error.to_string()))
    }
}

#[async_trait]
impl StrategyExecutionClient for HttpStrategyExecutionClient {
    async fn submit_order(&self, command: ExecutionOrderCommand) -> SdkResult<ExecutionOrderAck> {
        self.post_json("strategy-execution/orders", &command).await
    }

    async fn cancel_order(&self, command: ExecutionCancelCommand) -> SdkResult<ExecutionCancelAck> {
        self.post_json("strategy-execution/cancels", &command).await
    }

    async fn submit_raw_intent(&self, intent: ExecutionIntent) -> SdkResult<ExecutionIntentAck> {
        self.post_json("strategy-execution/intents", &intent).await
    }
}

#[derive(Clone)]
pub struct HttpStrategyPlatformClient {
    client: reqwest::Client,
    base_url: String,
}

impl HttpStrategyPlatformClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: trim_base_url(base_url.into()),
        }
    }

    pub async fn market_snapshot(
        &self,
        request: RuntimeMarketSnapshotRequest,
    ) -> SdkResult<RuntimeMarketSnapshot> {
        let url = format!("{}/strategy-platform/market-snapshot", self.base_url);
        let response = self
            .client
            .post(url)
            .json(&request)
            .send()
            .await
            .map_err(|error| StrategySdkError::ExecutionUnavailable(error.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(StrategySdkError::ExecutionRejected(format!(
                "remote platform endpoint returned HTTP {status}: {body}"
            )));
        }
        response
            .json::<RuntimeMarketSnapshot>()
            .await
            .map_err(|error| StrategySdkError::ExecutionUnavailable(error.to_string()))
    }

    pub async fn recent_fills(
        &self,
        request: RuntimeRecentFillsRequest,
    ) -> SdkResult<RuntimeRecentFills> {
        let url = format!("{}/strategy-platform/recent-fills", self.base_url);
        let response = self
            .client
            .post(url)
            .json(&request)
            .send()
            .await
            .map_err(|error| StrategySdkError::ExecutionUnavailable(error.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(StrategySdkError::ExecutionRejected(format!(
                "remote platform endpoint returned HTTP {status}: {body}"
            )));
        }
        response
            .json::<RuntimeRecentFills>()
            .await
            .map_err(|error| StrategySdkError::ExecutionUnavailable(error.to_string()))
    }

    pub async fn account_config(
        &self,
        request: RuntimeAccountConfigRequest,
    ) -> SdkResult<RuntimeAccountConfig> {
        let url = format!("{}/strategy-platform/account-config", self.base_url);
        let response = self
            .client
            .post(url)
            .json(&request)
            .send()
            .await
            .map_err(|error| StrategySdkError::ExecutionUnavailable(error.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(StrategySdkError::ExecutionRejected(format!(
                "remote platform endpoint returned HTTP {status}: {body}"
            )));
        }
        response
            .json::<RuntimeAccountConfig>()
            .await
            .map_err(|error| StrategySdkError::ExecutionUnavailable(error.to_string()))
    }

    pub async fn positions(&self, request: RuntimePositionsRequest) -> SdkResult<RuntimePositions> {
        let url = format!("{}/strategy-platform/positions", self.base_url);
        let response = self
            .client
            .post(url)
            .json(&request)
            .send()
            .await
            .map_err(|error| StrategySdkError::ExecutionUnavailable(error.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(StrategySdkError::ExecutionRejected(format!(
                "remote platform endpoint returned HTTP {status}: {body}"
            )));
        }
        response
            .json::<RuntimePositions>()
            .await
            .map_err(|error| StrategySdkError::ExecutionUnavailable(error.to_string()))
    }
}

fn trim_base_url(mut value: String) -> String {
    while value.ends_with('/') {
        value.pop();
    }
    value
}

/// Runtime services and immutable identity provided to a strategy instance.
#[derive(Clone)]
pub struct StrategyContext {
    instance_id: StrategyInstanceId,
    tenant_id: String,
    account_id: String,
    strategy_id: String,
    run_id: String,
    started_at: DateTime<Utc>,
    config: Value,
    execution: Arc<dyn StrategyExecutionClient>,
    metadata: BTreeMap<String, Value>,
}

impl StrategyContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        instance_id: StrategyInstanceId,
        tenant_id: impl Into<String>,
        account_id: impl Into<String>,
        strategy_id: impl Into<String>,
        run_id: impl Into<String>,
        config: Value,
        execution: Arc<dyn StrategyExecutionClient>,
    ) -> Self {
        Self {
            instance_id,
            tenant_id: tenant_id.into(),
            account_id: account_id.into(),
            strategy_id: strategy_id.into(),
            run_id: run_id.into(),
            started_at: Utc::now(),
            config,
            execution,
            metadata: BTreeMap::new(),
        }
    }

    pub fn instance_id(&self) -> &StrategyInstanceId {
        &self.instance_id
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    pub fn strategy_id(&self) -> &str {
        &self.strategy_id
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }

    pub fn config(&self) -> &Value {
        &self.config
    }

    pub fn execution(&self) -> Arc<dyn StrategyExecutionClient> {
        Arc::clone(&self.execution)
    }

    pub fn metadata(&self) -> &BTreeMap<String, Value> {
        &self.metadata
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }
}

/// Static declaration consumed by supervisors and control APIs.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StrategySpec {
    pub schema_version: u32,
    pub strategy_kind: String,
    pub display_name: String,
    pub version: String,
    pub description: Option<String>,
    pub config_schema: StrategyConfigSchema,
    pub snapshot_schema: StrategySnapshotSchema,
    pub supported_commands: Vec<StrategyCommandSchema>,
    pub risk_capabilities: Vec<RiskCapabilityDeclaration>,
    pub market_data_subscriptions: Vec<MarketDataSubscription>,
    pub required_account_permissions: Vec<RequiredAccountPermission>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

/// JSON-schema-compatible strategy config declaration.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StrategyConfigSchema {
    pub schema_version: u32,
    pub json_schema: Value,
}

/// JSON-schema-compatible secret-free snapshot declaration.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StrategySnapshotSchema {
    pub schema_version: u32,
    pub json_schema: Value,
}

/// Strategy command schema exposed to control/supervisor surfaces.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StrategyCommandSchema {
    pub command_kind: String,
    pub description: Option<String>,
    pub payload_schema: Value,
}

/// Runtime snapshot emitted by a strategy. Must never contain secrets.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StrategySnapshot {
    pub schema_version: u32,
    pub instance_id: StrategyInstanceId,
    pub strategy_kind: String,
    pub strategy_id: String,
    pub run_id: String,
    pub captured_at: DateTime<Utc>,
    pub status: StrategyStatus,
    pub payload: Value,
    #[serde(default)]
    pub health: Vec<StrategyHealthIssue>,
}

/// Strategy lifecycle/market/execution/operator event envelope.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum StrategyEvent {
    Started(StrategyLifecycleEvent),
    Stopping(StrategyLifecycleEvent),
    Execution(ExecutionEvent),
    MarketData(MarketDataEvent),
    Account(AccountEvent),
    OperatorCommand(StrategyCommand),
    Timer(TimerEvent),
}

/// Operator or supervisor command routed to a strategy.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StrategyCommand {
    pub schema_version: u32,
    pub command_id: String,
    pub instance_id: StrategyInstanceId,
    pub command_kind: String,
    pub requested_at: DateTime<Utc>,
    pub payload: Value,
    pub requested_by: Option<String>,
}

/// Strategy risk behavior declaration.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RiskCapabilityDeclaration {
    pub capability: RiskCapability,
    pub description: Option<String>,
    pub limits: Value,
}

/// Risk capabilities a strategy may request from the execution plane.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RiskCapability {
    PlaceOrders,
    CancelOrders,
    ReduceOnlyOrders,
    Hedging,
    CrossAccountRead,
    InventoryReservation,
    Custom(String),
}

/// Market data subscription requested by a strategy instance.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketDataSubscription {
    pub exchange_id: String,
    pub symbol: String,
    pub market_type: MarketType,
    pub channels: Vec<MarketDataChannel>,
}

/// Required account permission declaration for a strategy.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequiredAccountPermission {
    pub account_id: Option<String>,
    pub permission: AccountPermission,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AccountPermission {
    ReadBalances,
    ReadPositions,
    ReadOrders,
    ReadFills,
    TradeSpot,
    TradeMargin,
    TradePerpetual,
    CancelOrders,
    Custom(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MarketType {
    Spot,
    Margin,
    Perpetual,
    Futures,
    Option,
    Custom(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataChannel {
    Trades,
    Ticker,
    OrderBookTop,
    OrderBookDepth,
    Candles { interval: String },
    FundingRate,
    MarkPrice,
    IndexPrice,
    Custom(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StrategyStatus {
    Starting,
    Running,
    Stopping,
    Stopped,
    Degraded,
    Failed,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StrategyHealthIssue {
    pub severity: HealthSeverity,
    pub message: String,
    pub observed_at: DateTime<Utc>,
    pub details: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HealthSeverity {
    Info,
    Warning,
    Critical,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StrategyLifecycleEvent {
    pub schema_version: u32,
    pub instance_id: StrategyInstanceId,
    pub strategy_id: String,
    pub run_id: String,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct TimerEvent {
    pub schema_version: u32,
    pub timer_id: String,
    pub fired_at: DateTime<Utc>,
    #[serde(default)]
    pub payload: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct MarketDataEvent {
    pub schema_version: u32,
    pub exchange_id: String,
    pub symbol: String,
    pub received_at: DateTime<Utc>,
    pub payload: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RuntimeMarketSnapshotRequest {
    pub schema_version: u32,
    pub tenant_id: String,
    pub account_id: String,
    pub run_id: String,
    pub exchange_id: String,
    pub symbol: String,
    pub market_type: MarketType,
    pub requested_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RuntimeMarketSnapshot {
    pub schema_version: u32,
    pub exchange_id: String,
    pub symbol: String,
    pub market_type: MarketType,
    pub best_bid: f64,
    pub best_ask: f64,
    pub last_price: f64,
    pub mark_price: f64,
    pub tick_size: f64,
    pub step_size: f64,
    pub min_qty: Option<f64>,
    pub min_notional: Option<f64>,
    pub received_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RuntimeRecentFillsRequest {
    pub schema_version: u32,
    pub tenant_id: String,
    pub account_id: String,
    pub run_id: String,
    pub exchange_id: String,
    pub symbol: String,
    pub market_type: MarketType,
    pub start_time: Option<DateTime<Utc>>,
    pub limit: Option<u32>,
    pub requested_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RuntimeFill {
    pub schema_version: u32,
    pub exchange_id: String,
    pub symbol: String,
    pub market_type: MarketType,
    pub fill_id: Option<String>,
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub side: OrderSide,
    pub position_side: String,
    pub price: f64,
    pub quantity: f64,
    pub filled_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RuntimeRecentFills {
    pub schema_version: u32,
    pub fills: Vec<RuntimeFill>,
    pub received_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RuntimeAccountConfigRequest {
    pub schema_version: u32,
    pub tenant_id: String,
    pub account_id: String,
    pub run_id: String,
    pub exchange_id: String,
    pub symbol: String,
    pub market_type: MarketType,
    pub requested_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RuntimeAccountConfig {
    pub schema_version: u32,
    pub exchange_id: String,
    pub symbol: String,
    pub market_type: MarketType,
    pub position_mode: Option<String>,
    pub margin_mode: Option<String>,
    pub leverage: Option<u32>,
    pub max_leverage: Option<u32>,
    pub received_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RuntimePositionsRequest {
    pub schema_version: u32,
    pub tenant_id: String,
    pub account_id: String,
    pub run_id: String,
    pub exchange_id: String,
    pub symbol: String,
    pub market_type: MarketType,
    pub requested_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RuntimePosition {
    pub schema_version: u32,
    pub exchange_id: String,
    pub symbol: String,
    pub market_type: MarketType,
    pub position_side: String,
    pub quantity: f64,
    pub entry_price: f64,
    pub mark_price: f64,
    pub observed_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RuntimePositions {
    pub schema_version: u32,
    pub positions: Vec<RuntimePosition>,
    pub received_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct AccountEvent {
    pub schema_version: u32,
    pub account_id: String,
    pub received_at: DateTime<Utc>,
    pub payload: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ExecutionEvent {
    pub schema_version: u32,
    pub event_id: String,
    pub client_order_id: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub payload: Value,
}

/// Adapter-free order intent submitted by strategy code.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ExecutionOrderCommand {
    pub schema_version: u32,
    pub tenant_id: String,
    pub account_id: String,
    pub strategy_id: String,
    pub run_id: String,
    pub client_order_id: String,
    pub idempotency_key: String,
    pub risk_profile_id: String,
    pub requested_at: DateTime<Utc>,
    pub exchange_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub quantity: String,
    pub price: Option<String>,
    pub time_in_force: Option<TimeInForce>,
    pub reduce_only: bool,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ExecutionOrderAck {
    pub schema_version: u32,
    pub accepted: bool,
    pub client_order_id: String,
    pub execution_order_id: Option<String>,
    pub reason: Option<String>,
    pub received_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ExecutionCancelCommand {
    pub schema_version: u32,
    pub tenant_id: String,
    pub account_id: String,
    pub strategy_id: String,
    pub run_id: String,
    pub client_order_id: Option<String>,
    pub execution_order_id: Option<String>,
    pub idempotency_key: String,
    pub risk_profile_id: String,
    pub requested_at: DateTime<Utc>,
    pub exchange_id: String,
    pub symbol: String,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ExecutionCancelAck {
    pub schema_version: u32,
    pub accepted: bool,
    pub client_order_id: Option<String>,
    pub execution_order_id: Option<String>,
    pub reason: Option<String>,
    pub received_at: DateTime<Utc>,
}

/// Generic intent escape hatch for future execution-api message versions.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ExecutionIntent {
    pub schema_version: u32,
    pub intent_kind: String,
    pub tenant_id: String,
    pub account_id: String,
    pub strategy_id: String,
    pub run_id: String,
    pub idempotency_key: String,
    pub requested_at: DateTime<Utc>,
    pub payload: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ExecutionIntentAck {
    pub schema_version: u32,
    pub accepted: bool,
    pub intent_kind: String,
    pub reason: Option<String>,
    pub received_at: DateTime<Utc>,
    pub payload: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Market,
    Limit,
    PostOnly,
    ImmediateOrCancel,
    Custom(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    GoodTilCanceled,
    ImmediateOrCancel,
    FillOrKill,
    PostOnly,
    Custom(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MockExecutionClient {
        orders: Mutex<Vec<ExecutionOrderCommand>>,
    }

    #[async_trait]
    impl StrategyExecutionClient for MockExecutionClient {
        async fn submit_order(
            &self,
            command: ExecutionOrderCommand,
        ) -> SdkResult<ExecutionOrderAck> {
            self.orders
                .lock()
                .expect("orders mutex poisoned")
                .push(command.clone());

            Ok(ExecutionOrderAck {
                schema_version: 1,
                accepted: true,
                client_order_id: command.client_order_id,
                execution_order_id: Some("exec-1".to_string()),
                reason: None,
                received_at: Utc::now(),
            })
        }

        async fn cancel_order(
            &self,
            command: ExecutionCancelCommand,
        ) -> SdkResult<ExecutionCancelAck> {
            Ok(ExecutionCancelAck {
                schema_version: 1,
                accepted: true,
                client_order_id: command.client_order_id,
                execution_order_id: command.execution_order_id,
                reason: None,
                received_at: Utc::now(),
            })
        }

        async fn submit_raw_intent(
            &self,
            intent: ExecutionIntent,
        ) -> SdkResult<ExecutionIntentAck> {
            Ok(ExecutionIntentAck {
                schema_version: 1,
                accepted: true,
                intent_kind: intent.intent_kind,
                reason: None,
                received_at: Utc::now(),
                payload: json!({ "accepted_by": "mock" }),
            })
        }
    }

    struct MockStrategy {
        ctx: Option<StrategyContext>,
        handled_events: usize,
        stopped: bool,
    }

    impl MockStrategy {
        fn new() -> Self {
            Self {
                ctx: None,
                handled_events: 0,
                stopped: false,
            }
        }
    }

    #[async_trait]
    impl StrategyRuntime for MockStrategy {
        fn spec(&self) -> StrategySpec {
            StrategySpec {
                schema_version: 1,
                strategy_kind: "mock_strategy".to_string(),
                display_name: "Mock Strategy".to_string(),
                version: "0.1.0".to_string(),
                description: Some("Mock strategy for SDK tests".to_string()),
                config_schema: StrategyConfigSchema {
                    schema_version: 1,
                    json_schema: json!({
                        "type": "object",
                        "required": ["symbol"],
                        "properties": {
                            "symbol": { "type": "string" }
                        }
                    }),
                },
                snapshot_schema: StrategySnapshotSchema {
                    schema_version: 1,
                    json_schema: json!({
                        "type": "object",
                        "properties": {
                            "handled_events": { "type": "integer" }
                        }
                    }),
                },
                supported_commands: vec![StrategyCommandSchema {
                    command_kind: "rebalance_now".to_string(),
                    description: Some("Trigger immediate rebalance evaluation".to_string()),
                    payload_schema: json!({ "type": "object" }),
                }],
                risk_capabilities: vec![RiskCapabilityDeclaration {
                    capability: RiskCapability::PlaceOrders,
                    description: Some("Places a single mock limit order".to_string()),
                    limits: json!({ "max_notional": "10" }),
                }],
                market_data_subscriptions: vec![MarketDataSubscription {
                    exchange_id: "paper".to_string(),
                    symbol: "BTC/USDT".to_string(),
                    market_type: MarketType::Spot,
                    channels: vec![MarketDataChannel::OrderBookTop],
                }],
                required_account_permissions: vec![RequiredAccountPermission {
                    account_id: None,
                    permission: AccountPermission::TradeSpot,
                    reason: Some("Mock order submission".to_string()),
                }],
                metadata: BTreeMap::new(),
            }
        }

        async fn start(&mut self, ctx: StrategyContext) -> anyhow::Result<()> {
            let command = ExecutionOrderCommand {
                schema_version: 1,
                tenant_id: ctx.tenant_id().to_string(),
                account_id: ctx.account_id().to_string(),
                strategy_id: ctx.strategy_id().to_string(),
                run_id: ctx.run_id().to_string(),
                client_order_id: "mock-order-1".to_string(),
                idempotency_key: "mock-idempotency-1".to_string(),
                risk_profile_id: "test-risk".to_string(),
                requested_at: Utc::now(),
                exchange_id: "paper".to_string(),
                symbol: "BTC/USDT".to_string(),
                side: OrderSide::Buy,
                order_type: OrderType::Limit,
                quantity: "0.001".to_string(),
                price: Some("50000".to_string()),
                time_in_force: Some(TimeInForce::GoodTilCanceled),
                reduce_only: false,
                metadata: BTreeMap::new(),
            };

            ctx.execution().submit_order(command).await?;
            self.ctx = Some(ctx);
            Ok(())
        }

        async fn stop(&mut self) -> anyhow::Result<()> {
            self.stopped = true;
            Ok(())
        }

        async fn handle_event(&mut self, event: StrategyEvent) -> anyhow::Result<()> {
            if matches!(event, StrategyEvent::MarketData(_)) {
                self.handled_events += 1;
            }
            Ok(())
        }

        async fn snapshot(&self) -> anyhow::Result<StrategySnapshot> {
            let ctx = self.ctx.as_ref().expect("strategy started");
            Ok(StrategySnapshot {
                schema_version: 1,
                instance_id: ctx.instance_id().clone(),
                strategy_kind: self.spec().strategy_kind,
                strategy_id: ctx.strategy_id().to_string(),
                run_id: ctx.run_id().to_string(),
                captured_at: Utc::now(),
                status: if self.stopped {
                    StrategyStatus::Stopped
                } else {
                    StrategyStatus::Running
                },
                payload: json!({ "handled_events": self.handled_events }),
                health: Vec::new(),
            })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn mock_strategy_should_use_context_execution_client_and_snapshot_state() {
        let execution = Arc::new(MockExecutionClient::default());
        let ctx = StrategyContext::new(
            StrategyInstanceId::new("instance-1"),
            "tenant-1",
            "account-1",
            "strategy-1",
            "run-1",
            json!({ "symbol": "BTC/USDT" }),
            execution.clone(),
        );
        let mut strategy = MockStrategy::new();

        strategy.start(ctx).await.expect("strategy should start");
        strategy
            .handle_event(StrategyEvent::MarketData(MarketDataEvent {
                schema_version: 1,
                exchange_id: "paper".to_string(),
                symbol: "BTC/USDT".to_string(),
                received_at: Utc::now(),
                payload: json!({ "bid": "49999", "ask": "50001" }),
            }))
            .await
            .expect("strategy should handle market data");

        let snapshot = strategy
            .snapshot()
            .await
            .expect("snapshot should serialize");
        let order_count = execution
            .orders
            .lock()
            .expect("orders mutex poisoned")
            .len();

        assert_eq!(order_count, 1);
        assert_eq!(snapshot.instance_id.as_str(), "instance-1");
        assert_eq!(snapshot.status, StrategyStatus::Running);
        assert_eq!(snapshot.payload.get("handled_events"), Some(&json!(1)));

        strategy.stop().await.expect("strategy should stop");
        let stopped_snapshot = strategy
            .snapshot()
            .await
            .expect("snapshot should serialize");
        assert_eq!(stopped_snapshot.status, StrategyStatus::Stopped);
    }

    #[test]
    fn strategy_spec_contract_should_round_trip_schemas_and_subscriptions() {
        #[derive(Debug, Deserialize)]
        struct MockConfig {
            symbol: String,
        }

        let strategy = MockStrategy::new();
        let spec = strategy.spec();
        let encoded = serde_json::to_value(&spec).expect("spec should serialize");
        let decoded: StrategySpec =
            serde_json::from_value(encoded).expect("spec should deserialize");
        let config: MockConfig =
            serde_json::from_value(json!({ "symbol": "BTC/USDT" })).expect("config should parse");

        assert_eq!(config.symbol, "BTC/USDT");
        assert_eq!(decoded.strategy_kind, "mock_strategy");
        assert_eq!(decoded.supported_commands[0].command_kind, "rebalance_now");
        assert_eq!(
            decoded.market_data_subscriptions[0].channels,
            vec![MarketDataChannel::OrderBookTop]
        );
        assert_eq!(
            decoded.required_account_permissions[0].permission,
            AccountPermission::TradeSpot
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn raw_execution_intent_should_round_trip_through_sdk_client() {
        let execution = MockExecutionClient::default();
        let ack = execution
            .submit_raw_intent(ExecutionIntent {
                schema_version: 1,
                intent_kind: "dry_run_order_plan".to_string(),
                tenant_id: "tenant-1".to_string(),
                account_id: "account-1".to_string(),
                strategy_id: "strategy-1".to_string(),
                run_id: "run-1".to_string(),
                idempotency_key: "strategy-1:run-1:intent-1".to_string(),
                requested_at: Utc::now(),
                payload: json!({
                    "dry_run": true,
                    "orders": []
                }),
            })
            .await
            .expect("intent should be accepted");

        assert!(ack.accepted);
        assert_eq!(ack.intent_kind, "dry_run_order_plan");
        assert_eq!(ack.payload["accepted_by"], json!("mock"));
    }
}
