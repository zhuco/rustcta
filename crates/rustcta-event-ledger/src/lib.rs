//! Append-only event ledger contracts for RustCTA.
//!
//! The ledger stores ordered, schema-versioned events that can rebuild order,
//! fill, account, and audit read models. Implementations must reject payloads
//! containing secret-like field names before persisting records.

use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, LiquidityRole, MarketType, OrderSide, OrderStatus,
    PositionSide, RunId, SchemaVersion, StrategyId, TenantId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

pub const EVENT_LEDGER_SCHEMA_VERSION: SchemaVersion = SchemaVersion::current();

pub type Result<T> = std::result::Result<T, LedgerError>;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LedgerError {
    #[error("event contains forbidden secret-like field {field_path}")]
    SecretField { field_path: String },
    #[error("ledger sequence overflow")]
    SequenceOverflow,
    #[error("ledger decode error at line {line}: {message}")]
    Decode { line: usize, message: String },
    #[error("ledger storage error: {0}")]
    Storage(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    StrategyLifecycleEvent,
    StrategySnapshotEvent,
    MarketDataHealthEvent,
    OrderCommandEvent,
    OrderAckEvent,
    CancelCommandEvent,
    CancelAckEvent,
    FillEvent,
    BalanceSnapshotEvent,
    PositionSnapshotEvent,
    RiskDecisionEvent,
    ReconciliationEvent,
    OperatorCommandEvent,
    AuditEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventIdentity {
    pub schema_version: SchemaVersion,
    pub tenant_id: TenantId,
    pub account_id: Option<AccountId>,
    pub strategy_id: Option<StrategyId>,
    pub run_id: Option<RunId>,
    pub correlation_id: Option<String>,
    pub command_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub source: String,
    pub occurred_at: DateTime<Utc>,
}

impl EventIdentity {
    pub fn new(tenant_id: TenantId, source: impl Into<String>, occurred_at: DateTime<Utc>) -> Self {
        Self {
            schema_version: EVENT_LEDGER_SCHEMA_VERSION,
            tenant_id,
            account_id: None,
            strategy_id: None,
            run_id: None,
            correlation_id: None,
            command_id: None,
            idempotency_key: None,
            source: source.into(),
            occurred_at,
        }
    }

    pub fn with_account(mut self, account_id: AccountId) -> Self {
        self.account_id = Some(account_id);
        self
    }

    pub fn with_strategy_run(mut self, strategy_id: StrategyId, run_id: RunId) -> Self {
        self.strategy_id = Some(strategy_id);
        self.run_id = Some(run_id);
        self
    }

    pub fn with_command(mut self, command_id: impl Into<String>) -> Self {
        self.command_id = Some(command_id.into());
        self
    }

    pub fn with_idempotency_key(mut self, idempotency_key: impl Into<String>) -> Self {
        self.idempotency_key = Some(idempotency_key.into());
        self
    }

    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LedgerEvent {
    pub schema_version: SchemaVersion,
    pub sequence: u64,
    pub kind: EventKind,
    pub identity: EventIdentity,
    pub payload: LedgerPayload,
    pub recorded_at: DateTime<Utc>,
}

impl LedgerEvent {
    pub fn new(kind: EventKind, identity: EventIdentity, payload: LedgerPayload) -> Self {
        Self {
            schema_version: EVENT_LEDGER_SCHEMA_VERSION,
            sequence: 0,
            kind,
            identity,
            payload,
            recorded_at: Utc::now(),
        }
    }

    pub fn order(record: OrderLifecycleRecord) -> Self {
        Self::new(
            record.event_kind,
            record.identity.clone(),
            LedgerPayload::OrderLifecycle(record),
        )
    }

    pub fn fill(record: FillLedgerRecord) -> Self {
        Self::new(
            EventKind::FillEvent,
            record.identity.clone(),
            LedgerPayload::Fill(record),
        )
    }

    pub fn balance_snapshot(record: BalanceSnapshotRecord) -> Self {
        Self::new(
            EventKind::BalanceSnapshotEvent,
            record.identity.clone(),
            LedgerPayload::BalanceSnapshot(record),
        )
    }

    pub fn audit(record: AuditRecord) -> Self {
        Self::new(
            EventKind::AuditEvent,
            record.identity.clone(),
            LedgerPayload::Audit(record),
        )
    }

    pub fn operator_command(record: AuditRecord) -> Self {
        Self::new(
            EventKind::OperatorCommandEvent,
            record.identity.clone(),
            LedgerPayload::Audit(record),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "payload_type", content = "payload", rename_all = "snake_case")]
pub enum LedgerPayload {
    OrderLifecycle(OrderLifecycleRecord),
    Fill(FillLedgerRecord),
    BalanceSnapshot(BalanceSnapshotRecord),
    Audit(AuditRecord),
    Raw(Value),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderLifecycleRecord {
    pub schema_version: SchemaVersion,
    pub identity: EventIdentity,
    pub event_kind: EventKind,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: CanonicalSymbol,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub status: OrderStatus,
    pub requested_quantity: f64,
    pub requested_price: Option<f64>,
    pub filled_quantity: f64,
    pub average_fill_price: Option<f64>,
    pub message: Option<String>,
    #[serde(default)]
    pub metadata: Value,
}

impl OrderLifecycleRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identity: EventIdentity,
        event_kind: EventKind,
        exchange_id: ExchangeId,
        market_type: MarketType,
        canonical_symbol: CanonicalSymbol,
        client_order_id: impl Into<String>,
        side: OrderSide,
        position_side: PositionSide,
        status: OrderStatus,
        requested_quantity: f64,
    ) -> Self {
        Self {
            schema_version: EVENT_LEDGER_SCHEMA_VERSION,
            identity,
            event_kind,
            exchange_id,
            market_type,
            canonical_symbol,
            client_order_id: client_order_id.into(),
            exchange_order_id: None,
            side,
            position_side,
            status,
            requested_quantity,
            requested_price: None,
            filled_quantity: 0.0,
            average_fill_price: None,
            message: None,
            metadata: Value::Null,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FillLedgerRecord {
    pub schema_version: SchemaVersion,
    pub identity: EventIdentity,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: CanonicalSymbol,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub fill_id: Option<String>,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub liquidity_role: LiquidityRole,
    pub price: f64,
    pub quantity: f64,
    pub quote_quantity: Option<f64>,
    pub fee_asset: Option<String>,
    pub fee_amount: Option<f64>,
    pub realized_pnl: Option<f64>,
    pub filled_at: DateTime<Utc>,
    #[serde(default)]
    pub metadata: Value,
}

impl FillLedgerRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identity: EventIdentity,
        exchange_id: ExchangeId,
        market_type: MarketType,
        canonical_symbol: CanonicalSymbol,
        side: OrderSide,
        position_side: PositionSide,
        liquidity_role: LiquidityRole,
        price: f64,
        quantity: f64,
        filled_at: DateTime<Utc>,
    ) -> Self {
        Self {
            schema_version: EVENT_LEDGER_SCHEMA_VERSION,
            identity,
            exchange_id,
            market_type,
            canonical_symbol,
            client_order_id: None,
            exchange_order_id: None,
            fill_id: None,
            side,
            position_side,
            liquidity_role,
            price,
            quantity,
            quote_quantity: None,
            fee_asset: None,
            fee_amount: None,
            realized_pnl: None,
            filled_at,
            metadata: Value::Null,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BalanceSnapshotRecord {
    pub schema_version: SchemaVersion,
    pub identity: EventIdentity,
    pub exchange_id: ExchangeId,
    pub market_type: MarketType,
    pub balances: Vec<AssetBalanceRecord>,
    pub observed_at: DateTime<Utc>,
    #[serde(default)]
    pub metadata: Value,
}

impl BalanceSnapshotRecord {
    pub fn new(
        identity: EventIdentity,
        exchange_id: ExchangeId,
        market_type: MarketType,
        balances: Vec<AssetBalanceRecord>,
        observed_at: DateTime<Utc>,
    ) -> Self {
        Self {
            schema_version: EVENT_LEDGER_SCHEMA_VERSION,
            identity,
            exchange_id,
            market_type,
            balances,
            observed_at,
            metadata: Value::Null,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AssetBalanceRecord {
    pub schema_version: SchemaVersion,
    pub asset: String,
    pub total: f64,
    pub available: f64,
    pub locked: f64,
    pub reserved: f64,
}

impl AssetBalanceRecord {
    pub fn new(
        asset: impl Into<String>,
        total: f64,
        available: f64,
        locked: f64,
        reserved: f64,
    ) -> Self {
        Self {
            schema_version: EVENT_LEDGER_SCHEMA_VERSION,
            asset: asset.into(),
            total,
            available,
            locked,
            reserved,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditRecord {
    pub schema_version: SchemaVersion,
    pub identity: EventIdentity,
    pub actor: AuditActor,
    pub action: String,
    pub outcome: AuditOutcome,
    pub message: Option<String>,
    #[serde(default)]
    pub metadata: Value,
}

impl AuditRecord {
    pub fn new(
        identity: EventIdentity,
        actor: AuditActor,
        action: impl Into<String>,
        outcome: AuditOutcome,
    ) -> Self {
        Self {
            schema_version: EVENT_LEDGER_SCHEMA_VERSION,
            identity,
            actor,
            action: action.into(),
            outcome,
            message: None,
            metadata: Value::Null,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditActor {
    pub schema_version: SchemaVersion,
    pub actor_type: AuditActorType,
    pub actor_id: String,
}

impl AuditActor {
    pub fn new(actor_type: AuditActorType, actor_id: impl Into<String>) -> Self {
        Self {
            schema_version: EVENT_LEDGER_SCHEMA_VERSION,
            actor_type,
            actor_id: actor_id.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditActorType {
    Operator,
    Strategy,
    Supervisor,
    Gateway,
    ExecutionRouter,
    System,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    Accepted,
    Rejected,
    Succeeded,
    Failed,
}

#[async_trait]
pub trait LedgerWriter: Send + Sync {
    async fn append(&self, event: LedgerEvent) -> Result<LedgerEvent>;
}

#[async_trait]
pub trait LedgerReader: Send + Sync {
    async fn replay(&self, from_sequence: Option<u64>) -> Result<Vec<LedgerEvent>>;

    async fn replay_kind(
        &self,
        kind: EventKind,
        from_sequence: Option<u64>,
    ) -> Result<Vec<LedgerEvent>> {
        let events = self.replay(from_sequence).await?;
        Ok(events
            .into_iter()
            .filter(|event| event.kind == kind)
            .collect())
    }
}

pub trait LedgerStore: LedgerWriter + LedgerReader {}

impl<T> LedgerStore for T where T: LedgerWriter + LedgerReader {}

#[derive(Debug, Clone, Default)]
pub struct InMemoryLedger {
    events: Arc<Mutex<Vec<LedgerEvent>>>,
}

impl InMemoryLedger {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> Result<usize> {
        Ok(self.lock_events()?.len())
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.lock_events()?.is_empty())
    }

    fn lock_events(&self) -> Result<std::sync::MutexGuard<'_, Vec<LedgerEvent>>> {
        self.events
            .lock()
            .map_err(|_| LedgerError::Storage("in-memory ledger lock poisoned".to_string()))
    }
}

#[async_trait]
impl LedgerWriter for InMemoryLedger {
    async fn append(&self, mut event: LedgerEvent) -> Result<LedgerEvent> {
        reject_secret_fields(&event)?;

        let mut events = self.lock_events()?;
        let next_sequence = events
            .last()
            .map(|last| {
                last.sequence
                    .checked_add(1)
                    .ok_or(LedgerError::SequenceOverflow)
            })
            .transpose()?
            .unwrap_or(1);

        event.schema_version = EVENT_LEDGER_SCHEMA_VERSION;
        event.sequence = next_sequence;
        event.recorded_at = Utc::now();
        events.push(event.clone());
        Ok(event)
    }
}

#[async_trait]
impl LedgerReader for InMemoryLedger {
    async fn replay(&self, from_sequence: Option<u64>) -> Result<Vec<LedgerEvent>> {
        let from_sequence = from_sequence.unwrap_or(1);
        let mut events: Vec<_> = self
            .lock_events()?
            .iter()
            .filter(|event| event.sequence >= from_sequence)
            .cloned()
            .collect();
        events.sort_by_key(|event| event.sequence);
        Ok(events)
    }
}

#[derive(Debug, Clone)]
pub struct JsonlLedger {
    path: Arc<PathBuf>,
    lock: Arc<Mutex<()>>,
}

impl JsonlLedger {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: Arc::new(path.into()),
            lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    fn lock_file(&self) -> Result<std::sync::MutexGuard<'_, ()>> {
        self.lock
            .lock()
            .map_err(|_| LedgerError::Storage("jsonl ledger lock poisoned".to_string()))
    }

    fn ensure_parent_dir(&self) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                LedgerError::Storage(format!(
                    "failed to create ledger directory {}: {error}",
                    parent.display()
                ))
            })?;
        }
        Ok(())
    }

    fn read_events_locked(&self) -> Result<Vec<LedgerEvent>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let file = std::fs::File::open(self.path()).map_err(|error| {
            LedgerError::Storage(format!(
                "failed to open ledger {}: {error}",
                self.path.display()
            ))
        })?;
        let reader = std::io::BufReader::new(file);
        let mut events = Vec::new();
        for (index, line) in reader.lines().enumerate() {
            let line_number = index + 1;
            let line = line.map_err(|error| LedgerError::Decode {
                line: line_number,
                message: error.to_string(),
            })?;
            if line.trim().is_empty() {
                continue;
            }
            let event = serde_json::from_str::<LedgerEvent>(&line).map_err(|error| {
                LedgerError::Decode {
                    line: line_number,
                    message: error.to_string(),
                }
            })?;
            reject_secret_fields(&event)?;
            events.push(event);
        }
        events.sort_by_key(|event| event.sequence);
        Ok(events)
    }

    fn next_sequence(events: &[LedgerEvent]) -> Result<u64> {
        events
            .iter()
            .map(|event| event.sequence)
            .max()
            .map(|sequence| sequence.checked_add(1).ok_or(LedgerError::SequenceOverflow))
            .transpose()
            .map(|sequence| sequence.unwrap_or(1))
    }
}

#[async_trait]
impl LedgerWriter for JsonlLedger {
    async fn append(&self, mut event: LedgerEvent) -> Result<LedgerEvent> {
        reject_secret_fields(&event)?;

        let _guard = self.lock_file()?;
        self.ensure_parent_dir()?;
        let events = self.read_events_locked()?;
        event.schema_version = EVENT_LEDGER_SCHEMA_VERSION;
        event.sequence = Self::next_sequence(&events)?;
        event.recorded_at = Utc::now();

        let serialized = serde_json::to_string(&event)
            .map_err(|error| LedgerError::Storage(error.to_string()))?;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.path())
            .map_err(|error| {
                LedgerError::Storage(format!(
                    "failed to append ledger {}: {error}",
                    self.path.display()
                ))
            })?;
        writeln!(file, "{serialized}")
            .map_err(|error| LedgerError::Storage(format!("failed to write ledger: {error}")))?;
        file.sync_data()
            .map_err(|error| LedgerError::Storage(format!("failed to sync ledger: {error}")))?;
        Ok(event)
    }
}

#[async_trait]
impl LedgerReader for JsonlLedger {
    async fn replay(&self, from_sequence: Option<u64>) -> Result<Vec<LedgerEvent>> {
        let _guard = self.lock_file()?;
        let from_sequence = from_sequence.unwrap_or(1);
        Ok(self
            .read_events_locked()?
            .into_iter()
            .filter(|event| event.sequence >= from_sequence)
            .collect())
    }
}

pub fn reject_secret_fields<T>(record: &T) -> Result<()>
where
    T: Serialize + ?Sized,
{
    let value =
        serde_json::to_value(record).map_err(|error| LedgerError::Storage(error.to_string()))?;
    find_secret_field(&value, "$").map_or(Ok(()), |field_path| {
        Err(LedgerError::SecretField { field_path })
    })
}

pub fn secret_key_name_is_forbidden(key: &str) -> bool {
    let normalized = key.trim().to_ascii_lowercase().replace(['-', ' '], "_");
    let forbidden = matches!(
        normalized.as_str(),
        "api_key"
            | "apikey"
            | "api_secret"
            | "apisecret"
            | "secret"
            | "secret_key"
            | "secretkey"
            | "private_key"
            | "privatekey"
            | "passphrase"
            | "password"
            | "authorization"
            | "auth_token"
            | "authtoken"
            | "access_token"
            | "accesstoken"
            | "refresh_token"
            | "refreshtoken"
            | "signature"
    );

    forbidden
        || [
            "_api_key",
            "_api_secret",
            "_secret_key",
            "_private_key",
            "_passphrase",
            "_password",
            "_authorization",
            "_auth_token",
            "_access_token",
            "_refresh_token",
            "_signature",
        ]
        .iter()
        .any(|suffix| normalized.ends_with(suffix))
}

fn find_secret_field(value: &Value, path: &str) -> Option<String> {
    match value {
        Value::Object(map) => {
            for (key, nested) in map {
                let field_path = format!("{path}.{key}");
                if secret_key_name_is_forbidden(key) {
                    return Some(field_path);
                }
                if let Some(found) = find_secret_field(nested, &field_path) {
                    return Some(found);
                }
            }
            None
        }
        Value::Array(values) => values
            .iter()
            .enumerate()
            .find_map(|(index, nested)| find_secret_field(nested, &format!("{path}[{index}]"))),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> EventIdentity {
        EventIdentity::new(
            TenantId::new("tenant-a").expect("tenant id"),
            "unit-test",
            Utc::now(),
        )
        .with_account(AccountId::new("account-a").expect("account id"))
        .with_strategy_run(
            StrategyId::new("strategy-a").expect("strategy id"),
            RunId::new("run-a").expect("run id"),
        )
    }

    fn exchange_id() -> ExchangeId {
        ExchangeId::new("binance").expect("exchange id")
    }

    fn symbol() -> CanonicalSymbol {
        CanonicalSymbol::parse("BTC/USDT").expect("symbol")
    }

    fn order_event(client_order_id: &str) -> LedgerEvent {
        LedgerEvent::order(OrderLifecycleRecord::new(
            identity(),
            EventKind::OrderCommandEvent,
            exchange_id(),
            MarketType::Spot,
            symbol(),
            client_order_id,
            OrderSide::Buy,
            PositionSide::None,
            OrderStatus::New,
            1.0,
        ))
    }

    #[tokio::test]
    async fn append_and_replay_should_preserve_sequence_order() {
        let ledger = InMemoryLedger::new();

        let first = ledger
            .append(order_event("client-1"))
            .await
            .expect("append first event");
        let second = ledger
            .append(LedgerEvent::fill(FillLedgerRecord::new(
                identity(),
                exchange_id(),
                MarketType::Spot,
                symbol(),
                OrderSide::Buy,
                PositionSide::None,
                LiquidityRole::Taker,
                100.0,
                0.5,
                Utc::now(),
            )))
            .await
            .expect("append second event");
        let third = ledger
            .append(LedgerEvent::audit(AuditRecord::new(
                identity(),
                AuditActor::new(AuditActorType::Operator, "operator-a"),
                "cancel_requested",
                AuditOutcome::Accepted,
            )))
            .await
            .expect("append third event");

        assert_eq!(first.sequence, 1);
        assert_eq!(second.sequence, 2);
        assert_eq!(third.sequence, 3);

        let events = ledger.replay(None).await.expect("replay events");
        assert_eq!(
            events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );

        let from_second = ledger.replay(Some(2)).await.expect("replay from second");
        assert_eq!(
            from_second
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );

        let fills = ledger
            .replay_kind(EventKind::FillEvent, None)
            .await
            .expect("replay fills");
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].sequence, 2);
    }

    #[tokio::test]
    async fn append_should_reject_secret_like_fields() {
        let ledger = InMemoryLedger::new();
        let mut event = order_event("client-1");
        if let LedgerPayload::OrderLifecycle(record) = &mut event.payload {
            record.metadata = serde_json::json!({
                "exchange": "binance",
                "nested": {
                    "api_secret": "do-not-persist"
                }
            });
        }

        let error = ledger.append(event).await.expect_err("secret must reject");

        assert!(matches!(error, LedgerError::SecretField { .. }));
        assert_eq!(ledger.len().expect("ledger len"), 0);
        assert!(ledger.is_empty().expect("ledger is empty"));
    }

    #[tokio::test]
    async fn jsonl_ledger_should_persist_and_replay_events() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-jsonl-ledger-test-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = temp_dir.join("events.jsonl");
        let ledger = JsonlLedger::new(&path);

        let first = ledger
            .append(order_event("client-1"))
            .await
            .expect("append first event");
        let second = ledger
            .append(LedgerEvent::audit(AuditRecord::new(
                identity(),
                AuditActor::new(AuditActorType::Supervisor, "supervisor"),
                "started",
                AuditOutcome::Succeeded,
            )))
            .await
            .expect("append second event");

        assert_eq!(first.sequence, 1);
        assert_eq!(second.sequence, 2);

        let reopened = JsonlLedger::new(&path);
        let events = reopened.replay(None).await.expect("replay all");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence, 1);
        assert_eq!(events[1].sequence, 2);

        let from_second = reopened.replay(Some(2)).await.expect("replay second");
        assert_eq!(from_second.len(), 1);
        assert_eq!(from_second[0].sequence, 2);

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[tokio::test]
    async fn jsonl_ledger_should_reject_secret_like_fields_before_write() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-jsonl-ledger-secret-test-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let path = temp_dir.join("events.jsonl");
        let ledger = JsonlLedger::new(&path);
        let mut event = order_event("client-1");
        if let LedgerPayload::OrderLifecycle(record) = &mut event.payload {
            record.metadata = serde_json::json!({
                "api_key": "do-not-write"
            });
        }

        let error = ledger.append(event).await.expect_err("secret must reject");

        assert!(matches!(error, LedgerError::SecretField { .. }));
        assert!(!path.exists());
        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[test]
    fn persisted_structs_should_include_schema_version() {
        let event = order_event("client-1");
        let value = serde_json::to_value(event).expect("serialize event");

        assert_eq!(value["schema_version"]["major"], 1);
        assert_eq!(value["identity"]["schema_version"]["major"], 1);
        assert_eq!(
            value["payload"]["payload"]["schema_version"]["major"],
            serde_json::json!(1)
        );
    }

    #[test]
    fn secret_detector_should_block_common_secret_key_names() {
        assert!(secret_key_name_is_forbidden("api_secret"));
        assert!(secret_key_name_is_forbidden("Authorization"));
        assert!(secret_key_name_is_forbidden("private_key"));
        assert!(secret_key_name_is_forbidden("exchange_api_secret"));
        assert!(!secret_key_name_is_forbidden("configured"));
    }
}
