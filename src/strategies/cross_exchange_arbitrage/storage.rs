use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc, Arc,
};
use std::thread;

use crate::execution::{FillEvent, OrderState, PrivateEvent};
use crate::market::{CanonicalSymbol, ExchangeId};

use super::{
    ArbSignal, FundingSettlement, HedgeRecordReadModel, HedgeRepairTaskReadModel, MarketSnapshot,
    NormalizedDepthSnapshot, Opportunity, OpportunityRecord, SimulatedBundleState,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CrossArbStorageEvent {
    MarketSnapshot(ArbitrageMarketSnapshotRecord),
    DetectedOpportunity(OpportunityRecord),
    OrderTransition(ArbitrageOrderRecord),
    Fill(ArbitrageFillRecord),
    Position(ArbitragePositionRecord),
    Pnl(ArbitragePnlRecord),
    RiskEvent(ArbitrageRiskEventRecord),
    ExchangeHealth(ExchangeHealthEventRecord),
    Opportunity(Opportunity),
    Signal(ArbSignal),
    Bundle(SimulatedBundleState),
    HedgeRecord(HedgeRecordReadModel),
    HedgeRepairTask(HedgeRepairTaskReadModel),
    FundingSettlement(FundingSettlement),
    PrivateEvent(PrivateEvent),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArbitrageMarketSnapshotRecord {
    pub timestamp: DateTime<Utc>,
    pub exchange: ExchangeId,
    pub symbol: CanonicalSymbol,
    pub exchange_symbol: String,
    pub best_bid: Option<f64>,
    pub best_bid_quantity: Option<f64>,
    pub best_ask: Option<f64>,
    pub best_ask_quantity: Option<f64>,
    pub sequence: Option<u64>,
}

impl From<&NormalizedDepthSnapshot> for ArbitrageMarketSnapshotRecord {
    fn from(snapshot: &NormalizedDepthSnapshot) -> Self {
        let best_bid = snapshot.best_bid();
        let best_ask = snapshot.best_ask();
        Self {
            timestamp: snapshot.received_at,
            exchange: snapshot.exchange.clone(),
            symbol: snapshot.symbol.clone(),
            exchange_symbol: snapshot.exchange_symbol.clone(),
            best_bid: best_bid.map(|level| level.price),
            best_bid_quantity: best_bid.map(|level| level.quantity),
            best_ask: best_ask.map(|level| level.price),
            best_ask_quantity: best_ask.map(|level| level.quantity),
            sequence: snapshot.sequence,
        }
    }
}

impl From<&MarketSnapshot> for ArbitrageMarketSnapshotRecord {
    fn from(snapshot: &MarketSnapshot) -> Self {
        let best_bid = snapshot.book.best_bid();
        let best_ask = snapshot.book.best_ask();
        Self {
            timestamp: snapshot.book.recv_ts,
            exchange: snapshot.book.exchange.clone(),
            symbol: snapshot.book.canonical_symbol.clone(),
            exchange_symbol: snapshot.book.exchange_symbol.symbol.clone(),
            best_bid: best_bid.map(|level| level.price),
            best_bid_quantity: best_bid.map(|level| level.quantity),
            best_ask: best_ask.map(|level| level.price),
            best_ask_quantity: best_ask.map(|level| level.quantity),
            sequence: snapshot.book.sequence,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArbitrageOrderRecord {
    pub timestamp: DateTime<Utc>,
    pub exchange: ExchangeId,
    pub symbol: CanonicalSymbol,
    pub side: String,
    pub order_type: String,
    pub status: String,
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub price: Option<f64>,
    pub quantity: f64,
    pub filled_quantity: f64,
    pub average_fill_price: Option<f64>,
    pub latency_ms: Option<i64>,
}

impl From<&OrderState> for ArbitrageOrderRecord {
    fn from(order: &OrderState) -> Self {
        Self {
            timestamp: order.updated_at,
            exchange: order.exchange.clone(),
            symbol: order.canonical_symbol.clone(),
            side: format!("{:?}", order.side),
            order_type: format!("{:?}", order.order_type),
            status: format!("{:?}", order.status),
            order_id: order.exchange_order_id.clone(),
            client_order_id: order.client_order_id.clone(),
            price: order.price,
            quantity: order.quantity,
            filled_quantity: order.filled_quantity,
            average_fill_price: order.average_fill_price,
            latency_ms: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArbitrageFillRecord {
    pub timestamp: DateTime<Utc>,
    pub exchange: ExchangeId,
    pub symbol: CanonicalSymbol,
    pub side: String,
    pub price: f64,
    pub quantity: f64,
    pub fee: f64,
    pub fee_asset: Option<String>,
    pub liquidity_role: String,
    pub order_id: Option<String>,
    pub client_order_id: Option<String>,
    pub latency_ms: Option<i64>,
}

impl ArbitrageFillRecord {
    pub fn signed_quote_pnl(&self) -> f64 {
        let quote = self.price * self.quantity;
        let gross = if self.side.eq_ignore_ascii_case("sell") {
            quote
        } else {
            -quote
        };
        gross - self.fee
    }
}

impl From<&FillEvent> for ArbitrageFillRecord {
    fn from(fill: &FillEvent) -> Self {
        Self {
            timestamp: fill.received_at,
            exchange: fill.exchange.clone(),
            symbol: fill.canonical_symbol.clone(),
            side: format!("{:?}", fill.side),
            price: fill.price,
            quantity: fill.quantity,
            fee: fill.fee.unwrap_or(0.0),
            fee_asset: fill.fee_asset.clone(),
            liquidity_role: format!("{:?}", fill.liquidity),
            order_id: fill.exchange_order_id.clone(),
            client_order_id: fill.client_order_id.clone(),
            latency_ms: Some(
                fill.received_at
                    .signed_duration_since(fill.filled_at)
                    .num_milliseconds()
                    .max(0),
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArbitragePositionRecord {
    pub timestamp: DateTime<Utc>,
    pub exchange: ExchangeId,
    pub symbol: CanonicalSymbol,
    pub base_quantity: f64,
    pub notional_usdt: f64,
    pub residual_quantity: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArbitragePnlRecord {
    pub timestamp: DateTime<Utc>,
    pub symbol: Option<CanonicalSymbol>,
    pub realized_pnl_usdt: f64,
    pub unrealized_pnl_usdt: f64,
    pub fee_usdt: f64,
    pub equity_usdt: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArbitrageRiskEventRecord {
    pub timestamp: DateTime<Utc>,
    pub scope: String,
    pub exchange: Option<ExchangeId>,
    pub symbol: Option<CanonicalSymbol>,
    pub risk_type: String,
    pub action: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeHealthEventRecord {
    pub timestamp: DateTime<Utc>,
    pub exchange: ExchangeId,
    pub ws_stale: bool,
    pub rest_failure_rate: f64,
    pub order_reject_rate: f64,
    pub cancel_failure_rate: f64,
    pub private_stream_delay_ms: i64,
    pub balance_reconciliation_mismatch_usdt: f64,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredCrossArbEvent {
    pub event_id: u64,
    pub recorded_at: DateTime<Utc>,
    pub event: CrossArbStorageEvent,
}

pub trait StorageSink {
    fn record(&mut self, event: CrossArbStorageEvent, recorded_at: DateTime<Utc>);
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct InMemoryStorageSink {
    events: Vec<StoredCrossArbEvent>,
    next_event_id: u64,
}

impl InMemoryStorageSink {
    pub fn events(&self) -> &[StoredCrossArbEvent] {
        &self.events
    }
}

impl StorageSink for InMemoryStorageSink {
    fn record(&mut self, event: CrossArbStorageEvent, recorded_at: DateTime<Utc>) {
        self.next_event_id += 1;
        self.events.push(StoredCrossArbEvent {
            event_id: self.next_event_id,
            recorded_at,
            event,
        });
        if self.events.len() > 10_000 {
            let overflow = self.events.len() - 10_000;
            self.events.drain(0..overflow);
        }
    }
}

#[derive(Debug, Clone)]
pub struct JsonlStorageSink {
    path: PathBuf,
    next_event_id: u64,
}

impl JsonlStorageSink {
    pub fn new(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let next_event_id = existing_jsonl_lines(&path)?.saturating_add(1);
        Ok(Self {
            path,
            next_event_id,
        })
    }

    pub fn for_day(dir: impl AsRef<Path>, day: DateTime<Utc>) -> std::io::Result<Self> {
        Self::new(
            dir.as_ref()
                .join(format!("cross_arb_events_{}.jsonl", day.format("%Y%m%d"))),
        )
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl StorageSink for JsonlStorageSink {
    fn record(&mut self, event: CrossArbStorageEvent, recorded_at: DateTime<Utc>) {
        let stored = StoredCrossArbEvent {
            event_id: self.next_event_id,
            recorded_at,
            event,
        };
        self.next_event_id += 1;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .expect("open cross-arb jsonl storage file");
        serde_json::to_writer(&mut file, &stored).expect("serialize cross-arb jsonl event");
        file.write_all(b"\n")
            .expect("write cross-arb jsonl newline");
    }
}

#[derive(Debug)]
pub struct AsyncJsonlStorageSink {
    sender: mpsc::Sender<StoredCrossArbEvent>,
    next_event_id: u64,
    healthy: Arc<AtomicBool>,
}

impl AsyncJsonlStorageSink {
    pub fn new(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let next_event_id = existing_jsonl_lines(&path)?.saturating_add(1);
        let (sender, receiver) = mpsc::channel::<StoredCrossArbEvent>();
        let healthy = Arc::new(AtomicBool::new(true));
        let writer_healthy = Arc::clone(&healthy);
        thread::Builder::new()
            .name("cross-arb-jsonl-recorder".to_string())
            .spawn(move || {
                while let Ok(event) = receiver.recv() {
                    if append_stored_event(&path, &event).is_err() {
                        writer_healthy.store(false, Ordering::SeqCst);
                    }
                }
            })
            .map_err(std::io::Error::other)?;
        Ok(Self {
            sender,
            next_event_id,
            healthy,
        })
    }

    pub fn for_day(dir: impl AsRef<Path>, day: DateTime<Utc>) -> std::io::Result<Self> {
        Self::new(
            dir.as_ref()
                .join(format!("cross_arb_events_{}.jsonl", day.format("%Y%m%d"))),
        )
    }

    pub fn healthy(&self) -> bool {
        self.healthy.load(Ordering::SeqCst)
    }
}

impl StorageSink for AsyncJsonlStorageSink {
    fn record(&mut self, event: CrossArbStorageEvent, recorded_at: DateTime<Utc>) {
        let stored = StoredCrossArbEvent {
            event_id: self.next_event_id,
            recorded_at,
            event,
        };
        self.next_event_id += 1;
        if self.sender.send(stored).is_err() {
            self.healthy.store(false, Ordering::SeqCst);
        }
    }
}

fn append_stored_event(path: &Path, stored: &StoredCrossArbEvent) -> std::io::Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, stored)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    file.write_all(b"\n")?;
    Ok(())
}

fn existing_jsonl_lines(path: &Path) -> std::io::Result<u64> {
    match fs::read_to_string(path) {
        Ok(raw) => Ok(raw.lines().count() as u64),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(err) => Err(err),
    }
}

pub fn load_jsonl_storage_events(
    dir: impl AsRef<Path>,
) -> std::io::Result<Vec<StoredCrossArbEvent>> {
    let dir = dir.as_ref();
    let mut paths = match fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "jsonl"))
            .collect::<Vec<_>>(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err),
    };
    paths.sort();

    let mut events = Vec::new();
    for path in paths {
        let file = fs::File::open(path)?;
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<StoredCrossArbEvent>(&line) {
                Ok(event) => events.push(event),
                Err(error) => log::warn!("skip malformed cross-arb jsonl event: {error}"),
            }
        }
    }
    events.sort_by(|left, right| {
        left.recorded_at
            .cmp(&right.recorded_at)
            .then_with(|| left.event_id.cmp(&right.event_id))
    });
    Ok(events)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::RuntimeMode;
    use crate::market::{CanonicalSymbol, ExchangeId};
    use crate::strategies::cross_exchange_arbitrage::ArbSignal;
    use crate::strategies::cross_exchange_arbitrage::{
        FundingModel, OpportunityDecision, OpportunityRecord, PositionSide,
    };

    #[test]
    fn storage_should_record_in_memory_events() {
        let now = Utc::now();
        let mut storage = InMemoryStorageSink::default();
        storage.record(
            CrossArbStorageEvent::Signal(ArbSignal::noop(RuntimeMode::Simulation, now)),
            now,
        );

        assert_eq!(storage.events().len(), 1);
        assert_eq!(storage.events()[0].event_id, 1);
    }

    #[test]
    fn storage_should_persist_funding_settlements_to_jsonl() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let now = Utc::now();
        let mut storage = JsonlStorageSink::for_day(tempdir.path(), now).expect("jsonl sink");
        let settlement = FundingModel::settle_leg(
            "bundle-1",
            ExchangeId::Binance,
            CanonicalSymbol::new("BTC", "USDT"),
            PositionSide::Long,
            100.0,
            0.0003,
            Some(65000.0),
            now,
        );

        storage.record(CrossArbStorageEvent::FundingSettlement(settlement), now);

        let raw = std::fs::read_to_string(storage.path()).expect("jsonl contents");
        let stored = raw.lines().next().expect("one event");
        let event: StoredCrossArbEvent = serde_json::from_str(stored).expect("stored event");
        assert_eq!(event.event_id, 1);
        assert!(matches!(
            event.event,
            CrossArbStorageEvent::FundingSettlement(_)
        ));
    }

    #[test]
    fn storage_should_record_private_events_in_memory() {
        let now = Utc::now();
        let mut storage = InMemoryStorageSink::default();
        let event = PrivateEvent::new(
            ExchangeId::Binance,
            crate::execution::PrivateEventKind::Heartbeat,
            now,
        );

        storage.record(CrossArbStorageEvent::PrivateEvent(event), now);

        assert_eq!(storage.events().len(), 1);
        assert!(matches!(
            storage.events()[0].event,
            CrossArbStorageEvent::PrivateEvent(_)
        ));
    }

    #[test]
    fn storage_should_insert_opportunity_record() {
        let now = Utc::now();
        let mut storage = InMemoryStorageSink::default();
        let opportunity = OpportunityRecord {
            timestamp: now,
            symbol: CanonicalSymbol::new("BTC", "USDT"),
            buy_exchange: ExchangeId::Binance,
            sell_exchange: ExchangeId::Okx,
            buy_price: 100.0,
            sell_price: 101.0,
            raw_spread_bps: 100.0,
            estimated_net_spread_bps: 90.0,
            estimated_notional: 100.0,
            decision: OpportunityDecision::Rejected,
            reason: "below_min_net_spread".to_string(),
        };

        storage.record(
            CrossArbStorageEvent::DetectedOpportunity(opportunity.clone()),
            now,
        );

        assert!(matches!(
            &storage.events()[0].event,
            CrossArbStorageEvent::DetectedOpportunity(stored) if stored == &opportunity
        ));
    }

    #[test]
    fn storage_should_insert_order_record() {
        let now = Utc::now();
        let mut storage = InMemoryStorageSink::default();
        let order = ArbitrageOrderRecord {
            timestamp: now,
            exchange: ExchangeId::Binance,
            symbol: CanonicalSymbol::new("BTC", "USDT"),
            side: "Buy".to_string(),
            order_type: "Market".to_string(),
            status: "Filled".to_string(),
            order_id: Some("order-1".to_string()),
            client_order_id: Some("client-1".to_string()),
            price: Some(100.0),
            quantity: 1.0,
            filled_quantity: 1.0,
            average_fill_price: Some(100.0),
            latency_ms: Some(12),
        };

        storage.record(CrossArbStorageEvent::OrderTransition(order.clone()), now);

        assert!(matches!(
            &storage.events()[0].event,
            CrossArbStorageEvent::OrderTransition(stored) if stored == &order
        ));
    }

    #[test]
    fn storage_should_insert_fill_record() {
        let now = Utc::now();
        let mut storage = InMemoryStorageSink::default();
        let fill = ArbitrageFillRecord {
            timestamp: now,
            exchange: ExchangeId::Okx,
            symbol: CanonicalSymbol::new("BTC", "USDT"),
            side: "Sell".to_string(),
            price: 102.0,
            quantity: 1.0,
            fee: 0.05,
            fee_asset: Some("USDT".to_string()),
            liquidity_role: "Taker".to_string(),
            order_id: Some("order-2".to_string()),
            client_order_id: Some("client-2".to_string()),
            latency_ms: Some(18),
        };

        storage.record(CrossArbStorageEvent::Fill(fill.clone()), now);

        assert!(matches!(
            &storage.events()[0].event,
            CrossArbStorageEvent::Fill(stored) if stored == &fill
        ));
    }
}
