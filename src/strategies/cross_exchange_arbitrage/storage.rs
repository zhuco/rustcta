use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::execution::PrivateEvent;

use super::{ArbSignal, FundingSettlement, Opportunity, SimulatedBundleState};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CrossArbStorageEvent {
    Opportunity(Opportunity),
    Signal(ArbSignal),
    Bundle(SimulatedBundleState),
    FundingSettlement(FundingSettlement),
    PrivateEvent(PrivateEvent),
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

fn existing_jsonl_lines(path: &Path) -> std::io::Result<u64> {
    match fs::read_to_string(path) {
        Ok(raw) => Ok(raw.lines().count() as u64),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::RuntimeMode;
    use crate::market::{CanonicalSymbol, ExchangeId};
    use crate::strategies::cross_exchange_arbitrage::ArbSignal;
    use crate::strategies::cross_exchange_arbitrage::{FundingModel, PositionSide};

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
}
