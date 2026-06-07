use std::fs;
use std::path::Path;

use anyhow::Result;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::utils::rotating_file;

use super::{OpportunityRecord, RecorderConfig, SimulatedTradeRecord};

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum RecorderEvent {
    Opportunity(OpportunityRecord),
    SimulatedTrade(SimulatedTradeRecord),
}

#[derive(Clone)]
pub struct StrategyRecorder {
    tx: mpsc::Sender<RecorderEvent>,
}

impl StrategyRecorder {
    pub async fn start(config: &RecorderConfig) -> Result<Self> {
        if let Some(parent) = Path::new(&config.jsonl_path).parent() {
            fs::create_dir_all(parent)?;
        }
        if let Some(parent) = Path::new(&config.csv_path).parent() {
            fs::create_dir_all(parent)?;
        }
        let (tx, mut rx) = mpsc::channel::<RecorderEvent>(4096);
        let config = config.clone();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                if let Err(error) = append_jsonl(&config.jsonl_path, &event) {
                    log::error!("spot_spot_taker_arbitrage recorder jsonl error: {}", error);
                }
                if config.enable_csv_recording {
                    if let Err(error) = append_csv(&config.csv_path, &event) {
                        log::error!("spot_spot_taker_arbitrage recorder csv error: {}", error);
                    }
                }
            }
        });
        Ok(Self { tx })
    }

    pub async fn record_opportunity(&self, record: OpportunityRecord) {
        let _ = self.tx.send(RecorderEvent::Opportunity(record)).await;
    }

    pub async fn record_trade(&self, record: SimulatedTradeRecord) {
        let _ = self.tx.send(RecorderEvent::SimulatedTrade(record)).await;
    }
}

fn append_jsonl(path: &str, event: &RecorderEvent) -> std::io::Result<()> {
    rotating_file::append_json_line(path, event)
}

fn append_csv(path: &str, event: &RecorderEvent) -> std::io::Result<()> {
    const HEADER: &[u8] = b"event_type,timestamp,symbol,buy_exchange,sell_exchange,buy_price,sell_price,raw_spread_bps,net_spread_bps,notional,quantity,accepted,rejection_reason,rejection_detail,buy_fee_bps,sell_fee_bps,fee_source_buy,fee_source_sell,platform_discount_applied,estimated_total_fee,estimated_net_pnl,buy_book_age_ms,sell_book_age_ms,buy_book_source,sell_book_source,buy_latency_ms,sell_latency_ms,gross_pnl,net_pnl,fees\n";

    let row = match event {
        RecorderEvent::Opportunity(record) => format!(
            "opportunity,{},{},{},{},{},{},{},{},{},{},{},{:?},{:?},{},{},{:?},{:?},{},{},{},{},{},{:?},{:?},{:?},{:?},,,",
            record.timestamp.to_rfc3339(),
            record.symbol,
            record.buy_exchange,
            record.sell_exchange,
            record.buy_price,
            record.sell_price,
            record.raw_spread_bps,
            record.estimated_net_spread_bps,
            record.executable_notional,
            record.quantity,
            record.accepted,
            record.rejection_reason,
            record.rejection_detail,
            record.buy_fee_bps,
            record.sell_fee_bps,
            record.fee_source_buy,
            record.fee_source_sell,
            record.platform_discount_applied,
            record.estimated_total_fee,
            record.estimated_net_pnl,
            record.buy_book_age_ms,
            record.sell_book_age_ms,
            record.buy_book_source,
            record.sell_book_source,
            record.buy_latency_ms,
            record.sell_latency_ms
        ),
        RecorderEvent::SimulatedTrade(record) => format!(
            "simulated_trade,{},{},{},{},{},{},,,{},{},,,,,,,,,,,,,,,,,{},{},{}",
            record.timestamp.to_rfc3339(),
            record.symbol,
            record.buy_exchange,
            record.sell_exchange,
            record.buy_avg_price,
            record.sell_avg_price,
            record.notional,
            record.quantity,
            record.gross_pnl,
            record.net_pnl,
            record.buy_fee + record.sell_fee
        ),
    };
    let mut row = row.into_bytes();
    row.push(b'\n');
    rotating_file::append_record(
        path,
        Some(HEADER),
        &row,
        rotating_file::DEFAULT_MAX_FILE_BYTES,
        rotating_file::DEFAULT_MAX_FILES,
    )
}

#[cfg(test)]
pub fn append_jsonl_for_test(path: &str, event: &RecorderEvent) -> std::io::Result<()> {
    append_jsonl(path, event)
}
