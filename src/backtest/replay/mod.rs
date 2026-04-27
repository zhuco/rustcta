use std::path::Path;

use anyhow::Result;
use chrono::{DateTime, Utc};

use crate::backtest::data::depth_dataset::DepthDatasetReader;
use crate::backtest::data::kline_dataset::KlineDatasetReader;
use crate::backtest::data::trade_dataset::TradeDatasetReader;
use crate::backtest::schema::{BacktestEvent, DepthDeltaEvent, KlineEvent, TradeEvent};

#[derive(Debug, Clone)]
pub struct KlineReplay {
    events: Vec<BacktestEvent>,
}

#[derive(Debug, Clone)]
pub struct DepthReplay {
    events: Vec<BacktestEvent>,
}

#[derive(Debug, Clone)]
pub struct TradeReplay {
    events: Vec<BacktestEvent>,
}

#[derive(Debug, Clone)]
pub struct ReplayPartition {
    pub start_index: usize,
    pub end_index: usize,
    pub start_ts: DateTime<Utc>,
    pub end_ts: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct MergedReplay {
    events: Vec<BacktestEvent>,
}

impl KlineReplay {
    pub fn load_binance_futures(
        root: impl AsRef<Path>,
        symbol: &str,
        interval: &str,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> Result<Self> {
        let reader = KlineDatasetReader::new(root);
        let (manifest, mut klines) = reader.read_binance_futures_klines(symbol, interval)?;
        klines.sort_by_key(|kline| kline.close_time);

        let mut events = klines
            .into_iter()
            .filter(|kline| start.is_none_or(|ts| kline.close_time >= ts))
            .filter(|kline| end.is_none_or(|ts| kline.close_time <= ts))
            .map(|kline| {
                BacktestEvent::Kline(KlineEvent {
                    exchange: manifest.exchange.clone(),
                    market: manifest.market.clone(),
                    symbol: manifest.symbol.clone(),
                    interval: manifest.interval.clone(),
                    exchange_ts: kline.close_time,
                    logical_ts: kline.close_time,
                    kline,
                })
            })
            .collect::<Vec<_>>();

        sort_events(&mut events);

        Ok(Self { events })
    }

    pub fn collect_events(&self) -> Vec<BacktestEvent> {
        self.events.clone()
    }

    pub fn plan_partitions(&self, partitions: usize) -> Vec<ReplayPartition> {
        if self.events.is_empty() || partitions == 0 {
            return Vec::new();
        }

        let chunk_size = self.events.len().div_ceil(partitions);
        let mut planned = Vec::new();
        let mut start_index = 0usize;

        while start_index < self.events.len() {
            let end_index = (start_index + chunk_size).min(self.events.len());
            let start_ts = self.events[start_index].logical_ts();
            let end_ts = self.events[end_index - 1].logical_ts();

            planned.push(ReplayPartition {
                start_index,
                end_index,
                start_ts,
                end_ts,
            });

            start_index = end_index;
        }

        planned
    }
}

impl DepthReplay {
    pub fn load_binance_futures(
        root: impl AsRef<Path>,
        symbol: &str,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> Result<Self> {
        let reader = DepthDatasetReader::new(root);
        let (manifest, mut deltas) = reader.read_binance_futures_depth_deltas(symbol)?;
        deltas.sort_by_key(|delta| (delta.logical_ts, delta.final_update_id));

        let mut events = deltas
            .into_iter()
            .filter(|delta| start.is_none_or(|ts| delta.logical_ts >= ts))
            .filter(|delta| end.is_none_or(|ts| delta.logical_ts <= ts))
            .map(|delta| {
                BacktestEvent::DepthDelta(DepthDeltaEvent {
                    exchange: manifest.exchange.clone(),
                    market: manifest.market.clone(),
                    symbol: manifest.symbol.clone(),
                    exchange_ts: delta.exchange_ts,
                    logical_ts: delta.logical_ts,
                    first_update_id: delta.first_update_id,
                    final_update_id: delta.final_update_id,
                    bids: delta.bids,
                    asks: delta.asks,
                })
            })
            .collect::<Vec<_>>();

        sort_events(&mut events);

        Ok(Self { events })
    }

    pub fn collect_events(&self) -> Vec<BacktestEvent> {
        self.events.clone()
    }
}

impl TradeReplay {
    pub fn load_binance_futures(
        root: impl AsRef<Path>,
        symbol: &str,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
    ) -> Result<Self> {
        let reader = TradeDatasetReader::new(root);
        let (manifest, mut trades) = reader.read_binance_futures_trades(symbol)?;
        trades.sort_by(|lhs, rhs| lhs.timestamp.cmp(&rhs.timestamp).then(lhs.id.cmp(&rhs.id)));

        let mut events = trades
            .into_iter()
            .filter(|trade| start.is_none_or(|ts| trade.timestamp >= ts))
            .filter(|trade| end.is_none_or(|ts| trade.timestamp <= ts))
            .map(|trade| {
                BacktestEvent::Trade(TradeEvent {
                    exchange: manifest.exchange.clone(),
                    market: manifest.market.clone(),
                    symbol: manifest.symbol.clone(),
                    exchange_ts: trade.timestamp,
                    logical_ts: trade.timestamp,
                    trade,
                })
            })
            .collect::<Vec<_>>();

        sort_events(&mut events);

        Ok(Self { events })
    }

    pub fn collect_events(&self) -> Vec<BacktestEvent> {
        self.events.clone()
    }
}

impl MergedReplay {
    pub fn from_streams(streams: Vec<Vec<BacktestEvent>>) -> Self {
        let mut events = streams.into_iter().flatten().collect::<Vec<_>>();
        sort_events(&mut events);
        Self { events }
    }

    pub fn collect_events(&self) -> Vec<BacktestEvent> {
        self.events.clone()
    }
}

fn sort_events(events: &mut [BacktestEvent]) {
    events.sort_by_key(|event| (event.logical_ts(), event.stable_priority()));
}
