use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use anyhow::{anyhow, Result};

use super::{
    detect_opportunities_for_pair_with_source, execute_paper_taker_taker,
    fee_model_with_strategy_overrides, BookCache, BookRecord, BookSource, CommonSymbolRules,
    PaperInventory, ReplayReport, ReplayReportBuilder, RiskState, SpotSpotTakerArbitrageConfig,
};
use crate::exchanges::unified::{
    MarketType, OrderBookSnapshot, OrderType, SymbolRule, SymbolStatus, TimeInForce,
};
use crate::execution::FeeModel;
use crate::risk::DisabledRegistry;

pub async fn run_replay_mode(config: SpotSpotTakerArbitrageConfig) -> Result<ReplayReport> {
    config.validate_safe_mode()?;
    let mut events = load_book_records(&config.replay.input_path)?;
    events.sort_by_key(BookRecord::timestamp_local);
    let cache = BookCache::default();
    let mut report = ReplayReportBuilder::default();
    let mut inventory = PaperInventory::from_config(&config)?;
    let fee_model = fee_model_with_strategy_overrides(
        FeeModel::load_or_default(&config.fee_config_path),
        &config,
    );
    let disabled_registry = DisabledRegistry::load_or_empty(&config.disabled_registry_path);
    inventory.exclude_unmanaged_positions(&disabled_registry);
    let mut risk = RiskState::new(&config);
    let rules = replay_symbol_rules(&config);

    for event in events {
        let timestamp = event.timestamp_local();
        if config
            .replay
            .start_time
            .is_some_and(|start| timestamp < start)
            || config.replay.end_time.is_some_and(|end| timestamp > end)
        {
            continue;
        }
        report.record_book_event(timestamp);
        let cached = event.into_cached();
        let exchange = cached.exchange.clone();
        let symbol = cached.symbol.clone();
        let snapshot = cached.into_snapshot();
        cache.update_book(snapshot, BookSource::Replay).await;

        let Some(rules) = rules.get(&symbol) else {
            continue;
        };
        let Some(mexc_book) = cache.get_book("mexc", &symbol).await else {
            continue;
        };
        let Some(coinex_book) = cache.get_book("coinex", &symbol).await else {
            continue;
        };
        let mexc_snapshot = mexc_book.into_snapshot();
        let coinex_snapshot = coinex_book.into_snapshot();
        let opportunities = detect_opportunities_for_pair_with_source(
            &config,
            rules,
            &mexc_snapshot,
            &coinex_snapshot,
            &inventory,
            &risk,
            &fee_model,
            &disabled_registry,
            BookSource::Replay,
            BookSource::Replay,
        );
        for opportunity in opportunities {
            report.record_opportunity(opportunity.clone());
            if opportunity.accepted {
                match execute_paper_taker_taker(
                    &config,
                    &mut inventory,
                    &opportunity,
                    rules,
                    &mexc_snapshot,
                    &coinex_snapshot,
                ) {
                    Ok(trade) => {
                        risk.record_trade(&trade);
                        risk.apply_trade_cooldown(&trade.symbol, config.cooldown_ms_after_trade);
                        report.record_trade(trade);
                    }
                    Err(reason) => {
                        risk.record_rejection(&opportunity.symbol, reason);
                    }
                }
            } else if let Some(reason) = opportunity.rejection_reason {
                risk.record_rejection(&opportunity.symbol, reason);
            }
        }
        log::trace!(
            "spot_spot_taker_arbitrage replay applied book exchange={} symbol={}",
            exchange,
            symbol
        );
    }

    let final_report = report.build(config.symbols.len());
    write_replay_report(&config.replay.output_path, &final_report)?;
    Ok(final_report)
}

pub fn load_book_records(path: &str) -> Result<Vec<BookRecord>> {
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut records = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        records.push(serde_json::from_str::<BookRecord>(&line)?);
    }
    Ok(records)
}

pub fn write_replay_report(path: &str, report: &ReplayReport) -> Result<()> {
    if let Some(parent) = Path::new(path).parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, report)?;
    writeln!(file)?;
    Ok(())
}

pub fn replay_symbol_rules(
    config: &SpotSpotTakerArbitrageConfig,
) -> HashMap<String, CommonSymbolRules> {
    config
        .symbols
        .iter()
        .map(|symbol| {
            let normalized = symbol.trim().to_ascii_uppercase();
            (
                normalized.clone(),
                CommonSymbolRules {
                    mexc: fallback_rule("mexc", &normalized, &config.quote_asset),
                    coinex: fallback_rule("coinex", &normalized, &config.quote_asset),
                    gateio: None,
                    bitget: None,
                },
            )
        })
        .collect()
}

fn fallback_rule(exchange: &str, symbol: &str, quote_asset: &str) -> SymbolRule {
    let base = symbol
        .strip_suffix(quote_asset)
        .filter(|base| !base.is_empty())
        .unwrap_or("BASE");
    SymbolRule {
        exchange: exchange.to_string(),
        market_type: MarketType::Spot,
        internal_symbol: symbol.to_string(),
        exchange_symbol: symbol.to_string(),
        base_asset: base.to_string(),
        quote_asset: quote_asset.to_string(),
        price_precision: 8,
        quantity_precision: 8,
        tick_size: 0.00000001,
        step_size: 0.00000001,
        min_quantity: 0.0,
        min_notional: 0.0,
        max_quantity: None,
        supported_order_types: vec![OrderType::Market, OrderType::IOC],
        supported_time_in_force: vec![TimeInForce::IOC],
        status: SymbolStatus::Trading,
        raw_metadata: None,
    }
}

pub fn ensure_replay_is_network_free(config: &SpotSpotTakerArbitrageConfig) -> Result<()> {
    if config.market_data_mode != super::MarketDataMode::Replay {
        return Err(anyhow!("not replay mode"));
    }
    Ok(())
}
