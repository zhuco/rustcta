use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::exchanges::unified::{SymbolRule, SymbolStatus};

use crate::scanner::{normalize_exchange_name, role_for, ExchangeOperationalRole};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SymbolCoverageRecord {
    pub internal_symbol: String,
    pub available_exchanges: Vec<String>,
    pub tradable_exchanges: Vec<String>,
    pub scan_eligible_exchanges: Vec<String>,
    pub read_only_validated_exchanges: Vec<String>,
    pub future_execution_candidate_exchanges: Vec<String>,
    pub symbol_rule_warnings: Vec<String>,
    pub fee_warnings: Vec<String>,
    pub book_health_warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExchangePairCoverage {
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub common_symbol_count: usize,
    pub fresh_book_symbol_count: usize,
    pub fee_known_symbol_count: usize,
    pub scan_eligible_symbol_count: usize,
    pub live_dry_run_eligible_symbol_count: usize,
}

pub fn build_symbol_coverage(
    rules: &[SymbolRule],
    roles: &BTreeMap<String, ExchangeOperationalRole>,
) -> Vec<SymbolCoverageRecord> {
    let mut by_symbol: BTreeMap<String, Vec<&SymbolRule>> = BTreeMap::new();
    for rule in rules {
        if rule.market_type != crate::exchanges::unified::MarketType::Spot {
            continue;
        }
        by_symbol
            .entry(rule.internal_symbol.trim().to_ascii_uppercase())
            .or_default()
            .push(rule);
    }
    by_symbol
        .into_iter()
        .map(|(symbol, symbol_rules)| coverage_for_symbol(symbol, symbol_rules, roles))
        .filter(|record| record.available_exchanges.len() >= 2)
        .collect()
}

fn coverage_for_symbol(
    internal_symbol: String,
    rules: Vec<&SymbolRule>,
    roles: &BTreeMap<String, ExchangeOperationalRole>,
) -> SymbolCoverageRecord {
    let mut available = BTreeSet::new();
    let mut tradable = BTreeSet::new();
    let mut scan = BTreeSet::new();
    let mut readonly = BTreeSet::new();
    let mut future = BTreeSet::new();
    let mut warnings = Vec::new();
    let mut base_quote: Option<(&str, &str)> = None;

    for rule in rules {
        let exchange = normalize_exchange_name(&rule.exchange);
        available.insert(exchange.clone());
        if let Some((base, quote)) = base_quote {
            if base != rule.base_asset || quote != rule.quote_asset {
                warnings.push(format!(
                    "{} maps {} as {}/{} instead of {}/{}",
                    exchange, internal_symbol, rule.base_asset, rule.quote_asset, base, quote
                ));
            }
        } else {
            base_quote = Some((&rule.base_asset, &rule.quote_asset));
        }
        if rule.status == SymbolStatus::Trading {
            tradable.insert(exchange.clone());
        } else {
            warnings.push(format!("{} status is {:?}", exchange, rule.status));
            continue;
        }
        let role = role_for(roles, &exchange);
        if role.scan_enabled() {
            scan.insert(exchange.clone());
        }
        if role.authenticated_read_allowed() {
            readonly.insert(exchange.clone());
        }
        if role.future_live_candidate() {
            future.insert(exchange);
        }
    }

    SymbolCoverageRecord {
        internal_symbol,
        available_exchanges: available.into_iter().collect(),
        tradable_exchanges: tradable.into_iter().collect(),
        scan_eligible_exchanges: scan.into_iter().collect(),
        read_only_validated_exchanges: readonly.into_iter().collect(),
        future_execution_candidate_exchanges: future.into_iter().collect(),
        symbol_rule_warnings: warnings,
        fee_warnings: Vec::new(),
        book_health_warnings: Vec::new(),
    }
}

pub fn build_pair_coverage(records: &[SymbolCoverageRecord]) -> Vec<ExchangePairCoverage> {
    let exchanges = ["gateio", "bitget", "mexc", "coinex", "kucoin"];
    let mut out = Vec::new();
    for buy in exchanges {
        for sell in exchanges {
            if buy == sell {
                continue;
            }
            let mut common = 0;
            let mut scan = 0;
            let mut live_dry = 0;
            for record in records {
                let has_buy = record.available_exchanges.iter().any(|item| item == buy);
                let has_sell = record.available_exchanges.iter().any(|item| item == sell);
                if has_buy && has_sell {
                    common += 1;
                }
                let scan_buy = record
                    .scan_eligible_exchanges
                    .iter()
                    .any(|item| item == buy);
                let scan_sell = record
                    .scan_eligible_exchanges
                    .iter()
                    .any(|item| item == sell);
                if scan_buy && scan_sell {
                    scan += 1;
                }
                let live_buy = record
                    .future_execution_candidate_exchanges
                    .iter()
                    .any(|item| item == buy);
                let live_sell = record
                    .future_execution_candidate_exchanges
                    .iter()
                    .any(|item| item == sell);
                if live_buy && live_sell {
                    live_dry += 1;
                }
            }
            out.push(ExchangePairCoverage {
                buy_exchange: buy.to_string(),
                sell_exchange: sell.to_string(),
                common_symbol_count: common,
                fresh_book_symbol_count: 0,
                fee_known_symbol_count: common,
                scan_eligible_symbol_count: scan,
                live_dry_run_eligible_symbol_count: live_dry,
            });
        }
    }
    out
}
