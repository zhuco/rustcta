use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    FeeRateSnapshot, FundingRateSnapshot, FundingRatesResponse, SymbolRules, SymbolRulesResponse,
};
use rustcta_types::{MarketType, OrderBookSnapshot};

use crate::core::{
    select_exchange_funding, ExchangeScanError, FundingCoreConfig, FundingInstrument,
    FundingScanReport, FundingSnapshot, FundingSymbol,
};

#[derive(Debug, Clone, PartialEq)]
pub struct GatewayFundingScanBundle {
    pub exchange: String,
    pub symbol_rules: SymbolRulesResponse,
    pub funding_rates: FundingRatesResponse,
    pub fees: Vec<FeeRateSnapshot>,
    pub order_books: Vec<OrderBookSnapshot>,
    pub errors: Vec<ExchangeScanError>,
}

pub fn build_report_from_gateway_scan_bundles(
    config: &FundingCoreConfig,
    generated_at: DateTime<Utc>,
    bundles: impl IntoIterator<Item = GatewayFundingScanBundle>,
) -> FundingScanReport {
    let mut selections = Vec::new();
    let mut errors = Vec::new();
    for bundle in bundles {
        let exchange = bundle.exchange;
        if bundle.symbol_rules.rules.is_empty() {
            errors.push(scan_error(
                &exchange,
                "symbol_rules",
                "no symbol rules returned",
            ));
        }
        if bundle.funding_rates.rates.is_empty() {
            errors.push(scan_error(
                &exchange,
                "funding_rates",
                "no funding rates returned",
            ));
        } else {
            errors.extend(validate_gateway_funding_snapshots(
                &exchange,
                &bundle.funding_rates,
                config,
            ));
        }
        if bundle.fees.is_empty() {
            errors.push(scan_error(&exchange, "fees", "no fee rates returned"));
        }
        if bundle.order_books.is_empty() {
            errors.push(scan_error(
                &exchange,
                "order_book",
                "no order book snapshot returned",
            ));
        }
        errors.extend(bundle.errors);

        let instruments = bundle
            .symbol_rules
            .rules
            .iter()
            .map(instrument_from_symbol_rules)
            .collect::<Vec<_>>();
        let snapshots = bundle
            .funding_rates
            .rates
            .iter()
            .filter_map(funding_snapshot_from_gateway)
            .collect::<Vec<_>>();
        selections.push(select_exchange_funding(
            exchange,
            snapshots,
            &instruments,
            config,
            generated_at,
        ));
    }

    FundingScanReport {
        generated_at,
        threshold: config.selection.min_funding_rate,
        threshold_pct: config.selection.min_funding_rate * 100.0,
        selections,
        errors,
    }
}

pub fn instrument_from_symbol_rules(rule: &SymbolRules) -> FundingInstrument {
    let canonical = funding_symbol_from_scope(&rule.symbol.canonical_symbol)
        .unwrap_or_else(|| FundingSymbol::new(&rule.base_asset, &rule.quote_asset));
    let status = if rule.supports_limit_orders || rule.supports_market_orders {
        "trading"
    } else {
        "disabled"
    };
    FundingInstrument {
        exchange: rule.symbol.exchange.to_string(),
        canonical_symbol: canonical,
        exchange_symbol: rule.symbol.exchange_symbol.symbol.clone(),
        contract_type: match rule.symbol.market_type {
            MarketType::Perpetual => "perpetual".to_string(),
            other => format!("{other:?}").to_ascii_lowercase(),
        },
        status: status.to_string(),
        contract_size: 1.0,
        quantity_step: parse_decimal(rule.quantity_increment.as_deref()).unwrap_or(1.0),
        min_qty: parse_decimal(rule.min_quantity.as_deref()).unwrap_or_default(),
        min_notional: parse_decimal(rule.min_notional.as_deref()).unwrap_or_default(),
    }
}

pub fn funding_snapshot_from_gateway(snapshot: &FundingRateSnapshot) -> Option<FundingSnapshot> {
    Some(FundingSnapshot {
        exchange: snapshot.symbol.exchange.to_string(),
        canonical_symbol: funding_symbol_from_scope(&snapshot.symbol.canonical_symbol)?,
        exchange_symbol: Some(snapshot.symbol.exchange_symbol.symbol.clone()),
        funding_rate: parse_decimal(Some(snapshot.funding_rate.as_str()))?,
        predicted_funding_rate: parse_decimal(snapshot.predicted_funding_rate.as_deref()),
        mark_price: parse_decimal(snapshot.mark_price.as_deref()),
        index_price: parse_decimal(snapshot.index_price.as_deref()),
        next_funding_time: snapshot.next_funding_time,
        recv_ts: snapshot.updated_at,
    })
}

fn validate_gateway_funding_snapshots(
    exchange: &str,
    response: &FundingRatesResponse,
    config: &FundingCoreConfig,
) -> Vec<ExchangeScanError> {
    let mut errors = Vec::new();
    for snapshot in &response.rates {
        let symbol = snapshot.symbol.exchange_symbol.symbol.as_str();
        if parse_decimal(Some(snapshot.funding_rate.as_str())).is_none() {
            errors.push(scan_error(
                exchange,
                "funding_rates",
                format!("{symbol} funding_rate is not a finite decimal"),
            ));
        }
        if snapshot
            .mark_price
            .as_deref()
            .and_then(|value| parse_decimal(Some(value)))
            .is_none()
        {
            errors.push(scan_error(
                exchange,
                "funding_rates",
                format!("{symbol} missing usable mark_price"),
            ));
        }
        if config.selection.require_next_funding_time && snapshot.next_funding_time.is_none() {
            errors.push(scan_error(
                exchange,
                "funding_rates",
                format!("{symbol} missing next_funding_time required for settlement-window scan"),
            ));
        }
    }
    errors
}

fn funding_symbol_from_scope(
    symbol: &Option<rustcta_exchange_api::CanonicalSymbol>,
) -> Option<FundingSymbol> {
    let symbol = symbol.as_ref()?;
    Some(FundingSymbol::new(
        symbol.base_asset(),
        symbol.quote_asset(),
    ))
}

fn parse_decimal(value: Option<&str>) -> Option<f64> {
    value?
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
}

fn scan_error(
    exchange: &str,
    stage: &'static str,
    message: impl Into<String>,
) -> ExchangeScanError {
    ExchangeScanError {
        exchange: exchange.to_string(),
        stage,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use rustcta_exchange_api::{
        FeeRateSnapshot, FundingRateSnapshot, FundingRatesResponse, ResponseMetadata, SymbolRules,
        SymbolRulesResponse, SymbolScope, EXCHANGE_API_SCHEMA_VERSION,
    };
    use rustcta_types::{
        CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderBookLevel, OrderBookSnapshot,
        SchemaVersion,
    };

    use super::*;

    #[test]
    fn gateway_scan_bundles_should_feed_live_selection_for_target_contract_venues() {
        let now = Utc::now();
        let mut config = FundingCoreConfig {
            mode: "live".to_string(),
            ..FundingCoreConfig::default()
        };
        config.selection.max_seconds_to_settlement_at_scan = Some(3600);
        config.execution.notional_usdt = 10.0;

        let bundles = ["aster", "mexc", "kucoinfutures", "bybit"]
            .into_iter()
            .map(|exchange| gateway_bundle(exchange, now))
            .collect::<Vec<_>>();
        let report = build_report_from_gateway_scan_bundles(&config, now, bundles);

        assert!(report.errors.is_empty());
        assert_eq!(report.selections.len(), 4);
        for selection in &report.selections {
            let selected = selection.selected.as_ref().expect("selected candidate");
            assert!(selected.mark_price.is_some());
            assert!(selected.next_funding_time.is_some());
            assert!(selected
                .seconds_to_settlement
                .is_some_and(|secs| secs <= 3600));
        }
    }

    #[test]
    fn gateway_scan_bundles_should_report_missing_scanner_stages() {
        let now = Utc::now();
        let exchange = ExchangeId::new("aster").expect("exchange");
        let metadata = ResponseMetadata::new(exchange.clone(), now);
        let report = build_report_from_gateway_scan_bundles(
            &FundingCoreConfig::default(),
            now,
            [GatewayFundingScanBundle {
                exchange: exchange.to_string(),
                symbol_rules: SymbolRulesResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata: metadata.clone(),
                    rules: Vec::new(),
                },
                funding_rates: FundingRatesResponse {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    metadata,
                    rates: Vec::new(),
                },
                fees: Vec::new(),
                order_books: Vec::new(),
                errors: Vec::new(),
            }],
        );

        let stages = report
            .errors
            .iter()
            .map(|error| error.stage)
            .collect::<Vec<_>>();
        assert_eq!(
            stages,
            vec!["symbol_rules", "funding_rates", "fees", "order_book"]
        );
    }

    #[test]
    fn gateway_scan_bundles_should_report_unusable_funding_snapshots() {
        let now = Utc::now();
        let config = FundingCoreConfig::default();
        let mut bundle = gateway_bundle("mexc", now);
        let snapshot = bundle.funding_rates.rates.first_mut().expect("snapshot");
        snapshot.funding_rate = "NaN".to_string();
        snapshot.mark_price = None;
        snapshot.next_funding_time = None;

        let report = build_report_from_gateway_scan_bundles(&config, now, [bundle]);

        assert_eq!(report.selections.len(), 1);
        assert!(report.selections[0].selected.is_none());
        let messages = report
            .errors
            .iter()
            .filter(|error| error.stage == "funding_rates")
            .map(|error| error.message.as_str())
            .collect::<Vec<_>>();
        assert!(
            messages
                .iter()
                .any(|message| message.contains("funding_rate is not a finite decimal")),
            "{messages:?}"
        );
        assert!(
            messages
                .iter()
                .any(|message| message.contains("missing usable mark_price")),
            "{messages:?}"
        );
        assert!(
            messages
                .iter()
                .any(|message| message.contains("missing next_funding_time")),
            "{messages:?}"
        );
    }

    #[test]
    fn gateway_scan_bundles_should_allow_missing_next_funding_time_when_config_allows_it() {
        let now = Utc::now();
        let mut config = FundingCoreConfig::default();
        config.selection.require_next_funding_time = false;
        let mut bundle = gateway_bundle("bybit", now);
        bundle.funding_rates.rates[0].next_funding_time = None;

        let report = build_report_from_gateway_scan_bundles(&config, now, [bundle]);

        assert!(report.errors.iter().all(|error| {
            !(error.stage == "funding_rates" && error.message.contains("next_funding_time"))
        }));
    }

    fn gateway_bundle(exchange: &str, now: DateTime<Utc>) -> GatewayFundingScanBundle {
        let exchange_id = ExchangeId::new(exchange).expect("exchange");
        let metadata = ResponseMetadata::new(exchange_id.clone(), now);
        let symbol = symbol_scope(&exchange_id);
        GatewayFundingScanBundle {
            exchange: exchange.to_string(),
            symbol_rules: SymbolRulesResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata: metadata.clone(),
                rules: vec![SymbolRules {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    symbol: symbol.clone(),
                    base_asset: "BTC".to_string(),
                    quote_asset: "USDT".to_string(),
                    price_increment: Some("0.1".to_string()),
                    quantity_increment: Some("0.00001".to_string()),
                    min_price: None,
                    max_price: None,
                    min_quantity: Some("0.00001".to_string()),
                    max_quantity: None,
                    min_notional: Some("5".to_string()),
                    max_notional: None,
                    price_precision: None,
                    quantity_precision: None,
                    supports_market_orders: true,
                    supports_limit_orders: true,
                    supports_post_only: true,
                    supports_reduce_only: true,
                    updated_at: now,
                }],
            },
            funding_rates: FundingRatesResponse {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                metadata,
                rates: vec![FundingRateSnapshot {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    symbol: symbol.clone(),
                    funding_rate: "-0.006".to_string(),
                    predicted_funding_rate: Some("-0.0055".to_string()),
                    funding_time: None,
                    next_funding_time: Some(now + Duration::minutes(30)),
                    mark_price: Some("65000".to_string()),
                    index_price: Some("64995".to_string()),
                    open_interest: Some("12345".to_string()),
                    turnover_24h: Some("1000000".to_string()),
                    volume_24h: Some("100".to_string()),
                    source: Some(format!("{exchange}.fixture")),
                    updated_at: now,
                }],
            },
            fees: vec![FeeRateSnapshot {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                symbol: symbol.clone(),
                maker_rate: "0.0002".to_string(),
                taker_rate: "0.0005".to_string(),
                source: Some(format!("{exchange}.fixture")),
                updated_at: now,
            }],
            order_books: vec![OrderBookSnapshot {
                schema_version: SchemaVersion::current(),
                exchange_id,
                market_type: MarketType::Perpetual,
                canonical_symbol: CanonicalSymbol::new("BTC", "USDT").expect("symbol"),
                exchange_symbol: Some(symbol.exchange_symbol.clone()),
                bids: vec![OrderBookLevel {
                    price: 64_999.0,
                    quantity: 1.0,
                }],
                asks: vec![OrderBookLevel {
                    price: 65_001.0,
                    quantity: 1.0,
                }],
                sequence: Some(1),
                exchange_timestamp: Some(now),
                received_at: now,
                is_stale: false,
            }],
            errors: Vec::new(),
        }
    }

    fn symbol_scope(exchange: &ExchangeId) -> SymbolScope {
        SymbolScope {
            exchange: exchange.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("symbol")),
            exchange_symbol: ExchangeSymbol::new(
                exchange.clone(),
                MarketType::Perpetual,
                "BTCUSDT",
            )
            .expect("exchange symbol"),
        }
    }
}
