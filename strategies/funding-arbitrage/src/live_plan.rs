use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Datelike, Duration as ChronoDuration, TimeZone, Timelike, Utc};
use serde::Serialize;

use crate::core::{ExchangeFundingSelection, FundingCoreConfig, FundingScanReport, FundingSymbol};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FundingLivePlan {
    pub generated_at: DateTime<Utc>,
    pub threshold_pct: f64,
    pub notional_usdt: f64,
    pub entries: Vec<FundingLiveExchangePlan>,
    pub skipped: Vec<FundingLiveExchangeSkip>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FundingLiveExchangePlan {
    pub exchange: String,
    pub canonical_symbol: FundingSymbol,
    pub exchange_symbol: String,
    pub funding_rate_pct: f64,
    pub next_funding_time: DateTime<Utc>,
    pub open_at: DateTime<Utc>,
    pub close_at: DateTime<Utc>,
    pub mark_price: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FundingLiveExchangeSkip {
    pub exchange: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FundingLiveExchangeResult {
    pub exchange: String,
    pub canonical_symbol: FundingSymbol,
    pub exchange_symbol: String,
    pub funding_rate_pct: f64,
    pub next_funding_time: DateTime<Utc>,
    pub notional_usdt: f64,
    pub planned_quantity: Option<f64>,
    pub open_at: DateTime<Utc>,
    pub close_at: DateTime<Utc>,
    pub status: FundingLiveStatus,
    pub error: Option<String>,
    pub balance_before_total_usdt: Option<f64>,
    pub balance_after_total_usdt: Option<f64>,
    pub balance_delta_usdt: Option<f64>,
    pub open_ack: Option<ActionAckSummary>,
    pub close_ack: Option<ActionAckSummary>,
    pub close_limit_ack: Option<ActionAckSummary>,
    pub close_limit_cancel_ack: Option<CancelAckSummary>,
    pub close_limit_attempts: u32,
    pub close_limit_timed_out: bool,
    pub close_market_fallback: bool,
    pub open_fill_summary: FillSummary,
    pub close_limit_fill_summary: FillSummary,
    pub close_fill_summary: FillSummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FundingLiveStatus {
    Planned,
    OpenSubmitted,
    CloseSubmitted,
    Closed,
    Failed,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct FillSummary {
    pub fills: usize,
    pub quantity: f64,
    pub quote_quantity: f64,
    pub avg_price: Option<f64>,
    pub fee_usdt: Option<f64>,
    pub realized_pnl_usdt: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ActionAckSummary {
    pub accepted: bool,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub status: String,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CancelAckSummary {
    pub accepted: bool,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub status: String,
    pub message: Option<String>,
}

pub fn build_live_plan(config: &FundingCoreConfig, report: &FundingScanReport) -> FundingLivePlan {
    build_live_plan_at(config, report, report.generated_at)
}

pub fn build_live_plan_at(
    config: &FundingCoreConfig,
    report: &FundingScanReport,
    now: DateTime<Utc>,
) -> FundingLivePlan {
    let mut entries = Vec::new();
    let mut skipped = Vec::new();

    for selection in &report.selections {
        if exchange_disabled(config, &selection.exchange) {
            skipped.push(FundingLiveExchangeSkip {
                exchange: selection.exchange.clone(),
                reason: "exchange disabled in funding universe config".to_string(),
            });
            continue;
        }
        match plan_entry_from_selection(config, selection, now) {
            Ok(entry) => entries.push(entry),
            Err(err) => skipped.push(FundingLiveExchangeSkip {
                exchange: selection.exchange.clone(),
                reason: err.to_string(),
            }),
        }
    }

    for error in &report.errors {
        skipped.push(FundingLiveExchangeSkip {
            exchange: error.exchange.clone(),
            reason: format!("scan error: {}", error.message),
        });
    }

    FundingLivePlan {
        generated_at: report.generated_at,
        threshold_pct: report.threshold_pct,
        notional_usdt: config.execution.notional_usdt,
        entries,
        skipped,
    }
}

pub fn plan_entry_from_selection(
    config: &FundingCoreConfig,
    selection: &ExchangeFundingSelection,
    now: DateTime<Utc>,
) -> Result<FundingLiveExchangePlan> {
    let candidate = selection
        .selected
        .as_ref()
        .ok_or_else(|| anyhow!("no selected funding candidate"))?;
    let next_funding_time = candidate
        .next_funding_time
        .ok_or_else(|| anyhow!("selected candidate has no next funding time"))?;
    let open_at = next_funding_time
        - ChronoDuration::seconds(config.execution.open_seconds_before_settlement);
    let close_at = next_funding_time
        + ChronoDuration::seconds(config.execution.close_seconds_after_settlement);
    if close_at <= now {
        bail!("close time already passed");
    }

    Ok(FundingLiveExchangePlan {
        exchange: selection.exchange.clone(),
        canonical_symbol: candidate.canonical_symbol.clone(),
        exchange_symbol: candidate
            .exchange_symbol
            .clone()
            .unwrap_or_else(|| candidate.canonical_symbol.as_pair()),
        funding_rate_pct: candidate.funding_rate_pct,
        next_funding_time,
        open_at,
        close_at,
        mark_price: candidate.mark_price,
    })
}

pub fn build_live_plan_markdown(plan: &FundingLivePlan) -> String {
    let mut content = format!(
        "### Funding Arbitrage Live Plan\n\
         > Time: {}\n\
         > Threshold: `{:.4}%`\n\
         > Notional per exchange: `{:.4} USDT`\n\n",
        plan.generated_at.format("%Y-%m-%d %H:%M:%S UTC"),
        plan.threshold_pct,
        plan.notional_usdt
    );

    if plan.entries.is_empty() {
        content.push_str("No exchange candidate is eligible for live execution.\n");
    } else {
        content.push_str("### Per-Exchange Execution\n\n");
        for entry in &plan.entries {
            content.push_str(&format!(
                "- `{}` `{}` funding <font color=\"warning\">{:+.4}%</font>, open `{}`, close `{}`, settlement `{}`\n",
                entry.exchange,
                entry.canonical_symbol,
                entry.funding_rate_pct,
                entry.open_at.format("%H:%M:%S UTC"),
                entry.close_at.format("%H:%M:%S UTC"),
                entry.next_funding_time.format("%Y-%m-%d %H:%M:%S UTC"),
            ));
        }
    }

    if !plan.skipped.is_empty() {
        content.push_str("\n### Skipped\n\n");
        for skip in &plan.skipped {
            content.push_str(&format!("- `{}`: {}\n", skip.exchange, skip.reason));
        }
    }
    content
}

pub fn build_live_result_markdown(results: &[FundingLiveExchangeResult]) -> String {
    let mut content = format!(
        "### Funding Arbitrage Live Result\n> Time: {}\n\n",
        Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
    );
    if results.is_empty() {
        content.push_str("No orders were submitted in this cycle.\n");
        return content;
    }

    for result in results {
        let status = match result.status {
            FundingLiveStatus::Closed => "<font color=\"info\">closed</font>",
            FundingLiveStatus::Failed => "<font color=\"warning\">failed</font>",
            FundingLiveStatus::Planned => "planned",
            FundingLiveStatus::OpenSubmitted => "open_submitted",
            FundingLiveStatus::CloseSubmitted => "close_submitted",
        };
        let balance_delta = result
            .balance_delta_usdt
            .map(|value| format!("{:+.8}", value))
            .unwrap_or_else(|| "unknown".to_string());
        let fill_pnl = sum_optional_values(
            [
                result.close_limit_fill_summary.realized_pnl_usdt,
                result.close_fill_summary.realized_pnl_usdt,
            ]
            .into_iter(),
        )
        .map(|value| format!("{:+.8}", value))
        .unwrap_or_else(|| "unknown".to_string());
        let open_avg = result
            .open_fill_summary
            .avg_price
            .map(|value| format!("{:.8}", value))
            .unwrap_or_else(|| "-".to_string());
        let close_avg =
            combined_close_avg(&result.close_limit_fill_summary, &result.close_fill_summary)
                .map(|value| format!("{:.8}", value))
                .unwrap_or_else(|| "-".to_string());
        let fee = total_fee_usdt(result)
            .map(|value| format!("{:.8}", value))
            .unwrap_or_else(|| "unknown".to_string());
        let close_method = close_method_text(result);

        content.push_str(&format!(
            "### `{}` `{}`\n\
             - Status: {}\n\
             - Funding rate: `{:+.4}%`\n\
             - Quantity: `{}`, open avg: `{}`, close avg: `{}`\n\
             - Close method: `{}`, limit attempts: `{}`, limit timed out: `{}`\n\
             - Balance delta USDT: <font color=\"comment\">{}</font>\n\
             - Fill realized_pnl USDT: `{}`, fee USDT: `{}`\n",
            result.exchange,
            result.canonical_symbol,
            status,
            result.funding_rate_pct,
            option_f64(result.planned_quantity),
            open_avg,
            close_avg,
            close_method,
            result.close_limit_attempts,
            result.close_limit_timed_out,
            balance_delta,
            fill_pnl,
            fee,
        ));
        if let Some(error) = &result.error {
            content.push_str(&format!("- Error: `{}`\n", error));
        }
        content.push('\n');
    }
    content
}

impl FundingLiveExchangeResult {
    pub fn from_plan(config: &FundingCoreConfig, entry: &FundingLiveExchangePlan) -> Self {
        Self {
            exchange: entry.exchange.clone(),
            canonical_symbol: entry.canonical_symbol.clone(),
            exchange_symbol: entry.exchange_symbol.clone(),
            funding_rate_pct: entry.funding_rate_pct,
            next_funding_time: entry.next_funding_time,
            notional_usdt: config.execution.notional_usdt,
            planned_quantity: None,
            open_at: entry.open_at,
            close_at: entry.close_at,
            status: FundingLiveStatus::Planned,
            error: None,
            balance_before_total_usdt: None,
            balance_after_total_usdt: None,
            balance_delta_usdt: None,
            open_ack: None,
            close_ack: None,
            close_limit_ack: None,
            close_limit_cancel_ack: None,
            close_limit_attempts: 0,
            close_limit_timed_out: false,
            close_market_fallback: false,
            open_fill_summary: FillSummary::default(),
            close_limit_fill_summary: FillSummary::default(),
            close_fill_summary: FillSummary::default(),
        }
    }

    pub fn fail(&mut self, err: impl std::fmt::Display) {
        self.status = FundingLiveStatus::Failed;
        self.error = Some(err.to_string());
    }
}

pub fn funding_client_order_id(
    exchange: &str,
    action: &str,
    suffix: i64,
    attempt: Option<u32>,
) -> String {
    let exchange_code = compact_exchange_code(exchange);
    match attempt {
        Some(attempt) => format!("fa-{exchange_code}-{action}-{suffix}-{attempt}"),
        None => format!("fa-{exchange_code}-{action}-{suffix}"),
    }
}

pub fn next_scan_time(now: DateTime<Utc>, scan_minute: u32) -> Result<DateTime<Utc>> {
    if scan_minute > 59 {
        bail!("scan_minute must be <= 59");
    }
    let current_hour = Utc
        .with_ymd_and_hms(
            now.year(),
            now.month(),
            now.day(),
            now.hour(),
            scan_minute,
            0,
        )
        .single()
        .ok_or_else(|| anyhow!("failed to build current hour scan time"))?;
    let next = if now < current_hour {
        current_hour
    } else {
        current_hour + ChronoDuration::hours(1)
    };
    Ok(next)
}

fn exchange_disabled(config: &FundingCoreConfig, exchange: &str) -> bool {
    !config
        .universe
        .enabled_exchanges
        .iter()
        .any(|enabled| enabled.eq_ignore_ascii_case(exchange))
}

fn compact_exchange_code(exchange: &str) -> &str {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "binance" => "bi",
        "bitget" => "bg",
        "gate" | "gateio" => "gt",
        "okx" => "ok",
        "bybit" => "bb",
        "mexc" => "mx",
        "coinex" => "cx",
        "kucoin" => "kc",
        "htx" | "huobi" => "hx",
        _ => "ot",
    }
}

fn option_f64(value: Option<f64>) -> String {
    value
        .map(|value| format!("{:.12}", value))
        .unwrap_or_else(|| "-".to_string())
}

fn combined_close_avg(limit: &FillSummary, market: &FillSummary) -> Option<f64> {
    let quantity = limit.quantity + market.quantity;
    (quantity > 0.0).then_some((limit.quote_quantity + market.quote_quantity) / quantity)
}

fn total_fee_usdt(result: &FundingLiveExchangeResult) -> Option<f64> {
    sum_optional_values(
        [
            result.open_fill_summary.fee_usdt,
            result.close_limit_fill_summary.fee_usdt,
            result.close_fill_summary.fee_usdt,
        ]
        .into_iter(),
    )
}

fn close_method_text(result: &FundingLiveExchangeResult) -> &'static str {
    match (
        result.close_limit_ack.is_some(),
        result.close_limit_fill_summary.quantity > 0.0,
        result.close_market_fallback,
    ) {
        (true, true, true) => "limit_then_market_partial",
        (true, true, false) => "limit",
        (true, false, true) => "limit_timeout_market",
        (false, _, true) => "market",
        _ => "none",
    }
}

fn sum_optional_values(values: impl Iterator<Item = Option<f64>>) -> Option<f64> {
    let mut seen = false;
    let sum = values
        .filter_map(|value| {
            seen = true;
            value
        })
        .sum::<f64>();
    seen.then_some(sum)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use crate::core::{
        ExchangeFundingSelection, ExchangeScanError, ExecutionConfig, FundingCandidate,
        FundingScanReport, SelectionConfig,
    };

    use super::*;

    #[test]
    fn live_plan_should_build_entries_and_skips_without_adapters() {
        let now = Utc.with_ymd_and_hms(2026, 6, 7, 11, 50, 0).unwrap();
        let settlement = Utc.with_ymd_and_hms(2026, 6, 7, 12, 0, 0).unwrap();
        let config = FundingCoreConfig {
            mode: "live".to_string(),
            selection: SelectionConfig {
                max_seconds_to_settlement_at_scan: Some(900),
                ..SelectionConfig::default()
            },
            execution: ExecutionConfig {
                open_seconds_before_settlement: 30,
                close_seconds_after_settlement: 45,
                ..ExecutionConfig::default()
            },
            ..FundingCoreConfig::default()
        };

        let report = FundingScanReport {
            generated_at: now,
            threshold: -0.005,
            threshold_pct: -0.5,
            selections: vec![
                ExchangeFundingSelection {
                    exchange: "binance".to_string(),
                    selected: Some(FundingCandidate {
                        exchange: "binance".to_string(),
                        canonical_symbol: FundingSymbol::new("btc", "usdt"),
                        exchange_symbol: Some("BTCUSDT".to_string()),
                        funding_rate: -0.006,
                        funding_rate_pct: -0.6,
                        predicted_funding_rate: None,
                        mark_price: Some(100_000.0),
                        index_price: None,
                        next_funding_time: Some(settlement),
                        seconds_to_settlement: Some(600),
                        snapshot_age_ms: 12,
                        qualifies: true,
                    }),
                    scanned_symbols: 10,
                    funding_snapshots: 3,
                    eligible_candidates: 1,
                    skipped_reason: None,
                },
                ExchangeFundingSelection {
                    exchange: "okx".to_string(),
                    selected: None,
                    scanned_symbols: 10,
                    funding_snapshots: 0,
                    eligible_candidates: 0,
                    skipped_reason: Some("no funding snapshots loaded".to_string()),
                },
            ],
            errors: vec![ExchangeScanError {
                exchange: "gate".to_string(),
                stage: "funding",
                message: "timeout".to_string(),
            }],
        };

        let plan = build_live_plan_at(&config, &report, now);

        assert_eq!(plan.entries.len(), 1);
        let entry = &plan.entries[0];
        assert_eq!(entry.exchange, "binance");
        assert_eq!(entry.canonical_symbol.as_pair(), "BTC/USDT");
        assert_eq!(
            entry.open_at,
            Utc.with_ymd_and_hms(2026, 6, 7, 11, 59, 30).unwrap()
        );
        assert_eq!(
            entry.close_at,
            Utc.with_ymd_and_hms(2026, 6, 7, 12, 0, 45).unwrap()
        );
        assert_eq!(plan.skipped.len(), 2);
        assert!(plan
            .skipped
            .iter()
            .any(|skip| skip.exchange == "okx" && skip.reason.contains("disabled")));
        assert!(plan
            .skipped
            .iter()
            .any(|skip| skip.exchange == "gate" && skip.reason.contains("timeout")));
    }

    #[test]
    fn live_plan_should_reject_past_close_window() {
        let now = Utc.with_ymd_and_hms(2026, 6, 7, 12, 1, 0).unwrap();
        let settlement = Utc.with_ymd_and_hms(2026, 6, 7, 12, 0, 0).unwrap();
        let mut config = FundingCoreConfig::default();
        config.execution.close_seconds_after_settlement = 30;
        let selection = ExchangeFundingSelection {
            exchange: "binance".to_string(),
            selected: Some(FundingCandidate {
                exchange: "binance".to_string(),
                canonical_symbol: FundingSymbol::new("eth", "usdt"),
                exchange_symbol: None,
                funding_rate: -0.01,
                funding_rate_pct: -1.0,
                predicted_funding_rate: None,
                mark_price: None,
                index_price: None,
                next_funding_time: Some(settlement),
                seconds_to_settlement: Some(-60),
                snapshot_age_ms: 1,
                qualifies: true,
            }),
            scanned_symbols: 1,
            funding_snapshots: 1,
            eligible_candidates: 1,
            skipped_reason: None,
        };

        let err = plan_entry_from_selection(&config, &selection, now).unwrap_err();

        assert!(err.to_string().contains("close time already passed"));
    }

    #[test]
    fn live_result_markdown_should_report_balance_delta_and_close_method() {
        let now = Utc.with_ymd_and_hms(2026, 6, 7, 12, 0, 0).unwrap();
        let result = FundingLiveExchangeResult {
            exchange: "binance".to_string(),
            canonical_symbol: FundingSymbol::new("btc", "usdt"),
            exchange_symbol: "BTCUSDT".to_string(),
            funding_rate_pct: -0.6,
            next_funding_time: now,
            notional_usdt: 10.0,
            planned_quantity: Some(0.001),
            open_at: now - ChronoDuration::seconds(1),
            close_at: now + ChronoDuration::seconds(1),
            status: FundingLiveStatus::Closed,
            error: None,
            balance_before_total_usdt: Some(100.0),
            balance_after_total_usdt: Some(100.12),
            balance_delta_usdt: Some(0.12),
            open_ack: None,
            close_ack: None,
            close_limit_ack: Some(ActionAckSummary {
                accepted: true,
                client_order_id: "fa-bi-cl-1-1".to_string(),
                exchange_order_id: Some("1".to_string()),
                status: "accepted".to_string(),
                message: None,
            }),
            close_limit_cancel_ack: None,
            close_limit_attempts: 1,
            close_limit_timed_out: false,
            close_market_fallback: false,
            open_fill_summary: FillSummary {
                avg_price: Some(100_000.0),
                fee_usdt: Some(0.01),
                ..FillSummary::default()
            },
            close_limit_fill_summary: FillSummary {
                quantity: 0.001,
                quote_quantity: 100.001,
                avg_price: Some(100_001.0),
                fee_usdt: Some(0.01),
                realized_pnl_usdt: Some(0.001),
                ..FillSummary::default()
            },
            close_fill_summary: FillSummary::default(),
        };

        let markdown = build_live_result_markdown(&[result]);

        assert!(markdown.contains("`binance` `BTC/USDT`"));
        assert!(markdown.contains("+0.12000000"));
        assert!(markdown.contains("Close method: `limit`"));
        assert!(markdown.contains("Balance delta USDT"));
    }

    #[test]
    fn next_scan_time_should_roll_to_next_hour_when_minute_passed() {
        let now = Utc.with_ymd_and_hms(2026, 5, 30, 11, 56, 0).unwrap();
        let next = next_scan_time(now, 55).unwrap();

        assert_eq!(next, Utc.with_ymd_and_hms(2026, 5, 30, 12, 55, 0).unwrap());
    }

    #[test]
    fn funding_client_order_id_should_fit_binance_limit() {
        let suffix = 1_780_217_879_939;
        let ids = [
            funding_client_order_id("binance", "op", suffix, None),
            funding_client_order_id("binance", "cl", suffix, Some(3)),
            funding_client_order_id("binance", "cm", suffix, None),
        ];

        for id in ids {
            assert!(
                id.len() < 36,
                "Binance requires client order id length < 36, got {} for {id}",
                id.len()
            );
        }
    }
}
