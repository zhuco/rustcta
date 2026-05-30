use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, Datelike, Duration as ChronoDuration, TimeZone, Timelike, Utc};
use futures_util::future::join_all;
use serde::Serialize;
use tokio::time::{sleep, sleep_until, Duration, Instant};

use crate::exchanges::registry::{
    build_trading_adapter_for_exchange_with_instruments, configured_position_mode, market_adapter,
};
use crate::execution::{
    ClosePositionCommand, ExchangeBalance, ExchangePosition, FillEvent, FillQuery, OrderAck,
    OrderCommand, OrderCommandStatus, OrderIntent, OrderSide, OrderType, PositionMode,
    PositionSide, TimeInForce, TradingAdapter, TradingCapabilities,
};
use crate::market::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, InstrumentMeta, MarketDataAdapter,
};

use super::config::FundingRateArbitrageConfig;
use super::notification::send_markdown_notification;
use super::scanner::{
    scan_funding_opportunities_with_timeout, ExchangeFundingSelection, FundingScanReport,
};

#[derive(Debug, Clone, Serialize)]
pub struct FundingLivePlan {
    pub generated_at: DateTime<Utc>,
    pub threshold_pct: f64,
    pub notional_usdt: f64,
    pub entries: Vec<FundingLiveExchangePlan>,
    pub skipped: Vec<FundingLiveExchangeSkip>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FundingLiveExchangePlan {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
    pub exchange_symbol: String,
    pub funding_rate_pct: f64,
    pub next_funding_time: DateTime<Utc>,
    pub open_at: DateTime<Utc>,
    pub close_at: DateTime<Utc>,
    pub mark_price: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FundingLiveExchangeSkip {
    pub exchange: ExchangeId,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct FundingLiveExchangeResult {
    pub exchange: ExchangeId,
    pub canonical_symbol: CanonicalSymbol,
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
    pub open_fill_summary: FillSummary,
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

#[derive(Debug, Clone, Default, Serialize)]
pub struct FillSummary {
    pub fills: usize,
    pub quantity: f64,
    pub quote_quantity: f64,
    pub avg_price: Option<f64>,
    pub fee_usdt: Option<f64>,
    pub realized_pnl_usdt: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ActionAckSummary {
    pub accepted: bool,
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub status: String,
    pub message: Option<String>,
}

impl From<OrderAck> for ActionAckSummary {
    fn from(ack: OrderAck) -> Self {
        Self {
            accepted: ack.accepted,
            client_order_id: ack.client_order_id,
            exchange_order_id: ack.exchange_order_id,
            status: format!("{:?}", ack.status),
            message: ack.message,
        }
    }
}

impl From<crate::execution::ClosePositionAck> for ActionAckSummary {
    fn from(ack: crate::execution::ClosePositionAck) -> Self {
        Self {
            accepted: ack.accepted,
            client_order_id: ack.client_order_id,
            exchange_order_id: ack.exchange_order_id,
            status: format!("{:?}", ack.status),
            message: ack.message,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct FundingLiveRunOptions {
    pub request_timeout_ms: Option<u64>,
    pub notify: bool,
}

pub async fn run_funding_live_once(
    config: FundingRateArbitrageConfig,
    options: FundingLiveRunOptions,
) -> Result<Vec<FundingLiveExchangeResult>> {
    require_live_mode(&config)?;
    let adapters = configured_market_adapters(&config);
    let report = scan_funding_opportunities_with_timeout(
        &adapters,
        &config,
        Utc::now(),
        options.request_timeout_ms,
    )
    .await;
    let plan = build_live_plan(&config, &report);
    println!("{}", serde_json::to_string_pretty(&plan)?);

    if options.notify {
        send_markdown_notification(&config, build_live_plan_markdown(&plan))
            .await
            .context("send funding live plan notification")?;
    }

    if plan.entries.is_empty() {
        return Ok(Vec::new());
    }

    let tasks = plan
        .entries
        .clone()
        .into_iter()
        .map(|entry| {
            let config = config.clone();
            tokio::spawn(async move { execute_exchange_plan(config, entry).await })
        })
        .collect::<Vec<_>>();

    let mut results = Vec::with_capacity(tasks.len());
    for joined in join_all(tasks).await {
        match joined {
            Ok(Ok(result)) => results.push(result),
            Ok(Err(err)) => {
                log::error!("funding live exchange task failed: {err:#}");
            }
            Err(err) => {
                log::error!("funding live exchange task join failed: {err:#}");
            }
        }
    }

    println!("{}", serde_json::to_string_pretty(&results)?);
    if options.notify {
        send_markdown_notification(&config, build_live_result_markdown(&results))
            .await
            .context("send funding live result notification")?;
    }
    Ok(results)
}

pub async fn run_funding_live_scheduler(
    config: FundingRateArbitrageConfig,
    options: FundingLiveRunOptions,
) -> Result<()> {
    require_live_mode(&config)?;
    loop {
        let next_scan_at = next_scan_time(Utc::now(), config.execution.scan_minute)?;
        let wait = next_scan_at.signed_duration_since(Utc::now());
        log::info!(
            "funding_arb_live next scan at {}",
            next_scan_at.format("%Y-%m-%d %H:%M:%S UTC")
        );
        sleep_chrono(wait).await;
        if let Err(err) = run_funding_live_once(config.clone(), options).await {
            log::error!("funding live cycle failed: {err:#}");
            if options.notify {
                let _ = send_markdown_notification(
                    &config,
                    format!(
                        "### 资金费率套利实盘周期失败\n> 时间：{}\n> 错误：`{}`\n",
                        Utc::now().format("%Y-%m-%d %H:%M:%S UTC"),
                        err
                    ),
                )
                .await;
            }
        }
    }
}

pub fn require_live_mode(config: &FundingRateArbitrageConfig) -> Result<()> {
    if config.is_live_mode() {
        Ok(())
    } else {
        Err(anyhow!(
            "funding_arb_live places real orders; set mode: live and pass --confirm-live-order"
        ))
    }
}

pub fn build_live_plan(
    config: &FundingRateArbitrageConfig,
    report: &FundingScanReport,
) -> FundingLivePlan {
    let mut entries = Vec::new();
    let mut skipped = Vec::new();

    for selection in &report.selections {
        if exchange_disabled(config, &selection.exchange) {
            skipped.push(FundingLiveExchangeSkip {
                exchange: selection.exchange.clone(),
                reason: "exchange disabled in exchanges config".to_string(),
            });
            continue;
        }
        match plan_entry_from_selection(config, selection) {
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

pub fn build_live_plan_markdown(plan: &FundingLivePlan) -> String {
    let mut content = format!(
        "### 资金费率套利实盘计划\n\
         > 时间：{}\n\
         > 阈值：`{:.4}%`\n\
         > 单交易所名义金额：`{:.4} USDT`\n\n",
        plan.generated_at.format("%Y-%m-%d %H:%M:%S UTC"),
        plan.threshold_pct,
        plan.notional_usdt
    );

    if plan.entries.is_empty() {
        content.push_str("当前没有满足实盘条件的交易所候选。\n");
    } else {
        content.push_str("### 将按交易所独立执行\n\n");
        for entry in &plan.entries {
            content.push_str(&format!(
                "- `{}` `{}` 资金费率 <font color=\"warning\">{:+.4}%</font>，开仓 `{}`，平仓 `{}`，结算 `{}`\n",
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
        content.push_str("\n### 跳过\n\n");
        for skip in &plan.skipped {
            content.push_str(&format!("- `{}`：{}\n", skip.exchange, skip.reason));
        }
    }
    content
}

pub fn build_live_result_markdown(results: &[FundingLiveExchangeResult]) -> String {
    let mut content = format!(
        "### 资金费率套利实盘结果\n> 时间：{}\n\n",
        Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
    );
    if results.is_empty() {
        content.push_str("本周期没有提交订单。\n");
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
            .unwrap_or_else(|| "未知".to_string());
        let fill_pnl = result
            .close_fill_summary
            .realized_pnl_usdt
            .map(|value| format!("{:+.8}", value))
            .unwrap_or_else(|| "未知".to_string());
        let open_avg = result
            .open_fill_summary
            .avg_price
            .map(|value| format!("{:.8}", value))
            .unwrap_or_else(|| "-".to_string());
        let close_avg = result
            .close_fill_summary
            .avg_price
            .map(|value| format!("{:.8}", value))
            .unwrap_or_else(|| "-".to_string());
        let fee = match (
            result.open_fill_summary.fee_usdt,
            result.close_fill_summary.fee_usdt,
        ) {
            (Some(open_fee), Some(close_fee)) => format!("{:.8}", open_fee + close_fee),
            _ => "未知".to_string(),
        };

        content.push_str(&format!(
            "### `{}` `{}`\n\
             - 状态：{}\n\
             - 资金费率：`{:+.4}%`\n\
             - 数量：`{}`，开仓均价：`{}`，平仓均价：`{}`\n\
             - 本交易所余额差 USDT：<font color=\"comment\">{}</font>\n\
             - 成交回报 realized_pnl USDT：`{}`，手续费 USDT：`{}`\n",
            result.exchange,
            result.canonical_symbol,
            status,
            result.funding_rate_pct,
            option_f64(result.planned_quantity),
            open_avg,
            close_avg,
            balance_delta,
            fill_pnl,
            fee,
        ));
        if let Some(error) = &result.error {
            content.push_str(&format!("- 错误：`{}`\n", error));
        }
        content.push('\n');
    }
    content
}

async fn execute_exchange_plan(
    config: FundingRateArbitrageConfig,
    entry: FundingLiveExchangePlan,
) -> Result<FundingLiveExchangeResult> {
    let mut result = FundingLiveExchangeResult::from_plan(&config, &entry);
    let exchange = entry.exchange.clone();
    let market = market_adapter(&exchange).ok_or_else(|| anyhow!("{exchange} market adapter"))?;
    let instruments = market
        .load_instruments()
        .await
        .with_context(|| format!("{exchange} load instruments"))?;
    let instrument = find_instrument(&instruments, &entry)
        .with_context(|| format!("{exchange} find instrument {}", entry.canonical_symbol))?;
    let adapter = build_trading_adapter_for_exchange_with_instruments(
        &config,
        &exchange,
        [instrument.clone()],
    )
    .with_context(|| format!("{exchange} build trading adapter"))?;
    let position_mode = configured_position_mode(&config, &exchange);
    let position_side = execution_position_side(position_mode, adapter.capabilities());

    if let Err(err) = preflight_account(&adapter, &instrument, position_side, &config).await {
        result.fail(err);
        return Ok(result);
    }

    result.balance_before_total_usdt = read_usdt_balance_total(adapter.as_ref()).await;

    let now = Utc::now();
    if now >= entry.next_funding_time {
        result.fail(anyhow!(
            "settlement time already passed: {}",
            entry.next_funding_time
        ));
        return Ok(result);
    }
    sleep_until_utc(entry.open_at).await;
    if Utc::now() >= entry.next_funding_time {
        result.fail(anyhow!("missed open window before settlement"));
        return Ok(result);
    }

    let reference_price = best_ask_price(market.as_ref(), &instrument)
        .await
        .or(entry.mark_price)
        .ok_or_else(|| anyhow!("{exchange} has no usable reference price"))?;
    let quantity = plan_quantity(config.execution.notional_usdt, reference_price, &instrument)?;
    result.planned_quantity = Some(quantity);

    let suffix = Utc::now().timestamp_millis();
    let open_ack = adapter
        .place_order(open_order_command(
            &config,
            &instrument,
            position_side,
            quantity,
            suffix,
        ))
        .await
        .with_context(|| format!("{exchange} submit open order"))?;
    let open_accepted = open_ack.accepted;
    let open_client_id = open_ack.client_order_id.clone();
    let open_exchange_id = open_ack.exchange_order_id.clone();
    result.open_ack = Some(open_ack.into());
    result.status = FundingLiveStatus::OpenSubmitted;
    if !open_accepted {
        result.fail(anyhow!("open order was not accepted"));
        return Ok(result);
    }

    sleep(Duration::from_secs(
        config.execution.order_readback_delay_secs,
    ))
    .await;
    let open_fills = query_fills(
        adapter.as_ref(),
        &exchange,
        &instrument.exchange_symbol,
        Some(open_client_id),
        open_exchange_id,
    )
    .await;
    result.open_fill_summary = summarize_fills(&open_fills);

    sleep_until_utc(entry.close_at).await;
    let close_quantity = close_quantity(
        adapter.as_ref(),
        &instrument,
        position_side,
        result.open_fill_summary.quantity,
        quantity,
    )
    .await;
    let mut close = ClosePositionCommand::market(
        exchange.clone(),
        instrument.canonical_symbol.clone(),
        instrument.exchange_symbol.clone(),
        position_side,
        close_quantity,
        format!("{}-funding-close-{suffix}", exchange.as_str()),
        Utc::now(),
    );
    close.max_slippage_pct = Some(config.execution.max_slippage_pct);
    let close_ack = adapter
        .close_position(close)
        .await
        .with_context(|| format!("{exchange} submit close order"))?;
    let close_accepted = close_ack.accepted;
    let close_client_id = close_ack.client_order_id.clone();
    let close_exchange_id = close_ack.exchange_order_id.clone();
    result.close_ack = Some(close_ack.into());
    result.status = FundingLiveStatus::CloseSubmitted;
    if !close_accepted {
        result.fail(anyhow!("close order was not accepted"));
        return Ok(result);
    }

    sleep(Duration::from_secs(
        config.execution.order_readback_delay_secs,
    ))
    .await;
    let close_fills = query_fills(
        adapter.as_ref(),
        &exchange,
        &instrument.exchange_symbol,
        Some(close_client_id),
        close_exchange_id,
    )
    .await;
    result.close_fill_summary = summarize_fills(&close_fills);
    result.balance_after_total_usdt = read_usdt_balance_total(adapter.as_ref()).await;
    result.balance_delta_usdt = match (
        result.balance_before_total_usdt,
        result.balance_after_total_usdt,
    ) {
        (Some(before), Some(after)) => Some(after - before),
        _ => None,
    };
    result.status = FundingLiveStatus::Closed;
    Ok(result)
}

impl FundingLiveExchangeResult {
    fn from_plan(config: &FundingRateArbitrageConfig, entry: &FundingLiveExchangePlan) -> Self {
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
            open_fill_summary: FillSummary::default(),
            close_fill_summary: FillSummary::default(),
        }
    }

    fn fail(&mut self, err: anyhow::Error) {
        self.status = FundingLiveStatus::Failed;
        self.error = Some(format!("{err:#}"));
    }
}

fn configured_market_adapters(
    config: &FundingRateArbitrageConfig,
) -> Vec<Box<dyn MarketDataAdapter + Send + Sync>> {
    config
        .universe
        .enabled_exchanges
        .iter()
        .filter(|exchange| !exchange_disabled(config, exchange))
        .filter_map(|exchange| {
            let adapter = market_adapter(exchange);
            if adapter.is_none() {
                log::warn!("{} market adapter is not registered", exchange);
            }
            adapter
        })
        .collect()
}

fn exchange_disabled(config: &FundingRateArbitrageConfig, exchange: &ExchangeId) -> bool {
    config
        .exchanges
        .get(exchange)
        .is_some_and(|entry| entry.enabled == Some(false))
}

fn plan_entry_from_selection(
    config: &FundingRateArbitrageConfig,
    selection: &ExchangeFundingSelection,
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
    if close_at <= Utc::now() {
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

fn find_instrument(
    instruments: &[InstrumentMeta],
    entry: &FundingLiveExchangePlan,
) -> Result<InstrumentMeta> {
    instruments
        .iter()
        .find(|instrument| {
            instrument.is_tradeable_usdt_perpetual()
                && instrument.canonical_symbol == entry.canonical_symbol
                && (instrument.exchange_symbol.symbol == entry.exchange_symbol
                    || entry.exchange_symbol == entry.canonical_symbol.as_pair())
        })
        .or_else(|| {
            instruments.iter().find(|instrument| {
                instrument.is_tradeable_usdt_perpetual()
                    && instrument.canonical_symbol == entry.canonical_symbol
            })
        })
        .cloned()
        .ok_or_else(|| anyhow!("tradeable USDT perpetual instrument not found"))
}

async fn preflight_account(
    adapter: &Arc<dyn TradingAdapter>,
    instrument: &InstrumentMeta,
    position_side: PositionSide,
    config: &FundingRateArbitrageConfig,
) -> Result<()> {
    let capabilities = adapter.capabilities();
    if !capabilities.supports_market_orders {
        bail!(
            "{} adapter does not support market orders",
            adapter.exchange()
        );
    }
    if !capabilities.supports_close_position {
        bail!(
            "{} adapter does not support close_position",
            adapter.exchange()
        );
    }
    if position_side == PositionSide::Long && !capabilities.supports_hedge_mode {
        bail!(
            "{} adapter does not report hedge-mode support",
            adapter.exchange()
        );
    }

    let open_orders = adapter
        .get_open_orders(Some(&instrument.exchange_symbol))
        .await
        .with_context(|| format!("{} read open orders", adapter.exchange()))?;
    if open_orders.iter().any(|order| order.is_open()) {
        bail!(
            "{} has open orders on {}",
            adapter.exchange(),
            instrument.canonical_symbol
        );
    }

    let positions = adapter
        .get_positions(Some(&instrument.exchange_symbol))
        .await
        .with_context(|| format!("{} read positions", adapter.exchange()))?;
    if !config.execution.allow_existing_symbol_position
        && has_nonzero_position(&positions, &instrument.canonical_symbol)
    {
        bail!(
            "{} has existing nonzero position on {}",
            adapter.exchange(),
            instrument.canonical_symbol
        );
    }
    Ok(())
}

async fn read_usdt_balance_total(adapter: &dyn TradingAdapter) -> Option<f64> {
    match adapter.get_balances().await {
        Ok(balances) => usdt_balance_total(&balances),
        Err(err) => {
            log::warn!("{} read balances failed: {err:#}", adapter.exchange());
            None
        }
    }
}

fn usdt_balance_total(balances: &[ExchangeBalance]) -> Option<f64> {
    balances
        .iter()
        .find(|balance| balance.asset.eq_ignore_ascii_case("USDT"))
        .map(|balance| balance.total)
}

async fn best_ask_price(
    market: &(dyn MarketDataAdapter + Send + Sync),
    instrument: &InstrumentMeta,
) -> Option<f64> {
    match market
        .fetch_orderbook_snapshot(&instrument.exchange_symbol, 5)
        .await
    {
        Ok(book) if book.is_usable() => book.best_ask().map(|level| level.price),
        Ok(book) => {
            log::warn!(
                "{} {} orderbook not usable: {:?}",
                market.exchange(),
                instrument.canonical_symbol,
                book.quality
            );
            None
        }
        Err(err) => {
            log::warn!(
                "{} {} orderbook fetch failed: {err:#}",
                market.exchange(),
                instrument.canonical_symbol
            );
            None
        }
    }
}

fn open_order_command(
    config: &FundingRateArbitrageConfig,
    instrument: &InstrumentMeta,
    position_side: PositionSide,
    quantity: f64,
    suffix: i64,
) -> OrderCommand {
    OrderCommand {
        command_id: format!("cmd-{}-funding-open-{suffix}", instrument.exchange.as_str()),
        bundle_id: format!(
            "{}-funding-{}-{suffix}",
            instrument.exchange.as_str(),
            instrument.canonical_symbol.as_pair().replace('/', "")
        ),
        exchange: instrument.exchange.clone(),
        canonical_symbol: instrument.canonical_symbol.clone(),
        exchange_symbol: instrument.exchange_symbol.clone(),
        intent: OrderIntent::HedgeLongTaker,
        side: OrderSide::Buy,
        position_side,
        order_type: OrderType::Market,
        quantity,
        price: None,
        time_in_force: TimeInForce::Ioc,
        post_only: false,
        reduce_only: false,
        client_order_id: format!("{}-funding-open-{suffix}", instrument.exchange.as_str()),
        max_slippage_pct: Some(config.execution.max_slippage_pct),
        status: OrderCommandStatus::Planned,
        created_at: Utc::now(),
    }
}

async fn query_fills(
    adapter: &dyn TradingAdapter,
    exchange: &ExchangeId,
    exchange_symbol: &ExchangeSymbol,
    client_order_id: Option<String>,
    exchange_order_id: Option<String>,
) -> Vec<FillEvent> {
    let mut query = FillQuery::for_order(
        exchange.clone(),
        exchange_symbol.clone(),
        client_order_id,
        exchange_order_id,
    );
    query.limit = Some(50);
    match adapter.get_fills(query).await {
        Ok(fills) => fills,
        Err(err) => {
            log::warn!("{} fill query failed: {err:#}", adapter.exchange());
            Vec::new()
        }
    }
}

fn summarize_fills(fills: &[FillEvent]) -> FillSummary {
    let quantity = fills.iter().map(|fill| fill.quantity).sum::<f64>();
    let quote_quantity = fills.iter().map(FillEvent::notional).sum::<f64>();
    let avg_price = (quantity > 0.0).then_some(quote_quantity / quantity);
    let fee_usdt = sum_usdt_fees(fills);
    let realized_pnl_usdt = sum_optional_values(fills.iter().map(|fill| fill.realized_pnl));

    FillSummary {
        fills: fills.len(),
        quantity,
        quote_quantity,
        avg_price,
        fee_usdt,
        realized_pnl_usdt,
    }
}

fn sum_usdt_fees(fills: &[FillEvent]) -> Option<f64> {
    let mut seen = false;
    let mut total = 0.0;
    for fill in fills {
        let Some(fee) = fill.fee else {
            continue;
        };
        let is_usdt_fee = fill
            .fee_asset
            .as_deref()
            .map(|asset| asset.eq_ignore_ascii_case("USDT"))
            .unwrap_or(true);
        if !is_usdt_fee {
            return None;
        }
        seen = true;
        total += fee;
    }
    seen.then_some(total)
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

async fn close_quantity(
    adapter: &dyn TradingAdapter,
    instrument: &InstrumentMeta,
    position_side: PositionSide,
    fill_quantity: f64,
    planned_quantity: f64,
) -> f64 {
    match adapter
        .get_positions(Some(&instrument.exchange_symbol))
        .await
    {
        Ok(positions) => positions
            .iter()
            .find(|position| position.position_side == position_side)
            .map(|position| position.quantity.abs())
            .filter(|quantity| *quantity > 0.0)
            .unwrap_or_else(|| {
                if fill_quantity > 0.0 {
                    fill_quantity
                } else {
                    planned_quantity
                }
            }),
        Err(err) => {
            log::warn!("{} position readback failed: {err:#}", adapter.exchange());
            if fill_quantity > 0.0 {
                fill_quantity
            } else {
                planned_quantity
            }
        }
    }
}

fn has_nonzero_position(positions: &[ExchangePosition], symbol: &CanonicalSymbol) -> bool {
    positions
        .iter()
        .any(|position| position.canonical_symbol == *symbol && position.quantity.abs() > 1e-9)
}

fn execution_position_side(
    position_mode: PositionMode,
    capabilities: TradingCapabilities,
) -> PositionSide {
    if position_mode.is_hedge() && capabilities.supports_hedge_mode {
        PositionSide::Long
    } else {
        PositionSide::Net
    }
}

fn plan_quantity(notional: f64, price: f64, instrument: &InstrumentMeta) -> Result<f64> {
    if !price.is_finite() || price <= 0.0 {
        bail!("invalid reference price {price}");
    }
    let contract_size = if instrument.contract_size.is_finite() && instrument.contract_size > 0.0 {
        instrument.contract_size
    } else {
        1.0
    };
    let quantity_step = if instrument.quantity_step.is_finite() && instrument.quantity_step > 0.0 {
        instrument.quantity_step
    } else {
        1.0
    };
    let min_quantity_for_notional = if instrument.min_notional > 0.0 {
        instrument.min_notional / (price * contract_size)
    } else {
        0.0
    };
    let raw_quantity = (notional / (price * contract_size))
        .max(instrument.min_qty)
        .max(min_quantity_for_notional);
    let steps = (raw_quantity / quantity_step).ceil().max(1.0);
    let quantity = normalize_number(steps * quantity_step);
    if quantity <= 0.0 || !quantity.is_finite() {
        bail!("planned quantity is invalid: {quantity}");
    }
    Ok(quantity)
}

fn normalize_number(value: f64) -> f64 {
    format!("{value:.12}")
        .trim_end_matches('0')
        .trim_end_matches('.')
        .parse()
        .unwrap_or(value)
}

async fn sleep_until_utc(target: DateTime<Utc>) {
    let wait = target.signed_duration_since(Utc::now());
    sleep_chrono(wait).await;
}

async fn sleep_chrono(wait: ChronoDuration) {
    if wait.num_milliseconds() <= 0 {
        return;
    }
    let wait = wait
        .to_std()
        .unwrap_or_else(|_| std::time::Duration::from_secs(0));
    sleep_until(Instant::now() + wait).await;
}

fn next_scan_time(now: DateTime<Utc>, scan_minute: u32) -> Result<DateTime<Utc>> {
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

fn option_f64(value: Option<f64>) -> String {
    value
        .map(|value| format!("{:.12}", value))
        .unwrap_or_else(|| "-".to_string())
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use crate::market::CanonicalSymbol;

    use super::*;

    #[test]
    fn live_result_markdown_should_report_each_exchange_balance_delta() {
        let result = FundingLiveExchangeResult {
            exchange: ExchangeId::Binance,
            canonical_symbol: CanonicalSymbol::new("BTC", "USDT"),
            exchange_symbol: "BTCUSDT".to_string(),
            funding_rate_pct: -0.6,
            next_funding_time: Utc::now(),
            notional_usdt: 10.0,
            planned_quantity: Some(0.001),
            open_at: Utc::now(),
            close_at: Utc::now(),
            status: FundingLiveStatus::Closed,
            error: None,
            balance_before_total_usdt: Some(100.0),
            balance_after_total_usdt: Some(100.12),
            balance_delta_usdt: Some(0.12),
            open_ack: None,
            close_ack: None,
            open_fill_summary: FillSummary {
                avg_price: Some(100_000.0),
                fee_usdt: Some(0.01),
                ..FillSummary::default()
            },
            close_fill_summary: FillSummary {
                avg_price: Some(100_001.0),
                fee_usdt: Some(0.01),
                realized_pnl_usdt: Some(0.001),
                ..FillSummary::default()
            },
        };

        let markdown = build_live_result_markdown(&[result]);

        assert!(markdown.contains("`binance` `BTC/USDT`"));
        assert!(markdown.contains("+0.12000000"));
        assert!(markdown.contains("本交易所余额差 USDT"));
    }

    #[test]
    fn next_scan_time_should_roll_to_next_hour_when_minute_passed() {
        let now = Utc.with_ymd_and_hms(2026, 5, 30, 11, 56, 0).unwrap();
        let next = next_scan_time(now, 55).unwrap();

        assert_eq!(next, Utc.with_ymd_and_hms(2026, 5, 30, 12, 55, 0).unwrap());
    }

    #[test]
    fn execution_position_side_should_fallback_to_net_when_hedge_is_not_supported() {
        let mut capabilities = TradingCapabilities::default();
        capabilities.supports_hedge_mode = false;

        assert_eq!(
            execution_position_side(PositionMode::Hedge, capabilities),
            PositionSide::Net
        );
    }

    #[test]
    fn execution_position_side_should_use_long_when_hedge_is_supported() {
        let mut capabilities = TradingCapabilities::default();
        capabilities.supports_hedge_mode = true;

        assert_eq!(
            execution_position_side(PositionMode::Hedge, capabilities),
            PositionSide::Long
        );
    }

    #[test]
    fn has_nonzero_position_should_only_match_current_symbol() {
        let positions = vec![ExchangePosition {
            exchange: ExchangeId::Bitget,
            canonical_symbol: CanonicalSymbol::new("NEWT", "USDT"),
            exchange_symbol: ExchangeSymbol::new(ExchangeId::Bitget, "NEWTUSDT"),
            position_side: PositionSide::Short,
            quantity: 74.0,
            entry_price: Some(0.07996),
            mark_price: Some(0.06953),
            unrealized_pnl: Some(0.77182),
            updated_at: Utc::now(),
        }];

        assert!(!has_nonzero_position(
            &positions,
            &CanonicalSymbol::new("VTHO", "USDT")
        ));
        assert!(has_nonzero_position(
            &positions,
            &CanonicalSymbol::new("NEWT", "USDT")
        ));
    }
}
