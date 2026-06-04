use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use clap::{Parser, ValueEnum};
use rustcta::exchanges::registry::market_adapter;
use rustcta::execution::{
    CancelCommand, ClosePositionCommand, OrderCommand, OrderCommandStatus, OrderIntent, OrderSide,
    OrderType, PositionMode, PositionSide, TimeInForce, TradingAdapter,
};
use rustcta::market::{CanonicalSymbol, ExchangeId, InstrumentMeta, RoundingMode};
use rustcta::strategies::cross_exchange_arbitrage::{
    build_trading_adapter_for_exchange_with_instruments, configured_position_mode,
    CrossExchangeArbitrageConfig,
};
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(
    name = "exchange_order_canary",
    version,
    about = "Safely test a private USDT perpetual trading path with a tiny open+close round trip"
)]
struct Args {
    #[arg(
        long,
        default_value = "config/cross_exchange_arbitrage_three_venues_50u.live-small.yml"
    )]
    config: PathBuf,
    #[arg(long, default_value = "bitget")]
    exchange: String,
    #[arg(long, default_value = "DOGE/USDT")]
    symbol: String,
    #[arg(long, value_enum, default_value_t = CanarySide::Long)]
    side: CanarySide,
    #[arg(long, value_enum, default_value_t = CanaryMode::MarketRoundTrip)]
    mode: CanaryMode,
    #[arg(long, default_value_t = 10.0)]
    notional_usdt: f64,
    #[arg(long, default_value_t = 12.0)]
    max_estimated_notional_usdt: f64,
    #[arg(long, default_value_t = 0.001)]
    max_slippage_pct: f64,
    #[arg(long, default_value_t = false)]
    execute: bool,
    #[arg(long, default_value_t = false)]
    confirm_live_order: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CanarySide {
    Long,
    Short,
}

impl CanarySide {
    fn order_side(self) -> OrderSide {
        match self {
            Self::Long => OrderSide::Buy,
            Self::Short => OrderSide::Sell,
        }
    }

    fn position_side_for(self, mode: PositionMode) -> PositionSide {
        match (self, mode) {
            (_, PositionMode::OneWay) => PositionSide::Net,
            (Self::Long, PositionMode::Hedge) => PositionSide::Long,
            (Self::Short, PositionMode::Hedge) => PositionSide::Short,
        }
    }

    fn open_intent(self) -> OrderIntent {
        match self {
            Self::Long => OrderIntent::HedgeLongTaker,
            Self::Short => OrderIntent::HedgeShortTaker,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CanaryMode {
    MarketRoundTrip,
    LimitPostOnly,
}

#[derive(Debug, Serialize)]
struct CanaryReport {
    generated_at: chrono::DateTime<Utc>,
    execute_requested: bool,
    live_order_confirmed: bool,
    exchange: String,
    symbol: String,
    side: String,
    mode: String,
    requested_notional_usdt: f64,
    planned_quantity: f64,
    estimated_notional_usdt: f64,
    max_estimated_notional_usdt: f64,
    best_bid: f64,
    best_ask: f64,
    planned_limit_price: Option<f64>,
    instrument: InstrumentSummary,
    account: AccountSummary,
    balances_before: usize,
    open_orders_before: usize,
    positions_before: usize,
    nonzero_positions_before: usize,
    net_position_qty_before: f64,
    fills_before: usize,
    open_ack: Option<ActionAckSummary>,
    close_ack: Option<ActionAckSummary>,
    cancel_ack: Option<CancelAckSummary>,
    positions_after: Option<usize>,
    nonzero_positions_after: Option<usize>,
    net_position_qty_after: Option<f64>,
    open_orders_after: Option<usize>,
    warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
struct InstrumentSummary {
    exchange_symbol: String,
    contract_size: f64,
    quantity_step: f64,
    min_qty: f64,
    min_notional: f64,
    price_tick: f64,
}

#[derive(Debug, Serialize)]
struct AccountSummary {
    position_mode: Option<String>,
    margin_mode: Option<String>,
    leverage: Option<u32>,
    max_leverage: Option<u32>,
}

#[derive(Debug, Serialize)]
struct ActionAckSummary {
    accepted: bool,
    client_order_id: String,
    exchange_order_id: Option<String>,
    status: String,
    message: Option<String>,
}

#[derive(Debug, Serialize)]
struct CancelAckSummary {
    accepted: bool,
    client_order_id: Option<String>,
    exchange_order_id: Option<String>,
    status: String,
    message: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let args = Args::parse();
    let config = load_config(&args.config)?;
    config.validate().context("invalid canary config")?;

    if args.execute && !args.confirm_live_order {
        bail!("--execute requires --confirm-live-order");
    }
    if args.execute && config.execution.dry_run {
        bail!("config execution.dry_run=true; set it false only for an intentional canary order");
    }
    if !args.execute && !config.execution.dry_run {
        bail!("config execution.dry_run=false requires --execute --confirm-live-order");
    }
    if !args.notional_usdt.is_finite() || args.notional_usdt <= 0.0 || args.notional_usdt > 10.0 {
        bail!("canary notional must be > 0 and <= 10 USDT");
    }
    if !args.max_estimated_notional_usdt.is_finite()
        || args.max_estimated_notional_usdt <= 0.0
        || args.max_estimated_notional_usdt > 20.0
    {
        bail!("max estimated canary notional must be > 0 and <= 20 USDT");
    }

    let exchange = parse_exchange(&args.exchange)?;
    if !config.universe.enabled_exchanges.contains(&exchange) {
        bail!("{} is not enabled in config universe", exchange.as_str());
    }
    let canonical = CanonicalSymbol::parse(&args.symbol)
        .ok_or_else(|| anyhow!("invalid --symbol {}; expected BASE/USDT", args.symbol))?;
    if canonical.quote() != "USDT" {
        bail!("canary only supports USDT quote symbols");
    }

    let market = market_adapter(&exchange)
        .ok_or_else(|| anyhow!("{} market adapter is not registered", exchange.as_str()))?;
    let instruments = market
        .load_instruments()
        .await
        .with_context(|| format!("load {} instruments", exchange.as_str()))?;
    let instrument = instruments
        .iter()
        .find(|item| item.canonical_symbol == canonical && item.exchange == exchange)
        .cloned()
        .ok_or_else(|| anyhow!("{} does not list {}", exchange.as_str(), canonical))?;
    if !instrument.is_tradeable_usdt_perpetual() {
        bail!(
            "{} is not a tradeable {} USDT perpetual",
            canonical,
            exchange.as_str()
        );
    }

    let book = market
        .fetch_orderbook_snapshot(&instrument.exchange_symbol, 5)
        .await
        .with_context(|| format!("fetch {} orderbook", exchange.as_str()))?;
    let best_bid = book
        .best_bid()
        .ok_or_else(|| anyhow!("{} orderbook has no bids", exchange.as_str()))?
        .price;
    let best_ask = book
        .best_ask()
        .ok_or_else(|| anyhow!("{} orderbook has no asks", exchange.as_str()))?
        .price;
    let reference_price = match args.side {
        CanarySide::Long => best_ask,
        CanarySide::Short => best_bid,
    };
    let planned_quantity = plan_quantity(args.notional_usdt, reference_price, &instrument)?;
    let estimated_notional = planned_quantity * reference_price * contract_size(&instrument);
    let planned_limit_price = match args.mode {
        CanaryMode::MarketRoundTrip => None,
        CanaryMode::LimitPostOnly => Some(plan_post_only_limit_price(
            args.side,
            best_bid,
            best_ask,
            &instrument,
        )?),
    };

    let position_mode = configured_position_mode(&config, &exchange);
    let adapter = build_adapter(&config, &exchange, [instrument.clone()])?;

    let balances_before = adapter.get_balances().await.context("read balances")?;
    let open_orders_before = adapter
        .get_open_orders(Some(&instrument.exchange_symbol))
        .await
        .context("read open orders before canary")?;
    if !open_orders_before.is_empty() {
        bail!(
            "refusing canary because {} open {} orders already exist for {}",
            open_orders_before.len(),
            exchange.as_str(),
            canonical
        );
    }
    let positions_before = adapter
        .get_positions(Some(&instrument.exchange_symbol))
        .await
        .context("read positions before canary")?;
    let nonzero_positions_before = nonzero_position_count(&positions_before, Some(&canonical));
    let net_position_qty_before = net_position_qty(&positions_before, Some(&canonical));
    if nonzero_positions_before > 0 {
        bail!(
            "refusing canary because {} nonzero {} positions already exist for {} (net_qty={})",
            nonzero_positions_before,
            exchange.as_str(),
            canonical,
            net_position_qty_before
        );
    }
    let account = adapter
        .get_symbol_account_config(&instrument.exchange_symbol)
        .await
        .context("read symbol account config")?;
    let mut fill_query = rustcta::execution::FillQuery::for_symbol(
        exchange.clone(),
        canonical.clone(),
        instrument.exchange_symbol.clone(),
    );
    fill_query.limit = Some(5);
    let fills_before = adapter
        .get_fills(fill_query)
        .await
        .context("read recent fills before canary")?;

    let mut warnings = Vec::new();
    if account.position_mode != Some(position_mode) {
        warnings.push(format!(
            "readback position_mode={:?}; configured {:?}",
            account.position_mode, position_mode
        ));
    }
    if account.leverage.is_none() {
        warnings.push(format!(
            "{} leverage readback is unavailable; verify venue UI before real execution",
            exchange.as_str()
        ));
    } else if account.leverage != Some(config.sizing.leverage.round().max(1.0) as u32) {
        warnings.push(format!(
            "readback leverage={:?}; expected {}",
            account.leverage,
            config.sizing.leverage.round().max(1.0) as u32
        ));
    }
    if estimated_notional > args.notional_usdt {
        warnings.push(format!(
            "estimated notional {estimated_notional:.4} exceeds requested {} USDT because of venue minimums/steps",
            args.notional_usdt
        ));
    }
    if estimated_notional > args.max_estimated_notional_usdt {
        bail!(
            "planned canary notional {estimated_notional:.4} exceeds safety cap {}; choose a lower-priced liquid symbol or raise --max-estimated-notional-usdt intentionally",
            args.max_estimated_notional_usdt
        );
    }

    let mut open_ack = None;
    let mut close_ack = None;
    let mut cancel_ack = None;
    let mut positions_after = None;
    let mut nonzero_positions_after = None;
    let mut net_position_qty_after = None;
    let mut open_orders_after = None;

    if args.execute {
        let suffix = Utc::now().timestamp_millis();
        let position_side = args.side.position_side_for(position_mode);
        match args.mode {
            CanaryMode::MarketRoundTrip => {
                let open = OrderCommand {
                    command_id: format!("cmd-{}-canary-open-{suffix}", exchange.as_str()),
                    bundle_id: format!("{}-canary-{suffix}", exchange.as_str()),
                    exchange: exchange.clone(),
                    canonical_symbol: canonical.clone(),
                    exchange_symbol: instrument.exchange_symbol.clone(),
                    intent: args.side.open_intent(),
                    side: args.side.order_side(),
                    position_side,
                    order_type: OrderType::Market,
                    quantity: planned_quantity,
                    price: None,
                    time_in_force: TimeInForce::Ioc,
                    post_only: false,
                    reduce_only: false,
                    client_order_id: format!("{}-canary-open-{suffix}", exchange.as_str()),
                    max_slippage_pct: Some(args.max_slippage_pct),
                    status: OrderCommandStatus::Planned,
                    created_at: Utc::now(),
                };
                let ack = adapter
                    .place_order(open)
                    .await
                    .context("place canary order")?;
                open_ack = Some(ActionAckSummary {
                    accepted: ack.accepted,
                    client_order_id: ack.client_order_id.clone(),
                    exchange_order_id: ack.exchange_order_id.clone(),
                    status: format!("{:?}", ack.status),
                    message: ack.message.clone(),
                });

                let close = ClosePositionCommand::market(
                    exchange.clone(),
                    canonical.clone(),
                    instrument.exchange_symbol.clone(),
                    position_side,
                    planned_quantity,
                    format!("{}-canary-close-{suffix}", exchange.as_str()),
                    Utc::now(),
                );
                let ack = adapter
                    .close_position(close)
                    .await
                    .context("close canary position")?;
                close_ack = Some(ActionAckSummary {
                    accepted: ack.accepted,
                    client_order_id: ack.client_order_id,
                    exchange_order_id: ack.exchange_order_id,
                    status: format!("{:?}", ack.status),
                    message: ack.message,
                });

                let positions = adapter
                    .get_positions(Some(&instrument.exchange_symbol))
                    .await
                    .context("read positions after canary")?;
                let orders = adapter
                    .get_open_orders(Some(&instrument.exchange_symbol))
                    .await
                    .context("read open orders after canary")?;
                positions_after = Some(positions.len());
                nonzero_positions_after =
                    Some(nonzero_position_count(&positions, Some(&canonical)));
                net_position_qty_after = Some(net_position_qty(&positions, Some(&canonical)));
                open_orders_after = Some(orders.len());
            }
            CanaryMode::LimitPostOnly => {
                let limit_price =
                    planned_limit_price.ok_or_else(|| anyhow!("planned limit price is missing"))?;
                let open_client_order_id =
                    format!("{}-canary-limit-open-{suffix}", exchange.as_str());
                let open = OrderCommand {
                    command_id: format!("cmd-{}-canary-limit-open-{suffix}", exchange.as_str()),
                    bundle_id: format!("{}-canary-limit-{suffix}", exchange.as_str()),
                    exchange: exchange.clone(),
                    canonical_symbol: canonical.clone(),
                    exchange_symbol: instrument.exchange_symbol.clone(),
                    intent: args.side.open_intent(),
                    side: args.side.order_side(),
                    position_side,
                    order_type: OrderType::Limit,
                    quantity: planned_quantity,
                    price: Some(limit_price),
                    time_in_force: TimeInForce::Gtc,
                    post_only: true,
                    reduce_only: false,
                    client_order_id: open_client_order_id.clone(),
                    max_slippage_pct: None,
                    status: OrderCommandStatus::Planned,
                    created_at: Utc::now(),
                };
                let ack = adapter
                    .place_order(open)
                    .await
                    .context("place post-only canary limit order")?;
                open_ack = Some(ActionAckSummary {
                    accepted: ack.accepted,
                    client_order_id: ack.client_order_id.clone(),
                    exchange_order_id: ack.exchange_order_id.clone(),
                    status: format!("{:?}", ack.status),
                    message: ack.message.clone(),
                });

                let cancel = CancelCommand {
                    exchange: exchange.clone(),
                    canonical_symbol: canonical.clone(),
                    exchange_symbol: instrument.exchange_symbol.clone(),
                    client_order_id: Some(open_client_order_id),
                    exchange_order_id: ack.exchange_order_id,
                    reason: Some("post-only canary cleanup".to_string()),
                    requested_at: Utc::now(),
                };
                let ack = adapter
                    .cancel_order(cancel)
                    .await
                    .context("cancel post-only canary limit order")?;
                cancel_ack = Some(CancelAckSummary {
                    accepted: ack.accepted,
                    client_order_id: ack.client_order_id,
                    exchange_order_id: ack.exchange_order_id,
                    status: format!("{:?}", ack.status),
                    message: ack.message,
                });

                let positions = adapter
                    .get_positions(Some(&instrument.exchange_symbol))
                    .await
                    .context("read positions after post-only canary")?;
                let orders = adapter
                    .get_open_orders(Some(&instrument.exchange_symbol))
                    .await
                    .context("read open orders after post-only canary")?;
                positions_after = Some(positions.len());
                nonzero_positions_after =
                    Some(nonzero_position_count(&positions, Some(&canonical)));
                net_position_qty_after = Some(net_position_qty(&positions, Some(&canonical)));
                open_orders_after = Some(orders.len());
            }
        }
    }

    let report = CanaryReport {
        generated_at: Utc::now(),
        execute_requested: args.execute,
        live_order_confirmed: args.confirm_live_order,
        exchange: exchange.as_str().to_string(),
        symbol: canonical.to_string(),
        side: format!("{:?}", args.side).to_ascii_lowercase(),
        mode: format!("{:?}", args.mode).to_ascii_lowercase(),
        requested_notional_usdt: args.notional_usdt,
        planned_quantity,
        estimated_notional_usdt: estimated_notional,
        max_estimated_notional_usdt: args.max_estimated_notional_usdt,
        best_bid,
        best_ask,
        planned_limit_price,
        instrument: InstrumentSummary {
            exchange_symbol: instrument.exchange_symbol.symbol.clone(),
            contract_size: contract_size(&instrument),
            quantity_step: instrument.quantity_step,
            min_qty: instrument.min_qty,
            min_notional: instrument.min_notional,
            price_tick: instrument.price_tick,
        },
        account: AccountSummary {
            position_mode: account.position_mode.map(|mode| format!("{mode:?}")),
            margin_mode: account.margin_mode.map(|mode| format!("{mode:?}")),
            leverage: account.leverage,
            max_leverage: account.max_leverage,
        },
        balances_before: balances_before.len(),
        open_orders_before: open_orders_before.len(),
        positions_before: positions_before.len(),
        nonzero_positions_before,
        net_position_qty_before,
        fills_before: fills_before.len(),
        open_ack,
        close_ack,
        cancel_ack,
        positions_after,
        nonzero_positions_after,
        net_position_qty_after,
        open_orders_after,
        warnings,
    };

    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn load_config(path: &PathBuf) -> Result<CrossExchangeArbitrageConfig> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config {}", path.display()))?;
    serde_yaml::from_str::<CrossExchangeArbitrageConfig>(&raw)
        .with_context(|| format!("failed to parse config {}", path.display()))
}

fn parse_exchange(value: &str) -> Result<ExchangeId> {
    let exchange = ExchangeId::from(value);
    match exchange {
        ExchangeId::Binance | ExchangeId::Bitget | ExchangeId::Gate => Ok(exchange),
        _ => bail!("canary currently supports binance, bitget, and gate; got {value}"),
    }
}

fn build_adapter(
    config: &CrossExchangeArbitrageConfig,
    exchange: &ExchangeId,
    instruments: impl IntoIterator<Item = InstrumentMeta>,
) -> Result<Arc<dyn TradingAdapter>> {
    build_trading_adapter_for_exchange_with_instruments(config, exchange, instruments)
}

fn plan_quantity(notional: f64, price: f64, instrument: &InstrumentMeta) -> Result<f64> {
    if !price.is_finite() || price <= 0.0 {
        bail!("invalid reference price {price}");
    }
    let contract_size = contract_size(instrument);
    let min_quantity_for_notional = if instrument.min_notional > 0.0 {
        instrument.min_notional / (price * contract_size)
    } else {
        0.0
    };
    let raw_quantity = (notional / (price * contract_size))
        .max(instrument.min_qty)
        .max(min_quantity_for_notional);
    let steps = (raw_quantity / instrument.quantity_step).ceil().max(1.0);
    let quantity = normalize_number(steps * instrument.quantity_step);
    if quantity <= 0.0 || !quantity.is_finite() {
        bail!("planned quantity is invalid: {quantity}");
    }
    Ok(quantity)
}

fn contract_size(instrument: &InstrumentMeta) -> f64 {
    if instrument.contract_size.is_finite() && instrument.contract_size > 0.0 {
        instrument.contract_size
    } else {
        1.0
    }
}

fn plan_post_only_limit_price(
    side: CanarySide,
    best_bid: f64,
    best_ask: f64,
    instrument: &InstrumentMeta,
) -> Result<f64> {
    if !best_bid.is_finite() || best_bid <= 0.0 || !best_ask.is_finite() || best_ask <= 0.0 {
        bail!("invalid orderbook for limit canary: bid={best_bid}, ask={best_ask}");
    }
    let raw = match side {
        CanarySide::Long => best_bid * 0.90,
        CanarySide::Short => best_ask * 1.10,
    };
    let price = match side {
        CanarySide::Long => instrument.quantize_price(raw, RoundingMode::Floor),
        CanarySide::Short => instrument.quantize_price(raw, RoundingMode::Ceil),
    };
    if !price.is_finite() || price <= 0.0 {
        bail!("planned post-only limit price is invalid: {price}");
    }
    Ok(price)
}

fn normalize_number(value: f64) -> f64 {
    format!("{value:.12}")
        .trim_end_matches('0')
        .trim_end_matches('.')
        .parse()
        .unwrap_or(value)
}

fn nonzero_position_count(
    positions: &[rustcta::execution::ExchangePosition],
    symbol: Option<&CanonicalSymbol>,
) -> usize {
    positions
        .iter()
        .filter(|position| {
            symbol.is_none_or(|symbol| position.canonical_symbol == *symbol)
                && position.quantity.abs() > 1e-9
        })
        .count()
}

fn net_position_qty(
    positions: &[rustcta::execution::ExchangePosition],
    symbol: Option<&CanonicalSymbol>,
) -> f64 {
    normalize_number(
        positions
            .iter()
            .filter(|position| symbol.is_none_or(|symbol| position.canonical_symbol == *symbol))
            .map(|position| match position.position_side {
                PositionSide::Long | PositionSide::Net => position.quantity,
                PositionSide::Short => -position.quantity,
            })
            .sum(),
    )
}
