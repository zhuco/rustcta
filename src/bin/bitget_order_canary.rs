#![allow(clippy::all)]
use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use clap::{Parser, ValueEnum};
use rustcta::core::config::ApiKeys;
use rustcta::exchanges::market_adapters::BitgetMarketAdapter;
use rustcta::exchanges::private_perp::{PrivatePerpExchange, PrivateRestAuth};
use rustcta::exchanges::trading_adapters::private_perp_trading_adapter_for_with_instruments;
use rustcta::execution::{
    ClosePositionCommand, OrderCommand, OrderCommandStatus, OrderIntent, OrderSide, OrderType,
    PositionMode, PositionSide, TimeInForce,
};
use rustcta::market::{CanonicalSymbol, ExchangeId, InstrumentMeta, MarketDataAdapter};
use rustcta::strategies::cross_exchange_arbitrage::{
    configured_position_mode, CrossExchangeArbitrageConfig,
};
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(
    name = "bitget_order_canary",
    version,
    about = "Safely test the Bitget USDT perpetual private trading path with a tiny round-trip order"
)]
struct Args {
    #[arg(
        long,
        default_value = "config/cross_exchange_arbitrage_bitget_50u.live-small.yml"
    )]
    config: PathBuf,
    #[arg(long, default_value = "BTC/USDT")]
    symbol: String,
    #[arg(long, value_enum, default_value_t = CanarySide::Long)]
    side: CanarySide,
    #[arg(long, default_value_t = 10.0)]
    notional_usdt: f64,
    #[arg(long, default_value_t = 11.0)]
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

    fn position_side(self) -> PositionSide {
        match self {
            Self::Long => PositionSide::Long,
            Self::Short => PositionSide::Short,
        }
    }

    fn open_intent(self) -> OrderIntent {
        match self {
            Self::Long => OrderIntent::HedgeLongTaker,
            Self::Short => OrderIntent::HedgeShortTaker,
        }
    }
}

#[derive(Debug, Serialize)]
struct CanaryReport {
    generated_at: chrono::DateTime<Utc>,
    execute_requested: bool,
    live_order_confirmed: bool,
    exchange: String,
    symbol: String,
    side: String,
    requested_notional_usdt: f64,
    planned_quantity: f64,
    estimated_notional_usdt: f64,
    max_estimated_notional_usdt: f64,
    best_bid: f64,
    best_ask: f64,
    instrument: InstrumentSummary,
    account: AccountSummary,
    balances_before: usize,
    open_orders_before: usize,
    positions_before: usize,
    open_ack: Option<ActionAckSummary>,
    close_ack: Option<ActionAckSummary>,
    positions_after: Option<usize>,
    open_orders_after: Option<usize>,
    warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
struct InstrumentSummary {
    exchange_symbol: String,
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

    let canonical = CanonicalSymbol::parse(&args.symbol)
        .ok_or_else(|| anyhow!("invalid --symbol {}; expected BASE/USDT", args.symbol))?;
    if canonical.quote() != "USDT" {
        bail!("bitget canary only supports USDT quote symbols");
    }

    let market = BitgetMarketAdapter;
    let instruments = market
        .load_instruments()
        .await
        .context("load Bitget instruments")?;
    let instrument = instruments
        .iter()
        .find(|item| item.canonical_symbol == canonical && item.exchange == ExchangeId::Bitget)
        .cloned()
        .ok_or_else(|| anyhow!("Bitget does not list {}", canonical))?;
    if !instrument.is_tradeable_usdt_perpetual() {
        bail!("{} is not a tradeable Bitget USDT perpetual", canonical);
    }

    let book = market
        .fetch_orderbook_snapshot(&instrument.exchange_symbol, 5)
        .await
        .context("fetch Bitget orderbook")?;
    let best_bid = book
        .best_bid()
        .ok_or_else(|| anyhow!("Bitget orderbook has no bids"))?
        .price;
    let best_ask = book
        .best_ask()
        .ok_or_else(|| anyhow!("Bitget orderbook has no asks"))?
        .price;
    let reference_price = match args.side {
        CanarySide::Long => best_ask,
        CanarySide::Short => best_bid,
    };
    let planned_quantity = plan_quantity(args.notional_usdt, reference_price, &instrument)?;
    let estimated_notional = planned_quantity * reference_price;

    let auth = private_rest_auth(&config)?;
    let position_mode = configured_position_mode(&config, &ExchangeId::Bitget);
    if position_mode != PositionMode::Hedge {
        bail!(
            "Bitget canary expects hedge mode; config has {:?}",
            position_mode
        );
    }
    let adapter = private_perp_trading_adapter_for_with_instruments(
        PrivatePerpExchange::Bitget,
        auth,
        position_mode,
        [instrument.clone()],
    )?;

    let balances_before = adapter.get_balances().await.context("read balances")?;
    let open_orders_before = adapter
        .get_open_orders(Some(&instrument.exchange_symbol))
        .await
        .context("read open orders before canary")?;
    if !open_orders_before.is_empty() {
        bail!(
            "refusing canary because {} open Bitget orders already exist for {}",
            open_orders_before.len(),
            canonical
        );
    }
    let positions_before = adapter
        .get_positions(Some(&instrument.exchange_symbol))
        .await
        .context("read positions before canary")?;
    let account = adapter
        .get_symbol_account_config(&instrument.exchange_symbol)
        .await
        .context("read symbol account config")?;

    let mut warnings = Vec::new();
    if account.position_mode != Some(PositionMode::Hedge) {
        warnings.push(format!(
            "readback position_mode={:?}; expected Hedge",
            account.position_mode
        ));
    }
    if estimated_notional > 10.0 {
        warnings.push(format!(
            "estimated notional {estimated_notional:.4} exceeds 10 USDT because of venue minimums"
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
    let mut positions_after = None;
    let mut open_orders_after = None;

    if args.execute {
        let suffix = Utc::now().timestamp_millis();
        let open = OrderCommand {
            command_id: format!("cmd-bg-canary-open-{suffix}"),
            bundle_id: format!("bg-canary-{suffix}"),
            exchange: ExchangeId::Bitget,
            canonical_symbol: canonical.clone(),
            exchange_symbol: instrument.exchange_symbol.clone(),
            intent: args.side.open_intent(),
            side: args.side.order_side(),
            position_side: args.side.position_side(),
            order_type: OrderType::Market,
            quantity: planned_quantity,
            price: None,
            time_in_force: TimeInForce::Ioc,
            post_only: false,
            reduce_only: false,
            client_order_id: format!("bg-canary-open-{suffix}"),
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
            ExchangeId::Bitget,
            canonical.clone(),
            instrument.exchange_symbol.clone(),
            args.side.position_side(),
            planned_quantity,
            format!("bg-canary-close-{suffix}"),
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
        open_orders_after = Some(orders.len());
    }

    let report = CanaryReport {
        generated_at: Utc::now(),
        execute_requested: args.execute,
        live_order_confirmed: args.confirm_live_order,
        exchange: "bitget".to_string(),
        symbol: canonical.to_string(),
        side: format!("{:?}", args.side).to_ascii_lowercase(),
        requested_notional_usdt: args.notional_usdt,
        planned_quantity,
        estimated_notional_usdt: estimated_notional,
        max_estimated_notional_usdt: args.max_estimated_notional_usdt,
        best_bid,
        best_ask,
        instrument: InstrumentSummary {
            exchange_symbol: instrument.exchange_symbol.symbol,
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
        open_ack,
        close_ack,
        positions_after,
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

fn private_rest_auth(config: &CrossExchangeArbitrageConfig) -> Result<PrivateRestAuth> {
    let runtime = config.exchanges.get(&ExchangeId::Bitget);
    let prefix = runtime
        .and_then(|runtime| runtime.env_prefix.as_deref())
        .unwrap_or("BITGET");
    let api_keys = ApiKeys::from_env(prefix).map_err(|err| anyhow!(err.to_string()))?;
    Ok(PrivateRestAuth {
        api_key: api_keys.api_key,
        api_secret: api_keys.api_secret,
        passphrase: api_keys.passphrase,
        demo_trading: runtime.map(|runtime| runtime.demo_trading).unwrap_or(false),
    })
}

fn plan_quantity(notional: f64, price: f64, instrument: &InstrumentMeta) -> Result<f64> {
    if !price.is_finite() || price <= 0.0 {
        bail!("invalid reference price {price}");
    }
    let min_quantity_for_notional = if instrument.min_notional > 0.0 {
        instrument.min_notional / price
    } else {
        0.0
    };
    let raw_quantity = (notional / price)
        .max(instrument.min_qty)
        .max(min_quantity_for_notional);
    let steps = (raw_quantity / instrument.quantity_step).ceil().max(1.0);
    let quantity = normalize_number(steps * instrument.quantity_step);
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
