#![allow(clippy::all)]
use std::fs;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use rustcta::exchanges::bitget::{BitgetSpotClient, BitgetSpotConfig};
use rustcta::exchanges::unified::{
    round_price_to_tick, round_quantity_to_step, ExchangeClient, MarketType, OrderRequest,
    OrderSide, OrderType, PositionSide, TimeInForce,
};
use rustcta::strategies::spot_spot_taker_arbitrage::SpotSpotTakerArbitrageConfig;
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(
    name = "bitget_spot_order_canary",
    version,
    about = "Safely test the Bitget spot live order path with one tiny order"
)]
struct Args {
    #[arg(
        long,
        default_value = "config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml"
    )]
    config: PathBuf,
    #[arg(long, default_value = "WLDUSDT")]
    symbol: String,
    #[arg(long, default_value_t = 3.2)]
    notional_usdt: f64,
    #[arg(long)]
    quantity: Option<f64>,
    #[arg(long, value_enum, default_value_t = SpotSide::Buy)]
    side: SpotSide,
    #[arg(long, default_value_t = false)]
    execute: bool,
    #[arg(long, default_value_t = false)]
    confirm_live_order: bool,
    #[arg(long)]
    query_order_id: Option<String>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum SpotSide {
    Buy,
    Sell,
}

impl SpotSide {
    fn order_side(self) -> OrderSide {
        match self {
            Self::Buy => OrderSide::Buy,
            Self::Sell => OrderSide::Sell,
        }
    }
}

#[derive(Debug, Serialize)]
struct Report {
    execute: bool,
    symbol: String,
    exchange_symbol: String,
    side: String,
    best_bid: Option<f64>,
    best_ask: f64,
    price: f64,
    quantity: f64,
    estimated_notional: f64,
    submitted: bool,
    order_id: Option<String>,
    status: Option<String>,
}

#[derive(Debug, Serialize)]
struct QueryReport {
    symbol: String,
    order_id: String,
    status: String,
    filled_quantity: f64,
    average_price: Option<f64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let args = Args::parse();
    if args.execute && !args.confirm_live_order {
        bail!("--execute requires --confirm-live-order");
    }
    let max_notional_usdt = match args.side {
        SpotSide::Buy => 3.2,
        SpotSide::Sell => 10.0,
    };
    if !args.notional_usdt.is_finite()
        || args.notional_usdt <= 0.0
        || args.notional_usdt > max_notional_usdt
    {
        bail!("--notional-usdt must be > 0 and <= {max_notional_usdt}");
    }
    if let Some(quantity) = args.quantity {
        if !quantity.is_finite() || quantity <= 0.0 {
            bail!("--quantity must be > 0");
        }
    }

    let config = load_config(&args.config)?;
    let mut bitget_config = BitgetSpotConfig::default();
    if !config.bitget.api_key.trim().is_empty() {
        bitget_config.api_key = config.bitget.api_key.clone();
    }
    if !config.bitget.api_secret.trim().is_empty() {
        bitget_config.api_secret = config.bitget.api_secret.clone();
    }
    if !config.bitget.passphrase.trim().is_empty() {
        bitget_config.passphrase = config.bitget.passphrase.clone();
    }
    if !config.bitget.base_url.trim().is_empty() {
        bitget_config.base_url = config.bitget.base_url.clone();
    }
    if !config.bitget.websocket_url.trim().is_empty() {
        bitget_config.websocket_url = config.bitget.websocket_url.clone();
    }
    bitget_config.dry_run = !args.execute;
    let client = BitgetSpotClient::new(bitget_config);
    let symbol = client.normalize_symbol(&args.symbol)?;
    if let Some(order_id) = args.query_order_id {
        let order = client
            .get_order(&symbol, &order_id)
            .await
            .context("query Bitget spot order")?;
        let report = QueryReport {
            symbol,
            order_id,
            status: format!("{:?}", order.status),
            filled_quantity: order.filled_quantity,
            average_price: order.average_price,
        };
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }
    let exchange_symbol = symbol.clone();
    let rules = client
        .load_symbol_rules()
        .await
        .context("load Bitget rules")?;
    let rule = rules
        .iter()
        .find(|rule| rule.internal_symbol.eq_ignore_ascii_case(&symbol))
        .or_else(|| {
            rules
                .iter()
                .find(|rule| rule.exchange_symbol.eq_ignore_ascii_case(&symbol))
        })
        .with_context(|| format!("Bitget rule not found for {symbol}"))?;
    let book = client
        .get_orderbook(&symbol, 5)
        .await
        .context("fetch Bitget orderbook")?;
    let best_bid = book.best_bid;
    let best_ask = book.best_ask.context("Bitget orderbook missing best ask")?;
    let price = match args.side {
        SpotSide::Buy => round_price_to_tick(best_ask * 1.001, rule.tick_size, true),
        SpotSide::Sell => {
            let bid = best_bid.context("Bitget orderbook missing best bid")?;
            round_price_to_tick(bid * 0.999, rule.tick_size, false)
        }
    };
    let raw_quantity = args.quantity.unwrap_or(args.notional_usdt / price);
    let quantity = round_quantity_to_step(raw_quantity, rule.step_size, false);
    let estimated_notional = quantity * price;
    if quantity <= 0.0
        || estimated_notional <= 0.0
        || estimated_notional > max_notional_usdt + 1e-12
    {
        bail!("planned order invalid quantity={quantity} estimated_notional={estimated_notional}");
    }

    let request = OrderRequest {
        market_type: MarketType::Spot,
        symbol: symbol.clone(),
        side: args.side.order_side(),
        position_side: PositionSide::None,
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::IOC),
        quantity,
        price: Some(price),
        client_order_id: None,
        reduce_only: false,
    };

    let mut report = Report {
        execute: args.execute,
        symbol,
        exchange_symbol,
        side: format!("{:?}", args.side),
        best_bid,
        best_ask,
        price,
        quantity,
        estimated_notional,
        submitted: false,
        order_id: None,
        status: None,
    };
    if args.execute {
        let order = client
            .place_order(request)
            .await
            .context("place Bitget spot order")?;
        report.submitted = true;
        report.order_id = Some(order.order_id);
        report.status = Some(format!("{:?}", order.status));
    }
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn load_config(path: &PathBuf) -> Result<SpotSpotTakerArbitrageConfig> {
    let raw =
        fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
    Ok(serde_yaml::from_str(&raw).context("parse spot spot config")?)
}
