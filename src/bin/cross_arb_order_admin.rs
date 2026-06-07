use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use clap::{Parser, ValueEnum};
use rustcta::execution::{
    CancelAck, CancelCommand, ClosePositionAck, ClosePositionCommand, ExchangePosition, OrderQuery,
    OrderState, PositionSide,
};
use rustcta::market::{exchange_symbol_for, CanonicalSymbol, ExchangeId, ExchangeSymbol};
use rustcta::strategies::cross_exchange_arbitrage::{
    build_trading_adapter_for_exchange, CrossExchangeArbitrageConfig,
};
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(
    name = "cross_arb_order_admin",
    version,
    about = "Query or cancel a private order through the cross-arb trading adapters"
)]
struct Args {
    #[arg(
        long,
        default_value = "config/cross_exchange_arbitrage_three_venues_hcr_10u_405symbols.live-small.yml"
    )]
    config: PathBuf,
    #[arg(long, value_enum)]
    action: AdminAction,
    #[arg(long, default_value = "binance")]
    exchange: String,
    #[arg(long)]
    symbol: Option<String>,
    #[arg(long)]
    exchange_order_id: Option<String>,
    #[arg(long)]
    client_order_id: Option<String>,
    #[arg(long, default_value_t = false)]
    execute: bool,
    #[arg(long, default_value_t = false)]
    confirm_cancel: bool,
    #[arg(long, default_value_t = false)]
    confirm_order: bool,
    #[arg(long)]
    quantity: Option<f64>,
    #[arg(long, value_enum)]
    position_side: Option<AdminPositionSide>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum AdminAction {
    ListOpen,
    ListPositions,
    Query,
    Cancel,
    ClosePosition,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
enum AdminPositionSide {
    Long,
    Short,
    Net,
}

impl From<AdminPositionSide> for PositionSide {
    fn from(value: AdminPositionSide) -> Self {
        match value {
            AdminPositionSide::Long => PositionSide::Long,
            AdminPositionSide::Short => PositionSide::Short,
            AdminPositionSide::Net => PositionSide::Net,
        }
    }
}

#[derive(Debug, Serialize)]
struct AdminReport {
    generated_at: chrono::DateTime<Utc>,
    action: AdminAction,
    execute: bool,
    exchange: ExchangeId,
    canonical_symbol: Option<CanonicalSymbol>,
    exchange_symbol: Option<ExchangeSymbol>,
    exchange_order_id: Option<String>,
    client_order_id: Option<String>,
    positions_before: Vec<ExchangePosition>,
    open_orders_before: Vec<OrderState>,
    query_result: Option<OrderState>,
    query_error: Option<String>,
    cancel_ack: Option<CancelAck>,
    cancel_error: Option<String>,
    close_ack: Option<ClosePositionAck>,
    close_error: Option<String>,
    positions_after: Option<Vec<ExchangePosition>>,
    open_orders_after: Option<Vec<OrderState>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let args = Args::parse();
    let config = load_config(&args.config)?;
    config
        .validate()
        .context("invalid cross arbitrage config")?;

    let exchange = parse_exchange(&args.exchange)?;
    let canonical_symbol = args
        .symbol
        .as_deref()
        .map(|symbol| {
            CanonicalSymbol::parse(symbol)
                .ok_or_else(|| anyhow!("invalid --symbol {symbol}; expected BASE/QUOTE"))
        })
        .transpose()?;
    if canonical_symbol.is_none()
        && !matches!(
            args.action,
            AdminAction::ListOpen | AdminAction::ListPositions
        )
    {
        bail!("--action query/cancel/close-position requires --symbol");
    }
    let exchange_symbol = canonical_symbol
        .as_ref()
        .map(|symbol| exchange_symbol_for(&exchange, symbol));
    let adapter = build_trading_adapter_for_exchange(&config, &exchange)
        .with_context(|| format!("build {} trading adapter", exchange.as_str()))?;

    let positions_before = adapter
        .get_positions(exchange_symbol.as_ref())
        .await
        .with_context(|| {
            format!(
                "read {} positions for {}",
                exchange.as_str(),
                canonical_symbol
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "all symbols".to_string())
            )
        })?;

    let open_orders_before = adapter
        .get_open_orders(exchange_symbol.as_ref())
        .await
        .with_context(|| {
            format!(
                "read {} open orders for {}",
                exchange.as_str(),
                canonical_symbol
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "all symbols".to_string())
            )
        })?;

    let mut query_result = None;
    let mut query_error = None;
    if matches!(args.action, AdminAction::Query | AdminAction::Cancel) {
        if args.exchange_order_id.is_none() && args.client_order_id.is_none() {
            bail!("--action query/cancel requires --exchange-order-id or --client-order-id");
        }
        match adapter
            .get_order(OrderQuery {
                exchange: exchange.clone(),
                exchange_symbol: exchange_symbol
                    .clone()
                    .expect("query/cancel requires --symbol"),
                client_order_id: args.client_order_id.clone(),
                exchange_order_id: args.exchange_order_id.clone(),
            })
            .await
        {
            Ok(order) => query_result = Some(order),
            Err(error) => query_error = Some(error.to_string()),
        }
    }

    let mut close_ack = None;
    let mut close_error = None;
    let mut positions_after = None;
    let mut cancel_ack = None;
    let mut cancel_error = None;
    let mut open_orders_after = None;
    if args.action == AdminAction::Cancel {
        if !args.execute || !args.confirm_cancel {
            bail!("--action cancel requires --execute --confirm-cancel");
        }
        match adapter
            .cancel_order(CancelCommand {
                exchange: exchange.clone(),
                canonical_symbol: canonical_symbol.clone().expect("cancel requires --symbol"),
                exchange_symbol: exchange_symbol.clone().expect("cancel requires --symbol"),
                client_order_id: args.client_order_id.clone(),
                exchange_order_id: args.exchange_order_id.clone(),
                reason: Some("manual cross-arb order admin cancel".to_string()),
                requested_at: Utc::now(),
            })
            .await
        {
            Ok(ack) => cancel_ack = Some(ack),
            Err(error) => cancel_error = Some(error.to_string()),
        }
        open_orders_after = Some(
            adapter
                .get_open_orders(exchange_symbol.as_ref())
                .await
                .with_context(|| {
                    format!(
                        "read {} open orders for {} after cancel",
                        exchange.as_str(),
                        canonical_symbol
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "all symbols".to_string())
                    )
                })?,
        );
    }
    if args.action == AdminAction::ClosePosition {
        if !args.execute || !args.confirm_order {
            bail!("--action close-position requires --execute --confirm-order");
        }
        let quantity = args
            .quantity
            .filter(|quantity| quantity.is_finite() && *quantity > 0.0)
            .ok_or_else(|| anyhow!("--action close-position requires positive --quantity"))?;
        let position_side = args
            .position_side
            .ok_or_else(|| anyhow!("--action close-position requires --position-side"))?
            .into();
        let requested_at = Utc::now();
        let client_order_id = format!(
            "xadm-cl-{}-{}",
            compact_exchange_code(&exchange),
            requested_at.timestamp_millis()
        );
        match adapter
            .close_position(ClosePositionCommand::market(
                exchange.clone(),
                canonical_symbol
                    .clone()
                    .expect("close-position requires --symbol"),
                exchange_symbol
                    .clone()
                    .expect("close-position requires --symbol"),
                position_side,
                quantity,
                client_order_id,
                requested_at,
            ))
            .await
        {
            Ok(ack) => close_ack = Some(ack),
            Err(error) => close_error = Some(error.to_string()),
        }
        positions_after = Some(
            adapter
                .get_positions(exchange_symbol.as_ref())
                .await
                .with_context(|| {
                    format!(
                        "read {} positions for {} after close-position",
                        exchange.as_str(),
                        canonical_symbol
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "all symbols".to_string())
                    )
                })?,
        );
        open_orders_after = Some(
            adapter
                .get_open_orders(exchange_symbol.as_ref())
                .await
                .with_context(|| {
                    format!(
                        "read {} open orders for {} after close-position",
                        exchange.as_str(),
                        canonical_symbol
                            .as_ref()
                            .map(ToString::to_string)
                            .unwrap_or_else(|| "all symbols".to_string())
                    )
                })?,
        );
    }

    let report = AdminReport {
        generated_at: Utc::now(),
        action: args.action,
        execute: args.execute,
        exchange,
        canonical_symbol,
        exchange_symbol,
        exchange_order_id: args.exchange_order_id,
        client_order_id: args.client_order_id,
        positions_before,
        open_orders_before,
        query_result,
        query_error,
        cancel_ack,
        cancel_error,
        close_ack,
        close_error,
        positions_after,
        open_orders_after,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn load_config(path: &PathBuf) -> Result<CrossExchangeArbitrageConfig> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    serde_yaml::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))
}

fn parse_exchange(value: &str) -> Result<ExchangeId> {
    match value.to_ascii_lowercase().as_str() {
        "binance" => Ok(ExchangeId::Binance),
        "bitget" => Ok(ExchangeId::Bitget),
        "gate" | "gateio" => Ok(ExchangeId::Gate),
        "bybit" => Ok(ExchangeId::Bybit),
        "mexc" => Ok(ExchangeId::Mexc),
        "htx" | "huobi" => Ok(ExchangeId::Htx),
        "kraken" => Ok(ExchangeId::Kraken),
        "okx" => Ok(ExchangeId::Okx),
        other => bail!("unsupported exchange {other}"),
    }
}

fn compact_exchange_code(exchange: &ExchangeId) -> String {
    match exchange {
        ExchangeId::Binance => "bi".to_string(),
        ExchangeId::Okx => "okx".to_string(),
        ExchangeId::Bitget => "bg".to_string(),
        ExchangeId::Gate => "gt".to_string(),
        ExchangeId::Bybit => "bb".to_string(),
        ExchangeId::Mexc => "mx".to_string(),
        ExchangeId::CoinEx => "cx".to_string(),
        ExchangeId::KuCoin => "kc".to_string(),
        ExchangeId::Htx => "htx".to_string(),
        ExchangeId::Kraken => "krk".to_string(),
        ExchangeId::Toobit => "tbt".to_string(),
        ExchangeId::Other(value) => {
            let compact = value
                .chars()
                .filter(|ch| ch.is_ascii_alphanumeric())
                .take(6)
                .collect::<String>();
            if compact.is_empty() {
                "oth".to_string()
            } else {
                compact
            }
        }
    }
}
