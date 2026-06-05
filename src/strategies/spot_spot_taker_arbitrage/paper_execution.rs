use chrono::Utc;

use super::{
    book_age_ms, configured_spot_pair, is_book_fresh, CommonSymbolRules, OpportunityRecord,
    PaperInventory, RejectionReason, SimulatedTradeRecord, SpotSpotTakerArbitrageConfig, SpotVenue,
};
use crate::exchanges::unified::{
    validate_min_notional, validate_quantity_step, OrderBookLevel, OrderBookSnapshot,
};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SimulatedLeg {
    pub average_price: f64,
    pub quantity: f64,
    pub notional: f64,
    pub fee: f64,
}

pub fn simulate_taker_buy(
    asks: &[OrderBookLevel],
    quantity: f64,
    taker_fee_bps: f64,
) -> Result<SimulatedLeg, RejectionReason> {
    consume_levels(asks, quantity, taker_fee_bps)
}

pub fn simulate_taker_sell(
    bids: &[OrderBookLevel],
    quantity: f64,
    taker_fee_bps: f64,
) -> Result<SimulatedLeg, RejectionReason> {
    consume_levels(bids, quantity, taker_fee_bps)
}

pub fn execute_paper_taker_taker(
    config: &SpotSpotTakerArbitrageConfig,
    inventory: &mut PaperInventory,
    opportunity: &OpportunityRecord,
    rules: &CommonSymbolRules,
    mexc_book: &OrderBookSnapshot,
    coinex_book: &OrderBookSnapshot,
) -> Result<SimulatedTradeRecord, RejectionReason> {
    let buy_exchange = parse_exchange(&opportunity.buy_exchange)?;
    let sell_exchange = parse_exchange(&opportunity.sell_exchange)?;
    let (left_exchange, right_exchange) = configured_spot_pair(&config.exchanges);
    let book_for = |exchange| {
        if exchange == left_exchange {
            mexc_book
        } else if exchange == right_exchange {
            coinex_book
        } else {
            mexc_book
        }
    };
    let buy_book = book_for(buy_exchange);
    let sell_book = book_for(sell_exchange);
    if !is_book_fresh(buy_book, config.stale_book_ms, config.max_book_latency_ms)
        || !is_book_fresh(sell_book, config.stale_book_ms, config.max_book_latency_ms)
    {
        return Err(RejectionReason::StaleBook);
    }

    let buy_rule = rules.for_exchange(buy_exchange);
    let sell_rule = rules.for_exchange(sell_exchange);
    validate_quantity_step(opportunity.quantity, buy_rule.step_size)
        .map_err(|_| RejectionReason::SymbolRule)?;
    validate_quantity_step(opportunity.quantity, sell_rule.step_size)
        .map_err(|_| RejectionReason::SymbolRule)?;
    validate_min_notional(
        opportunity.quantity,
        Some(opportunity.buy_price),
        buy_rule.min_notional,
    )
    .map_err(|_| RejectionReason::MinNotional)?;
    validate_min_notional(
        opportunity.quantity,
        Some(opportunity.sell_price),
        sell_rule.min_notional,
    )
    .map_err(|_| RejectionReason::MinNotional)?;

    let buy_fee_bps = opportunity.buy_fee_bps;
    let sell_fee_bps = opportunity.sell_fee_bps;
    let buy_leg = simulate_taker_buy(&buy_book.asks, opportunity.quantity, buy_fee_bps)?;
    let sell_leg = simulate_taker_sell(&sell_book.bids, opportunity.quantity, sell_fee_bps)?;
    let quote_needed = buy_leg.notional + buy_leg.fee;

    let mut quote_reservation = inventory.reserve(
        buy_exchange,
        &config.quote_asset,
        quote_needed,
        RejectionReason::InsufficientQuoteBalance,
    )?;
    let mut base_reservation = inventory.reserve(
        sell_exchange,
        &buy_rule.base_asset,
        opportunity.quantity,
        RejectionReason::InsufficientBaseBalance,
    )?;

    inventory.release(&mut quote_reservation);
    inventory.release(&mut base_reservation);
    inventory.settle_buy(
        buy_exchange,
        &buy_rule.base_asset,
        &config.quote_asset,
        buy_leg.quantity,
        buy_leg.notional,
        buy_leg.fee,
    );
    inventory.settle_sell(
        sell_exchange,
        &sell_rule.base_asset,
        &config.quote_asset,
        sell_leg.quantity,
        sell_leg.notional,
        sell_leg.fee,
    );

    let gross_pnl = sell_leg.notional - buy_leg.notional;
    let net_pnl = gross_pnl - buy_leg.fee - sell_leg.fee;
    inventory.add_pnl(gross_pnl, net_pnl);

    Ok(SimulatedTradeRecord {
        timestamp: Utc::now(),
        symbol: opportunity.symbol.clone(),
        buy_exchange: opportunity.buy_exchange.clone(),
        sell_exchange: opportunity.sell_exchange.clone(),
        buy_avg_price: buy_leg.average_price,
        sell_avg_price: sell_leg.average_price,
        quantity: opportunity.quantity,
        notional: buy_leg.notional,
        buy_fee: buy_leg.fee,
        sell_fee: sell_leg.fee,
        gross_pnl,
        net_pnl,
        latency_ms: buy_book
            .latency_ms
            .unwrap_or_default()
            .max(sell_book.latency_ms.unwrap_or_default()),
        order_book_age_ms: book_age_ms(buy_book).max(book_age_ms(sell_book)),
        execution_mode: "paper_taker_taker".to_string(),
    })
}

fn consume_levels(
    levels: &[OrderBookLevel],
    quantity: f64,
    taker_fee_bps: f64,
) -> Result<SimulatedLeg, RejectionReason> {
    let mut remaining = quantity;
    let mut notional = 0.0;
    for level in levels {
        if remaining <= 1e-12 {
            break;
        }
        let fill_qty = remaining.min(level.quantity);
        notional += fill_qty * level.price;
        remaining -= fill_qty;
    }
    if remaining > 1e-12 {
        return Err(RejectionReason::InsufficientDepth);
    }
    let fee = notional * taker_fee_bps / 10_000.0;
    Ok(SimulatedLeg {
        average_price: notional / quantity,
        quantity,
        notional,
        fee,
    })
}

fn parse_exchange(value: &str) -> Result<SpotVenue, RejectionReason> {
    match value.trim().to_ascii_lowercase().as_str() {
        "mexc" => Ok(SpotVenue::Mexc),
        "coinex" => Ok(SpotVenue::CoinEx),
        "gate" | "gateio" | "gate.io" => Ok(SpotVenue::GateIo),
        "bitget" => Ok(SpotVenue::Bitget),
        _ => Err(RejectionReason::PaperExecutionRejected),
    }
}
