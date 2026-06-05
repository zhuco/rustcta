use serde::{Deserialize, Serialize};

use crate::exchanges::unified::{OrderBookLevel, OrderBookSnapshot, OrderSide, SymbolRule};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ExecutablePriceRequest {
    pub side: OrderSide,
    pub target_quantity: Option<f64>,
    pub target_notional: Option<f64>,
    pub stale_book_ms: u64,
    pub quantity_rounding_tolerance: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutablePriceResult {
    pub side: OrderSide,
    pub requested_quantity: f64,
    pub executable_quantity: f64,
    pub requested_notional: f64,
    pub executable_notional: f64,
    pub best_price: Option<f64>,
    pub vwap: Option<f64>,
    pub worst_price: Option<f64>,
    pub slippage_bps: f64,
    pub levels_consumed: usize,
    pub full_fill_possible: bool,
    pub rejection_reason_optional: Option<String>,
}

impl ExecutablePriceResult {
    pub fn rejected(side: OrderSide, reason: impl Into<String>) -> Self {
        Self {
            side,
            requested_quantity: 0.0,
            executable_quantity: 0.0,
            requested_notional: 0.0,
            executable_notional: 0.0,
            best_price: None,
            vwap: None,
            worst_price: None,
            slippage_bps: 0.0,
            levels_consumed: 0,
            full_fill_possible: false,
            rejection_reason_optional: Some(reason.into()),
        }
    }
}

pub fn executable_vwap(
    book: &OrderBookSnapshot,
    rule: &SymbolRule,
    request: ExecutablePriceRequest,
) -> ExecutablePriceResult {
    if book.is_stale {
        return ExecutablePriceResult::rejected(request.side, "book is marked stale");
    }
    let age_ms = chrono::Utc::now()
        .signed_duration_since(book.received_at)
        .num_milliseconds()
        .max(0) as u64;
    if age_ms > request.stale_book_ms {
        return ExecutablePriceResult::rejected(
            request.side,
            format!("book age {age_ms}ms exceeds {}", request.stale_book_ms),
        );
    }
    let levels = match request.side {
        OrderSide::Buy => &book.asks,
        OrderSide::Sell => &book.bids,
    };
    let Some(best_price) = levels.first().map(|level| level.price).filter(|p| *p > 0.0) else {
        return ExecutablePriceResult::rejected(request.side, "book side is empty");
    };
    let requested_quantity = match (request.target_quantity, request.target_notional) {
        (Some(quantity), _) => quantity,
        (None, Some(notional)) => notional / best_price,
        (None, None) => {
            return ExecutablePriceResult::rejected(
                request.side,
                "target quantity or notional is required",
            )
        }
    };
    if requested_quantity <= 0.0 {
        return ExecutablePriceResult::rejected(request.side, "requested quantity is not positive");
    }
    if requested_quantity + 1e-12 < rule.min_quantity {
        return ExecutablePriceResult::rejected(
            request.side,
            format!(
                "requested quantity {} below min {}",
                requested_quantity, rule.min_quantity
            ),
        );
    }
    if rule.step_size > 0.0 {
        let rounded = (requested_quantity / rule.step_size).floor() * rule.step_size;
        if (requested_quantity - rounded).abs() > request.quantity_rounding_tolerance {
            return ExecutablePriceResult::rejected(
                request.side,
                format!("quantity {requested_quantity} is not step-aligned"),
            );
        }
    }
    consume_levels(request.side, levels, requested_quantity, best_price)
}

pub fn consume_levels(
    side: OrderSide,
    levels: &[OrderBookLevel],
    requested_quantity: f64,
    best_price: f64,
) -> ExecutablePriceResult {
    let mut remaining = requested_quantity;
    let mut notional = 0.0;
    let mut executable_quantity = 0.0;
    let mut levels_consumed = 0;
    let mut worst_price = None;
    for level in levels {
        if remaining <= 1e-12 {
            break;
        }
        if level.price <= 0.0 || level.quantity <= 0.0 {
            continue;
        }
        let take = remaining.min(level.quantity);
        notional += take * level.price;
        executable_quantity += take;
        remaining -= take;
        levels_consumed += 1;
        worst_price = Some(level.price);
    }
    let full_fill_possible = remaining <= 1e-12;
    let vwap = (executable_quantity > 0.0).then_some(notional / executable_quantity);
    let slippage_bps = match (side, vwap) {
        (OrderSide::Buy, Some(vwap)) => (vwap - best_price).max(0.0) / best_price * 10_000.0,
        (OrderSide::Sell, Some(vwap)) => (best_price - vwap).max(0.0) / best_price * 10_000.0,
        _ => 0.0,
    };
    ExecutablePriceResult {
        side,
        requested_quantity,
        executable_quantity,
        requested_notional: requested_quantity * best_price,
        executable_notional: notional,
        best_price: Some(best_price),
        vwap,
        worst_price,
        slippage_bps,
        levels_consumed,
        full_fill_possible,
        rejection_reason_optional: (!full_fill_possible).then(|| {
            format!(
                "insufficient depth requested_quantity={} executable_quantity={}",
                requested_quantity, executable_quantity
            )
        }),
    }
}

pub fn enforce_same_quantity(
    buy: &ExecutablePriceResult,
    sell: &ExecutablePriceResult,
    tolerance: f64,
) -> Result<f64, String> {
    if !buy.full_fill_possible {
        return Err(buy
            .rejection_reason_optional
            .clone()
            .unwrap_or_else(|| "buy leg cannot fully execute".to_string()));
    }
    if !sell.full_fill_possible {
        return Err(sell
            .rejection_reason_optional
            .clone()
            .unwrap_or_else(|| "sell leg cannot fully execute".to_string()));
    }
    if (buy.executable_quantity - sell.executable_quantity).abs() > tolerance {
        return Err(format!(
            "leg quantities differ buy={} sell={} tolerance={}",
            buy.executable_quantity, sell.executable_quantity, tolerance
        ));
    }
    Ok(buy.executable_quantity.min(sell.executable_quantity))
}
