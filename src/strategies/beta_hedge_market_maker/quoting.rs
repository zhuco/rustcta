use crate::core::types::OrderSide;

use super::inventory::HedgeInventoryLedger;
use super::risk::{HedgePhase, ProtectionDecision, ProtectionState};
use super::SymbolPrecision;

#[derive(Debug, Clone, Copy)]
pub struct MarketQuote {
    pub best_bid: f64,
    pub best_ask: f64,
    pub mid: f64,
}

#[derive(Debug, Clone)]
pub struct QuotePlan {
    pub level: usize,
    pub side: OrderSide,
    pub price: f64,
    pub quantity: f64,
    pub position_side: Option<String>,
    pub reduce_only: bool,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct QuoteParams {
    pub target_notional_usd: f64,
    pub inventory_gap_notional_usd: f64,
    pub base_order_notional: f64,
    pub levels_per_side: usize,
    pub base_spread_bps: f64,
    pub level_spacing_bps: f64,
    pub min_spread_bps: f64,
    pub max_inventory_bias_bps: f64,
    pub level_size_decay: f64,
    pub dual_position_mode: bool,
    pub independent_dual_side: bool,
}

pub fn build_quotes(
    market: MarketQuote,
    precision: SymbolPrecision,
    inventory: &HedgeInventoryLedger,
    protection: &ProtectionDecision,
    params: &QuoteParams,
) -> Vec<QuotePlan> {
    if protection.state == ProtectionState::Emergency || market.mid <= 0.0 {
        return Vec::new();
    }

    let gap_ratio = (params.inventory_gap_notional_usd
        / params
            .target_notional_usd
            .abs()
            .max(params.base_order_notional))
    .clamp(-1.0, 1.0);
    let price_bias_bps = gap_ratio * params.max_inventory_bias_bps;
    let size_bias = gap_ratio * 0.8;
    let mut quotes = Vec::new();

    if params.independent_dual_side && params.dual_position_mode {
        for level in 0..params.levels_per_side {
            let decay = params.level_size_decay.powi(level as i32);
            let level_spread_bps = params.base_spread_bps + params.level_spacing_bps * level as f64;
            let bid_spread_bps = (level_spread_bps - price_bias_bps).max(params.min_spread_bps);
            let ask_spread_bps = (level_spread_bps + price_bias_bps).max(params.min_spread_bps);

            push_independent_long_quotes(
                &mut quotes,
                market,
                precision,
                inventory,
                protection,
                params,
                level,
                decay,
                bid_spread_bps,
                ask_spread_bps,
                size_bias,
            );
            push_independent_short_quotes(
                &mut quotes,
                market,
                precision,
                inventory,
                protection,
                params,
                level,
                decay,
                bid_spread_bps,
                ask_spread_bps,
                size_bias,
            );
        }
        return quotes;
    }

    for level in 0..params.levels_per_side {
        let decay = params.level_size_decay.powi(level as i32);
        let level_spread_bps = params.base_spread_bps + params.level_spacing_bps * level as f64;
        let bid_spread_bps = (level_spread_bps - price_bias_bps).max(params.min_spread_bps);
        let ask_spread_bps = (level_spread_bps + price_bias_bps).max(params.min_spread_bps);

        match protection.phase {
            HedgePhase::BuildLong => {
                let bid_price =
                    quantize_bid(market.mid * (1.0 - bid_spread_bps / 10_000.0), precision);
                let bid_notional = build_notional(
                    params.inventory_gap_notional_usd,
                    level == 0,
                    params.base_order_notional
                        * decay
                        * (1.0 + size_bias).clamp(0.4, 1.8)
                        * protection.same_side_scale.max(0.0),
                    min_viable_notional(bid_price, precision),
                );
                maybe_push_quote(
                    &mut quotes,
                    QuoteDescriptor {
                        side: OrderSide::Buy,
                        price: bid_price,
                        notional_usd: bid_notional,
                        reduce_only: false,
                        position_side: dual_side("LONG", params.dual_position_mode),
                        reason: "build_long_bid".to_string(),
                        level,
                    },
                    precision,
                );

                if inventory.long_leg.quantity > precision.step_size {
                    let ask_price =
                        quantize_ask(market.mid * (1.0 + ask_spread_bps / 10_000.0), precision);
                    let ask_notional = recycle_notional(
                        params.base_order_notional * decay * (1.0 - size_bias).clamp(0.2, 1.2),
                        inventory.long_leg.notional_usd,
                        min_viable_notional(ask_price, precision),
                        level == 0,
                    );
                    maybe_push_quote(
                        &mut quotes,
                        QuoteDescriptor {
                            side: OrderSide::Sell,
                            price: ask_price,
                            notional_usd: ask_notional,
                            reduce_only: true,
                            position_side: dual_side("LONG", params.dual_position_mode),
                            reason: "recycle_long_ask".to_string(),
                            level,
                        },
                        precision,
                    );
                }
            }
            HedgePhase::ReduceLong => {
                if inventory.long_leg.quantity > precision.step_size {
                    let ask_notional = reduce_notional(
                        params.base_order_notional * decay,
                        inventory.long_leg.notional_usd,
                        (-params.inventory_gap_notional_usd).max(0.0),
                        min_viable_notional(
                            quantize_ask(
                                market.mid
                                    * (1.0
                                        + (params.min_spread_bps
                                            + level as f64 * params.level_spacing_bps)
                                            / 10_000.0),
                                precision,
                            ),
                            precision,
                        ),
                        level == 0,
                    );
                    maybe_push_quote(
                        &mut quotes,
                        QuoteDescriptor {
                            side: OrderSide::Sell,
                            price: quantize_ask(
                                market.mid
                                    * (1.0
                                        + (params.min_spread_bps
                                            + level as f64 * params.level_spacing_bps)
                                            / 10_000.0),
                                precision,
                            ),
                            notional_usd: ask_notional,
                            reduce_only: true,
                            position_side: dual_side("LONG", params.dual_position_mode),
                            reason: "reduce_long_only".to_string(),
                            level,
                        },
                        precision,
                    );
                }
            }
            HedgePhase::BuildShort => {
                let ask_price =
                    quantize_ask(market.mid * (1.0 + ask_spread_bps / 10_000.0), precision);
                let ask_notional = build_notional(
                    -params.inventory_gap_notional_usd,
                    level == 0,
                    params.base_order_notional
                        * decay
                        * (1.0 - size_bias).clamp(0.4, 1.8)
                        * protection.same_side_scale.max(0.0),
                    min_viable_notional(ask_price, precision),
                );
                maybe_push_quote(
                    &mut quotes,
                    QuoteDescriptor {
                        side: OrderSide::Sell,
                        price: ask_price,
                        notional_usd: ask_notional,
                        reduce_only: false,
                        position_side: dual_side("SHORT", params.dual_position_mode),
                        reason: "build_short_ask".to_string(),
                        level,
                    },
                    precision,
                );

                if inventory.short_leg.quantity > precision.step_size {
                    let bid_price =
                        quantize_bid(market.mid * (1.0 - bid_spread_bps / 10_000.0), precision);
                    let bid_notional = recycle_notional(
                        params.base_order_notional * decay * (1.0 + size_bias).clamp(0.2, 1.2),
                        inventory.short_leg.notional_usd,
                        min_viable_notional(bid_price, precision),
                        level == 0,
                    );
                    maybe_push_quote(
                        &mut quotes,
                        QuoteDescriptor {
                            side: OrderSide::Buy,
                            price: bid_price,
                            notional_usd: bid_notional,
                            reduce_only: true,
                            position_side: dual_side("SHORT", params.dual_position_mode),
                            reason: "recycle_short_bid".to_string(),
                            level,
                        },
                        precision,
                    );
                }
            }
            HedgePhase::ReduceShort => {
                if inventory.short_leg.quantity > precision.step_size {
                    let bid_notional = reduce_notional(
                        params.base_order_notional * decay,
                        inventory.short_leg.notional_usd,
                        params.inventory_gap_notional_usd.max(0.0),
                        min_viable_notional(
                            quantize_bid(
                                market.mid
                                    * (1.0
                                        - (params.min_spread_bps
                                            + level as f64 * params.level_spacing_bps)
                                            / 10_000.0),
                                precision,
                            ),
                            precision,
                        ),
                        level == 0,
                    );
                    maybe_push_quote(
                        &mut quotes,
                        QuoteDescriptor {
                            side: OrderSide::Buy,
                            price: quantize_bid(
                                market.mid
                                    * (1.0
                                        - (params.min_spread_bps
                                            + level as f64 * params.level_spacing_bps)
                                            / 10_000.0),
                                precision,
                            ),
                            notional_usd: bid_notional,
                            reduce_only: true,
                            position_side: dual_side("SHORT", params.dual_position_mode),
                            reason: "reduce_short_only".to_string(),
                            level,
                        },
                        precision,
                    );
                }
            }
            HedgePhase::Flat => {}
        }
    }

    quotes
}

#[derive(Debug, Clone)]
struct QuoteDescriptor {
    level: usize,
    side: OrderSide,
    price: f64,
    notional_usd: f64,
    position_side: Option<String>,
    reduce_only: bool,
    reason: String,
}

#[allow(clippy::too_many_arguments)]
fn push_independent_long_quotes(
    quotes: &mut Vec<QuotePlan>,
    market: MarketQuote,
    precision: SymbolPrecision,
    inventory: &HedgeInventoryLedger,
    protection: &ProtectionDecision,
    params: &QuoteParams,
    level: usize,
    decay: f64,
    bid_spread_bps: f64,
    ask_spread_bps: f64,
    size_bias: f64,
) {
    let bid_price = quantize_bid(market.mid * (1.0 - bid_spread_bps / 10_000.0), precision);
    let long_buy_notional = params.base_order_notional
        * decay
        * (1.0 + size_bias).clamp(0.4, 1.8)
        * protection.same_side_scale.max(0.0);
    maybe_push_quote(
        quotes,
        QuoteDescriptor {
            side: OrderSide::Buy,
            price: bid_price,
            notional_usd: long_buy_notional,
            reduce_only: false,
            position_side: dual_side("LONG", params.dual_position_mode),
            reason: "independent_long_bid".to_string(),
            level,
        },
        precision,
    );

    if inventory.long_leg.quantity <= precision.step_size {
        return;
    }

    let ask_price = quantize_ask(market.mid * (1.0 + ask_spread_bps / 10_000.0), precision);
    let long_sell_notional = recycle_notional(
        params.base_order_notional * decay * (1.0 - size_bias).clamp(0.2, 1.2),
        inventory.long_leg.notional_usd,
        min_viable_notional(ask_price, precision),
        level == 0,
    );
    maybe_push_quote(
        quotes,
        QuoteDescriptor {
            side: OrderSide::Sell,
            price: ask_price,
            notional_usd: long_sell_notional,
            reduce_only: true,
            position_side: dual_side("LONG", params.dual_position_mode),
            reason: "independent_long_ask".to_string(),
            level,
        },
        precision,
    );
}

#[allow(clippy::too_many_arguments)]
fn push_independent_short_quotes(
    quotes: &mut Vec<QuotePlan>,
    market: MarketQuote,
    precision: SymbolPrecision,
    inventory: &HedgeInventoryLedger,
    protection: &ProtectionDecision,
    params: &QuoteParams,
    level: usize,
    decay: f64,
    bid_spread_bps: f64,
    ask_spread_bps: f64,
    size_bias: f64,
) {
    let ask_price = quantize_ask(market.mid * (1.0 + ask_spread_bps / 10_000.0), precision);
    let short_sell_notional = params.base_order_notional
        * decay
        * (1.0 - size_bias).clamp(0.4, 1.8)
        * protection.same_side_scale.max(0.0);
    maybe_push_quote(
        quotes,
        QuoteDescriptor {
            side: OrderSide::Sell,
            price: ask_price,
            notional_usd: short_sell_notional,
            reduce_only: false,
            position_side: dual_side("SHORT", params.dual_position_mode),
            reason: "independent_short_ask".to_string(),
            level,
        },
        precision,
    );

    if inventory.short_leg.quantity <= precision.step_size {
        return;
    }

    let bid_price = quantize_bid(market.mid * (1.0 - bid_spread_bps / 10_000.0), precision);
    let short_buy_notional = recycle_notional(
        params.base_order_notional * decay * (1.0 + size_bias).clamp(0.2, 1.2),
        inventory.short_leg.notional_usd,
        min_viable_notional(bid_price, precision),
        level == 0,
    );
    maybe_push_quote(
        quotes,
        QuoteDescriptor {
            side: OrderSide::Buy,
            price: bid_price,
            notional_usd: short_buy_notional,
            reduce_only: true,
            position_side: dual_side("SHORT", params.dual_position_mode),
            reason: "independent_short_bid".to_string(),
            level,
        },
        precision,
    );
}

fn maybe_push_quote(
    quotes: &mut Vec<QuotePlan>,
    descriptor: QuoteDescriptor,
    precision: SymbolPrecision,
) {
    if descriptor.price <= 0.0 || descriptor.notional_usd <= 0.0 {
        return;
    }
    let quantity = quantize_qty(descriptor.notional_usd / descriptor.price, precision);
    if quantity <= 0.0 || quantity * descriptor.price < precision.min_notional {
        return;
    }
    quotes.push(QuotePlan {
        level: descriptor.level,
        side: descriptor.side,
        price: descriptor.price,
        quantity,
        position_side: descriptor.position_side,
        reduce_only: descriptor.reduce_only,
        reason: descriptor.reason,
    });
}

fn recycle_notional(
    raw_notional_usd: f64,
    available_notional_usd: f64,
    min_viable_notional_usd: f64,
    force_min_first_level: bool,
) -> f64 {
    if raw_notional_usd <= 0.0 || available_notional_usd <= 0.0 {
        return 0.0;
    }

    let available = available_notional_usd.max(0.0);
    let mut desired = raw_notional_usd.min(available.max(min_viable_notional_usd));

    if force_min_first_level && available >= min_viable_notional_usd {
        desired = desired.max(min_viable_notional_usd);
    }

    desired.min(available)
}

fn reduce_notional(
    raw_notional_usd: f64,
    available_notional_usd: f64,
    excess_notional_usd: f64,
    min_viable_notional_usd: f64,
    force_min_first_level: bool,
) -> f64 {
    let adaptive_notional = if excess_notional_usd > min_viable_notional_usd {
        raw_notional_usd.max((excess_notional_usd * 0.25).min(raw_notional_usd * 4.0))
    } else {
        raw_notional_usd
    };

    recycle_notional(
        adaptive_notional,
        available_notional_usd,
        min_viable_notional_usd,
        force_min_first_level,
    )
}

fn build_notional(
    remaining_gap_notional_usd: f64,
    force_min_first_level: bool,
    raw_notional_usd: f64,
    min_viable_notional_usd: f64,
) -> f64 {
    if raw_notional_usd <= 0.0 {
        return 0.0;
    }

    let remaining_gap = remaining_gap_notional_usd.max(0.0);
    let mut desired = raw_notional_usd.min(remaining_gap.max(min_viable_notional_usd));

    if force_min_first_level && remaining_gap >= min_viable_notional_usd {
        desired = desired.max(min_viable_notional_usd);
    }

    desired.min(remaining_gap.max(0.0))
}

fn min_viable_notional(price: f64, precision: SymbolPrecision) -> f64 {
    if price <= 0.0 {
        return precision.min_notional.max(0.0);
    }
    let min_qty = quantize_ceil(
        precision.min_notional / price,
        precision.step_size,
        precision.qty_digits,
    );
    (min_qty * price).max(precision.min_notional)
}

fn dual_side(position_side: &str, dual_position_mode: bool) -> Option<String> {
    if dual_position_mode {
        Some(position_side.to_string())
    } else {
        None
    }
}

fn quantize_bid(value: f64, precision: SymbolPrecision) -> f64 {
    quantize_floor(value, precision.tick_size, precision.price_digits)
}

fn quantize_ask(value: f64, precision: SymbolPrecision) -> f64 {
    quantize_ceil(value, precision.tick_size, precision.price_digits)
}

fn quantize_qty(value: f64, precision: SymbolPrecision) -> f64 {
    quantize_floor(value, precision.step_size, precision.qty_digits)
}

fn quantize_floor(value: f64, step: f64, digits: u32) -> f64 {
    if step <= 0.0 {
        return truncate_digits(value, digits);
    }
    truncate_digits((value / step).floor() * step, digits)
}

fn quantize_ceil(value: f64, step: f64, digits: u32) -> f64 {
    if step <= 0.0 {
        return truncate_digits(value, digits);
    }
    truncate_digits((value / step).ceil() * step, digits)
}

fn truncate_digits(val: f64, digits: u32) -> f64 {
    let factor = 10f64.powi(digits as i32);
    (val * factor).round() / factor
}

#[cfg(test)]
mod tests {
    use super::*;

    fn precision() -> SymbolPrecision {
        SymbolPrecision {
            tick_size: 0.0001,
            step_size: 1.0,
            price_digits: 4,
            qty_digits: 0,
            min_notional: 5.0,
        }
    }

    fn params() -> QuoteParams {
        QuoteParams {
            target_notional_usd: 200.0,
            inventory_gap_notional_usd: 150.0,
            base_order_notional: 8.0,
            levels_per_side: 2,
            base_spread_bps: 6.0,
            level_spacing_bps: 4.0,
            min_spread_bps: 2.0,
            max_inventory_bias_bps: 4.0,
            level_size_decay: 0.8,
            dual_position_mode: true,
            independent_dual_side: false,
        }
    }

    #[test]
    fn positive_gap_builds_long_bid_and_recycles_long_ask() {
        let mut params = params();
        params.inventory_gap_notional_usd = 40.0;
        let quotes = build_quotes(
            MarketQuote {
                best_bid: 0.1,
                best_ask: 0.1001,
                mid: 0.10005,
            },
            precision(),
            &HedgeInventoryLedger {
                long_leg: super::super::inventory::HedgeLegInventory {
                    quantity: 200.0,
                    entry_price: 0.1,
                    notional_usd: 20.0,
                },
                ..HedgeInventoryLedger::default()
            },
            &ProtectionDecision {
                phase: HedgePhase::BuildLong,
                state: ProtectionState::Normal,
                same_side_scale: 1.0,
                reason: "normal".to_string(),
            },
            &params,
        );

        assert!(quotes.iter().any(|quote| quote.side == OrderSide::Buy));
        assert!(quotes.iter().any(|quote| quote.side == OrderSide::Sell));
        let best_bid = quotes
            .iter()
            .filter(|quote| quote.side == OrderSide::Buy)
            .map(|quote| quote.price)
            .fold(0.0, f64::max);
        let best_ask = quotes
            .iter()
            .filter(|quote| quote.side == OrderSide::Sell)
            .map(|quote| quote.price)
            .fold(f64::MAX, f64::min);
        assert!(best_bid < best_ask);
    }

    #[test]
    fn no_recycle_quote_when_no_inventory_on_leg() {
        let quotes = build_quotes(
            MarketQuote {
                best_bid: 0.1,
                best_ask: 0.1001,
                mid: 0.10005,
            },
            precision(),
            &HedgeInventoryLedger::default(),
            &ProtectionDecision {
                phase: HedgePhase::BuildLong,
                state: ProtectionState::Normal,
                same_side_scale: 1.0,
                reason: "normal".to_string(),
            },
            &params(),
        );

        assert!(quotes.iter().all(|quote| quote.side == OrderSide::Buy));
    }

    #[test]
    fn large_positive_gap_keeps_one_recycle_ask_above_min_notional() {
        let quotes = build_quotes(
            MarketQuote {
                best_bid: 0.1,
                best_ask: 0.1001,
                mid: 0.10005,
            },
            precision(),
            &HedgeInventoryLedger {
                long_leg: super::super::inventory::HedgeLegInventory {
                    quantity: 2000.0,
                    entry_price: 0.1,
                    notional_usd: 200.0,
                },
                ..HedgeInventoryLedger::default()
            },
            &ProtectionDecision {
                phase: HedgePhase::BuildLong,
                state: ProtectionState::Normal,
                same_side_scale: 1.0,
                reason: "normal".to_string(),
            },
            &params(),
        );

        let sell_quotes: Vec<_> = quotes
            .iter()
            .filter(|quote| quote.side == OrderSide::Sell)
            .collect();
        assert!(!sell_quotes.is_empty());
        assert!(sell_quotes
            .iter()
            .any(|quote| quote.price * quote.quantity >= 5.0));
    }

    #[test]
    fn soft_limited_positive_gap_keeps_one_build_bid_above_min_notional() {
        let mut params = params();
        params.target_notional_usd = 603.08;
        params.inventory_gap_notional_usd = 29.86;

        let quotes = build_quotes(
            MarketQuote {
                best_bid: 0.0981,
                best_ask: 0.0982,
                mid: 0.09815,
            },
            precision(),
            &HedgeInventoryLedger {
                long_leg: super::super::inventory::HedgeLegInventory {
                    quantity: 5838.0,
                    entry_price: 0.0982,
                    notional_usd: 573.22,
                },
                ..HedgeInventoryLedger::default()
            },
            &ProtectionDecision {
                phase: HedgePhase::BuildLong,
                state: ProtectionState::SoftLimit,
                same_side_scale: 0.292136,
                reason: "soft".to_string(),
            },
            &params,
        );

        let buy_quotes: Vec<_> = quotes
            .iter()
            .filter(|quote| quote.side == OrderSide::Buy)
            .collect();
        assert!(!buy_quotes.is_empty());
        assert!(buy_quotes
            .iter()
            .any(|quote| quote.price * quote.quantity >= 5.0));
    }

    #[test]
    fn tiny_gap_does_not_force_min_notional_build_order() {
        let mut params = params();
        params.target_notional_usd = 603.08;
        params.inventory_gap_notional_usd = 3.5;

        let quotes = build_quotes(
            MarketQuote {
                best_bid: 0.0981,
                best_ask: 0.0982,
                mid: 0.09815,
            },
            precision(),
            &HedgeInventoryLedger::default(),
            &ProtectionDecision {
                phase: HedgePhase::BuildLong,
                state: ProtectionState::SoftLimit,
                same_side_scale: 0.292136,
                reason: "soft".to_string(),
            },
            &params,
        );

        assert!(quotes.iter().all(|quote| quote.side != OrderSide::Buy));
    }

    #[test]
    fn reduce_long_uses_adaptive_size_when_target_is_exceeded() {
        let mut params = params();
        params.target_notional_usd = 623.34;
        params.inventory_gap_notional_usd = -176.66;

        let quotes = build_quotes(
            MarketQuote {
                best_bid: 0.1,
                best_ask: 0.1001,
                mid: 0.10005,
            },
            precision(),
            &HedgeInventoryLedger {
                long_leg: super::super::inventory::HedgeLegInventory {
                    quantity: 8_000.0,
                    entry_price: 0.1,
                    notional_usd: 800.0,
                },
                net_quantity: 8_000.0,
                net_notional_usd: 800.0,
                gross_notional_usd: 800.0,
                mark_price: 0.1,
                ..HedgeInventoryLedger::default()
            },
            &ProtectionDecision {
                phase: HedgePhase::ReduceLong,
                state: ProtectionState::HardLimit,
                same_side_scale: 0.0,
                reason: "hard".to_string(),
            },
            &params,
        );

        assert!(quotes.iter().all(|quote| quote.side == OrderSide::Sell));
        assert!(quotes.iter().all(|quote| quote.reduce_only));
        assert!(quotes
            .iter()
            .any(|quote| quote.price * quote.quantity >= 30.0));
    }

    #[test]
    fn independent_dual_side_quotes_open_long_and_short_legs_together() {
        let mut params = params();
        params.independent_dual_side = true;
        params.inventory_gap_notional_usd = 0.0;

        let quotes = build_quotes(
            MarketQuote {
                best_bid: 0.1,
                best_ask: 0.1001,
                mid: 0.10005,
            },
            precision(),
            &HedgeInventoryLedger::default(),
            &ProtectionDecision {
                phase: HedgePhase::Flat,
                state: ProtectionState::Normal,
                same_side_scale: 1.0,
                reason: "normal".to_string(),
            },
            &params,
        );

        assert!(quotes.iter().any(|quote| {
            quote.side == OrderSide::Buy
                && quote.position_side.as_deref() == Some("LONG")
                && !quote.reduce_only
        }));
        assert!(quotes.iter().any(|quote| {
            quote.side == OrderSide::Sell
                && quote.position_side.as_deref() == Some("SHORT")
                && !quote.reduce_only
        }));
    }

    #[test]
    fn independent_dual_side_recycles_existing_legs_independently() {
        let mut params = params();
        params.independent_dual_side = true;

        let quotes = build_quotes(
            MarketQuote {
                best_bid: 0.1,
                best_ask: 0.1001,
                mid: 0.10005,
            },
            precision(),
            &HedgeInventoryLedger {
                long_leg: super::super::inventory::HedgeLegInventory {
                    quantity: 200.0,
                    entry_price: 0.1,
                    notional_usd: 20.0,
                },
                short_leg: super::super::inventory::HedgeLegInventory {
                    quantity: 200.0,
                    entry_price: 0.1,
                    notional_usd: 20.0,
                },
                ..HedgeInventoryLedger::default()
            },
            &ProtectionDecision {
                phase: HedgePhase::Flat,
                state: ProtectionState::Normal,
                same_side_scale: 1.0,
                reason: "normal".to_string(),
            },
            &params,
        );

        assert!(quotes.iter().any(|quote| {
            quote.side == OrderSide::Sell
                && quote.position_side.as_deref() == Some("LONG")
                && quote.reduce_only
        }));
        assert!(quotes.iter().any(|quote| {
            quote.side == OrderSide::Buy
                && quote.position_side.as_deref() == Some("SHORT")
                && quote.reduce_only
        }));
    }
}
