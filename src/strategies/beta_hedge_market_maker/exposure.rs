use chrono::{DateTime, Utc};

use crate::core::types::{Balance, Position};

const STABLE_QUOTES: &[&str] = &["USDT", "USDC", "FDUSD", "BUSD", "TUSD", "USDP", "DAI"];

#[derive(Debug, Clone, Default)]
pub struct PositionExposure {
    pub symbol: String,
    pub side: String,
    pub signed_quantity: f64,
    pub mark_price: f64,
    pub notional_usd: f64,
}

#[derive(Debug, Clone)]
pub struct PortfolioExposure {
    pub account_equity_usd: f64,
    pub portfolio_net_exposure_usd: f64,
    pub portfolio_core_exposure_usd: f64,
    pub hedge_symbol_exposure_usd: f64,
    pub hedge_target_notional_usd: f64,
    pub max_portfolio_net_exposure_usd: f64,
    pub positions: Vec<PositionExposure>,
    pub measured_at: DateTime<Utc>,
}

impl Default for PortfolioExposure {
    fn default() -> Self {
        Self {
            account_equity_usd: 0.0,
            portfolio_net_exposure_usd: 0.0,
            portfolio_core_exposure_usd: 0.0,
            hedge_symbol_exposure_usd: 0.0,
            hedge_target_notional_usd: 0.0,
            max_portfolio_net_exposure_usd: 0.0,
            positions: Vec::new(),
            measured_at: Utc::now(),
        }
    }
}

pub fn compute_account_equity_usd(balances: &[Balance]) -> f64 {
    balances
        .iter()
        .filter(|balance| {
            STABLE_QUOTES
                .iter()
                .any(|stable| balance.currency.eq_ignore_ascii_case(stable))
        })
        .map(|balance| balance.total.max(0.0))
        .sum()
}

pub fn signed_position_quantity(position: &Position) -> f64 {
    let raw_qty = if position.amount.abs() > f64::EPSILON {
        position.amount
    } else if position.size.abs() > f64::EPSILON {
        position.size
    } else if position.contracts.abs() > f64::EPSILON {
        position.contracts * position.contract_size.max(1.0)
    } else {
        0.0
    };

    match position.side.to_ascii_uppercase().as_str() {
        "LONG" | "BUY" => raw_qty.abs(),
        "SHORT" | "SELL" => -raw_qty.abs(),
        _ => raw_qty,
    }
}

pub fn position_notional_usd(position: &Position) -> f64 {
    let signed_qty = signed_position_quantity(position);
    let mark = effective_mark_price(position);
    signed_qty * mark
}

pub fn effective_mark_price(position: &Position) -> f64 {
    if position.mark_price > 0.0 {
        position.mark_price
    } else {
        position.entry_price.max(0.0)
    }
}

pub fn compute_portfolio_exposure(
    balances: &[Balance],
    positions: &[Position],
    hedge_symbol: &str,
    hedge_ratio: f64,
    target_inventory_notional_cap: f64,
    max_portfolio_exposure_multiple: f64,
) -> PortfolioExposure {
    let account_equity_usd = compute_account_equity_usd(balances);
    let mut snapshot = PortfolioExposure {
        account_equity_usd,
        max_portfolio_net_exposure_usd: account_equity_usd * max_portfolio_exposure_multiple,
        measured_at: Utc::now(),
        ..PortfolioExposure::default()
    };

    for position in positions {
        let notional_usd = position_notional_usd(position);
        if notional_usd.abs() < 1e-9 {
            continue;
        }

        let exposure = PositionExposure {
            symbol: position.symbol.clone(),
            side: position.side.clone(),
            signed_quantity: signed_position_quantity(position),
            mark_price: effective_mark_price(position),
            notional_usd,
        };

        snapshot.portfolio_net_exposure_usd += notional_usd;
        if position.symbol == hedge_symbol {
            snapshot.hedge_symbol_exposure_usd += notional_usd;
        } else {
            snapshot.portfolio_core_exposure_usd += notional_usd;
        }
        snapshot.positions.push(exposure);
    }

    let raw_target = -snapshot.portfolio_core_exposure_usd * hedge_ratio;
    snapshot.hedge_target_notional_usd = raw_target.clamp(
        -target_inventory_notional_cap.abs(),
        target_inventory_notional_cap.abs(),
    );

    snapshot
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::{Balance, MarketType, Position};

    fn make_position(symbol: &str, side: &str, qty: f64, entry: f64, mark: f64) -> Position {
        Position {
            symbol: symbol.to_string(),
            side: side.to_string(),
            contracts: qty,
            contract_size: 1.0,
            entry_price: entry,
            mark_price: mark,
            unrealized_pnl: 0.0,
            percentage: 0.0,
            margin: 0.0,
            margin_ratio: 0.0,
            leverage: Some(1),
            margin_type: Some("cross".to_string()),
            size: qty,
            amount: qty,
            timestamp: Utc::now(),
        }
    }

    #[test]
    fn computes_target_from_core_exposure_not_self_referential_total() {
        let balances = vec![Balance {
            currency: "USDT".to_string(),
            total: 733.36,
            free: 733.36,
            used: 0.0,
            market_type: MarketType::Futures,
        }];

        let positions = vec![
            make_position("BTC/USDT", "SHORT", 0.02, 80_000.0, 79_000.0),
            make_position("DOGE/USDC", "LONG", 2_000.0, 0.12, 0.10),
        ];

        let snapshot =
            compute_portfolio_exposure(&balances, &positions, "DOGE/USDC", 0.4, 650.0, 10.0);

        assert!((snapshot.portfolio_net_exposure_usd + 1_380.0).abs() < 1e-6);
        assert!((snapshot.portfolio_core_exposure_usd + 1_580.0).abs() < 1e-6);
        assert!((snapshot.hedge_target_notional_usd - 632.0).abs() < 1e-6);
        assert!((snapshot.max_portfolio_net_exposure_usd - 7_333.6).abs() < 1e-6);
    }

    #[test]
    fn parses_dual_side_quantities_into_signed_notional() {
        let long = make_position("DOGE/USDC", "LONG", 1_000.0, 0.1, 0.11);
        let short = make_position("DOGE/USDC", "SHORT", 500.0, 0.1, 0.11);

        assert_eq!(signed_position_quantity(&long), 1_000.0);
        assert_eq!(signed_position_quantity(&short), -500.0);
        assert!((position_notional_usd(&long) - 110.0).abs() < 1e-9);
        assert!((position_notional_usd(&short) + 55.0).abs() < 1e-9);
    }
}
