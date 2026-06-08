use crate::smart_money::{
    plan_portfolio_transitions, simulate_taker_market_order, Direction, OrderBookSnapshot,
    PortfolioAction, PortfolioLedger, SimulatedFill, TakerExecutionConfig, TargetPortfolio,
};
use rust_decimal::Decimal;
use std::collections::{BTreeMap, HashMap};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct RebalanceSimulationReport {
    pub fills: Vec<SimulatedFill>,
    pub missing_books: Vec<String>,
    pub final_nav_usdt: Decimal,
    pub final_gross_notional_usdt: Decimal,
    pub final_leverage: Decimal,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RebalanceSimulationError {
    #[error("target portfolio nav must be positive")]
    NonPositiveNav,
}

pub fn simulate_rebalance_to_target(
    ledger: &mut PortfolioLedger,
    current_notional: &BTreeMap<String, Decimal>,
    target: &TargetPortfolio,
    books: &HashMap<String, OrderBookSnapshot>,
    execution_config: &TakerExecutionConfig,
) -> Result<RebalanceSimulationReport, RebalanceSimulationError> {
    if target.nav_usdt <= Decimal::ZERO {
        return Err(RebalanceSimulationError::NonPositiveNav);
    }

    let transitions = plan_portfolio_transitions(current_notional, &target.target_notional);
    let mut fills = Vec::new();
    let mut missing_books = Vec::new();

    for transition in transitions {
        if transition.action == PortfolioAction::Noop || transition.delta_notional == Decimal::ZERO
        {
            continue;
        }
        let Some(book) = books.get(&transition.symbol) else {
            missing_books.push(transition.symbol);
            continue;
        };
        let direction = if transition.delta_notional > Decimal::ZERO {
            Direction::Long
        } else {
            Direction::Short
        };
        let requested_notional = transition.delta_notional.abs();
        match simulate_taker_market_order(book, direction, requested_notional, execution_config) {
            Ok(fill) => {
                ledger.apply_fill(&fill);
                fills.push(fill);
            }
            Err(_) => missing_books.push(transition.symbol),
        }
    }

    let marks = books
        .iter()
        .filter_map(|(symbol, book)| Some((symbol.clone(), book.mid_price()?)))
        .collect::<BTreeMap<_, _>>();
    ledger.mark_to_market(&marks);

    Ok(RebalanceSimulationReport {
        fills,
        missing_books,
        final_nav_usdt: ledger.nav.nav_usdt(),
        final_gross_notional_usdt: ledger.gross_notional_usdt(),
        final_leverage: ledger.leverage(),
    })
}
