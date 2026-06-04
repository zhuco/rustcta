use crate::smart_money::{Direction, NavState, PositionState, SimulatedFill};
use rust_decimal::Decimal;
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub struct PortfolioLedger {
    pub positions: BTreeMap<String, PositionState>,
    pub nav: NavState,
    pub max_leverage: Decimal,
}

impl PortfolioLedger {
    pub fn new(initial_capital_usdt: Decimal, max_leverage: Decimal) -> Self {
        Self {
            positions: BTreeMap::new(),
            nav: NavState {
                initial_capital_usdt,
                realized_pnl_usdt: Decimal::ZERO,
                unrealized_pnl_usdt: Decimal::ZERO,
                funding_pnl_usdt: Decimal::ZERO,
                fees_paid_usdt: Decimal::ZERO,
            },
            max_leverage,
        }
    }

    pub fn apply_fill(&mut self, fill: &SimulatedFill) {
        self.nav.apply_fill_fee(fill);
        let signed_qty = fill.filled_quantity * fill.direction.sign();
        let fill_price = fill.average_price.unwrap_or(Decimal::ZERO);
        let position = self
            .positions
            .entry(fill.symbol.clone())
            .or_insert_with(|| PositionState {
                symbol: fill.symbol.clone(),
                quantity: Decimal::ZERO,
                average_entry_price: Decimal::ZERO,
                notional_usdt: Decimal::ZERO,
                realized_pnl_usdt: Decimal::ZERO,
                unrealized_pnl_usdt: Decimal::ZERO,
                funding_pnl_usdt: Decimal::ZERO,
                fee_paid_usdt: Decimal::ZERO,
                margin_used_usdt: Decimal::ZERO,
            });
        position.fee_paid_usdt += fill.fee_usdt;
        apply_position_fill(position, signed_qty, fill_price);
        position.notional_usdt = position.quantity * fill_price;
        position.margin_used_usdt = position.notional_usdt.abs() / self.max_leverage;
        self.nav.realized_pnl_usdt = self
            .positions
            .values()
            .map(|position| position.realized_pnl_usdt)
            .sum();
        self.positions
            .retain(|_, position| position.quantity != Decimal::ZERO);
    }

    pub fn mark_to_market(&mut self, marks: &BTreeMap<String, Decimal>) {
        for (symbol, mark) in marks {
            if let Some(position) = self.positions.get_mut(symbol) {
                position.unrealized_pnl_usdt =
                    (mark - position.average_entry_price) * position.quantity;
                position.notional_usdt = position.quantity * *mark;
                position.margin_used_usdt = position.notional_usdt.abs() / self.max_leverage;
            }
        }
        self.nav.unrealized_pnl_usdt = self
            .positions
            .values()
            .map(|position| position.unrealized_pnl_usdt)
            .sum();
    }

    pub fn gross_notional_usdt(&self) -> Decimal {
        self.positions
            .values()
            .map(|position| position.notional_usdt.abs())
            .sum()
    }

    pub fn leverage(&self) -> Decimal {
        let nav = self.nav.nav_usdt();
        if nav > Decimal::ZERO {
            self.gross_notional_usdt() / nav
        } else {
            Decimal::ZERO
        }
    }
}

fn apply_position_fill(position: &mut PositionState, fill_qty: Decimal, fill_price: Decimal) {
    if fill_qty == Decimal::ZERO || fill_price <= Decimal::ZERO {
        return;
    }
    if position.quantity == Decimal::ZERO || position.quantity.signum() == fill_qty.signum() {
        let new_qty = position.quantity + fill_qty;
        let old_cost = position.average_entry_price * position.quantity.abs();
        let fill_cost = fill_price * fill_qty.abs();
        position.average_entry_price = if new_qty != Decimal::ZERO {
            (old_cost + fill_cost) / new_qty.abs()
        } else {
            Decimal::ZERO
        };
        position.quantity = new_qty;
        return;
    }

    let closing_qty = position.quantity.abs().min(fill_qty.abs());
    let realized_per_unit = if position.quantity > Decimal::ZERO {
        fill_price - position.average_entry_price
    } else {
        position.average_entry_price - fill_price
    };
    position.realized_pnl_usdt += realized_per_unit * closing_qty;
    let remaining_existing_qty = position.quantity.abs() - closing_qty;
    let remaining_fill_qty = fill_qty.abs() - closing_qty;

    if remaining_existing_qty > Decimal::ZERO {
        position.quantity = remaining_existing_qty * position.quantity.signum();
    } else if remaining_fill_qty > Decimal::ZERO {
        position.quantity = remaining_fill_qty * fill_qty.signum();
        position.average_entry_price = fill_price;
    } else {
        position.quantity = Decimal::ZERO;
        position.average_entry_price = Decimal::ZERO;
    }
}

trait DirectionSign {
    fn sign(self) -> Decimal;
}

impl DirectionSign for Direction {
    fn sign(self) -> Decimal {
        match self {
            Direction::Long => Decimal::ONE,
            Direction::Short => -Decimal::ONE,
            Direction::Flat => Decimal::ZERO,
        }
    }
}

trait DecimalSign {
    fn signum(self) -> Decimal;
}

impl DecimalSign for Decimal {
    fn signum(self) -> Decimal {
        if self > Decimal::ZERO {
            Decimal::ONE
        } else if self < Decimal::ZERO {
            -Decimal::ONE
        } else {
            Decimal::ZERO
        }
    }
}
