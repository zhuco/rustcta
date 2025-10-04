use super::planner::OrderPlan;
use super::MeanReversionStrategy;

use crate::core::types::OrderSide;
use crate::strategies::common::StrategyPosition;

impl MeanReversionStrategy {
    pub(super) async fn can_open_position(
        &self,
        snapshot: &super::model::SymbolSnapshot,
        plan: &OrderPlan,
    ) -> bool {
        if snapshot.position.is_some() {
            return false;
        }

        let total_positions = self.active_positions_count().await;
        if total_positions >= self.config.risk.max_positions {
            return false;
        }

        let pending_count = {
            let states = self.symbol_states.read().await;
            states
                .get(&plan.symbol)
                .map(|state| state.pending_orders.len())
                .unwrap_or(0)
        };
        if pending_count >= self.config.risk.max_positions_per_symbol {
            return false;
        }

        let net = self.current_net_exposure().await;
        let direction = if plan.side == OrderSide::Buy {
            1.0
        } else {
            -1.0
        };
        let projected = net + direction * plan.limit_price * plan.quantity;
        if projected.abs() > self.config.risk.max_net_exposure {
            return false;
        }

        true
    }

    async fn active_positions_count(&self) -> usize {
        let states = self.symbol_states.read().await;
        states
            .values()
            .filter(|state| state.position.is_some())
            .count()
    }

    async fn current_net_exposure(&self) -> f64 {
        let states = self.symbol_states.read().await;
        states
            .values()
            .filter_map(|state| state.position.as_ref())
            .map(|pos| {
                let direction = if pos.side == OrderSide::Buy {
                    1.0
                } else {
                    -1.0
                };
                direction * pos.remaining_qty * pos.entry_price
            })
            .sum()
    }

    pub(super) async fn update_status(&self) {
        let positions = {
            let states = self.symbol_states.read().await;
            states
                .iter()
                .filter_map(|(symbol, state)| {
                    state.position.as_ref().map(|pos| StrategyPosition {
                        symbol: symbol.clone(),
                        net_position: if pos.side == OrderSide::Buy {
                            pos.remaining_qty
                        } else {
                            -pos.remaining_qty
                        },
                        notional: pos.remaining_qty * pos.entry_price,
                    })
                })
                .collect::<Vec<_>>()
        };

        let mut status = self.status.write().await;
        status.positions = positions;
        status.updated_at = chrono::Utc::now();
    }
}
