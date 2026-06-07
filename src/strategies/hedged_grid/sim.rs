use chrono::{DateTime, Duration as ChronoDuration, Utc};

use super::engine::{FillEvent, GridEngine, MarketSnapshot, PositionState};
use super::ledger::OrderIntent;
use super::StrategyConfig;

pub struct SimulationSeries {
    pub prices: Vec<f64>,
    pub funding_rates: Vec<f64>,
    pub start_time: DateTime<Utc>,
    pub interval_secs: i64,
}

pub struct SimulationResult {
    pub final_equity: f64,
    pub max_drawdown: f64,
    pub total_funding: f64,
    pub trade_count: u64,
}

pub struct SimulationEngine {
    engine: GridEngine,
    initial_equity: f64,
    spread_bps: f64,
}

impl SimulationEngine {
    pub fn new(
        config: StrategyConfig,
        initial_equity: f64,
        spread_bps: f64,
    ) -> Result<Self, String> {
        let engine = GridEngine::new(config, true)?;
        Ok(Self {
            engine,
            initial_equity,
            spread_bps,
        })
    }

    pub fn run(mut self, series: SimulationSeries) -> SimulationResult {
        let mut cash = self.initial_equity;
        let mut total_funding = 0.0;
        let mut trade_count = 0u64;
        let mut max_drawdown = 0.0;
        let mut peak_equity = self.initial_equity;

        if series.prices.is_empty() {
            return SimulationResult {
                final_equity: self.initial_equity,
                max_drawdown,
                total_funding,
                trade_count,
            };
        }

        let first_price = series.prices[0];
        let first_snapshot = self.snapshot_at(first_price, series.start_time);
        let initial_position = PositionState {
            long_qty: 0.0,
            short_qty: 0.0,
            long_entry_price: 0.0,
            short_entry_price: 0.0,
            long_available: 0.0,
            short_available: 0.0,
            equity: self.initial_equity,
            maintenance_margin: 0.0,
            mark_price: first_snapshot.mark_price,
        };
        self.engine.update_position(initial_position);
        self.engine.rebuild_grid(&first_snapshot);

        for (idx, price) in series.prices.iter().enumerate() {
            let timestamp =
                series.start_time + ChronoDuration::seconds((idx as i64) * series.interval_secs);
            let snapshot = self.snapshot_at(*price, timestamp);
            let funding_rate = series.funding_rates.get(idx).cloned().unwrap_or(0.0);
            self.engine.update_funding_rate(funding_rate);
            self.engine.update_market(&snapshot);

            let mut fills = Vec::new();
            for order in self.engine.buy_orders() {
                if snapshot.last_price <= order.price {
                    fills.push(order);
                }
            }
            for order in self.engine.sell_orders() {
                if snapshot.last_price >= order.price {
                    fills.push(order);
                }
            }

            for order in fills {
                let intent = order.intent;
                trade_count += 1;
                match intent {
                    OrderIntent::OpenLongBuy => {
                        cash -= order.price * order.qty;
                    }
                    OrderIntent::CloseLongSell => {
                        cash += order.price * order.qty;
                    }
                    OrderIntent::OpenShortSell => {
                        cash += order.price * order.qty;
                    }
                    OrderIntent::CloseShortBuy => {
                        cash -= order.price * order.qty;
                    }
                }
                let fill = FillEvent {
                    order_id: order.id.clone(),
                    intent,
                    fill_qty: order.qty,
                    fill_price: order.price,
                    timestamp,
                    partial: false,
                };
                self.engine.handle_fill(fill, &snapshot);
            }

            let position = self.engine.position();
            let total_notional =
                (position.long_qty.abs() + position.short_qty.abs()) * snapshot.mark_price;
            let funding_cost = funding_rate * total_notional;
            cash -= funding_cost;
            total_funding += funding_cost;

            let equity = cash + (position.long_qty - position.short_qty) * snapshot.mark_price;
            if equity > peak_equity {
                peak_equity = equity;
            }
            let drawdown = if peak_equity > 0.0 {
                (peak_equity - equity) / peak_equity
            } else {
                0.0
            };
            if drawdown > max_drawdown {
                max_drawdown = drawdown;
            }
            let updated_position = PositionState {
                equity,
                mark_price: snapshot.mark_price,
                ..position
            };
            self.engine.update_position(updated_position);
        }

        SimulationResult {
            final_equity: cash
                + (self.engine.position().long_qty - self.engine.position().short_qty)
                    * self.engine.position().mark_price,
            max_drawdown,
            total_funding,
            trade_count,
        }
    }

    fn snapshot_at(&self, price: f64, timestamp: DateTime<Utc>) -> MarketSnapshot {
        let spread = self.spread_bps / 10_000.0;
        let best_bid = price * (1.0 - spread);
        let best_ask = price * (1.0 + spread);
        MarketSnapshot {
            best_bid,
            best_ask,
            last_price: price,
            mark_price: price,
            timestamp,
        }
    }
}
