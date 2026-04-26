use std::collections::HashMap;

use crate::core::types::OrderSide;
use crate::strategies::common::{MarketRegime, SharedSymbolData};
use crate::strategies::trend::config::{AllocationRegimesConfig, EntryAllocation};
use crate::strategies::trend::signal_generator::{SignalType, TradeSignal};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntryMode {
    MovingAverage,
    Fibonacci,
    Bollinger,
}

impl EntryMode {
    pub const ALL: [EntryMode; 3] = [
        EntryMode::MovingAverage,
        EntryMode::Fibonacci,
        EntryMode::Bollinger,
    ];

    pub fn label(&self) -> &'static str {
        match self {
            EntryMode::MovingAverage => "ma",
            EntryMode::Fibonacci => "fib",
            EntryMode::Bollinger => "bb",
        }
    }
}

#[derive(Debug, Clone)]
pub struct EntryAllocationSlice {
    pub mode: EntryMode,
    pub weight: f64,
    pub notional: f64,
}

#[derive(Debug, Clone)]
pub struct AllocationResult {
    pub regime: MarketRegime,
    pub total_notional: f64,
    pub per_mode: Vec<EntryAllocationSlice>,
    pub rejected: Vec<(EntryMode, String)>,
    pub adjustments: Vec<String>,
}

impl AllocationResult {
    pub fn is_empty(&self) -> bool {
        self.per_mode.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct EntryOrderPlan {
    pub mode: EntryMode,
    pub signal_type: SignalType,
    pub price: f64,
    pub side: OrderSide,
    pub notional: f64,
    pub weight: f64,
    pub rationale: String,
}

#[derive(Clone)]
pub struct CapitalAllocator {
    leverage: f64,
    base: EntryAllocation,
    regimes: AllocationRegimesConfig,
    fixed_notional: Option<f64>,
}

impl CapitalAllocator {
    pub fn new(
        leverage: f64,
        base: EntryAllocation,
        regimes: AllocationRegimesConfig,
        fixed_notional: Option<f64>,
    ) -> Self {
        Self {
            leverage: leverage.max(0.1),
            base,
            regimes,
            fixed_notional,
        }
    }

    pub fn planned_notional(&self, equity: f64) -> f64 {
        if let Some(value) = self.fixed_notional {
            value.max(0.0)
        } else {
            (equity.max(0.0)) * self.leverage
        }
    }

    pub fn allocate(
        &self,
        regime: MarketRegime,
        total_notional: f64,
        min_notional: f64,
    ) -> AllocationResult {
        let mut result = AllocationResult {
            regime,
            total_notional,
            per_mode: Vec::new(),
            rejected: Vec::new(),
            adjustments: Vec::new(),
        };

        if total_notional <= 0.0 {
            return result;
        }

        let weights = self.weights_for(regime).normalized();
        let modes_len = EntryMode::ALL.len() as f64;
        if total_notional < min_notional * modes_len {
            // 资金不足以覆盖所有模式时，集中在权重最高的模式
            let top_mode = EntryMode::ALL
                .iter()
                .copied()
                .max_by(|a, b| {
                    let wa = match a {
                        EntryMode::MovingAverage => weights.ma,
                        EntryMode::Fibonacci => weights.fib,
                        EntryMode::Bollinger => weights.bb,
                    };
                    let wb = match b {
                        EntryMode::MovingAverage => weights.ma,
                        EntryMode::Fibonacci => weights.fib,
                        EntryMode::Bollinger => weights.bb,
                    };
                    wa.partial_cmp(&wb).unwrap_or(std::cmp::Ordering::Equal)
                })
                .unwrap_or(EntryMode::MovingAverage);

            result.per_mode.push(EntryAllocationSlice {
                mode: top_mode,
                weight: 1.0,
                notional: total_notional,
            });
            return result;
        }

        let mut remaining: Vec<(EntryMode, f64)> = EntryMode::ALL
            .iter()
            .copied()
            .map(|mode| {
                let weight = match mode {
                    EntryMode::MovingAverage => weights.ma,
                    EntryMode::Fibonacci => weights.fib,
                    EntryMode::Bollinger => weights.bb,
                };
                (mode, weight.max(0.0))
            })
            .collect();

        if remaining.is_empty() {
            return result;
        }

        let mut remaining_total = total_notional;
        let mut assigned: Vec<(EntryMode, f64)> = Vec::new();

        loop {
            if remaining.is_empty() {
                break;
            }

            let weight_sum: f64 = remaining.iter().map(|(_, w)| w).sum();
            if weight_sum <= 0.0 {
                let equal = remaining_total / remaining.len() as f64;
                for (mode, _) in remaining.drain(..) {
                    assigned.push((mode, equal.max(min_notional)));
                }
                break;
            }

            let mut next = Vec::new();
            let mut updated = false;
            for (mode, weight) in remaining.into_iter() {
                let share = remaining_total * weight / weight_sum;
                if share < min_notional {
                    assigned.push((mode, min_notional));
                    remaining_total -= min_notional;
                    updated = true;
                } else {
                    next.push((mode, weight));
                }
            }

            if !updated {
                for (mode, weight) in next.into_iter() {
                    let share = remaining_total * weight / weight_sum;
                    assigned.push((mode, share.max(min_notional)));
                }
                break;
            }

            if remaining_total <= 0.0 {
                break;
            }

            remaining = next;
        }

        let sum_assigned: f64 = assigned.iter().map(|(_, v)| *v).sum();
        if sum_assigned > 0.0 && (sum_assigned - total_notional).abs() > 1e-6 {
            let scale = (total_notional / sum_assigned).min(1.0);
            for (_, value) in assigned.iter_mut() {
                *value *= scale;
            }
            if scale < 1.0 {
                result
                    .adjustments
                    .push("scaled allocations to match budget".to_string());
            }
        }

        for (mode, notional) in assigned.into_iter() {
            result.per_mode.push(EntryAllocationSlice {
                mode,
                weight: 1.0 / EntryMode::ALL.len() as f64,
                notional: notional.max(min_notional),
            });
        }

        result
    }

    fn weights_for(&self, regime: MarketRegime) -> EntryAllocation {
        match regime {
            MarketRegime::Trending => self.regimes.trending.clone(),
            MarketRegime::Extreme => self.regimes.extreme.clone(),
            MarketRegime::Ranging => self.regimes.ranging.clone(),
        }
    }
}

#[derive(Clone)]
pub struct EntryPlanner {
    base_improve_bps: f64,
}

impl EntryPlanner {
    pub fn new(base_improve_bps: f64) -> Self {
        Self {
            base_improve_bps: base_improve_bps.max(0.0),
        }
    }

    pub fn build_orders(
        &self,
        trade_signal: &TradeSignal,
        shared: &SharedSymbolData,
        allocation: &AllocationResult,
    ) -> Vec<EntryOrderPlan> {
        if allocation.per_mode.is_empty() {
            return Vec::new();
        }

        allocation
            .per_mode
            .iter()
            .filter_map(|slice| {
                let price = match slice.mode {
                    EntryMode::MovingAverage => {
                        self.price_from_moving_average(trade_signal, shared)
                    }
                    EntryMode::Fibonacci => self.price_from_fib(trade_signal, shared),
                    EntryMode::Bollinger => {
                        self.price_from_bollinger(trade_signal, shared, allocation.regime)
                    }
                }?;

                let price = self.apply_price_improve(price, trade_signal.side);

                if price <= 0.0 {
                    return None;
                }

                Some(EntryOrderPlan {
                    mode: slice.mode,
                    signal_type: trade_signal.signal_type.clone(),
                    price,
                    side: trade_signal.side,
                    notional: slice.notional,
                    weight: slice.weight,
                    rationale: format!(
                        "{} allocation {:.2}%",
                        slice.mode.label(),
                        slice.weight * 100.0
                    ),
                })
            })
            .collect()
    }

    fn price_from_moving_average(
        &self,
        trade_signal: &TradeSignal,
        shared: &SharedSymbolData,
    ) -> Option<f64> {
        let snapshot = match &shared.trend_snapshot {
            Some(s) => s,
            None => return Some(trade_signal.entry_price),
        };
        let base = (snapshot.ema_fast + snapshot.ema_slow) / 2.0;
        let offset = 0.0015;
        let price = match trade_signal.side {
            OrderSide::Buy => base * (1.0 - offset),
            OrderSide::Sell => base * (1.0 + offset),
        };
        Some(price)
    }

    fn price_from_fib(&self, trade_signal: &TradeSignal, shared: &SharedSymbolData) -> Option<f64> {
        let snapshot = match &shared.trend_snapshot {
            Some(s) => s,
            None => return Some(trade_signal.entry_price),
        };
        let anchors = &snapshot.fib_anchors;
        let price = match trade_signal.side {
            OrderSide::Buy => anchors.level_618,
            OrderSide::Sell => {
                let range = (anchors.swing_high - anchors.swing_low).abs();
                anchors.swing_low + range * 0.382
            }
        };
        Some(price)
    }

    fn price_from_bollinger(
        &self,
        trade_signal: &TradeSignal,
        shared: &SharedSymbolData,
        regime: MarketRegime,
    ) -> Option<f64> {
        let mid = shared.indicators.bollinger_5m.middle;
        let bias = match regime {
            MarketRegime::Trending => 0.0015,
            MarketRegime::Extreme => 0.0025,
            MarketRegime::Ranging => 0.0008,
        };
        let price = match trade_signal.side {
            OrderSide::Buy => mid * (1.0 - bias),
            OrderSide::Sell => mid * (1.0 + bias),
        };
        Some(price)
    }

    fn apply_price_improve(&self, price: f64, side: OrderSide) -> f64 {
        if self.base_improve_bps <= 0.0 || price <= 0.0 {
            return price;
        }
        let delta = self.base_improve_bps / 10_000.0;
        match side {
            OrderSide::Buy => price * (1.0 - delta),
            OrderSide::Sell => price * (1.0 + delta),
        }
    }
}

impl Default for EntryPlanner {
    fn default() -> Self {
        Self::new(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::OrderSide;
    use crate::strategies::common::{SharedSymbolData, TrendFibAnchors, TrendIndicatorSnapshot};
    use crate::strategies::mean_reversion::config::SymbolConfig;
    use crate::strategies::mean_reversion::indicators::{BollingerSnapshot, IndicatorOutputs};
    use crate::strategies::mean_reversion::model::{SymbolSnapshot, VolumeSnapshot};
    use crate::strategies::trend::signal_generator::{
        SignalMetadata, SignalType, TakeProfit, TradeSignal,
    };
    use chrono::Utc;
    use std::collections::HashMap;

    #[test]
    fn allocator_rebalances_when_below_min_notional() {
        let allocator = CapitalAllocator::new(
            1.0,
            EntryAllocation {
                ma: 0.6,
                fib: 0.25,
                bb: 0.15,
            },
            AllocationRegimesConfig::default(),
            None,
        );

        let result = allocator.allocate(MarketRegime::Trending, 10.0, 6.0);
        assert_eq!(result.per_mode.len(), 1);
        assert_eq!(result.per_mode[0].mode, EntryMode::MovingAverage);
        assert!((result.per_mode[0].notional - 10.0).abs() < 1e-6);
    }

    #[test]
    fn entry_planner_builds_three_orders() {
        let planner = EntryPlanner::default();
        let trade_signal = TradeSignal {
            symbol: "BTC/USDC".to_string(),
            signal_type: SignalType::TrendBreakout,
            side: OrderSide::Buy,
            entry_price: 100.0,
            stop_loss: 95.0,
            take_profits: vec![TakeProfit {
                price: 110.0,
                ratio: 0.5,
            }],
            suggested_size: 0.1,
            risk_reward_ratio: 2.0,
            confidence: 80.0,
            timeframe_aligned: true,
            has_structure_support: true,
            expire_time: Utc::now(),
            metadata: SignalMetadata {
                trend_strength: 60.0,
                volume_confirmed: true,
                key_level_nearby: false,
                pattern_name: None,
                generated_at: Utc::now(),
            },
        };

        let snapshot = SymbolSnapshot {
            config: SymbolConfig {
                symbol: "BTC/USDC".to_string(),
                enabled: true,
                min_quote_volume_5m: 0.0,
                depth_multiplier: 1.0,
                depth_levels: 5,
                enforce_spread_rule: false,
                blackout_windows: Vec::new(),
                overrides: HashMap::new(),
                allow_short: Some(true),
            },
            one_minute: Vec::new(),
            five_minute: Vec::new(),
            fifteen_minute: Vec::new(),
            one_hour: Vec::new(),
            bbw_history: vec![],
            mid_history: vec![],
            sigma_history: vec![],
            long_position: None,
            short_position: None,
            last_depth: None,
            last_volume: Some(VolumeSnapshot {
                window_minutes: 5,
                quote_volume: 0.0,
                timestamp: Utc::now(),
            }),
            frozen: false,
        };

        let indicators = IndicatorOutputs {
            timestamp: Utc::now(),
            last_price: 100.0,
            bollinger_5m: BollingerSnapshot {
                upper: 105.0,
                middle: 100.0,
                lower: 95.0,
                sigma: 1.0,
                band_percent: 0.5,
                z_score: 0.0,
            },
            bollinger_15m: BollingerSnapshot {
                upper: 104.0,
                middle: 99.0,
                lower: 94.0,
                sigma: 1.0,
                band_percent: 0.5,
                z_score: 0.0,
            },
            rsi: 55.0,
            atr: 2.0,
            adx: 20.0,
            bbw: 0.05,
            bbw_percentile: 0.5,
            slope_metric: 0.2,
            choppiness: None,
            recent_volume_quote: 0.0,
            volume_window_minutes: 5,
        };

        let shared = SharedSymbolData {
            snapshot,
            indicators,
            updated_at: Utc::now(),
            trend_snapshot: Some(TrendIndicatorSnapshot {
                ema_fast: 101.0,
                ema_slow: 99.0,
                ema_slope_1m: 0.01,
                ema_slope_5m: 0.02,
                atr_5m: 2.0,
                atr_15m: 3.0,
                boll_mid: 100.0,
                boll_bandwidth: 0.05,
                fib_anchors: TrendFibAnchors {
                    swing_high: 110.0,
                    swing_low: 90.0,
                    level_382: 102.36,
                    level_500: 100.0,
                    level_618: 97.64,
                },
                ofi: 0.0,
                orderbook_imbalance: 0.0,
                data_quality_score: 100.0,
                regime: MarketRegime::Trending,
                updated_at: Utc::now(),
            }),
        };

        let allocation = AllocationResult {
            regime: MarketRegime::Trending,
            total_notional: 100.0,
            per_mode: vec![
                EntryAllocationSlice {
                    mode: EntryMode::MovingAverage,
                    weight: 0.4,
                    notional: 40.0,
                },
                EntryAllocationSlice {
                    mode: EntryMode::Fibonacci,
                    weight: 0.3,
                    notional: 30.0,
                },
                EntryAllocationSlice {
                    mode: EntryMode::Bollinger,
                    weight: 0.3,
                    notional: 30.0,
                },
            ],
            rejected: Vec::new(),
            adjustments: Vec::new(),
        };

        let orders = planner.build_orders(&trade_signal, &shared, &allocation);
        assert_eq!(orders.len(), 3);
        assert!(orders.iter().all(|o| o.notional > 0.0));
        let ma_order = orders
            .iter()
            .find(|o| matches!(o.mode, EntryMode::MovingAverage))
            .unwrap();
        assert!(ma_order.price < trade_signal.entry_price);
    }
}
