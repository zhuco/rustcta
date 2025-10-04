use super::{
    config::{IndicatorConfig, SymbolConfig},
    model::{IndicatorSnapshot, MarketRegime, RegimeDecision},
};

/// 震荡/趋势判定器
pub struct RangeRegimeClassifier<'a> {
    pub symbol_config: &'a SymbolConfig,
    pub indicator_config: &'a IndicatorConfig,
}

impl<'a> RangeRegimeClassifier<'a> {
    pub fn new(symbol_config: &'a SymbolConfig, indicator_config: &'a IndicatorConfig) -> Self {
        Self {
            symbol_config,
            indicator_config,
        }
    }

    pub fn evaluate(
        &self,
        snapshot: &IndicatorSnapshot,
        current_regime: MarketRegime,
        grid_active: bool,
    ) -> RegimeDecision {
        let filters = &self.symbol_config.regime_filters;

        let adx_ok = snapshot.adx <= filters.adx_max;
        let slope_abs = snapshot.slope_metric.abs();
        let slope_ok = slope_abs <= filters.slope_threshold;

        if !adx_ok || !slope_ok {
            let mut reasons = Vec::new();
            if !adx_ok {
                reasons.push(format!("ADX {:.2} > {:.2}", snapshot.adx, filters.adx_max));
            }
            if !slope_ok {
                reasons.push(format!(
                    "中轨斜率 {:.2} > {:.2}",
                    slope_abs, filters.slope_threshold
                ));
            }

            let new_regime = if snapshot.slope_metric >= 0.0 {
                MarketRegime::TrendUp
            } else {
                MarketRegime::TrendDown
            };

            return RegimeDecision {
                new_regime,
                activate_grid: false,
                deactivate_grid: grid_active,
                reason: Some(format!("趋势指标触发: {}", reasons.join(" | "))),
            };
        }

        let mut satisfied = 0usize;
        let mut reasons = Vec::new();

        if adx_ok {
            satisfied += 1;
            reasons.push(format!("ADX {:.2} <= {:.2}", snapshot.adx, filters.adx_max));
        }

        if snapshot.bbw_percentile <= filters.bbw_quantile {
            satisfied += 1;
            reasons.push(format!(
                "BBW分位 {:.2} <= {:.2}",
                snapshot.bbw_percentile, filters.bbw_quantile
            ));
        }

        if slope_ok {
            satisfied += 1;
            reasons.push(format!(
                "中轨斜率 {:.2} <= {:.2}",
                slope_abs, filters.slope_threshold
            ));
        }

        let rsi_cfg = &self.indicator_config.lower_timeframe.rsi;
        let rsi_in_range = snapshot.rsi >= rsi_cfg.oversold && snapshot.rsi <= rsi_cfg.overbought;
        if rsi_in_range {
            satisfied += 1;
            reasons.push(format!(
                "RSI {:.2} 位于 [{:.0}, {:.0}]",
                snapshot.rsi, rsi_cfg.oversold, rsi_cfg.overbought
            ));
        }

        let z_score_ok = snapshot.z_score.abs() <= 2.0;
        if z_score_ok {
            satisfied += 1;
            reasons.push(format!("Zscore {:.2} 绝对值 <= 2", snapshot.z_score));
        }

        let is_range = satisfied >= filters.require_conditions;

        if is_range {
            let mut decision = RegimeDecision {
                new_regime: MarketRegime::Range,
                activate_grid: !grid_active,
                deactivate_grid: false,
                reason: Some(reasons.join(" | ")),
            };
            if current_regime != MarketRegime::Range {
                decision.activate_grid = true;
            }
            decision
        } else {
            let new_regime = if snapshot.slope_metric >= 0.0 {
                MarketRegime::TrendUp
            } else {
                MarketRegime::TrendDown
            };

            let reason = format!(
                "条件满足 {} / {}，检测为趋势({:?})",
                satisfied, filters.require_conditions, new_regime
            );

            RegimeDecision {
                new_regime,
                activate_grid: false,
                deactivate_grid: grid_active,
                reason: Some(reason),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategies::range_grid::domain::config::{
        AccountConfig, AtrConfig, BollingerConfig, GridConfig, HigherTimeframeConfig,
        IndicatorConfig, LowerTimeframeConfig, MarketExitConfig, OrderConfig, RegimeFilterConfig,
        RsiConfig, SymbolConfig, SymbolPrecision,
    };

    fn sample_symbol(require_conditions: usize) -> SymbolConfig {
        SymbolConfig {
            config_id: "TEST".to_string(),
            enabled: true,
            account: AccountConfig {
                id: "acc".to_string(),
                exchange: "binance".to_string(),
                env_prefix: "BIN".to_string(),
            },
            symbol: "BTCUSDT".to_string(),
            precision: SymbolPrecision {
                price_digits: Some(2),
                amount_digits: Some(3),
            },
            grid: GridConfig {
                grid_spacing_pct: 0.3,
                levels_per_side: 3,
                base_order_notional: 100.0,
                max_position_notional: 900.0,
            },
            regime_filters: RegimeFilterConfig {
                min_liquidity_usd: 1_000_000.0,
                require_conditions,
                adx_max: 25.0,
                bbw_quantile: 0.5,
                slope_threshold: 0.2,
            },
            order: OrderConfig {
                post_only: true,
                tif: Some("GTC".to_string()),
            },
            market_exit: MarketExitConfig {
                use_market_order: true,
            },
        }
    }

    fn sample_indicator_config() -> IndicatorConfig {
        IndicatorConfig {
            lower_timeframe: LowerTimeframeConfig {
                timeframe: "5m".to_string(),
                bollinger: BollingerConfig { length: 20, k: 2.0 },
                rsi: RsiConfig {
                    length: 14,
                    overbought: 70.0,
                    oversold: 30.0,
                },
                atr: AtrConfig {
                    length: 14,
                    stop_multiplier: 2.5,
                    trail_multiplier: 1.5,
                },
            },
            higher_timeframe: HigherTimeframeConfig {
                timeframe: "15m".to_string(),
                adx_length: 14,
                adx_threshold: 20.0,
                bbw_window: 400,
                bbw_quantile: 0.35,
                slope_threshold: 0.1,
                choppiness: None,
            },
        }
    }

    #[test]
    fn classifier_identifies_range() {
        let symbol_cfg = sample_symbol(3);
        let indicator_cfg = sample_indicator_config();
        let classifier = RangeRegimeClassifier::new(&symbol_cfg, &indicator_cfg);

        let snapshot = IndicatorSnapshot {
            timestamp: chrono::Utc::now(),
            last_price: 100.0,
            lower_band_ratio: 0.5,
            z_score: 0.1,
            rsi: 50.0,
            atr: 1.5,
            adx: 15.0,
            bbw_percentile: 0.2,
            slope_metric: 0.05,
            choppiness: Some(70.0),
        };

        let decision = classifier.evaluate(&snapshot, MarketRegime::TrendUp, false);
        assert_eq!(decision.new_regime, MarketRegime::Range);
        assert!(decision.activate_grid);
        assert!(!decision.deactivate_grid);
    }

    #[test]
    fn classifier_identifies_trend() {
        let symbol_cfg = sample_symbol(2);
        let indicator_cfg = sample_indicator_config();
        let classifier = RangeRegimeClassifier::new(&symbol_cfg, &indicator_cfg);

        let snapshot = IndicatorSnapshot {
            timestamp: chrono::Utc::now(),
            last_price: 100.0,
            lower_band_ratio: 0.9,
            z_score: 3.0,
            rsi: 80.0,
            atr: 3.5,
            adx: 40.0,
            bbw_percentile: 0.8,
            slope_metric: 0.6,
            choppiness: Some(20.0),
        };

        let decision = classifier.evaluate(&snapshot, MarketRegime::Range, true);
        assert_ne!(decision.new_regime, MarketRegime::Range);
        assert!(decision.deactivate_grid);
    }
}
