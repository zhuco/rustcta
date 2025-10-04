use anyhow::{anyhow, Result};
use chrono::Utc;

use super::indicators::{compute_indicators, IndicatorOutputs};
use super::logging;
use super::model::{LiquiditySnapshot, SymbolMeta, SymbolSnapshot, SymbolState};
use super::utils::{ceil_to_step, round_price};
use super::MeanReversionStrategy;

use crate::core::types::{OrderSide, TradingPair};

#[derive(Clone)]
pub(super) struct OrderPlan {
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: f64,
    pub limit_price: f64,
    pub stop_price: f64,
    pub trailing_distance: f64,
    pub take_profit_primary: f64,
    pub take_profit_secondary: f64,
    pub improve: f64,
    pub indicators: IndicatorOutputs,
    pub liquidity: LiquiditySnapshot,
}

impl MeanReversionStrategy {
    pub(super) async fn evaluate_symbol(&self, symbol: &str) -> Result<()> {
        let snapshot = match self.snapshot_symbol(symbol).await {
            Some(snapshot) => snapshot,
            None => return Ok(()),
        };

        if snapshot.frozen || !snapshot.config.enabled {
            return Ok(());
        }

        let indicators = match compute_indicators(
            &snapshot,
            &self.config.indicators,
            &self.config.indicators.volume,
        ) {
            Ok(val) => val,
            Err(err) => {
                log::debug!("指标计算不足 {}: {}", symbol, err);
                return Ok(());
            }
        };

        let range_score = self.evaluate_range_conditions(&indicators);
        let regime_label = if range_score >= 2 { "Range" } else { "Trend" };
        logging::info(
            Some(symbol),
            format!(
                "横盘评分 {:.2} -> {} (ADX {:.2}, BBW分位 {:.2}, slope {:.2})",
                range_score,
                regime_label,
                indicators.adx,
                indicators.bbw_percentile,
                indicators.slope_metric
            ),
        );
        if range_score < 2 {
            return Ok(());
        }

        let liquidity = self.refresh_liquidity(symbol, &snapshot).await?;

        if !self.check_liquidity_constraints(&snapshot, &liquidity, &indicators) {
            return Ok(());
        }

        let allow_short = snapshot
            .config
            .allow_short
            .unwrap_or(self.config.account.allow_short)
            && self.config.account.allow_short;

        let symbol_meta = self.get_symbol_meta(symbol).await?;

        let mut plans = Vec::new();
        if let Some(plan) = self.build_order_plan(
            symbol,
            &snapshot,
            &indicators,
            &liquidity,
            &symbol_meta,
            OrderSide::Buy,
        )? {
            logging::debug(
                Some(symbol),
                format!(
                    "多头信号满足: band% {:.3}, z {:.3}, RSI {:.1}",
                    indicators.bollinger_5m.band_percent,
                    indicators.bollinger_5m.z_score,
                    indicators.rsi
                ),
            );
            logging::info(
                Some(symbol),
                format!(
                    "挂单计划 -> side=BUY qty={:.4} limit={:.6} stop={:.6} tp1={:.6} tp2={:.6}",
                    plan.quantity,
                    plan.limit_price,
                    plan.stop_price,
                    plan.take_profit_primary,
                    plan.take_profit_secondary
                ),
            );
            plans.push(plan);
        }

        if allow_short {
            if let Some(plan) = self.build_order_plan(
                symbol,
                &snapshot,
                &indicators,
                &liquidity,
                &symbol_meta,
                OrderSide::Sell,
            )? {
                logging::debug(
                    Some(symbol),
                    format!(
                        "空头信号满足: band% {:.3}, z {:.3}, RSI {:.1}",
                        indicators.bollinger_5m.band_percent,
                        indicators.bollinger_5m.z_score,
                        indicators.rsi
                    ),
                );
                logging::info(
                    Some(symbol),
                    format!(
                        "挂单计划 -> side=SELL qty={:.4} limit={:.6} stop={:.6} tp1={:.6} tp2={:.6}",
                        plan.quantity,
                        plan.limit_price,
                        plan.stop_price,
                        plan.take_profit_primary,
                        plan.take_profit_secondary
                    ),
                );
                plans.push(plan);
            }
        }

        for plan in plans {
            if self.can_open_position(&snapshot, &plan).await {
                if let Err(err) = self.submit_order(plan).await {
                    logging::error(Some(symbol), format!("提交订单失败: {}", err));
                }
            } else {
                logging::debug(Some(symbol), "风控阻止发单");
            }
        }

        Ok(())
    }

    fn evaluate_range_conditions(&self, indicators: &IndicatorOutputs) -> u8 {
        let mut passes = 0;
        if indicators.adx < self.config.indicators.adx.threshold {
            passes += 1;
        }
        if indicators.bbw_percentile < self.config.indicators.bbw.percentile {
            passes += 1;
        }
        if indicators.slope_metric < self.config.indicators.slope.threshold {
            passes += 1;
        }
        if let Some(ci) = indicators.choppiness {
            if let Some(chop_cfg) = &self.config.indicators.choppiness {
                if chop_cfg.enabled && ci >= chop_cfg.threshold {
                    passes += 1;
                }
            }
        }
        passes
    }

    async fn refresh_liquidity(
        &self,
        symbol: &str,
        snapshot: &SymbolSnapshot,
    ) -> Result<LiquiditySnapshot> {
        let requested_levels = snapshot.config.depth_levels.max(1) as usize;
        let api_limit = self.normalize_orderbook_limit(requested_levels);
        let exchange_symbol = self.exchange_symbol(symbol);
        let ticker = self
            .account
            .exchange
            .get_ticker(&exchange_symbol, self.market_type)
            .await?;
        let orderbook = self
            .account
            .exchange
            .get_orderbook(&exchange_symbol, self.market_type, Some(api_limit as u32))
            .await?;

        let trade_fee = match self
            .account
            .exchange
            .get_trade_fee(&exchange_symbol, self.market_type)
            .await
        {
            Ok(fee) => Some(fee),
            Err(err) => {
                log::debug!("获取手续费失败 {}: {}", symbol, err);
                None
            }
        };

        let maker_fee = trade_fee
            .as_ref()
            .and_then(|f| f.maker_fee)
            .or_else(|| trade_fee.as_ref().map(|f| f.maker))
            .unwrap_or(0.0);

        let total_bid_depth: f64 = orderbook
            .bids
            .iter()
            .take(requested_levels)
            .map(|level| level[0] * level[1])
            .sum();
        let total_ask_depth: f64 = orderbook
            .asks
            .iter()
            .take(requested_levels)
            .map(|level| level[0] * level[1])
            .sum();

        let spread = if ticker.ask > 0.0 && ticker.bid > 0.0 {
            (ticker.ask - ticker.bid).max(0.0)
        } else {
            0.0
        };

        let snapshot_liquidity = LiquiditySnapshot {
            bid_price: ticker.bid,
            ask_price: ticker.ask,
            spread,
            total_bid_depth,
            total_ask_depth,
            maker_fee: Some(maker_fee),
            timestamp: Utc::now(),
        };

        {
            let mut states = self.symbol_states.write().await;
            if let Some(state) = states.get_mut(symbol) {
                state.last_depth = Some(snapshot_liquidity.clone());
                state.last_liquidity_check = Some(snapshot_liquidity.timestamp);
            }
        }

        Ok(snapshot_liquidity)
    }

    fn check_liquidity_constraints(
        &self,
        snapshot: &SymbolSnapshot,
        liquidity: &LiquiditySnapshot,
        indicators: &IndicatorOutputs,
    ) -> bool {
        let volume_threshold = snapshot
            .config
            .min_quote_volume_5m
            .max(self.config.liquidity.min_recent_quote_volume);
        if indicators.recent_volume_quote < volume_threshold {
            log::debug!(
                "{} 成交量不足 {:.0} < {:.0}",
                snapshot.config.symbol,
                indicators.recent_volume_quote,
                volume_threshold
            );
            return false;
        }

        if liquidity.maker_fee.unwrap_or(0.0) > self.config.liquidity.maker_fee_threshold {
            log::debug!(
                "{} maker费率过高 {:.5}",
                snapshot.config.symbol,
                liquidity.maker_fee.unwrap_or(0.0)
            );
            return false;
        }

        true
    }

    pub(super) async fn get_symbol_meta(&self, symbol: &str) -> Result<SymbolMeta> {
        if let Some(meta) = self.symbol_meta.read().await.get(symbol).cloned() {
            return Ok(meta);
        }
        self.ensure_symbol_metadata(symbol).await?;
        let meta = self
            .symbol_meta
            .read()
            .await
            .get(symbol)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("{} metadata unavailable", symbol))?;
        Ok(meta)
    }

    fn build_order_plan(
        &self,
        symbol: &str,
        snapshot: &SymbolSnapshot,
        indicators: &IndicatorOutputs,
        liquidity: &LiquiditySnapshot,
        symbol_meta: &SymbolMeta,
        side: OrderSide,
    ) -> Result<Option<OrderPlan>> {
        let boll = &indicators.bollinger_5m;
        let boll_cfg = &self.config.indicators.bollinger;
        let rsi_cfg = &self.config.indicators.rsi;

        let long_entry = boll.band_percent <= boll_cfg.entry_band_pct_long
            || boll.z_score <= boll_cfg.entry_z_long;
        let short_entry = boll.band_percent >= boll_cfg.entry_band_pct_short
            || boll.z_score >= boll_cfg.entry_z_short;
        let rsi_long = indicators.rsi <= rsi_cfg.long_threshold;
        let rsi_short = indicators.rsi >= rsi_cfg.short_threshold;

        match side {
            OrderSide::Buy => {
                if !(long_entry && rsi_long) {
                    return Ok(None);
                }
            }
            OrderSide::Sell => {
                if !(short_entry && rsi_short) {
                    return Ok(None);
                }
            }
        }

        let price_ref = indicators.last_price.max(1e-6);
        let mid_price = if liquidity.bid_price > 0.0 && liquidity.ask_price > 0.0 {
            (liquidity.bid_price + liquidity.ask_price) / 2.0
        } else {
            price_ref
        };
        let spread_pct = if mid_price > 0.0 {
            liquidity.spread / mid_price
        } else {
            0.0
        };

        let exec_cfg = &self.config.execution;
        let base = exec_cfg.base_improve;
        let spread_component = 2.0 * spread_pct;
        let fee_component = liquidity.maker_fee.unwrap_or(0.0).abs() * 2.0;
        let atr_component = exec_cfg.alpha_atr * (indicators.atr / price_ref);
        let sigma_component =
            exec_cfg.beta_sigma * (indicators.bollinger_5m.sigma / price_ref.abs().max(1e-6));

        let improve = base
            .max(spread_component)
            .max(fee_component)
            .max(atr_component)
            .max(sigma_component)
            .max(0.0001);

        if spread_pct > improve / 2.0 {
            log::debug!(
                "{} 点差 {:.5} 超过改善阈值 {:.5}",
                symbol,
                spread_pct,
                improve / 2.0
            );
            return Ok(None);
        }

        let direction = if side == OrderSide::Buy { 1.0 } else { -1.0 };
        let raw_price = if side == OrderSide::Buy {
            price_ref * (1.0 - improve)
        } else {
            price_ref * (1.0 + improve)
        };
        let reference_price = match side {
            OrderSide::Buy => raw_price.min(liquidity.bid_price.max(1e-6)),
            OrderSide::Sell => raw_price.max(liquidity.ask_price.max(1e-6)),
        };

        let limit_price = round_price(
            reference_price,
            symbol_meta.tick_size,
            symbol_meta.price_precision,
            side,
        )?;
        if limit_price <= 0.0 {
            return Ok(None);
        }

        let stop_distance = self.config.indicators.atr.initial_stop_k * indicators.atr;
        if stop_distance <= 0.0 {
            return Ok(None);
        }

        let stop_price = limit_price - direction * stop_distance;
        if stop_price <= 0.0 {
            return Ok(None);
        }

        let trailing_distance = self.config.indicators.atr.trailing_stop_k * indicators.atr;

        let risk_qty = self.config.risk.per_trade_risk / stop_distance.abs().max(1e-9);
        let notional_limit_qty = self.config.risk.per_trade_notional / limit_price;
        let mut quantity = risk_qty.min(notional_limit_qty);

        let step_size = symbol_meta.step_size.max(0.0);
        if step_size > 0.0 {
            quantity = quantity.max(step_size);
        }
        quantity = quantity.max(symbol_meta.min_order_size);
        quantity = ceil_to_step(
            quantity,
            symbol_meta.step_size,
            symbol_meta.amount_precision,
        );

        if let Some(min_notional) = symbol_meta.min_notional {
            let required_qty = ceil_to_step(
                min_notional / limit_price,
                symbol_meta.step_size,
                symbol_meta.amount_precision,
            );
            quantity = quantity.max(required_qty);
        }

        quantity = ceil_to_step(
            quantity,
            symbol_meta.step_size,
            symbol_meta.amount_precision,
        );

        if quantity <= 0.0 || !quantity.is_finite() {
            return Ok(None);
        }

        let notional = quantity * limit_price;
        if let Some(min_notional) = symbol_meta.min_notional {
            if notional + f64::EPSILON < min_notional {
                log::debug!(
                    "{} 名义金额不足: {:.6} < {:.6}",
                    symbol,
                    notional,
                    min_notional
                );
                return Ok(None);
            }
        }

        if notional <= 0.0 {
            return Ok(None);
        }

        let depth_multiplier = snapshot
            .config
            .depth_multiplier
            .max(self.config.liquidity.depth_multiplier);
        let depth_ok = match side {
            OrderSide::Buy => liquidity.total_bid_depth >= notional * depth_multiplier,
            OrderSide::Sell => liquidity.total_ask_depth >= notional * depth_multiplier,
        };
        if !depth_ok {
            log::debug!(
                "{} 深度不足: 需要 {:.2} 实际 {:.2}",
                symbol,
                notional * depth_multiplier,
                if side == OrderSide::Buy {
                    liquidity.total_bid_depth
                } else {
                    liquidity.total_ask_depth
                }
            );
            return Ok(None);
        }

        let take_profit_primary = boll.middle;
        let take_profit_secondary = boll.middle + direction * boll.sigma * 0.75;

        Ok(Some(OrderPlan {
            symbol: symbol.to_string(),
            side,
            quantity,
            limit_price,
            stop_price,
            trailing_distance,
            take_profit_primary,
            take_profit_secondary,
            improve,
            indicators: indicators.clone(),
            liquidity: liquidity.clone(),
        }))
    }
    fn normalize_orderbook_limit(&self, requested: usize) -> usize {
        if requested == 0 {
            return 1;
        }

        match self.account.exchange_name.as_str() {
            "binance" => {
                const ALLOWED: [usize; 7] = [5, 10, 20, 50, 100, 500, 1000];
                ALLOWED
                    .iter()
                    .copied()
                    .find(|&limit| limit >= requested)
                    .unwrap_or(*ALLOWED.last().unwrap())
            }
            _ => requested,
        }
    }
}

pub(super) fn compute_latest_bbw(
    state: &SymbolState,
    period: usize,
    std_dev: f64,
) -> Option<(f64, f64, f64)> {
    if state.fifteen_minute.len() < period {
        return None;
    }
    let len = state.fifteen_minute.len();
    let start = len - period;
    let closes: Vec<f64> = state
        .fifteen_minute
        .iter()
        .skip(start)
        .map(|k| k.close)
        .collect();
    let (upper, middle, lower) =
        crate::utils::indicators::functions::bollinger_bands(&closes, period, std_dev)?;
    let sigma = (upper - middle) / std_dev.max(1e-9);
    let bbw = if middle.abs() < 1e-9 {
        0.0
    } else {
        (upper - lower) / middle
    };
    Some((bbw, middle, sigma))
}
