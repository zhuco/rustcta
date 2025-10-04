use super::{PoissonMarketMaker, Result};
use crate::core::{
    error::ExchangeError,
    types::{MarketType, OrderRequest, OrderSide, OrderType},
};
use crate::strategies::common::{
    RiskAction, RiskDecision, RiskNotifyLevel, StrategyRiskLimits, StrategySnapshot,
};
use crate::strategies::poisson_market_maker::domain::{MMStrategyState, PoissonMMConfig};
use chrono::Utc;

impl PoissonMarketMaker {
    pub(crate) fn build_risk_limits(config: &PoissonMMConfig) -> StrategyRiskLimits {
        StrategyRiskLimits {
            warning_scale_factor: Some(0.8),
            danger_scale_factor: Some(0.4),
            stop_loss_pct: Some(config.risk.stop_loss_pct),
            max_inventory_notional: None,
            max_daily_loss: Some(config.risk.max_daily_loss),
            max_consecutive_losses: None,
            inventory_skew_limit: Some(config.risk.inventory_skew_limit),
            max_unrealized_loss: Some(config.risk.max_unrealized_loss),
        }
    }

    pub(crate) fn build_risk_snapshot(
        &self,
        state: &MMStrategyState,
        current_price: f64,
    ) -> StrategySnapshot {
        let mut snapshot = StrategySnapshot::new(self.config.name.clone());
        snapshot.exposure.notional = current_price * state.inventory;
        snapshot.exposure.net_inventory = state.inventory;
        if self.config.trading.max_inventory > 0.0 {
            snapshot.exposure.inventory_ratio = Some(
                (current_price * state.inventory.abs() / self.config.trading.max_inventory)
                    .min(10.0),
            );
        }

        snapshot.performance.realized_pnl = state.total_pnl;
        snapshot.performance.unrealized_pnl = state.inventory * (current_price - state.avg_price);
        snapshot.performance.daily_pnl = Some(state.daily_pnl);
        snapshot.performance.timestamp = Utc::now();
        snapshot.risk_limits = Some(self.risk_limits.clone());
        snapshot
    }

    pub(crate) async fn apply_risk_decision(&self, decision: RiskDecision) -> Result<()> {
        match decision.action {
            RiskAction::None => Ok(()),
            RiskAction::Notify { level, message } => {
                match level {
                    RiskNotifyLevel::Info => log::info!("ℹ️ 风险提示: {}", message),
                    RiskNotifyLevel::Warning => log::warn!("⚠️ 风险警告: {}", message),
                    RiskNotifyLevel::Danger => log::error!("❗ 风险危险: {}", message),
                }
                Ok(())
            }
            RiskAction::ScaleDown {
                scale_factor,
                reason,
            } => {
                log::warn!(
                    "⚠️ 泊松策略触发缩减: {} (scale={:.2})",
                    reason,
                    scale_factor
                );
                self.cancel_all_orders().await?;
                if scale_factor <= 0.0 {
                    self.close_all_positions().await?;
                }
                Ok(())
            }
            RiskAction::Halt { reason } => {
                log::error!("❌ 泊松策略触发停机: {}", reason);
                self.cancel_all_orders().await?;
                self.close_all_positions().await?;
                Ok(())
            }
        }
    }

    /// 风险检查
    pub(crate) async fn check_risk_limits(&self) -> Result<()> {
        let current_price = *self.current_price.read().await;
        if current_price <= 0.0 {
            return Ok(());
        }

        let snapshot = {
            let state = self.state.lock().await;
            if state.avg_price <= 0.0 {
                return Ok(());
            }
            self.build_risk_snapshot(&state, current_price)
        };

        let decision = self.risk_evaluator.evaluate(&snapshot).await;
        self.apply_risk_decision(decision).await?;

        let state = self.state.lock().await;

        // 计算未实现盈亏
        let unrealized_pnl = state.inventory * (current_price - state.avg_price);

        // 检查止损
        if unrealized_pnl < -self.config.risk.max_unrealized_loss {
            log::warn!("⚠️ 触发止损，未实现亏损: {:.2} USDC", unrealized_pnl);

            if self.config.risk.wechat_alerts {
                let symbol = self
                    .symbol_info
                    .read()
                    .await
                    .as_ref()
                    .map(|info| format!("{}/{}", info.base_asset, info.quote_asset))
                    .unwrap_or_else(|| self.config.trading.symbol.clone());
                let message = format!(
                    "🚨 【泊松策略止损】\n\
                     ⚠️ 交易对: {}\n\
                     💸 未实现亏损: {:.2} USDC\n\
                     🎯 止损阈值: -{:.2} USDC\n\
                     📊 当前库存: {:.3}\n\
                     ⏰ 时间: {}",
                    symbol,
                    unrealized_pnl,
                    self.config.risk.max_unrealized_loss,
                    state.inventory,
                    Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
                );
                crate::utils::webhook::notify_critical("PoissonMM", &message).await;
            }

            drop(state);
            self.close_all_positions().await?;
            return Ok(());
        }

        // 检查库存偏斜
        // max_inventory是USDT价值，直接计算当前库存的USDT价值
        let current_inventory_value = state.inventory.abs() * current_price;
        let inventory_ratio = current_inventory_value / self.config.trading.max_inventory;
        if inventory_ratio > self.config.risk.inventory_skew_limit {
            log::error!(
                "❌ 库存偏斜过大: {:.1}%，立即平仓！",
                inventory_ratio * 100.0
            );

            if self.config.risk.wechat_alerts {
                let symbol = self
                    .symbol_info
                    .read()
                    .await
                    .as_ref()
                    .map(|info| format!("{}/{}", info.base_asset, info.quote_asset))
                    .unwrap_or_else(|| self.config.trading.symbol.clone());
                let message = format!(
                    "⚠️ 【泊松策略减仓】\n\
                     📈 交易对: {}\n\
                     📊 当前库存: {:.3} (价值: {:.2} USDC)\n\
                     ⚖️ 库存偏斜: {:.1}%\n\
                     🎯 偏斜阈值: {:.1}%\n\
                     💵 当前价格: {:.4}\n\
                     🔄 将平仓50%库存\n\
                     ⏰ 时间: {}",
                    symbol,
                    state.inventory,
                    current_inventory_value,
                    inventory_ratio * 100.0,
                    self.config.risk.inventory_skew_limit * 100.0,
                    current_price,
                    Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
                );
                crate::utils::webhook::notify_error("PoissonMM", &message).await;
            }

            // 立即取消所有挂单
            self.cancel_all_orders().await?;

            // 使用市价单平仓50%库存
            let position_to_close_raw = state.inventory * 0.5;
            // 应用精度处理，确保符合交易所要求
            let position_to_close = self.round_quantity(position_to_close_raw);

            if position_to_close.abs() > 0.001 {
                let account = self
                    .account_manager
                    .get_account(&self.config.account.account_id)
                    .ok_or_else(|| ExchangeError::Other("账户不存在".to_string()))?;
                let exchange = account.exchange.clone();
                let symbol = self.config.trading.symbol.clone();

                if position_to_close > 0.0 {
                    // 多头，需要卖出
                    log::warn!(
                        "📉 市价卖出 {} {} 以降低库存（原始数量: {}）",
                        position_to_close,
                        symbol,
                        position_to_close_raw
                    );
                    let order_req = OrderRequest {
                        symbol: symbol.clone(),
                        side: OrderSide::Sell,
                        order_type: OrderType::Market,
                        amount: position_to_close,
                        price: None,
                        client_order_id: Some(format!("POISSON_RISK_{}", Utc::now().timestamp())),
                        market_type: MarketType::Futures,
                        params: None,
                        time_in_force: None,
                        reduce_only: Some(true),
                        post_only: None,
                    };
                    match exchange.create_order(order_req).await {
                        Ok(_) => log::info!("✅ 平仓订单已提交"),
                        Err(e) => log::error!("平仓失败: {}", e),
                    }
                } else {
                    // 空头，需要买入
                    log::warn!(
                        "📈 市价买入 {} {} 以降低库存（原始数量: {}）",
                        position_to_close.abs(),
                        symbol,
                        position_to_close_raw.abs()
                    );
                    let order_req = OrderRequest {
                        symbol: symbol.clone(),
                        side: OrderSide::Buy,
                        order_type: OrderType::Market,
                        amount: position_to_close.abs(),
                        price: None,
                        client_order_id: Some(format!("POISSON_RISK_{}", Utc::now().timestamp())),
                        market_type: MarketType::Futures,
                        params: None,
                        time_in_force: None,
                        reduce_only: Some(true),
                        post_only: None,
                    };
                    match exchange.create_order(order_req).await {
                        Ok(_) => log::info!("✅ 平仓订单已提交"),
                        Err(e) => log::error!("平仓失败: {}", e),
                    }
                }

                // 暂停策略60秒，期间每10秒更新一次仓位
                log::warn!("⏸️ 暂停策略60秒等待平仓完成");
                for i in 0..6 {
                    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

                    // 更新仓位状态
                    if let Err(e) = self.update_position_status().await {
                        log::error!("更新仓位失败: {}", e);
                    } else {
                        // 检查仓位是否已经平掉
                        let updated_state = self.state.lock().await;
                        let updated_inventory = updated_state.inventory.abs();
                        let current_price = *self.current_price.read().await;
                        let inventory_value = updated_inventory * current_price;
                        let inventory_ratio = inventory_value / self.config.trading.max_inventory;

                        let symbol = self
                            .symbol_info
                            .read()
                            .await
                            .as_ref()
                            .map(|info| format!("{}/{}", info.base_asset, info.quote_asset))
                            .unwrap_or_else(|| "UNKNOWN".to_string());
                        log::info!(
                            "📊 平仓进度 {}/6: 当前库存 {:.3} {}，偏斜 {:.1}%",
                            i + 1,
                            updated_state.inventory,
                            symbol,
                            inventory_ratio * 100.0
                        );

                        // 如果仓位已经恢复正常，提前结束等待
                        if inventory_ratio < self.config.risk.inventory_skew_limit * 0.8 {
                            log::info!("✅ 仓位已恢复正常，继续做市");

                            // 发送恢复通知
                            let recovery_message = format!(
                                "✅ 【泊松策略恢复】\n\
                                 📈 交易对: {}\n\
                                 📊 当前库存: {:.3}\n\
                                 ⚖️ 库存偏斜: {:.1}%\n\
                                 💵 当前价格: {:.4}\n\
                                 ✅ 策略已恢复正常做市\n\
                                 ⏰ 时间: {}",
                                symbol,
                                updated_state.inventory,
                                inventory_ratio * 100.0,
                                current_price,
                                Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
                            );
                            if self.config.risk.wechat_alerts {
                                crate::utils::webhook::notify_error("PoissonMM", &recovery_message)
                                    .await;
                            }
                            break;
                        }
                    }
                }
            }
        }

        // 检查日亏损
        if state.daily_pnl < -self.config.risk.max_daily_loss {
            log::error!("❌ 达到日最大亏损限制: {:.2} USDC", state.daily_pnl);
            drop(state);
            *self.running.write().await = false;
        }

        Ok(())
    }
}
