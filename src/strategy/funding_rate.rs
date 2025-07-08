use crate::error::AppError;
use crate::exchange::binance::Binance;
use crate::exchange::binance_model::{FundingRate, ExchangeInfo, Filter};
use crate::exchange::traits::Exchange;
use crate::utils::symbol::{MarketType, Symbol};
use crate::utils::precision::{adjust_price_by_filter, adjust_quantity_by_filter, validate_min_notional};
use log::{info, warn, error};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use tokio::time::{sleep, Instant};
use serde::{Deserialize, Serialize};
use crate::SHUTDOWN;
use std::sync::atomic::Ordering;
use crate::{strategy_info, strategy_warn, strategy_error};


/// 资金费率策略配置
#[derive(Debug, Clone)]
pub struct FundingRateConfig {
    /// 策略名称
    pub name: String,
    /// 是否启用
    pub enabled: bool,
    /// 默认下单金额(USDT)
    pub position_size_usd: f64,
    /// 资金费率阈值(绝对值)
    pub rate_threshold: f64,
    /// 结算前多少毫秒下单
    pub open_offset_ms: i64,
    /// 结算后多少毫秒平仓
    pub close_offset_ms: i64,
    /// 检查间隔(秒)
    pub check_interval_seconds: u64,
}

impl Default for FundingRateConfig {
    fn default() -> Self {
        Self {
            name: "FundingRateStrategy".to_string(),
            enabled: true,
            position_size_usd: 100.0,
            rate_threshold: 0.0001, // 0.01%
            open_offset_ms: 1000,   // 结算前1秒
            close_offset_ms: 20,    // 结算后20毫秒
            check_interval_seconds: 3600, // 每小时检查一次
        }
    }
}

/// 资金费率套利机会
#[derive(Debug, Clone)]
pub struct ArbitrageOpportunity {
    pub symbol: String,
    pub funding_rate: f64,
    pub mark_price: f64,
    pub next_funding_time: i64,
    pub price_precision: i32,
    pub quantity_precision: i32,
    pub min_qty: f64,
    pub min_notional: f64,
}

/// 资金费率策略
pub struct FundingRateStrategy {
    config: FundingRateConfig,
    exchange: Arc<Binance>,
}

impl FundingRateStrategy {
    /// 创建新的资金费率策略实例
    pub fn new(config: FundingRateConfig, exchange: Arc<Binance>) -> Self {
        Self { config, exchange }
    }

    /// 运行策略
    pub async fn run(&mut self) -> Result<(), AppError> {
        strategy_info!(&self.config.name, "资金费率策略启动");
        strategy_info!(&self.config.name, "配置: 下单金额={}USDT, 费率阈值={:.4}%, 结算前{}ms开仓, 结算后{}ms平仓", 
            self.config.position_size_usd,
            self.config.rate_threshold * 100.0,
            self.config.open_offset_ms,
            self.config.close_offset_ms
        );
        // 首次启动时立即检查一次
        if let Err(e) = self.check_and_find_opportunity().await {
            strategy_error!(&self.config.name, "首次检查失败: {}", e);
        }

        // 定期检查：每小时的第58分
        loop {
            if SHUTDOWN.load(Ordering::SeqCst) {
                strategy_info!(&self.config.name, "收到关闭信号，策略退出");
                break;
            }

            // 计算下次检查时间（每小时第58分）
            let next_check_time = self.calculate_next_check_time();
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
            let wait_duration = (next_check_time - now).max(0) as u64;
            
            strategy_info!(&self.config.name, "下次检查时间: {}ms，等待{}秒", 
                next_check_time, wait_duration / 1000);
            
            // 等待到下次检查时间
            if wait_duration > 0 {
                tokio::time::sleep(Duration::from_millis(wait_duration)).await;
            }
            
            // 再次检查是否收到关闭信号
            if SHUTDOWN.load(Ordering::SeqCst) {
                break;
            }
            
            // 执行检查
            if let Err(e) = self.check_and_find_opportunity().await {
                strategy_error!(&self.config.name, "检查套利机会失败: {}", e);
            }
        }

        Ok(())
    }

    /// 计算下次检查时间（每小时第58分）
    fn calculate_next_check_time(&self) -> i64 {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
        
        // 转换为秒和分钟
        let current_seconds = (now / 1000) % 3600; // 当前小时内的秒数
        let current_minutes = current_seconds / 60; // 当前分钟
        
        // 计算到第58分的秒数
        let target_seconds = 58 * 60; // 第58分对应的秒数
        
        let next_check_seconds = if current_seconds < target_seconds {
            // 如果还没到第58分，等到这个小时的第58分
            target_seconds
        } else {
            // 如果已经过了第58分，等到下个小时的第58分
            target_seconds + 3600
        };
        
        // 计算绝对时间戳
        let hour_start = (now / 1000 / 3600) * 3600 * 1000; // 当前小时开始的时间戳（毫秒）
        hour_start + (next_check_seconds as i64 * 1000)
    }

    /// 检查并寻找套利机会（只查找，不执行）
    async fn check_and_find_opportunity(&self) -> Result<(), AppError> {
        // 获取所有永续合约的资金费率
        let funding_rates = self.exchange.get_funding_rates().await?;
        let best_opportunity = self.find_best_opportunity(funding_rates).await?;

        if let Some(opportunity) = best_opportunity {
            strategy_info!(&self.config.name, "找到最佳套利机会:");
            strategy_info!(&self.config.name, "  交易对: {}", opportunity.symbol);
            strategy_info!(&self.config.name, "  资金费率: {:.6} ({:.4}%)", opportunity.funding_rate, opportunity.funding_rate * 100.0);
            strategy_info!(&self.config.name, "  下次结算时间: {}ms", opportunity.next_funding_time);
            // 执行套利
            if let Err(e) = self.execute_arbitrage(opportunity).await {
                strategy_error!(&self.config.name, "执行套利失败: {}", e);
            }
        } else {
            strategy_info!(&self.config.name, "未找到满足条件的套利机会");
        }

        Ok(())
    }

    /// 寻找最佳套利机会
    async fn find_best_opportunity(&self, funding_rates: Vec<FundingRate>) -> Result<Option<ArbitrageOpportunity>, AppError> {
        let mut best_rate = 0.0;
        let mut best_opportunity: Option<ArbitrageOpportunity> = None;

        // 获取交易所信息以获取精度信息
        let exchange_info = self.exchange.get_exchange_info(MarketType::UsdFutures).await?;

        for rate_info in funding_rates {
            let abs_rate = rate_info.last_funding_rate.abs();
            
            // 只考虑USDT永续合约
            if !rate_info.symbol.ends_with("USDT") {
                continue;
            }

            // 检查费率是否超过阈值
            if abs_rate < self.config.rate_threshold {
                continue;
            }

            // 检查是否是当前最高费率
            if abs_rate <= best_rate {
                continue;
            }

            // 查找交易对信息获取精度
            if let Some(symbol_info) = exchange_info.symbols.iter().find(|s| s.symbol == rate_info.symbol) {
                let opportunity = ArbitrageOpportunity {
                    symbol: rate_info.symbol.clone(),
                    funding_rate: rate_info.last_funding_rate,
                    mark_price: rate_info.mark_price,
                    next_funding_time: rate_info.next_funding_time,
                    price_precision: symbol_info.price_precision,
                    quantity_precision: symbol_info.quantity_precision,
                    min_qty: symbol_info.filters.iter()
                        .find_map(|f| {
                            if let Filter::LotSize { min_qty, .. } = f {
                                min_qty.parse().ok()
                            } else {
                                None
                            }
                        })
                        .unwrap_or(0.001),
                    min_notional: symbol_info.filters.iter()
                        .find_map(|f| {
                            if let Filter::MinNotional { notional } = f {
                                notional.parse().ok()
                            } else {
                                None
                            }
                        })
                        .unwrap_or(5.0),
                };

                best_rate = abs_rate;
                best_opportunity = Some(opportunity);
            }
        }

        Ok(best_opportunity)
    }

    /// 执行套利
    async fn execute_arbitrage(&self, opportunity: ArbitrageOpportunity) -> Result<(), AppError> {
        strategy_info!(&self.config.name, "开始执行套利: {}", opportunity.symbol);

        // 计算下单数量
        let base_quantity = self.config.position_size_usd / opportunity.mark_price;
        let quantity = self.adjust_quantity(base_quantity, &opportunity)?;
        
        strategy_info!(&self.config.name, "计算下单数量: 基础数量={:.6}, 调整后数量={:.6}", 
            base_quantity, quantity);

        // 验证最小名义价值
        if opportunity.mark_price * quantity < opportunity.min_notional {
            return Err(AppError::Other(format!(
                "下单金额{:.2}小于最小名义价值{:.2}",
                opportunity.mark_price * quantity,
                opportunity.min_notional
            )));
        }

        // 确定开仓方向：资金费率为正时做空，为负时做多
        let side = if opportunity.funding_rate > 0.0 { "SELL" } else { "BUY" };
        let close_side = if side == "SELL" { "BUY" } else { "SELL" };
        
        strategy_info!(&self.config.name, "开仓方向: {}, 平仓方向: {}", side, close_side);

        // 等待到开仓时间
        let open_time = opportunity.next_funding_time - self.config.open_offset_ms;
        self.wait_until_timestamp(open_time, "开仓").await?;

        // 创建Symbol对象
        let symbol = Symbol::from_str(&opportunity.symbol, MarketType::UsdFutures)
                .map_err(|e| AppError::Other(e))?;

        // 执行开仓
        strategy_info!(&self.config.name, "执行开仓: {} {} {:.6}", side, opportunity.symbol, quantity);
        let open_order = self.exchange.place_order(
            &symbol,
            side,
            "MARKET",
            quantity,
            None,
        ).await?;
        
        strategy_info!(&self.config.name, "开仓成功: 订单ID={}, 成交价格={:.4}", 
            open_order.order_id, open_order.price);

        // 等待到平仓时间
        let close_time = opportunity.next_funding_time + self.config.close_offset_ms;
        self.wait_until_timestamp(close_time, "平仓").await?;

        // 执行平仓
        strategy_info!(&self.config.name, "执行平仓: {} {} {:.6}", close_side, opportunity.symbol, quantity);
        let close_order = self.exchange.place_order(
            &symbol,
            close_side,
            "MARKET",
            quantity,
            None,
        ).await?;
        
        strategy_info!(&self.config.name, "平仓成功: 订单ID={}, 成交价格={:.4}", 
            open_order.order_id, close_order.price);

        // 计算收益
        let trade_pnl = if side == "BUY" {
            (close_order.price - open_order.price) * quantity
        } else {
            (open_order.price - close_order.price) * quantity
        };
        
        let funding_payment = -opportunity.funding_rate * self.config.position_size_usd;
        let total_pnl = trade_pnl + funding_payment;
        Ok(())
    }

    /// 调整数量精度
    fn adjust_quantity(&self, quantity: f64, opportunity: &ArbitrageOpportunity) -> Result<f64, AppError> {
        let precision = opportunity.quantity_precision as u32;
        let factor = 10_f64.powi(precision as i32);
        let adjusted = (quantity * factor).floor() / factor;
        
        if adjusted < opportunity.min_qty {
            return Err(AppError::Other(format!(
                "调整后数量{:.6}小于最小数量{:.6}",
                adjusted, opportunity.min_qty
            )));
        }
        
        Ok(adjusted)
    }

    /// 等待到指定时间戳
    async fn wait_until_timestamp(&self, timestamp_ms: i64, action: &str) -> Result<(), AppError> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let wait_ms = timestamp_ms - now_ms;
        
        if wait_ms > 0 {
            strategy_info!(&self.config.name, "等待{:.2}秒后{}", wait_ms as f64 / 1000.0, action);
            sleep(Duration::from_millis(wait_ms as u64)).await;
        } else {
            strategy_info!(&self.config.name, "时间已到，立即{}", action);
        }
        
        Ok(())
    }
}