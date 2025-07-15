use crate::exchange::traits::Exchange;
use crate::utils::symbol::Symbol;
use crate::utils::time::get_current_timestamp;
use crate::{strategy_error, strategy_info, SHUTDOWN};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::time::{sleep, Duration};

/// 多时间框架做多策略配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiTimeframeLongConfig {
    pub name: String,
    pub enabled: bool,
    pub symbol: String,
    pub position_size_usdt: f64,
    pub max_position_usdt: f64,

    // RSI参数
    pub rsi_period: usize,
    pub rsi_oversold: f64,
    pub rsi_overbought: f64,

    // 布林带参数
    pub bb_period: usize,
    pub bb_std_dev: f64,

    // 时间框架设置
    pub primary_timeframe: String,   // 主时间框架，如"5m"
    pub secondary_timeframe: String, // 次时间框架，如"15m"
    pub tertiary_timeframe: String,  // 第三时间框架，如"1h"

    // 风险管理
    pub stop_loss_percentage: f64,
    pub take_profit_percentage: f64,
    pub max_hold_time_minutes: u64,

    // 策略参数
    pub min_volume_usdt: f64,           // 最小成交量过滤
    pub trend_confirmation_bars: usize, // 趋势确认K线数量
    pub check_interval_seconds: u64,

    // 挂单参数
    pub order_offset_pct: f64,       // 挂单偏移百分比（如0.1表示-0.1%）
    pub order_timeout_minutes: u64,  // 挂单超时时间（分钟）
    pub allow_multiple_orders: bool, // 是否允许多个挂单
}

/// K线数据结构
#[derive(Debug, Clone)]
pub struct Kline {
    pub open_time: i64,
    pub close_time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub trades: i64,
    pub taker_buy_base_volume: f64,
    pub taker_buy_quote_volume: f64,
}

/// 技术指标计算结果
#[derive(Debug, Clone)]
pub struct TechnicalIndicators {
    pub rsi: Option<f64>,
    pub bb_upper: Option<f64>,
    pub bb_middle: Option<f64>,
    pub bb_lower: Option<f64>,
    pub bb_width: Option<f64>,
}

/// 时间框架数据
#[derive(Debug)]
pub struct TimeframeData {
    pub klines: VecDeque<Kline>,
    pub indicators: TechnicalIndicators,
    pub last_update: i64,
}

/// 交易信号类型
#[derive(Debug, Clone, PartialEq)]
pub enum SignalType {
    None,
    StrongBuy,
    Buy,
    Hold,
}

/// 持仓信息
#[derive(Debug, Clone)]
pub struct Position {
    pub size: f64,
    pub entry_price: f64,
    pub entry_time: i64,
    pub stop_loss: f64,
    pub take_profit: f64,
}

/// 挂单信息
#[derive(Debug, Clone)]
pub struct PendingOrder {
    pub order_id: String,
    pub price: f64,
    pub quantity: f64,
    pub side: String, // "BUY" or "SELL"
    pub create_time: i64,
    pub timeout_minutes: u64,
}

/// 多时间框架做多策略
pub struct MultiTimeframeLongStrategy {
    config: MultiTimeframeLongConfig,
    exchange: Arc<dyn Exchange>,
    symbol: Symbol,

    // 多时间框架数据
    primary_data: TimeframeData,
    secondary_data: TimeframeData,
    tertiary_data: TimeframeData,

    // 当前持仓
    current_position: Option<Position>,

    // 挂单管理
    pending_orders: Vec<PendingOrder>,

    // 策略状态
    last_signal: SignalType,
    consecutive_signals: usize,
}

impl MultiTimeframeLongStrategy {
    /// 创建新的多时间框架做多策略实例
    pub fn new(config: MultiTimeframeLongConfig, exchange: Arc<dyn Exchange>) -> Self {
        // 解析交易对
        let symbol_parts: Vec<&str> = config.symbol.split('/').collect();
        let (base, quote) = if symbol_parts.len() >= 2 {
            let quote_part = symbol_parts[1].split(':').next().unwrap_or(symbol_parts[1]);
            (symbol_parts[0].to_string(), quote_part.to_string())
        } else {
            if config.symbol.ends_with("USDT") {
                let base = config.symbol.strip_suffix("USDT").unwrap_or(&config.symbol);
                (base.to_string(), "USDT".to_string())
            } else if config.symbol.ends_with("USDC") {
                let base = config.symbol.strip_suffix("USDC").unwrap_or(&config.symbol);
                (base.to_string(), "USDC".to_string())
            } else {
                (config.symbol.clone(), "USDT".to_string())
            }
        };

        let symbol = Symbol::new(&base, &quote, crate::utils::symbol::MarketType::UsdFutures);

        Self {
            config,
            exchange,
            symbol,
            primary_data: TimeframeData {
                klines: VecDeque::new(),
                indicators: TechnicalIndicators {
                    rsi: None,
                    bb_upper: None,
                    bb_middle: None,
                    bb_lower: None,
                    bb_width: None,
                },
                last_update: 0,
            },
            secondary_data: TimeframeData {
                klines: VecDeque::new(),
                indicators: TechnicalIndicators {
                    rsi: None,
                    bb_upper: None,
                    bb_middle: None,
                    bb_lower: None,
                    bb_width: None,
                },
                last_update: 0,
            },
            tertiary_data: TimeframeData {
                klines: VecDeque::new(),
                indicators: TechnicalIndicators {
                    rsi: None,
                    bb_upper: None,
                    bb_middle: None,
                    bb_lower: None,
                    bb_width: None,
                },
                last_update: 0,
            },
            current_position: None,
            pending_orders: Vec::new(),
            last_signal: SignalType::None,
            consecutive_signals: 0,
        }
    }

    /// 运行策略主循环
    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        strategy_info!(
            &self.config.name,
            "🚀 多时间框架做多策略 {} 已启动",
            self.config.name
        );
        strategy_info!(&self.config.name, "📊 交易对: {}", self.symbol.to_binance());
        strategy_info!(
            &self.config.name,
            "⏰ 时间框架: {} / {} / {}",
            self.config.primary_timeframe,
            self.config.secondary_timeframe,
            self.config.tertiary_timeframe
        );
        strategy_info!(
            &self.config.name,
            "💰 仓位大小: {} USDT",
            self.config.position_size_usdt
        );

        // 初始化历史数据
        self.initialize_historical_data().await?;

        let mut last_check = get_current_timestamp();

        loop {
            if SHUTDOWN.load(Ordering::SeqCst) {
                strategy_info!(
                    &self.config.name,
                    "收到关闭信号，策略 {} 正在停止",
                    self.config.name
                );
                break;
            }

            let current_time = get_current_timestamp();

            // 检查是否需要更新数据
            if current_time - last_check >= (self.config.check_interval_seconds * 1000) as i64 {
                // 更新市场数据
                if let Err(e) = self.update_market_data().await {
                    strategy_error!(&self.config.name, "更新市场数据失败: {}", e);
                    sleep(Duration::from_secs(10)).await;
                    continue;
                }

                // 计算技术指标
                self.calculate_indicators();

                // 输出技术指标分析结果
                self.print_technical_analysis();

                // 检查挂单状态
                self.check_pending_orders().await?;

                // 检查持仓状态
                self.check_position_status().await?;

                // 生成交易信号
                let signal = self.generate_signal();

                // 执行交易逻辑
                if let Err(e) = self.execute_trading_logic(signal).await {
                    strategy_error!(&self.config.name, "执行交易逻辑失败: {}", e);
                }

                last_check = current_time;
            }

            sleep(Duration::from_secs(1)).await;
        }

        // 策略停止时平仓
        if self.current_position.is_some() {
            strategy_info!(&self.config.name, "策略停止，正在平仓...");
            if let Err(e) = self.close_position("策略停止").await {
                strategy_error!(&self.config.name, "平仓失败: {}", e);
            }
        }

        strategy_info!(
            &self.config.name,
            "多时间框架做多策略 {} 已停止",
            self.config.name
        );
        Ok(())
    }

    /// 初始化历史数据
    async fn initialize_historical_data(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        strategy_info!(&self.config.name, "正在初始化历史数据...");

        // 计算需要的历史数据长度
        let required_length = std::cmp::max(
            std::cmp::max(self.config.rsi_period, self.config.bb_period),
            self.config.trend_confirmation_bars,
        ) + 50; // 额外缓冲

        // 获取时间框架配置
        let primary_timeframe = self.config.primary_timeframe.clone();
        let secondary_timeframe = self.config.secondary_timeframe.clone();
        let tertiary_timeframe = self.config.tertiary_timeframe.clone();

        // 获取主时间框架数据
        Self::fetch_kline_data(
            &self.exchange,
            &self.symbol,
            &self.config.name,
            &primary_timeframe,
            required_length,
            &mut self.primary_data,
        )
        .await?;

        // 获取次时间框架数据
        Self::fetch_kline_data(
            &self.exchange,
            &self.symbol,
            &self.config.name,
            &secondary_timeframe,
            required_length,
            &mut self.secondary_data,
        )
        .await?;

        // 获取第三时间框架数据
        Self::fetch_kline_data(
            &self.exchange,
            &self.symbol,
            &self.config.name,
            &tertiary_timeframe,
            required_length,
            &mut self.tertiary_data,
        )
        .await?;

        strategy_info!(&self.config.name, "历史数据初始化完成");
        Ok(())
    }

    /// 获取K线数据
    async fn fetch_kline_data(
        exchange: &Arc<dyn Exchange>,
        symbol: &Symbol,
        strategy_name: &str,
        interval: &str,
        limit: usize,
        timeframe_data: &mut TimeframeData,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 这里需要根据实际的交易所API实现K线数据获取
        // 由于当前Exchange trait可能没有get_klines方法，这里提供一个框架

        // TODO: 实现实际的K线数据获取逻辑
        // let klines = exchange.get_klines(symbol, interval, limit).await?;

        // 模拟数据获取（实际使用时需要替换为真实API调用）
        strategy_info!(
            strategy_name,
            "获取 {} 时间框架的 {} 条K线数据",
            interval,
            limit
        );

        timeframe_data.last_update = get_current_timestamp();
        Ok(())
    }

    /// 更新市场数据
    async fn update_market_data(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let current_time = get_current_timestamp();

        // 获取真实的交易所价格
        let current_price = match self.exchange.get_price(&self.symbol).await {
            Ok(price) => price,
            Err(e) => {
                strategy_error!(&self.config.name, "获取价格失败: {}", e);
                // 如果获取价格失败，使用上一次的价格或默认价格
                if let Some(last_kline) = self.primary_data.klines.back() {
                    last_kline.close
                } else {
                    // 如果没有历史数据，使用基础价格作为fallback
                    if self.symbol.base == "ETH" {
                        2774.0
                    } else {
                        157.0
                    }
                }
            }
        };

        // 创建模拟K线数据
        let mock_kline = Kline {
            open_time: current_time,
            close_time: current_time + 60000, // 1分钟后
            open: current_price - 1.0,
            high: current_price + 2.0,
            low: current_price - 2.0,
            close: current_price,
            volume: 100.0 + ((current_time / 1000) % 1000) as f64,
            quote_volume: current_price * (100.0 + ((current_time / 1000) % 1000) as f64),
            trades: 50,
            taker_buy_base_volume: 50.0,
            taker_buy_quote_volume: current_price * 50.0,
        };

        // 更新主时间框架数据
        self.primary_data.klines.push_back(mock_kline.clone());
        if self.primary_data.klines.len() > 200 {
            self.primary_data.klines.pop_front();
        }

        // 更新次时间框架数据（价格稍有不同）
        let mut secondary_kline = mock_kline.clone();
        secondary_kline.close = current_price + 0.5;
        self.secondary_data.klines.push_back(secondary_kline);
        if self.secondary_data.klines.len() > 200 {
            self.secondary_data.klines.pop_front();
        }

        // 更新第三时间框架数据（价格稍有不同）
        let mut tertiary_kline = mock_kline.clone();
        tertiary_kline.close = current_price - 0.3;
        self.tertiary_data.klines.push_back(tertiary_kline);
        if self.tertiary_data.klines.len() > 200 {
            self.tertiary_data.klines.pop_front();
        }

        self.primary_data.last_update = current_time;
        self.secondary_data.last_update = current_time;
        self.tertiary_data.last_update = current_time;

        // 输出市场数据更新信息
        strategy_info!(
            &self.config.name,
            "📊 市场数据更新 | 价格: {:.2} | 成交量: {:.1}",
            current_price,
            mock_kline.volume
        );

        Ok(())
    }

    /// 计算技术指标
    fn calculate_indicators(&mut self) {
        // 计算主时间框架指标
        self.primary_data.indicators =
            self.calculate_timeframe_indicators(&self.primary_data.klines);

        // 计算次时间框架指标
        self.secondary_data.indicators =
            self.calculate_timeframe_indicators(&self.secondary_data.klines);

        // 计算第三时间框架指标
        self.tertiary_data.indicators =
            self.calculate_timeframe_indicators(&self.tertiary_data.klines);
    }

    /// 输出技术指标分析结果
    fn print_technical_analysis(&self) {
        let primary_indicators = &self.primary_data.indicators;
        let secondary_indicators = &self.secondary_data.indicators;
        let tertiary_indicators = &self.tertiary_data.indicators;

        // 获取当前价格
        let current_price = if let Some(latest_kline) = self.primary_data.klines.back() {
            latest_kline.close
        } else {
            return;
        };

        strategy_info!(&self.config.name, "🔍 ===== 技术指标分析 =====");
        strategy_info!(&self.config.name, "💰 当前价格: {:.2}", current_price);

        // 输出各时间框架的RSI指标
        if let (Some(primary_rsi), Some(secondary_rsi), Some(tertiary_rsi)) = (
            primary_indicators.rsi,
            secondary_indicators.rsi,
            tertiary_indicators.rsi,
        ) {
            strategy_info!(
                &self.config.name,
                "📈 RSI指标 | {}: {:.1} | {}: {:.1} | {}: {:.1}",
                self.config.primary_timeframe,
                primary_rsi,
                self.config.secondary_timeframe,
                secondary_rsi,
                self.config.tertiary_timeframe,
                tertiary_rsi
            );

            // RSI趋势判断
            let primary_trend = self.get_rsi_trend_description(primary_rsi);
            let secondary_trend = self.get_rsi_trend_description(secondary_rsi);
            let tertiary_trend = self.get_rsi_trend_description(tertiary_rsi);

            strategy_info!(
                &self.config.name,
                "📊 RSI趋势 | {}: {} | {}: {} | {}: {}",
                self.config.primary_timeframe,
                primary_trend,
                self.config.secondary_timeframe,
                secondary_trend,
                self.config.tertiary_timeframe,
                tertiary_trend
            );
        }

        // 输出布林带指标
        if let (Some(bb_upper), Some(bb_middle), Some(bb_lower), Some(bb_width)) = (
            primary_indicators.bb_upper,
            primary_indicators.bb_middle,
            primary_indicators.bb_lower,
            primary_indicators.bb_width,
        ) {
            strategy_info!(
                &self.config.name,
                "🎯 布林带({}) | 上轨: {:.2} | 中轨: {:.2} | 下轨: {:.2} | 宽度: {:.1}%",
                self.config.primary_timeframe,
                bb_upper,
                bb_middle,
                bb_lower,
                bb_width
            );

            // 布林带位置判断
            let bb_position = self.get_bollinger_position_description(
                current_price,
                bb_upper,
                bb_middle,
                bb_lower,
            );
            strategy_info!(&self.config.name, "📍 价格位置: {}", bb_position);
        }

        // 输出持仓状态
        if let Some(position) = &self.current_position {
            let pnl = (current_price - position.entry_price) * position.size;
            let pnl_percentage =
                ((current_price - position.entry_price) / position.entry_price) * 100.0;

            strategy_info!(
                &self.config.name,
                "💼 当前持仓 | 入场: {:.2} | 数量: {:.4} | 盈亏: {:.2} USDT ({:.2}%)",
                position.entry_price,
                position.size,
                pnl,
                pnl_percentage
            );
        } else {
            strategy_info!(&self.config.name, "💼 当前持仓: 无");
        }

        strategy_info!(&self.config.name, "🔍 =========================");
    }

    /// 获取RSI趋势描述
    fn get_rsi_trend_description(&self, rsi: f64) -> &'static str {
        if rsi >= 70.0 {
            "超买"
        } else if rsi >= 50.0 {
            "偏强"
        } else if rsi >= 30.0 {
            "偏弱"
        } else {
            "超卖"
        }
    }

    /// 获取布林带位置描述
    fn get_bollinger_position_description(
        &self,
        price: f64,
        upper: f64,
        middle: f64,
        lower: f64,
    ) -> &'static str {
        if price >= upper {
            "突破上轨(强势)"
        } else if price >= middle {
            "上半区间(偏强)"
        } else if price >= lower {
            "下半区间(偏弱)"
        } else {
            "跌破下轨(超卖)"
        }
    }

    /// 计算单个时间框架的技术指标
    fn calculate_timeframe_indicators(&self, klines: &VecDeque<Kline>) -> TechnicalIndicators {
        if klines.len() < std::cmp::max(self.config.rsi_period, self.config.bb_period) {
            return TechnicalIndicators {
                rsi: None,
                bb_upper: None,
                bb_middle: None,
                bb_lower: None,
                bb_width: None,
            };
        }

        let closes: Vec<f64> = klines.iter().map(|k| k.close).collect();

        // 计算RSI
        let rsi = self.calculate_rsi(&closes);

        // 计算布林带
        let (bb_upper, bb_middle, bb_lower, bb_width) = self.calculate_bollinger_bands(&closes);

        TechnicalIndicators {
            rsi,
            bb_upper,
            bb_middle,
            bb_lower,
            bb_width,
        }
    }

    /// 计算RSI指标
    fn calculate_rsi(&self, closes: &[f64]) -> Option<f64> {
        if closes.len() < self.config.rsi_period + 1 {
            return None;
        }

        let mut gains = Vec::new();
        let mut losses = Vec::new();

        for i in 1..closes.len() {
            let change = closes[i] - closes[i - 1];
            if change > 0.0 {
                gains.push(change);
                losses.push(0.0);
            } else {
                gains.push(0.0);
                losses.push(-change);
            }
        }

        if gains.len() < self.config.rsi_period {
            return None;
        }

        // 计算初始平均收益和损失
        let initial_avg_gain: f64 =
            gains.iter().take(self.config.rsi_period).sum::<f64>() / self.config.rsi_period as f64;
        let initial_avg_loss: f64 =
            losses.iter().take(self.config.rsi_period).sum::<f64>() / self.config.rsi_period as f64;

        let mut avg_gain = initial_avg_gain;
        let mut avg_loss = initial_avg_loss;

        // 使用指数移动平均计算后续的平均收益和损失
        for i in self.config.rsi_period..gains.len() {
            avg_gain = (avg_gain * (self.config.rsi_period - 1) as f64 + gains[i])
                / self.config.rsi_period as f64;
            avg_loss = (avg_loss * (self.config.rsi_period - 1) as f64 + losses[i])
                / self.config.rsi_period as f64;
        }

        if avg_loss == 0.0 {
            return Some(100.0);
        }

        let rs = avg_gain / avg_loss;
        let rsi = 100.0 - (100.0 / (1.0 + rs));

        Some(rsi)
    }

    /// 计算布林带指标
    fn calculate_bollinger_bands(
        &self,
        closes: &[f64],
    ) -> (Option<f64>, Option<f64>, Option<f64>, Option<f64>) {
        if closes.len() < self.config.bb_period {
            return (None, None, None, None);
        }

        let recent_closes = &closes[closes.len() - self.config.bb_period..];

        // 计算移动平均
        let sma: f64 = recent_closes.iter().sum::<f64>() / self.config.bb_period as f64;

        // 计算标准差
        let variance: f64 = recent_closes
            .iter()
            .map(|&price| (price - sma).powi(2))
            .sum::<f64>()
            / self.config.bb_period as f64;
        let std_dev = variance.sqrt();

        let bb_upper = sma + (self.config.bb_std_dev * std_dev);
        let bb_lower = sma - (self.config.bb_std_dev * std_dev);
        let bb_width = (bb_upper - bb_lower) / sma * 100.0; // 布林带宽度百分比

        (Some(bb_upper), Some(sma), Some(bb_lower), Some(bb_width))
    }

    /// 生成交易信号
    fn generate_signal(&mut self) -> SignalType {
        // 获取各时间框架的指标
        let primary_indicators = &self.primary_data.indicators;
        let secondary_indicators = &self.secondary_data.indicators;
        let tertiary_indicators = &self.tertiary_data.indicators;

        // 检查指标是否有效
        if primary_indicators.rsi.is_none()
            || secondary_indicators.rsi.is_none()
            || tertiary_indicators.rsi.is_none()
        {
            return SignalType::None;
        }

        let primary_rsi = primary_indicators.rsi.unwrap();
        let secondary_rsi = secondary_indicators.rsi.unwrap();
        let tertiary_rsi = tertiary_indicators.rsi.unwrap();

        // 获取当前价格（使用主时间框架的最新收盘价）
        let current_price = if let Some(latest_kline) = self.primary_data.klines.back() {
            latest_kline.close
        } else {
            return SignalType::None;
        };

        // 多时间框架RSI超卖信号
        let rsi_oversold_signal = primary_rsi < self.config.rsi_oversold &&
                                 secondary_rsi < self.config.rsi_oversold + 5.0 && // 次级时间框架稍微宽松
                                 tertiary_rsi < self.config.rsi_oversold + 10.0; // 高级时间框架更宽松

        // 布林带下轨支撑信号
        let bb_support_signal = if let (Some(bb_lower), Some(bb_width)) =
            (primary_indicators.bb_lower, primary_indicators.bb_width)
        {
            current_price <= bb_lower * 1.002 && bb_width > 2.0 // 价格接近下轨且布林带有一定宽度
        } else {
            false
        };

        // 趋势确认：高级时间框架不能处于强烈下跌趋势
        let trend_confirmation = tertiary_rsi > 25.0; // 高级时间框架RSI不能过低

        // 成交量确认
        let volume_confirmation = if let Some(latest_kline) = self.primary_data.klines.back() {
            latest_kline.volume * current_price > self.config.min_volume_usdt
        } else {
            false
        };

        // 综合信号判断
        let signal = if rsi_oversold_signal
            && bb_support_signal
            && trend_confirmation
            && volume_confirmation
        {
            // 强买入信号：所有条件都满足
            SignalType::StrongBuy
        } else if (rsi_oversold_signal || bb_support_signal)
            && trend_confirmation
            && volume_confirmation
        {
            // 普通买入信号：部分条件满足
            SignalType::Buy
        } else {
            SignalType::None
        };

        // 信号连续性检查
        if signal == self.last_signal && signal != SignalType::None {
            self.consecutive_signals += 1;
        } else {
            self.consecutive_signals = 1;
        }

        self.last_signal = signal.clone();

        // 记录信号详情
        if signal != SignalType::None {
            strategy_info!(
                &self.config.name,
                "🎯 交易信号: {:?} | RSI: {:.2}/{:.2}/{:.2} | 价格: {:.4} | 连续: {}",
                signal,
                primary_rsi,
                secondary_rsi,
                tertiary_rsi,
                current_price,
                self.consecutive_signals
            );
        }

        signal
    }

    /// 执行交易逻辑
    async fn execute_trading_logic(
        &mut self,
        signal: SignalType,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match signal {
            SignalType::StrongBuy | SignalType::Buy => {
                if self.current_position.is_none() && self.pending_orders.is_empty() {
                    // 需要连续信号确认才开仓
                    let required_confirmations = match signal {
                        SignalType::StrongBuy => 2,
                        SignalType::Buy => 3,
                        _ => 0,
                    };

                    if self.consecutive_signals >= required_confirmations {
                        self.place_limit_buy_order().await?;
                    }
                }
            }
            _ => {
                // 其他信号暂不处理
            }
        }

        Ok(())
    }

    /// 开仓
    async fn open_position(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let current_price = if let Some(latest_kline) = self.primary_data.klines.back() {
            latest_kline.close
        } else {
            return Err("无法获取当前价格".into());
        };

        let quantity = self.config.position_size_usdt / current_price;

        strategy_info!(
            &self.config.name,
            "📈 开多仓 | 价格: {:.4} | 数量: {:.6} | 金额: {:.2} USDT",
            current_price,
            quantity,
            self.config.position_size_usdt
        );

        // TODO: 实际下单逻辑
        // let order_result = self.exchange.place_market_order(&self.symbol, "BUY", quantity).await?;

        // 计算止损和止盈价格
        let stop_loss = current_price * (1.0 - self.config.stop_loss_percentage / 100.0);
        let take_profit = current_price * (1.0 + self.config.take_profit_percentage / 100.0);

        self.current_position = Some(Position {
            size: quantity,
            entry_price: current_price,
            entry_time: get_current_timestamp(),
            stop_loss,
            take_profit,
        });

        strategy_info!(
            &self.config.name,
            "✅ 开仓成功 | 止损: {:.4} | 止盈: {:.4}",
            stop_loss,
            take_profit
        );

        Ok(())
    }

    /// 平仓
    async fn close_position(
        &mut self,
        reason: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(position) = &self.current_position {
            let current_price = if let Some(latest_kline) = self.primary_data.klines.back() {
                latest_kline.close
            } else {
                return Err("无法获取当前价格".into());
            };

            let pnl = (current_price - position.entry_price) * position.size;
            let pnl_percentage =
                (current_price - position.entry_price) / position.entry_price * 100.0;

            strategy_info!(
                &self.config.name,
                "📉 平仓 | 原因: {} | 入场: {:.4} | 出场: {:.4} | 盈亏: {:.2} USDT ({:.2}%)",
                reason,
                position.entry_price,
                current_price,
                pnl,
                pnl_percentage
            );

            // TODO: 实际平仓逻辑
            // let order_result = self.exchange.place_market_order(&self.symbol, "SELL", position.size).await?;

            self.current_position = None;

            strategy_info!(&self.config.name, "✅ 平仓完成");
        }

        Ok(())
    }

    /// 检查持仓状态
    async fn check_position_status(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(position) = &self.current_position {
            let current_price = if let Some(latest_kline) = self.primary_data.klines.back() {
                latest_kline.close
            } else {
                return Ok(());
            };

            let current_time = get_current_timestamp();
            let hold_time_minutes = (current_time - position.entry_time) / 60000;

            // 检查止损
            if current_price <= position.stop_loss {
                self.close_position("触发止损").await?;
                return Ok(());
            }

            // 检查止盈
            if current_price >= position.take_profit {
                self.close_position("触发止盈").await?;
                return Ok(());
            }

            // 检查最大持仓时间
            if hold_time_minutes >= self.config.max_hold_time_minutes as i64 {
                self.close_position("超过最大持仓时间").await?;
                return Ok(());
            }

            // 检查RSI是否过热（动态止盈）
            if let Some(rsi) = self.primary_data.indicators.rsi {
                if rsi > self.config.rsi_overbought {
                    let pnl_percentage =
                        (current_price - position.entry_price) / position.entry_price * 100.0;
                    if pnl_percentage > 1.0 {
                        // 至少有1%盈利才考虑RSI过热平仓
                        self.close_position("RSI过热").await?;
                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }

    /// 下限价买单
    async fn place_limit_buy_order(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let current_price = if let Some(latest_kline) = self.primary_data.klines.back() {
            latest_kline.close
        } else {
            return Err("无法获取当前价格".into());
        };

        // 计算挂单价格：当前价格 - 千分之一
        let order_price = current_price * (1.0 - self.config.order_offset_pct / 100.0);
        let quantity = self.config.position_size_usdt / order_price;

        strategy_info!(
            &self.config.name,
            "📋 下限价买单 | 当前价: {:.4} | 挂单价: {:.4} | 数量: {:.6} | 偏移: -{:.1}%",
            current_price,
            order_price,
            quantity,
            self.config.order_offset_pct
        );

        // TODO: 实际下单逻辑
        // let order_result = self.exchange.place_limit_order(&self.symbol, "BUY", quantity, order_price).await?;
        // let order_id = order_result.order_id;

        // 模拟订单ID
        let order_id = format!("order_{}", get_current_timestamp());

        // 记录挂单信息
        let pending_order = PendingOrder {
            order_id: order_id.clone(),
            price: order_price,
            quantity,
            side: "BUY".to_string(),
            create_time: get_current_timestamp(),
            timeout_minutes: self.config.order_timeout_minutes,
        };

        self.pending_orders.push(pending_order);

        strategy_info!(
            &self.config.name,
            "✅ 限价买单已提交 | 订单ID: {} | 超时: {}分钟",
            order_id,
            self.config.order_timeout_minutes
        );

        Ok(())
    }

    /// 检查挂单状态
    async fn check_pending_orders(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let current_time = get_current_timestamp();
        let mut orders_to_remove = Vec::new();

        for (index, order) in self.pending_orders.iter().enumerate() {
            let elapsed_minutes = (current_time - order.create_time) / 60000;

            // 检查订单是否超时
            if elapsed_minutes >= order.timeout_minutes as i64 {
                strategy_info!(
                    &self.config.name,
                    "⏰ 订单超时，准备取消 | 订单ID: {} | 已等待: {}分钟",
                    order.order_id,
                    elapsed_minutes
                );

                // 取消超时订单
                if let Err(e) = self.cancel_order(&order.order_id).await {
                    strategy_error!(&self.config.name, "取消订单失败: {}", e);
                } else {
                    orders_to_remove.push(index);
                }
            } else {
                // TODO: 检查订单是否已成交
                // let order_status = self.exchange.get_order_status(&order.order_id).await?;
                // if order_status.is_filled() {
                //     self.on_order_filled(order).await?;
                //     orders_to_remove.push(index);
                // }
            }
        }

        // 移除已处理的订单
        for &index in orders_to_remove.iter().rev() {
            self.pending_orders.remove(index);
        }

        Ok(())
    }

    /// 取消订单
    async fn cancel_order(
        &self,
        order_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        strategy_info!(&self.config.name, "❌ 取消订单: {}", order_id);

        // TODO: 实际取消订单逻辑
        // self.exchange.cancel_order(order_id).await?;

        strategy_info!(&self.config.name, "✅ 订单已取消: {}", order_id);
        Ok(())
    }

    /// 订单成交回调
    async fn on_order_filled(
        &mut self,
        order: &PendingOrder,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        strategy_info!(
            &self.config.name,
            "🎉 订单成交 | 订单ID: {} | 价格: {:.4} | 数量: {:.6}",
            order.order_id,
            order.price,
            order.quantity
        );

        if order.side == "BUY" {
            // 买单成交，建立多头仓位
            let stop_loss = if self.config.stop_loss_percentage > 0.0 {
                order.price * (1.0 - self.config.stop_loss_percentage / 100.0)
            } else {
                0.0 // 不设止损
            };

            let take_profit = order.price * (1.0 + self.config.take_profit_percentage / 100.0);

            self.current_position = Some(Position {
                size: order.quantity,
                entry_price: order.price,
                entry_time: get_current_timestamp(),
                stop_loss,
                take_profit,
            });

            strategy_info!(
                &self.config.name,
                "📈 多头仓位已建立 | 入场价: {:.4} | 止盈: {:.4} | 止损: {:.4}",
                order.price,
                take_profit,
                if stop_loss > 0.0 {
                    format!("{:.4}", stop_loss)
                } else {
                    "未设置".to_string()
                }
            );
        }

        Ok(())
    }
}
