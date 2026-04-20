use crate::core::{
    config::{ApiKeys, Config},
    types::{
        AccountSnapshot, Balance, BatchOrderRequest, BatchOrderResponse, Deposit, DepositAddress,
        ExchangeInfo, IndexPriceKline, Interval, Kline, MarkPriceKline, MarketType, OpenInterest,
        Order, OrderBook, OrderRequest, OrderSide, Position, Result, Statistics24h, Ticker, Trade,
        TradeFee, TradingPair, Withdrawal,
    },
    websocket::WebSocketClient,
};
use async_trait::async_trait;
use std::collections::HashMap;

/// 交易所通用接口trait
#[async_trait]
pub trait Exchange: Send + Sync {
    /// 获取交易所名称
    fn name(&self) -> &str;

    /// 支持向下转型到具体类型
    fn as_any(&self) -> &dyn std::any::Any;

    /// 获取交易所信息
    async fn exchange_info(&self) -> Result<ExchangeInfo>;

    /// 获取余额 - 支持现货和期货
    async fn get_balance(&self, market_type: MarketType) -> Result<Vec<Balance>>;

    /// 获取行情信息
    async fn get_ticker(&self, symbol: &str, market_type: MarketType) -> Result<Ticker>;

    /// 获取所有行情信息
    async fn get_all_tickers(&self, market_type: MarketType) -> Result<Vec<Ticker>>;

    /// 获取订单簿
    async fn get_orderbook(
        &self,
        symbol: &str,
        market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<OrderBook>;

    /// 创建订单
    async fn create_order(&self, order_request: OrderRequest) -> Result<Order>;

    /// 取消订单
    async fn cancel_order(
        &self,
        order_id: &str,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<Order>;

    /// 获取订单状态
    async fn get_order(
        &self,
        order_id: &str,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<Order>;

    /// 获取活跃订单
    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
    ) -> Result<Vec<Order>>;

    /// 获取订单历史
    async fn get_order_history(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<Vec<Order>>;

    /// 获取成交记录
    async fn get_trades(
        &self,
        symbol: &str,
        market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<Vec<Trade>>;

    /// 获取我的成交记录
    async fn get_my_trades(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<Vec<Trade>>;

    // === 新增高级功能 ===

    /// 获取K线数据
    async fn get_klines(
        &self,
        symbol: &str,
        interval: Interval,
        market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<Vec<Kline>>;

    /// 获取24小时统计数据
    async fn get_24h_statistics(
        &self,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<Statistics24h>;

    /// 获取所有24小时统计数据
    async fn get_all_24h_statistics(&self, market_type: MarketType) -> Result<Vec<Statistics24h>>;

    /// 获取交易手续费
    async fn get_trade_fee(&self, symbol: &str, market_type: MarketType) -> Result<TradeFee>;

    /// 获取账户资产快照
    async fn get_account_snapshot(&self, market_type: MarketType) -> Result<AccountSnapshot>;

    /// 获取持仓信息(仅期货)
    async fn get_positions(&self, symbol: Option<&str>) -> Result<Vec<Position>>;

    /// 设置杠杆(仅期货)
    async fn set_leverage(&self, symbol: &str, leverage: u32) -> Result<()>;

    /// 批量取消订单
    async fn cancel_all_orders(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
    ) -> Result<Vec<Order>>;

    /// 批量取消指定订单ID列表
    async fn cancel_multiple_orders(
        &self,
        order_ids: Vec<String>,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<Vec<Order>> {
        // 默认实现：逐个取消
        let mut results = Vec::new();
        for order_id in order_ids {
            match self.cancel_order(&order_id, symbol, market_type).await {
                Ok(order) => results.push(order),
                Err(e) => log::warn!("取消订单 {} 失败: {}", order_id, e),
            }
        }
        Ok(results)
    }

    /// 获取服务器时间
    async fn get_server_time(&self) -> Result<chrono::DateTime<chrono::Utc>>;

    /// 测试连接
    async fn ping(&self) -> Result<()>;

    // === 批量操作和交易对管理 ===

    /// 批量下单
    async fn create_batch_orders(
        &self,
        batch_request: BatchOrderRequest,
    ) -> Result<BatchOrderResponse>;

    /// 获取所有现货交易对
    async fn get_all_spot_symbols(&self) -> Result<Vec<TradingPair>>;

    /// 获取所有期货交易对
    async fn get_all_futures_symbols(&self) -> Result<Vec<TradingPair>>;

    /// 获取特定交易对详细信息
    async fn get_symbol_info(&self, symbol: &str, market_type: MarketType) -> Result<TradingPair>;

    // === WebSocket 功能 ===

    /// 创建 WebSocket 客户端
    async fn create_websocket_client(
        &self,
        market_type: MarketType,
    ) -> Result<Box<dyn WebSocketClient>>;

    /// 获取 WebSocket URL
    fn get_websocket_url(&self, market_type: MarketType) -> String {
        // 默认实现，子类可以覆盖
        format!("wss://stream.{}.com", self.name())
    }

    /// 创建用户数据流（返回ListenKey）
    async fn create_user_data_stream(&self, market_type: MarketType) -> Result<String> {
        // 默认实现返回错误，需要交易所具体实现
        log::warn!(
            "🔍 Exchange trait 默认实现的 create_user_data_stream 被调用了! market_type: {:?}",
            market_type
        );
        Err(crate::core::error::ExchangeError::NotSupported(
            "用户数据流不支持".to_string(),
        ))
    }

    /// 延长用户数据流有效期
    async fn keepalive_user_data_stream(
        &self,
        listen_key: &str,
        market_type: MarketType,
    ) -> Result<()> {
        // 默认实现返回错误，需要交易所具体实现
        Err(crate::core::error::ExchangeError::NotSupported(
            "用户数据流不支持".to_string(),
        ))
    }

    /// 关闭用户数据流
    async fn close_user_data_stream(
        &self,
        listen_key: &str,
        market_type: MarketType,
    ) -> Result<()> {
        // 默认实现返回错误，需要交易所具体实现
        Err(crate::core::error::ExchangeError::NotSupported(
            "用户数据流不支持".to_string(),
        ))
    }

    // === 高级交易功能（可选实现） ===

    /// 修改订单（支持修改价格和/或数量）
    /// 注意：不同交易所有不同限制
    async fn modify_order(
        &self,
        order_id: &str,
        symbol: &str,
        new_price: Option<f64>,
        new_amount: Option<f64>,
        market_type: MarketType,
    ) -> Result<Order> {
        // 默认实现：取消原订单并创建新订单
        let original = self.get_order(order_id, symbol, market_type).await?;
        self.cancel_order(order_id, symbol, market_type).await?;

        let order_request = OrderRequest {
            symbol: symbol.to_string(),
            market_type,
            side: original.side,
            order_type: original.order_type,
            amount: new_amount.unwrap_or(original.amount),
            price: new_price.or(original.price),
            params: None,
            client_order_id: None,
            time_in_force: None,
            reduce_only: None,
            post_only: None,
        };

        self.create_order(order_request).await
    }

    /// 获取资金费率历史（期货）
    async fn get_funding_rate_history(
        &self,
        _symbol: &str,
        _start_time: Option<chrono::DateTime<chrono::Utc>>,
        _end_time: Option<chrono::DateTime<chrono::Utc>>,
        _limit: Option<u32>,
    ) -> Result<Vec<crate::core::types::FundingRateHistory>> {
        Err(crate::core::error::ExchangeError::NotSupported(
            "资金费率历史查询未实现".to_string(),
        ))
    }

    /// 账户间资金划转（现货<->期货<->杠杆）
    async fn transfer_between_accounts(
        &self,
        _currency: &str,
        _amount: f64,
        _from_account: &str,
        _to_account: &str,
    ) -> Result<crate::core::types::TransferResult> {
        Err(crate::core::error::ExchangeError::NotSupported(
            "账户间转账未实现".to_string(),
        ))
    }

    /// 获取聚合深度（按价格级别聚合）
    async fn get_aggregated_orderbook(
        &self,
        symbol: &str,
        market_type: MarketType,
        level: u32,
    ) -> Result<OrderBook> {
        // 默认实现：获取深度后在客户端聚合
        let orderbook = self.get_orderbook(symbol, market_type, Some(100)).await?;

        // 根据级别计算聚合精度
        let tick_size = match level {
            0 => return Ok(orderbook), // 原始精度
            1 => 0.1,
            2 => 1.0,
            3 => 10.0,
            4 => 100.0,
            _ => 1000.0,
        };

        Ok(aggregate_orderbook(&orderbook, tick_size))
    }

    /// 获取标记价格（期货）
    async fn get_mark_price(
        &self,
        _symbol: Option<&str>,
    ) -> Result<Vec<crate::core::types::MarkPrice>> {
        Err(crate::core::error::ExchangeError::NotSupported(
            "标记价格查询未实现".to_string(),
        ))
    }

    /// 获取仓位风险（期货）
    async fn get_position_risk(
        &self,
        _symbol: Option<&str>,
    ) -> Result<Vec<crate::core::types::PositionRisk>> {
        Err(crate::core::error::ExchangeError::NotSupported(
            "仓位风险查询未实现".to_string(),
        ))
    }

    /// 设置持仓模式（单向/双向）
    async fn set_position_mode(&self, _dual_side: bool) -> Result<()> {
        Err(crate::core::error::ExchangeError::NotSupported(
            "持仓模式设置未实现".to_string(),
        ))
    }

    /// 调整仓位保证金（逐仓）
    async fn adjust_position_margin(
        &self,
        _symbol: &str,
        _amount: f64,
        _add: bool,
    ) -> Result<crate::core::types::PositionMargin> {
        Err(crate::core::error::ExchangeError::NotSupported(
            "仓位保证金调整未实现".to_string(),
        ))
    }

    // === 高级订单类型 ===

    /// 创建止损单
    async fn create_stop_order(
        &self,
        symbol: &str,
        side: OrderSide,
        amount: f64,
        stop_price: f64,
        price: Option<f64>,
        market_type: MarketType,
    ) -> Result<Order> {
        Err(crate::core::error::ExchangeError::NotSupported(
            "止损单未实现".to_string(),
        ))
    }

    /// 创建跟踪止损单
    async fn create_trailing_stop_order(
        &self,
        symbol: &str,
        side: OrderSide,
        amount: f64,
        trail_percent: Option<f64>,
        trail_value: Option<f64>,
        market_type: MarketType,
    ) -> Result<Order> {
        Err(crate::core::error::ExchangeError::NotSupported(
            "跟踪止损单未实现".to_string(),
        ))
    }

    /// 按金额市价买入
    async fn create_market_order_with_cost(
        &self,
        symbol: &str,
        cost: f64,
        market_type: MarketType,
    ) -> Result<Order> {
        Err(crate::core::error::ExchangeError::NotSupported(
            "按金额市价买入未实现".to_string(),
        ))
    }

    // === 扩展市场数据 ===

    /// 获取未平仓合约量
    async fn get_open_interest(&self, symbol: &str) -> Result<OpenInterest> {
        Err(crate::core::error::ExchangeError::NotSupported(
            "未平仓合约量查询未实现".to_string(),
        ))
    }

    /// 获取标记价格K线
    async fn get_mark_price_klines(
        &self,
        symbol: &str,
        interval: Interval,
        limit: Option<u32>,
    ) -> Result<Vec<MarkPriceKline>> {
        Err(crate::core::error::ExchangeError::NotSupported(
            "标记价格K线查询未实现".to_string(),
        ))
    }

    /// 获取指数价格K线
    async fn get_index_price_klines(
        &self,
        symbol: &str,
        interval: Interval,
        limit: Option<u32>,
    ) -> Result<Vec<IndexPriceKline>> {
        Err(crate::core::error::ExchangeError::NotSupported(
            "指数价格K线查询未实现".to_string(),
        ))
    }

    // === 账户管理扩展 ===

    /// 获取充值地址
    async fn get_deposit_address(
        &self,
        currency: &str,
        network: Option<&str>,
    ) -> Result<DepositAddress> {
        Err(crate::core::error::ExchangeError::NotSupported(
            "充值地址查询未实现".to_string(),
        ))
    }

    /// 获取充值记录
    async fn get_deposit_history(
        &self,
        currency: Option<&str>,
        limit: Option<u32>,
    ) -> Result<Vec<Deposit>> {
        Err(crate::core::error::ExchangeError::NotSupported(
            "充值记录查询未实现".to_string(),
        ))
    }

    /// 获取提现记录
    async fn get_withdrawal_history(
        &self,
        currency: Option<&str>,
        limit: Option<u32>,
    ) -> Result<Vec<Withdrawal>> {
        Err(crate::core::error::ExchangeError::NotSupported(
            "提现记录查询未实现".to_string(),
        ))
    }

    /// 提现
    async fn withdraw(
        &self,
        currency: &str,
        amount: f64,
        address: &str,
        tag: Option<&str>,
        network: Option<&str>,
    ) -> Result<Withdrawal> {
        Err(crate::core::error::ExchangeError::NotSupported(
            "提现功能未实现".to_string(),
        ))
    }
}

/// 基础交易所实现
#[derive(Clone)]
pub struct BaseExchange {
    pub name: String,
    pub config: Config,
    pub api_keys: ApiKeys,
    pub client: reqwest::Client,
}

impl BaseExchange {
    /// 创建新的交易所实例
    pub fn new(name: String, config: Config, api_keys: ApiKeys) -> Self {
        let client = reqwest::Client::builder()
            .user_agent("RustCTA/0.1.0")
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("创建HTTP客户端失败");

        Self {
            name,
            config,
            api_keys,
            client,
        }
    }

    /// 获取交易所特定配置
    pub fn get_exchange_config(&self) -> Result<&crate::core::config::ExchangeConfig> {
        // 先尝试从配置中获取
        match self.config.get_exchange_config(&self.name.to_lowercase()) {
            Ok(config) => Ok(config),
            Err(_) => {
                // 如果没有配置，返回一个错误（但先记录日志）
                log::debug!("交易所 {} 没有配置，使用默认设置", self.name);
                // 暂时返回错误，后续可以考虑返回默认配置
                Err(crate::core::error::ExchangeError::ConfigError(format!(
                    "交易所 {} 没有配置",
                    self.name
                )))
            }
        }
    }
}

/// 聚合订单簿深度的辅助函数
pub fn aggregate_orderbook(orderbook: &OrderBook, tick_size: f64) -> OrderBook {
    let mut aggregated_bids: HashMap<i64, f64> = HashMap::new();
    let mut aggregated_asks: HashMap<i64, f64> = HashMap::new();

    // 聚合买单
    for bid in &orderbook.bids {
        let price_level = (bid[0] / tick_size).floor() as i64;
        let entry = aggregated_bids.entry(price_level).or_insert(0.0);
        *entry += bid[1];
    }

    // 聚合卖单
    for ask in &orderbook.asks {
        let price_level = (ask[0] / tick_size).ceil() as i64;
        let entry = aggregated_asks.entry(price_level).or_insert(0.0);
        *entry += ask[1];
    }

    // 转换回数组格式
    let mut bids: Vec<[f64; 2]> = aggregated_bids
        .into_iter()
        .map(|(level, amount)| [level as f64 * tick_size, amount])
        .collect();
    bids.sort_by(|a, b| b[0].partial_cmp(&a[0]).unwrap());

    let mut asks: Vec<[f64; 2]> = aggregated_asks
        .into_iter()
        .map(|(level, amount)| [level as f64 * tick_size, amount])
        .collect();
    asks.sort_by(|a, b| a[0].partial_cmp(&b[0]).unwrap());

    OrderBook {
        symbol: orderbook.symbol.clone(),
        bids,
        asks,
        timestamp: orderbook.timestamp,
        info: serde_json::Value::Null,
    }
}

// 为Box<dyn Exchange>实现Exchange trait，使其可以透明使用
#[async_trait]
impl Exchange for Box<dyn Exchange> {
    fn name(&self) -> &str {
        self.as_ref().name()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self.as_ref().as_any()
    }

    async fn exchange_info(&self) -> Result<ExchangeInfo> {
        self.as_ref().exchange_info().await
    }

    async fn get_balance(&self, market_type: MarketType) -> Result<Vec<Balance>> {
        self.as_ref().get_balance(market_type).await
    }

    async fn get_ticker(&self, symbol: &str, market_type: MarketType) -> Result<Ticker> {
        self.as_ref().get_ticker(symbol, market_type).await
    }

    async fn get_all_tickers(&self, market_type: MarketType) -> Result<Vec<Ticker>> {
        self.as_ref().get_all_tickers(market_type).await
    }

    async fn get_orderbook(
        &self,
        symbol: &str,
        market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<OrderBook> {
        self.as_ref()
            .get_orderbook(symbol, market_type, limit)
            .await
    }

    async fn create_order(&self, order: OrderRequest) -> Result<Order> {
        self.as_ref().create_order(order).await
    }

    async fn cancel_order(
        &self,
        order_id: &str,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<Order> {
        self.as_ref()
            .cancel_order(order_id, symbol, market_type)
            .await
    }

    async fn cancel_all_orders(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
    ) -> Result<Vec<Order>> {
        self.as_ref().cancel_all_orders(symbol, market_type).await
    }

    async fn get_order(
        &self,
        order_id: &str,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<Order> {
        self.as_ref().get_order(order_id, symbol, market_type).await
    }

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
    ) -> Result<Vec<Order>> {
        self.as_ref().get_open_orders(symbol, market_type).await
    }

    async fn get_order_history(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<Vec<Order>> {
        self.as_ref()
            .get_order_history(symbol, market_type, limit)
            .await
    }

    async fn get_trades(
        &self,
        symbol: &str,
        market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<Vec<Trade>> {
        self.as_ref().get_trades(symbol, market_type, limit).await
    }

    async fn get_klines(
        &self,
        symbol: &str,
        interval: Interval,
        market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<Vec<Kline>> {
        self.as_ref()
            .get_klines(symbol, interval, market_type, limit)
            .await
    }

    async fn get_24h_statistics(
        &self,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<Statistics24h> {
        self.as_ref().get_24h_statistics(symbol, market_type).await
    }

    async fn get_all_24h_statistics(&self, market_type: MarketType) -> Result<Vec<Statistics24h>> {
        self.as_ref().get_all_24h_statistics(market_type).await
    }

    async fn get_my_trades(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<Vec<Trade>> {
        self.as_ref()
            .get_my_trades(symbol, market_type, limit)
            .await
    }

    async fn get_trade_fee(&self, symbol: &str, market_type: MarketType) -> Result<TradeFee> {
        self.as_ref().get_trade_fee(symbol, market_type).await
    }

    async fn get_positions(&self, symbol: Option<&str>) -> Result<Vec<Position>> {
        self.as_ref().get_positions(symbol).await
    }

    async fn get_account_snapshot(&self, market_type: MarketType) -> Result<AccountSnapshot> {
        self.as_ref().get_account_snapshot(market_type).await
    }

    async fn set_leverage(&self, symbol: &str, leverage: u32) -> Result<()> {
        self.as_ref().set_leverage(symbol, leverage).await
    }

    async fn get_server_time(&self) -> Result<chrono::DateTime<chrono::Utc>> {
        self.as_ref().get_server_time().await
    }

    async fn ping(&self) -> Result<()> {
        self.as_ref().ping().await
    }

    async fn create_batch_orders(
        &self,
        batch_request: BatchOrderRequest,
    ) -> Result<BatchOrderResponse> {
        self.as_ref().create_batch_orders(batch_request).await
    }

    async fn get_all_spot_symbols(&self) -> Result<Vec<TradingPair>> {
        self.as_ref().get_all_spot_symbols().await
    }

    async fn get_all_futures_symbols(&self) -> Result<Vec<TradingPair>> {
        self.as_ref().get_all_futures_symbols().await
    }

    async fn get_symbol_info(&self, symbol: &str, market_type: MarketType) -> Result<TradingPair> {
        self.as_ref().get_symbol_info(symbol, market_type).await
    }

    async fn get_open_interest(&self, symbol: &str) -> Result<OpenInterest> {
        self.as_ref().get_open_interest(symbol).await
    }

    async fn get_mark_price_klines(
        &self,
        symbol: &str,
        interval: Interval,
        limit: Option<u32>,
    ) -> Result<Vec<MarkPriceKline>> {
        self.as_ref()
            .get_mark_price_klines(symbol, interval, limit)
            .await
    }

    async fn get_index_price_klines(
        &self,
        symbol: &str,
        interval: Interval,
        limit: Option<u32>,
    ) -> Result<Vec<IndexPriceKline>> {
        self.as_ref()
            .get_index_price_klines(symbol, interval, limit)
            .await
    }

    async fn create_websocket_client(
        &self,
        market_type: MarketType,
    ) -> Result<Box<dyn WebSocketClient>> {
        self.as_ref().create_websocket_client(market_type).await
    }

    async fn get_deposit_address(
        &self,
        coin: &str,
        network: Option<&str>,
    ) -> Result<DepositAddress> {
        self.as_ref().get_deposit_address(coin, network).await
    }

    async fn get_deposit_history(
        &self,
        currency: Option<&str>,
        limit: Option<u32>,
    ) -> Result<Vec<Deposit>> {
        self.as_ref().get_deposit_history(currency, limit).await
    }

    async fn withdraw(
        &self,
        coin: &str,
        amount: f64,
        address: &str,
        network: Option<&str>,
        memo: Option<&str>,
    ) -> Result<Withdrawal> {
        self.as_ref()
            .withdraw(coin, amount, address, network, memo)
            .await
    }

    async fn modify_order(
        &self,
        order_id: &str,
        symbol: &str,
        new_price: Option<f64>,
        new_amount: Option<f64>,
        market_type: MarketType,
    ) -> Result<Order> {
        self.as_ref()
            .modify_order(order_id, symbol, new_price, new_amount, market_type)
            .await
    }

    async fn get_funding_rate_history(
        &self,
        symbol: &str,
        start_time: Option<chrono::DateTime<chrono::Utc>>,
        end_time: Option<chrono::DateTime<chrono::Utc>>,
        limit: Option<u32>,
    ) -> Result<Vec<crate::core::types::FundingRateHistory>> {
        self.as_ref()
            .get_funding_rate_history(symbol, start_time, end_time, limit)
            .await
    }

    async fn transfer_between_accounts(
        &self,
        currency: &str,
        amount: f64,
        from_account: &str,
        to_account: &str,
    ) -> Result<crate::core::types::TransferResult> {
        self.as_ref()
            .transfer_between_accounts(currency, amount, from_account, to_account)
            .await
    }

    fn get_websocket_url(&self, market_type: MarketType) -> String {
        self.as_ref().get_websocket_url(market_type)
    }

    async fn create_user_data_stream(&self, market_type: MarketType) -> Result<String> {
        self.as_ref().create_user_data_stream(market_type).await
    }

    async fn keepalive_user_data_stream(
        &self,
        listen_key: &str,
        market_type: MarketType,
    ) -> Result<()> {
        self.as_ref()
            .keepalive_user_data_stream(listen_key, market_type)
            .await
    }

    async fn close_user_data_stream(
        &self,
        listen_key: &str,
        market_type: MarketType,
    ) -> Result<()> {
        self.as_ref()
            .close_user_data_stream(listen_key, market_type)
            .await
    }

    async fn cancel_multiple_orders(
        &self,
        order_ids: Vec<String>,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<Vec<Order>> {
        self.as_ref()
            .cancel_multiple_orders(order_ids, symbol, market_type)
            .await
    }

    async fn set_position_mode(&self, dual_side: bool) -> Result<()> {
        self.as_ref().set_position_mode(dual_side).await
    }

    async fn get_aggregated_orderbook(
        &self,
        symbol: &str,
        market_type: MarketType,
        levels: u32,
    ) -> Result<OrderBook> {
        self.as_ref()
            .get_aggregated_orderbook(symbol, market_type, levels)
            .await
    }

    async fn get_mark_price(
        &self,
        symbol: Option<&str>,
    ) -> Result<Vec<crate::core::types::MarkPrice>> {
        self.as_ref().get_mark_price(symbol).await
    }

    async fn get_position_risk(
        &self,
        symbol: Option<&str>,
    ) -> Result<Vec<crate::core::types::PositionRisk>> {
        self.as_ref().get_position_risk(symbol).await
    }

    async fn adjust_position_margin(
        &self,
        symbol: &str,
        amount: f64,
        add: bool,
    ) -> Result<crate::core::types::PositionMargin> {
        self.as_ref()
            .adjust_position_margin(symbol, amount, add)
            .await
    }

    async fn create_stop_order(
        &self,
        symbol: &str,
        side: OrderSide,
        amount: f64,
        stop_price: f64,
        price: Option<f64>,
        market_type: MarketType,
    ) -> Result<Order> {
        self.as_ref()
            .create_stop_order(symbol, side, amount, stop_price, price, market_type)
            .await
    }

    async fn create_trailing_stop_order(
        &self,
        symbol: &str,
        side: OrderSide,
        amount: f64,
        trail_percent: Option<f64>,
        trail_value: Option<f64>,
        market_type: MarketType,
    ) -> Result<Order> {
        self.as_ref()
            .create_trailing_stop_order(
                symbol,
                side,
                amount,
                trail_percent,
                trail_value,
                market_type,
            )
            .await
    }

    async fn create_market_order_with_cost(
        &self,
        symbol: &str,
        cost: f64,
        market_type: MarketType,
    ) -> Result<Order> {
        self.as_ref()
            .create_market_order_with_cost(symbol, cost, market_type)
            .await
    }

    async fn get_withdrawal_history(
        &self,
        currency: Option<&str>,
        limit: Option<u32>,
    ) -> Result<Vec<crate::core::types::Withdrawal>> {
        self.as_ref().get_withdrawal_history(currency, limit).await
    }
}
