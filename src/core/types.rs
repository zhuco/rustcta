use chrono::{DateTime, Utc};
/// 统一的类型定义模块
/// 整合了所有交易相关的数据结构
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============= 基础类型定义 =============

/// 结果类型别名
pub type Result<T> = std::result::Result<T, crate::core::error::ExchangeError>;

/// 市场类型
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarketType {
    Spot,
    Futures,
}

/// 账户类型
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AccountType {
    Spot,
    Futures,
    Margin,
    Savings,
    Mining,
    Options,
}

// ============= 基础交易数据 =============

/// 交易对符号
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Symbol {
    pub base: String,
    pub quote: String,
    pub symbol: String,
    pub market_type: MarketType,
    pub exchange_specific: Option<String>,
}

/// 账户余额
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Balance {
    pub currency: String,
    pub total: f64,
    pub free: f64,
    pub used: f64,
    pub market_type: MarketType,
}

/// 行情数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ticker {
    pub symbol: String,
    pub high: f64,
    pub low: f64,
    pub bid: f64,
    pub ask: f64,
    pub last: f64,
    pub volume: f64,
    pub timestamp: DateTime<Utc>,
}

/// 订单簿
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBook {
    pub symbol: String,
    pub bids: Vec<[f64; 2]>,
    pub asks: Vec<[f64; 2]>,
    pub timestamp: DateTime<Utc>,
}

/// 增强的订单簿（带订单数量）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnhancedOrderBook {
    pub symbol: String,
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
    pub timestamp: DateTime<Utc>,
    pub nonce: Option<u64>,
    pub sequence: Option<u64>,
}

/// 订单簿层级
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookLevel {
    pub price: f64,
    pub quantity: f64,
    pub order_count: Option<u32>,
}

// ============= 订单相关 =============

/// 订单方向
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

impl std::fmt::Display for OrderSide {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// 订单类型
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
    StopLimit,
    StopMarket,
    TakeProfitLimit,
    TakeProfitMarket,
    TrailingStop,
}

/// 订单状态
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderStatus {
    Open,
    Closed,
    Canceled,
    Expired,
    Rejected,
    Pending,
    PartiallyFilled,
    Triggered,
}

/// 时间有效性
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TimeInForce {
    GTC, // Good Till Cancel
    IOC, // Immediate Or Cancel
    FOK, // Fill Or Kill
    GTD, // Good Till Date
    GTX, // Good Till Crossing
    POC, // Post Only Cancel
}

/// 订单请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub amount: f64,
    pub price: Option<f64>,
    pub market_type: MarketType,
    #[serde(default)]
    pub params: Option<HashMap<String, String>>,
    #[serde(default)]
    pub client_order_id: Option<String>,
    #[serde(default)]
    pub time_in_force: Option<String>,
    #[serde(default)]
    pub reduce_only: Option<bool>,
    #[serde(default)]
    pub post_only: Option<bool>,
}

impl OrderRequest {
    /// 创建一个简单的订单请求（向后兼容）
    pub fn new(
        symbol: String,
        side: OrderSide,
        order_type: OrderType,
        amount: f64,
        price: Option<f64>,
        market_type: MarketType,
    ) -> Self {
        Self {
            symbol,
            side,
            order_type,
            amount,
            price,
            market_type,
            params: None,
            client_order_id: None,
            time_in_force: None,
            reduce_only: None,
            post_only: None,
        }
    }
}

/// 高级订单请求（支持止损、止盈、跟踪止损等）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvancedOrderRequest {
    // 基础订单信息
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub amount: f64,
    pub price: Option<f64>,
    pub market_type: MarketType,

    // 高级订单参数
    pub stop_price: Option<f64>,
    pub trigger_price: Option<f64>,
    pub trail_percent: Option<f64>,
    pub trail_value: Option<f64>,
    pub iceberg_qty: Option<f64>,
    pub visible_size: Option<f64>,

    // 高级执行参数
    pub time_in_force: Option<TimeInForce>,
    pub good_till_date: Option<DateTime<Utc>>,
    pub client_order_id: Option<String>,
    pub reduce_only: Option<bool>,
    pub post_only: Option<bool>,
    pub close_position: Option<bool>,

    // 成本相关
    pub cost: Option<f64>,
    pub quote_order_qty: Option<f64>,

    // 自定义参数
    pub params: Option<HashMap<String, serde_json::Value>>,
}

/// 订单
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub amount: f64,
    pub price: Option<f64>,
    pub filled: f64,
    pub remaining: f64,
    pub status: OrderStatus,
    pub market_type: MarketType,
    pub timestamp: DateTime<Utc>,
    pub last_trade_timestamp: Option<DateTime<Utc>>,
    pub info: serde_json::Value,
}

// ============= 交易相关 =============

/// 成交记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub amount: f64,
    pub price: f64,
    pub timestamp: DateTime<Utc>,
    pub order_id: Option<String>,
    pub fee: Option<Fee>,
}

/// 订单成交明细
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderTrade {
    pub id: String,
    pub order_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub price: f64,
    pub amount: f64,
    pub cost: f64,
    pub fee: Option<Fee>,
    pub timestamp: DateTime<Utc>,
    pub is_maker: bool,
}

/// 手续费
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fee {
    pub currency: String,
    pub cost: f64,
    pub rate: Option<f64>,
}

/// 交易手续费率
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeFee {
    pub symbol: String,
    pub maker: f64,
    pub taker: f64,
    pub percentage: bool,
    pub tier_based: bool,
    // 兼容字段
    pub maker_fee: Option<f64>,
    pub taker_fee: Option<f64>,
}

// ============= K线和统计数据 =============

/// K线数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Kline {
    pub symbol: String,
    pub interval: String,
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub trade_count: u64,
}

/// 标记价格K线
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarkPriceKline {
    pub symbol: String,
    pub interval: String,
    pub open_time: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: Option<f64>,
    pub close_time: DateTime<Utc>,
}

/// 指数价格K线
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexPriceKline {
    pub symbol: String,
    pub interval: String,
    pub open_time: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub close_time: DateTime<Utc>,
}

/// 时间间隔
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Interval {
    OneMinute,
    ThreeMinutes,
    FiveMinutes,
    FifteenMinutes,
    ThirtyMinutes,
    OneHour,
    TwoHours,
    FourHours,
    SixHours,
    EightHours,
    TwelveHours,
    OneDay,
    ThreeDays,
    OneWeek,
    OneMonth,
}

impl Interval {
    pub fn from_string(s: &str) -> Result<Self> {
        match s {
            "1m" => Ok(Interval::OneMinute),
            "3m" => Ok(Interval::ThreeMinutes),
            "5m" => Ok(Interval::FiveMinutes),
            "15m" => Ok(Interval::FifteenMinutes),
            "30m" => Ok(Interval::ThirtyMinutes),
            "1h" => Ok(Interval::OneHour),
            "2h" => Ok(Interval::TwoHours),
            "4h" => Ok(Interval::FourHours),
            "6h" => Ok(Interval::SixHours),
            "8h" => Ok(Interval::EightHours),
            "12h" => Ok(Interval::TwelveHours),
            "1d" => Ok(Interval::OneDay),
            "3d" => Ok(Interval::ThreeDays),
            "1w" => Ok(Interval::OneWeek),
            "1M" => Ok(Interval::OneMonth),
            _ => Err(crate::core::error::ExchangeError::Other(format!(
                "Invalid interval: {}",
                s
            ))),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            Interval::OneMinute => "1m",
            Interval::ThreeMinutes => "3m",
            Interval::FiveMinutes => "5m",
            Interval::FifteenMinutes => "15m",
            Interval::ThirtyMinutes => "30m",
            Interval::OneHour => "1h",
            Interval::TwoHours => "2h",
            Interval::FourHours => "4h",
            Interval::SixHours => "6h",
            Interval::EightHours => "8h",
            Interval::TwelveHours => "12h",
            Interval::OneDay => "1d",
            Interval::ThreeDays => "3d",
            Interval::OneWeek => "1w",
            Interval::OneMonth => "1M",
        }
        .to_string()
    }

    pub fn to_exchange_format(&self, exchange: &str) -> String {
        // 大部分交易所使用相同的格式
        self.to_string()
    }
}

/// 24小时统计数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Statistics24h {
    pub symbol: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub change: f64,
    pub change_percent: f64,
    pub timestamp: DateTime<Utc>,
    // 兼容字段
    pub price_change: Option<f64>,
    pub price_change_percent: Option<f64>,
    pub weighted_avg_price: Option<f64>,
    pub open_price: Option<f64>,
    pub high_price: Option<f64>,
    pub low_price: Option<f64>,
    pub close_price: Option<f64>,
    pub count: Option<u64>,
}

// ============= 期货相关 =============

/// 持仓信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub side: String,
    #[serde(default)]
    pub contracts: f64,
    pub contract_size: f64,
    pub entry_price: f64,
    pub mark_price: f64,
    pub unrealized_pnl: f64,
    pub percentage: f64,
    pub margin: f64,
    pub margin_ratio: f64,
    #[serde(default)]
    pub leverage: Option<u32>,
    #[serde(default)]
    pub margin_type: Option<String>,
    // 兼容字段 (size是contracts的别名)
    #[serde(default)]
    pub size: f64,
    #[serde(default)]
    pub amount: f64,
    pub timestamp: DateTime<Utc>,
}

/// 标记价格
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarkPrice {
    pub symbol: String,
    pub mark_price: f64,
    pub index_price: f64,
    pub estimated_settle_price: f64,
    pub last_funding_rate: f64,
    pub next_funding_time: DateTime<Utc>,
    pub timestamp: DateTime<Utc>,
}

/// 资金费率历史
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingRateHistory {
    pub symbol: String,
    pub funding_rate: f64,
    pub funding_time: DateTime<Utc>,
    pub mark_price: Option<f64>,
}

/// 资金费用记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingHistory {
    pub symbol: String,
    pub income: f64,
    pub income_type: String,
    pub asset: String,
    pub timestamp: DateTime<Utc>,
}

/// 仓位风险
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionRisk {
    pub symbol: String,
    pub position_amount: f64,
    pub entry_price: f64,
    pub mark_price: f64,
    pub unrealized_profit: f64,
    pub liquidation_price: f64,
    pub leverage: u32,
    pub margin_type: String,
    pub isolated_margin: f64,
    pub is_auto_add_margin: bool,
    pub position_side: String,
    pub notional: f64,
    pub isolated_wallet: f64,
    pub update_time: DateTime<Utc>,
}

/// 仓位保证金
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionMargin {
    pub symbol: String,
    pub position_side: String,
    pub amount: f64,
    pub margin_type: String,
    pub timestamp: DateTime<Utc>,
}

/// 未平仓合约量
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenInterest {
    pub symbol: String,
    pub open_interest: f64,
    pub open_interest_value: f64,
    pub timestamp: DateTime<Utc>,
}

/// 杠杆档位
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeverageTier {
    pub tier: u32,
    pub notional_floor: f64,
    pub notional_cap: f64,
    pub max_leverage: u32,
    pub maintenance_margin_rate: f64,
    pub initial_margin_rate: f64,
}

/// 杠杆信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeverageInfo {
    pub symbol: String,
    pub tiers: Vec<LeverageTier>,
}

// ============= 账户管理 =============

/// 账户快照
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountSnapshot {
    pub account_type: String,
    pub balances: Vec<Balance>,
    pub timestamp: DateTime<Utc>,
    // 兼容字段
    pub total_balance_btc: Option<f64>,
    pub total_balance_usdt: Option<f64>,
}

/// 资金划转结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferResult {
    pub transfer_id: String,
    pub currency: String,
    pub amount: f64,
    pub from_account: String,
    pub to_account: String,
    pub timestamp: DateTime<Utc>,
    pub status: String,
}

/// 账本记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedgerEntry {
    pub id: String,
    pub currency: String,
    pub amount: f64,
    pub balance: f64,
    pub fee: Option<f64>,
    pub direction: LedgerDirection,
    pub reference_id: Option<String>,
    pub info: String,
    pub timestamp: DateTime<Utc>,
}

/// 账本方向
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LedgerDirection {
    In,
    Out,
}

/// 收益类型
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IncomeType {
    Transfer,
    WelcomeBonus,
    FundingFee,
    RealizedPnl,
    Commission,
    Insurance,
    Referral,
    CommissionRebate,
    Other,
}

/// 收益历史
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncomeHistory {
    pub symbol: Option<String>,
    pub income_type: IncomeType,
    pub income: f64,
    pub asset: String,
    pub info: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub tran_id: Option<String>,
}

// ============= 充值提现 =============

/// 充值地址
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositAddress {
    pub currency: String,
    pub address: String,
    pub tag: Option<String>,
    pub network: Option<String>,
}

/// 充值记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Deposit {
    pub id: String,
    pub tx_id: String,
    pub currency: String,
    pub amount: f64,
    pub address: String,
    pub tag: Option<String>,
    pub status: DepositStatus,
    pub network: Option<String>,
    pub confirmations: u32,
    pub required_confirmations: u32,
    pub timestamp: DateTime<Utc>,
    pub credited_at: Option<DateTime<Utc>>,
}

/// 充值状态
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DepositStatus {
    Pending,
    Confirming,
    Success,
    Failed,
}

/// 提现记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Withdrawal {
    pub id: String,
    pub tx_id: Option<String>,
    pub currency: String,
    pub amount: f64,
    pub fee: f64,
    pub address: String,
    pub tag: Option<String>,
    pub status: WithdrawalStatus,
    pub network: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// 提现状态
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WithdrawalStatus {
    Pending,
    Processing,
    Success,
    Failed,
    Cancelled,
}

/// 提现请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithdrawRequest {
    pub currency: String,
    pub amount: f64,
    pub address: String,
    pub tag: Option<String>,
    pub network: Option<String>,
    pub name: Option<String>,
    pub internal_transfer: Option<bool>,
}

// ============= 交易对和市场信息 =============

/// 交易对信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingPair {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub status: String,
    pub min_order_size: f64,
    pub max_order_size: f64,
    pub tick_size: f64,
    pub step_size: f64,
    pub min_notional: Option<f64>,
    pub is_trading: bool,
    pub market_type: MarketType,
}

/// 市场信息（完整）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Market {
    pub id: String,
    pub symbol: String,
    pub base: String,
    pub quote: String,
    pub base_id: String,
    pub quote_id: String,
    pub active: bool,
    pub market_type: MarketType,
    pub spot: bool,
    pub margin: bool,
    pub future: bool,
    pub option: bool,
    pub contract: bool,
    pub linear: bool,
    pub inverse: bool,
    pub contract_size: Option<f64>,
    pub expiry: Option<DateTime<Utc>>,
    pub strike: Option<f64>,
    pub option_type: Option<String>,
    pub precision: MarketPrecision,
    pub limits: MarketLimits,
    pub info: HashMap<String, serde_json::Value>,
}

/// 市场精度
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketPrecision {
    pub amount: u32,
    pub price: u32,
    pub cost: Option<u32>,
    pub base: Option<u32>,
    pub quote: Option<u32>,
}

/// 市场限制
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketLimits {
    pub amount: MinMax,
    pub price: MinMax,
    pub cost: Option<MinMax>,
    pub leverage: Option<MinMax>,
}

/// 最小最大值
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MinMax {
    pub min: Option<f64>,
    pub max: Option<f64>,
}

/// 币种信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Currency {
    pub id: String,
    pub code: String,
    pub name: String,
    pub active: bool,
    pub fee: f64,
    pub precision: u32,
    pub deposit_enabled: bool,
    pub withdraw_enabled: bool,
    pub networks: Vec<CurrencyNetwork>,
}

/// 币种网络
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrencyNetwork {
    pub network: String,
    pub id: String,
    pub name: String,
    pub active: bool,
    pub deposit_enabled: bool,
    pub withdraw_enabled: bool,
    pub fee: f64,
    pub precision: u32,
    pub min_withdraw: f64,
    pub max_withdraw: f64,
    pub min_deposit: f64,
}

/// 交易所信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeInfo {
    pub name: String,
    pub symbols: Vec<Symbol>,
    pub currencies: Vec<String>,
    pub spot_enabled: bool,
    pub futures_enabled: bool,
}

/// 交易所状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeStatus {
    pub status: String,
    pub updated: DateTime<Utc>,
    pub eta: Option<DateTime<Utc>>,
    pub url: Option<String>,
}

// ============= 批量操作 =============

/// 批量订单请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchOrderRequest {
    pub orders: Vec<OrderRequest>,
    pub market_type: MarketType,
}

/// 批量订单响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchOrderResponse {
    pub successful_orders: Vec<Order>,
    pub failed_orders: Vec<BatchOrderError>,
}

/// 批量订单错误
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchOrderError {
    pub order_request: OrderRequest,
    pub error_message: String,
    pub error_code: Option<String>,
}

// ============= WebSocket相关 =============

/// WebSocket消息类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WsMessage {
    Text(String),
    Ticker(Ticker),
    OrderBook(OrderBook),
    Trade(Trade),
    Kline(Kline),
    Balance(Balance),
    Order(Order),
    Position(Position),
    ExecutionReport(ExecutionReport),
    Error(String),
}

/// 订单执行报告
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReport {
    pub symbol: String,
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub status: OrderStatus,
    pub price: f64,
    pub amount: f64,
    pub executed_amount: f64,
    pub executed_price: f64,
    pub commission: f64,
    pub commission_asset: String,
    pub timestamp: DateTime<Utc>,
    pub is_maker: bool,
}

/// WebSocket订阅请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsSubscription {
    pub channel: WsChannel,
    pub symbol: Option<String>,
    pub interval: Option<String>,
    pub depth: Option<u32>,
}

/// WebSocket频道类型
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WsChannel {
    Ticker,
    Tickers,
    OrderBook,
    Trades,
    Kline,
    MarkPrice,
    IndexPrice,
    FundingRate,
    OpenInterest,
    Balance,
    Orders,
    MyTrades,
    Positions,
}

// ============= 精度和验证 =============

/// 精度模式
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecisionMode {
    pub mode: PrecisionType,
    pub value: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PrecisionType {
    DecimalPlaces,
    SignificantDigits,
    TickSize,
}

/// 订单验证规则
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderValidation {
    pub symbol: String,
    pub min_amount: f64,
    pub max_amount: f64,
    pub min_price: f64,
    pub max_price: f64,
    pub min_cost: f64,
    pub amount_precision: PrecisionMode,
    pub price_precision: PrecisionMode,
}

// ============= 其他辅助类型 =============

/// 深度更新
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthUpdate {
    pub symbol: String,
    pub bids: Vec<[f64; 2]>,
    pub asks: Vec<[f64; 2]>,
    pub last_update_id: u64,
    pub timestamp: DateTime<Utc>,
}

/// 订单簿层级（简化版）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookLevel {
    pub price: f64,
    pub quantity: f64,
}

/// 历史持仓
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionHistory {
    pub symbol: String,
    pub side: String,
    pub entry_price: f64,
    pub exit_price: f64,
    pub quantity: f64,
    pub realized_pnl: f64,
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
}
