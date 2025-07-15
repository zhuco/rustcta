use super::binance_model::{AccountInfo, ExchangeInfo, Kline, Order, WsEvent};
use crate::error::AppError;
use crate::utils::symbol::{MarketType, Symbol};
use std::future::Future;
use std::pin::Pin;

pub type OnMessageCallback =
    Box<dyn FnMut(WsEvent) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

pub trait Exchange: Send + Sync {
    fn get_price<'a>(
        &'a self,
        symbol: &'a Symbol,
    ) -> Pin<Box<dyn Future<Output = Result<f64, AppError>> + Send + 'a>>;
    fn place_order<'a>(
        &'a self,
        symbol: &'a Symbol,
        side: &'a str,
        order_type: &'a str,
        quantity: f64,
        price: Option<f64>,
    ) -> Pin<Box<dyn Future<Output = Result<Order, AppError>> + Send + 'a>>;

    /// 批量下单，每批最多5个订单
    fn place_batch_orders<'a>(
        &'a self,
        symbol: &'a Symbol,
        orders: Vec<(String, String, f64, Option<f64>)>, // (side, order_type, quantity, price)
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Order>, AppError>> + Send + 'a>>;
    fn get_order<'a>(
        &'a self,
        symbol: &'a Symbol,
        order_id: i64,
    ) -> Pin<Box<dyn Future<Output = Result<Order, AppError>> + Send + 'a>>;
    fn cancel_order<'a>(
        &'a self,
        symbol: &'a Symbol,
        order_id: i64,
    ) -> Pin<Box<dyn Future<Output = Result<Order, AppError>> + Send + 'a>>;
    fn cancel_all_orders<'a>(
        &'a self,
        symbol: &'a Symbol,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + 'a>>;
    fn get_open_orders<'a>(
        &'a self,
        symbol: &'a Symbol,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Order>, AppError>> + Send + 'a>>;
    fn get_klines<'a>(
        &'a self,
        symbol: &'a Symbol,
        interval: &'a str,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<i32>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Kline>, AppError>> + Send + 'a>>;
    fn get_exchange_info<'a>(
        &'a self,
        market_type: MarketType,
    ) -> Pin<Box<dyn Future<Output = Result<ExchangeInfo, AppError>> + Send + 'a>>;
    fn get_account_info<'a>(
        &'a self,
        market_type: MarketType,
    ) -> Pin<Box<dyn Future<Output = Result<AccountInfo, AppError>> + Send + 'a>>;
    fn connect_ws<'a>(
        &'a self,
        market_type: MarketType,
        on_message: OnMessageCallback,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + 'a>>;
    
    /// 设置杠杆倍数
    fn set_leverage<'a>(
        &'a self,
        symbol: &'a Symbol,
        leverage: i32,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + 'a>>;
}
