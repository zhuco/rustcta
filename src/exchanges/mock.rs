use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use chrono::Utc;

use crate::core::{
    error::ExchangeError,
    exchange::Exchange,
    types::{
        AccountSnapshot, Balance, BatchOrderError, BatchOrderRequest, BatchOrderResponse,
        ExchangeInfo, Interval, MarketType, OpenInterest, Order, OrderBook, OrderRequest,
        OrderSide, OrderStatus, Position, Result, Statistics24h, Ticker, Trade, TradeFee,
        TradingPair,
    },
    websocket::WebSocketClient,
};

/// 简化的离线模拟交易所实现，供无网络环境下策略运行使用
#[derive(Clone)]
pub struct MockExchange {
    name: String,
    orders: Arc<Mutex<HashMap<String, Vec<Order>>>>,
    balances: Arc<Mutex<Vec<Balance>>>,
    positions: Arc<Mutex<HashMap<String, Position>>>,
    order_counter: Arc<AtomicU64>,
}

impl MockExchange {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            orders: Arc::new(Mutex::new(HashMap::new())),
            balances: Arc::new(Mutex::new(vec![Balance {
                currency: "USDC".to_string(),
                total: 1_000_000.0,
                free: 1_000_000.0,
                used: 0.0,
                market_type: MarketType::Spot,
            }])),
            positions: Arc::new(Mutex::new(HashMap::new())),
            order_counter: Arc::new(AtomicU64::new(1)),
        }
    }

    fn next_order_id(&self) -> String {
        let id = self.order_counter.fetch_add(1, Ordering::Relaxed);
        format!("MOCK-ORDER-{}", id)
    }

    fn mock_price(&self, symbol: &str) -> f64 {
        let base = symbol
            .bytes()
            .fold(0u64, |acc, b| acc.wrapping_add(b as u64))
            .max(1) as f64;
        (base % 3000.0).max(10.0)
    }

    fn build_order(&self, request: OrderRequest, id: String) -> Order {
        let price = request
            .price
            .unwrap_or_else(|| self.mock_price(&request.symbol));
        Order {
            id,
            symbol: request.symbol.clone(),
            side: request.side.clone(),
            order_type: request.order_type.clone(),
            amount: request.amount,
            price: Some(price),
            filled: 0.0,
            remaining: request.amount,
            status: OrderStatus::Open,
            market_type: request.market_type,
            timestamp: Utc::now(),
            last_trade_timestamp: None,
            info: serde_json::json!({
                "mock": true,
                "source": "offline_mode"
            }),
        }
    }

    fn upsert_position(&self, order: &Order) {
        let mut positions = self.positions.lock().unwrap();
        let entry = positions
            .entry(order.symbol.clone())
            .or_insert_with(|| Position {
                symbol: order.symbol.clone(),
                side: "BOTH".to_string(),
                contracts: 0.0,
                contract_size: 1.0,
                entry_price: order.price.unwrap_or(0.0),
                mark_price: order.price.unwrap_or(0.0),
                unrealized_pnl: 0.0,
                percentage: 0.0,
                margin: 0.0,
                margin_ratio: 0.0,
                leverage: Some(1),
                margin_type: Some("cross".to_string()),
                size: 0.0,
                amount: 0.0,
                timestamp: Utc::now(),
            });

        let delta = if matches!(order.side, OrderSide::Buy) {
            order.amount
        } else {
            -order.amount
        };

        entry.contracts += delta;
        entry.size = entry.contracts;
        entry.amount = entry.contracts;
        entry.mark_price = order.price.unwrap_or(entry.mark_price);
        entry.timestamp = Utc::now();
    }
}

#[async_trait]
impl Exchange for MockExchange {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn exchange_info(&self) -> Result<ExchangeInfo> {
        Ok(ExchangeInfo {
            name: self.name.clone(),
            symbols: vec![],
            currencies: vec!["USDC".to_string(), "BTC".to_string(), "ETH".to_string()],
            spot_enabled: true,
            futures_enabled: true,
        })
    }

    async fn get_balance(&self, market_type: MarketType) -> Result<Vec<Balance>> {
        let mut balances = self.balances.lock().unwrap();
        for bal in balances.iter_mut() {
            bal.market_type = market_type;
        }
        Ok(balances.clone())
    }

    async fn get_ticker(&self, symbol: &str, _market_type: MarketType) -> Result<Ticker> {
        let mid = self.mock_price(symbol);
        Ok(Ticker {
            symbol: symbol.to_string(),
            high: mid * 1.01,
            low: mid * 0.99,
            bid: mid * 0.999,
            ask: mid * 1.001,
            last: mid,
            volume: 50_000.0,
            timestamp: Utc::now(),
        })
    }

    async fn get_all_tickers(&self, market_type: MarketType) -> Result<Vec<Ticker>> {
        let mut tickers = Vec::new();
        for symbol in ["BTC/USDC", "ETH/USDC", "SOL/USDC", "DCR/USDT"] {
            tickers.push(self.get_ticker(symbol, market_type).await?);
        }
        Ok(tickers)
    }

    async fn create_user_data_stream(&self, _market_type: MarketType) -> Result<String> {
        Ok(format!("mock_listen_key_{}", Utc::now().timestamp_millis()))
    }

    async fn keepalive_user_data_stream(
        &self,
        _listen_key: &str,
        _market_type: MarketType,
    ) -> Result<()> {
        Ok(())
    }

    async fn get_orderbook(
        &self,
        symbol: &str,
        _market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<OrderBook> {
        let mid = self.mock_price(symbol);
        let depth = limit.unwrap_or(5);
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        for i in 0..depth {
            let offset = i as f64 * 0.001;
            bids.push([mid * (1.0 - offset), 1.0 + i as f64 * 0.1]);
            asks.push([mid * (1.0 + offset), 1.0 + i as f64 * 0.1]);
        }

        Ok(OrderBook {
            symbol: symbol.to_string(),
            bids,
            asks,
            timestamp: Utc::now(),
            info: serde_json::Value::Null,
        })
    }

    async fn create_order(&self, order_request: OrderRequest) -> Result<Order> {
        let order_id = order_request
            .client_order_id
            .clone()
            .unwrap_or_else(|| self.next_order_id());
        let order = self.build_order(order_request.clone(), order_id);

        let mut orders = self.orders.lock().unwrap();
        orders
            .entry(order.symbol.clone())
            .or_insert_with(Vec::new)
            .push(order.clone());

        self.upsert_position(&order);

        Ok(order)
    }

    async fn cancel_order(
        &self,
        order_id: &str,
        symbol: &str,
        _market_type: MarketType,
    ) -> Result<Order> {
        let mut orders = self.orders.lock().unwrap();
        if let Some(list) = orders.get_mut(symbol) {
            if let Some(idx) = list.iter().position(|o| o.id == order_id) {
                let mut order = list.remove(idx);
                order.status = OrderStatus::Canceled;
                return Ok(order);
            }
        }
        Err(ExchangeError::OrderNotFound {
            order_id: order_id.to_string(),
            symbol: symbol.to_string(),
        })
    }

    async fn get_order(
        &self,
        order_id: &str,
        symbol: &str,
        _market_type: MarketType,
    ) -> Result<Order> {
        let orders = self.orders.lock().unwrap();
        if let Some(list) = orders.get(symbol) {
            if let Some(order) = list.iter().find(|o| o.id == order_id) {
                return Ok(order.clone());
            }
        }
        Err(ExchangeError::OrderNotFound {
            order_id: order_id.to_string(),
            symbol: symbol.to_string(),
        })
    }

    async fn cancel_all_orders(
        &self,
        symbol: Option<&str>,
        _market_type: MarketType,
    ) -> Result<Vec<Order>> {
        let mut orders = self.orders.lock().unwrap();
        if let Some(sym) = symbol {
            Ok(orders.remove(sym).unwrap_or_default())
        } else {
            let mut canceled = Vec::new();
            for (_, list) in orders.drain() {
                canceled.extend(list);
            }
            Ok(canceled)
        }
    }

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
        _market_type: MarketType,
    ) -> Result<Vec<Order>> {
        let orders = self.orders.lock().unwrap();
        if let Some(sym) = symbol {
            Ok(orders.get(sym).cloned().unwrap_or_default())
        } else {
            Ok(orders.values().cloned().flatten().collect())
        }
    }

    async fn get_order_history(
        &self,
        symbol: Option<&str>,
        _market_type: MarketType,
        _limit: Option<u32>,
    ) -> Result<Vec<Order>> {
        self.get_open_orders(symbol, MarketType::Spot).await
    }

    async fn get_trades(
        &self,
        _symbol: &str,
        _market_type: MarketType,
        _limit: Option<u32>,
    ) -> Result<Vec<Trade>> {
        Ok(vec![])
    }

    async fn get_my_trades(
        &self,
        _symbol: Option<&str>,
        _market_type: MarketType,
        _limit: Option<u32>,
    ) -> Result<Vec<Trade>> {
        Ok(vec![])
    }

    async fn get_klines(
        &self,
        symbol: &str,
        interval: Interval,
        _market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<Vec<crate::core::types::Kline>> {
        let mut data = Vec::new();
        let bars = limit.unwrap_or(10);
        let base_price = self.mock_price(symbol);
        for i in 0..bars {
            let offset = i as f64 * 0.001;
            data.push(crate::core::types::Kline {
                symbol: symbol.to_string(),
                interval: interval.to_string(),
                open_time: Utc::now() - chrono::Duration::minutes((bars - i) as i64),
                close_time: Utc::now() - chrono::Duration::minutes((bars - i - 1) as i64),
                open: base_price * (1.0 - offset),
                high: base_price * (1.0 + offset),
                low: base_price * (1.0 - offset * 1.1),
                close: base_price * (1.0 + offset * 0.9),
                volume: 1_000.0 + i as f64 * 20.0,
                quote_volume: (1_000.0 + i as f64 * 20.0) * base_price,
                trade_count: 100,
            });
        }
        Ok(data)
    }

    async fn get_24h_statistics(
        &self,
        symbol: &str,
        _market_type: MarketType,
    ) -> Result<Statistics24h> {
        let mid = self.mock_price(symbol);
        Ok(Statistics24h {
            symbol: symbol.to_string(),
            open: mid,
            high: mid * 1.01,
            low: mid * 0.99,
            close: mid,
            volume: 50_000.0,
            quote_volume: 50_000.0 * mid,
            change: 0.0,
            change_percent: 0.0,
            timestamp: Utc::now(),
            price_change: None,
            price_change_percent: None,
            weighted_avg_price: None,
            open_price: Some(mid),
            high_price: Some(mid * 1.01),
            low_price: Some(mid * 0.99),
            close_price: Some(mid),
            count: Some(0),
        })
    }

    async fn get_all_24h_statistics(&self, market_type: MarketType) -> Result<Vec<Statistics24h>> {
        let mut stats = Vec::new();
        for symbol in ["BTC/USDC", "ETH/USDC", "SOL/USDC", "DCR/USDT"] {
            stats.push(self.get_24h_statistics(symbol, market_type).await?);
        }
        Ok(stats)
    }

    async fn get_open_interest(&self, symbol: &str) -> Result<OpenInterest> {
        Ok(OpenInterest {
            symbol: symbol.to_string(),
            open_interest: 10_000.0,
            open_interest_value: 10_000.0,
            timestamp: Utc::now(),
        })
    }

    async fn get_trade_fee(&self, symbol: &str, _market_type: MarketType) -> Result<TradeFee> {
        Ok(TradeFee {
            symbol: symbol.to_string(),
            maker: 0.0002,
            taker: 0.0004,
            percentage: true,
            tier_based: false,
            maker_fee: Some(0.02),
            taker_fee: Some(0.04),
        })
    }

    async fn get_account_snapshot(&self, market_type: MarketType) -> Result<AccountSnapshot> {
        let account_type = match market_type {
            MarketType::Spot => "spot",
            MarketType::Futures => "futures",
        };
        Ok(AccountSnapshot {
            account_type: account_type.to_string(),
            balances: self.get_balance(market_type).await?,
            timestamp: Utc::now(),
            total_balance_btc: None,
            total_balance_usdt: None,
        })
    }

    async fn get_positions(&self, _symbol: Option<&str>) -> Result<Vec<Position>> {
        Ok(self.positions.lock().unwrap().values().cloned().collect())
    }

    async fn set_leverage(&self, _symbol: &str, _leverage: u32) -> Result<()> {
        Ok(())
    }

    async fn get_server_time(&self) -> Result<chrono::DateTime<chrono::Utc>> {
        Ok(Utc::now())
    }

    async fn ping(&self) -> Result<()> {
        Ok(())
    }

    async fn create_batch_orders(
        &self,
        batch_request: BatchOrderRequest,
    ) -> Result<BatchOrderResponse> {
        let mut successful = Vec::new();
        let mut failed = Vec::new();

        for request in batch_request.orders {
            match self.create_order(request.clone()).await {
                Ok(order) => successful.push(order),
                Err(err) => failed.push(BatchOrderError {
                    order_request: request,
                    error_message: err.to_string(),
                    error_code: None,
                }),
            }
        }

        Ok(BatchOrderResponse {
            successful_orders: successful,
            failed_orders: failed,
        })
    }

    async fn get_all_spot_symbols(&self) -> Result<Vec<TradingPair>> {
        Ok(vec![
            TradingPair {
                symbol: "BTCUSDC".to_string(),
                base_asset: "BTC".to_string(),
                quote_asset: "USDC".to_string(),
                status: "TRADING".to_string(),
                min_order_size: 0.0001,
                max_order_size: 10.0,
                tick_size: 0.01,
                step_size: 0.0001,
                min_notional: Some(10.0),
                is_trading: true,
                market_type: MarketType::Spot,
            },
            TradingPair {
                symbol: "DCRUSDT".to_string(),
                base_asset: "DCR".to_string(),
                quote_asset: "USDT".to_string(),
                status: "TRADING".to_string(),
                min_order_size: 0.001,
                max_order_size: 1000.0,
                tick_size: 0.0001,
                step_size: 0.001,
                min_notional: Some(5.0),
                is_trading: true,
                market_type: MarketType::Spot,
            },
        ])
    }

    async fn get_all_futures_symbols(&self) -> Result<Vec<TradingPair>> {
        let mut pairs = self.get_all_spot_symbols().await?;
        for pair in pairs.iter_mut() {
            pair.market_type = MarketType::Futures;
        }
        Ok(pairs)
    }

    async fn get_symbol_info(&self, symbol: &str, market_type: MarketType) -> Result<TradingPair> {
        Ok(TradingPair {
            symbol: symbol.replace('/', ""),
            base_asset: symbol.split('/').next().unwrap_or("BASE").to_string(),
            quote_asset: symbol.split('/').nth(1).unwrap_or("USDC").to_string(),
            status: "TRADING".to_string(),
            min_order_size: 0.001,
            max_order_size: 1000.0,
            tick_size: 0.0001,
            step_size: 0.001,
            min_notional: Some(5.0),
            is_trading: true,
            market_type,
        })
    }

    async fn create_websocket_client(
        &self,
        _market_type: MarketType,
    ) -> Result<Box<dyn WebSocketClient>> {
        Err(ExchangeError::NotSupported(
            "Mock exchange does not provide WebSocket connections".to_string(),
        ))
    }

    fn get_websocket_url(&self, _market_type: MarketType) -> String {
        "wss://mock.local/ws".to_string()
    }
}
