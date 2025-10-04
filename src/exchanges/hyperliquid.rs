use crate::core::exchange::{Exchange, MarketType};
use crate::core::error::ExchangeError;
use crate::core::types::*;
use crate::core::websocket::WebSocketClient;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ethers_core::types::H256;
use ethers_signers::{LocalWallet, Signer};
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};
use serde_json;
use sha2::{Digest, Sha256};
use std::any::Any;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

/// Hyperliquid交易所实现
pub struct HyperliquidExchange {
    client: Client,
    wallet_address: String,
    api_wallet_address: Option<String>,
    api_wallet: Option<LocalWallet>,
    base_url: String,
    ws_url: String,
    nonce: std::sync::atomic::AtomicU64,
}

impl HyperliquidExchange {
    /// 创建新的Hyperliquid交易所实例
    pub fn new(
        wallet_address: String,
        api_wallet_address: Option<String>,
        api_private_key: Option<String>,
        use_testnet: bool,
    ) -> Self {
        let (base_url, ws_url) = if use_testnet {
            (
                "https://api.hyperliquid-testnet.xyz".to_string(),
                "wss://api.hyperliquid-testnet.xyz/ws".to_string(),
            )
        } else {
            (
                "https://api.hyperliquid.xyz".to_string(),
                "wss://api.hyperliquid.xyz/ws".to_string(),
            )
        };

        let api_wallet = if let Some(pk) = api_private_key {
            LocalWallet::from_str(&pk).ok()
        } else {
            None
        };

        Self {
            client: Client::new(),
            wallet_address,
            api_wallet_address,
            api_wallet,
            base_url,
            ws_url,
            nonce: std::sync::atomic::AtomicU64::new(
                chrono::Utc::now().timestamp_millis() as u64,
            ),
        }
    }

    /// 生成请求签名
    async fn sign_request(
        &self,
        action: &serde_json::Value,
        nonce: u64,
    ) -> Result<String, ExchangeError> {
        let wallet = self
            .api_wallet
            .as_ref()
            .ok_or_else(|| ExchangeError::ConfigError("API wallet not configured".to_string()))?;

        let message = serde_json::json!({
            "action": action,
            "nonce": nonce,
            "vaultAddress": self.wallet_address,
        });

        let message_str = serde_json::to_string(&message)
            .map_err(|e| ExchangeError::ParseError(e.to_string()))?;

        // 使用keccak256哈希
        let hasher = ethers_core::utils::keccak256(message_str.as_bytes());
        let hash = H256::from_slice(&hasher);
        
        let signature = wallet
            .sign_hash(hash)
            .await
            .map_err(|e| ExchangeError::AuthError(e.to_string()))?;

        Ok(format!("0x{}", hex::encode(signature.to_vec())))
    }

    /// 获取下一个nonce
    fn get_nonce(&self) -> u64 {
        self.nonce
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// 发送公开API请求
    async fn public_request(
        &self,
        endpoint: &str,
        params: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value, ExchangeError> {
        let url = format!("{}/info{}", self.base_url, endpoint);
        
        let response = if let Some(p) = params {
            self.client
                .post(&url)
                .json(p)
                .send()
                .await
                .map_err(|e| ExchangeError::NetworkError(e.to_string()))?
        } else {
            self.client
                .get(&url)
                .send()
                .await
                .map_err(|e| ExchangeError::NetworkError(e.to_string()))?
        };

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(ExchangeError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                error_text
            )));
        }

        response
            .json()
            .await
            .map_err(|e| ExchangeError::ParseError(e.to_string()))
    }

    /// 发送私有API请求
    async fn private_request(
        &self,
        action: serde_json::Value,
    ) -> Result<serde_json::Value, ExchangeError> {
        let nonce = self.get_nonce();
        let signature = self.sign_request(&action, nonce).await?;

        let request = serde_json::json!({
            "action": action,
            "nonce": nonce,
            "signature": signature,
            "vaultAddress": self.wallet_address,
        });

        let url = format!("{}/exchange", self.base_url);
        let response = self
            .client
            .post(&url)
            .json(&request)
            .send()
            .await
            .map_err(|e| ExchangeError::NetworkError(e.to_string()))?;

        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(ExchangeError::ApiError(format!(
                "HTTP {}: {}",
                response.status(),
                error_text
            )));
        }

        response
            .json()
            .await
            .map_err(|e| ExchangeError::ParseError(e.to_string()))
    }

    /// 转换Hyperliquid的交易对格式到标准格式
    fn normalize_symbol(&self, symbol: &str) -> String {
        // Hyperliquid使用 "BTC-PERP" 格式
        // 标准格式是 "BTC/USDC"
        symbol.replace("-PERP", "/USDC")
    }

    /// 转换标准格式到Hyperliquid格式
    fn to_hyperliquid_symbol(&self, symbol: &str) -> String {
        // 标准格式 "BTC/USDC" 转换为 "BTC-PERP"
        symbol.replace("/USDC", "-PERP").replace("/USDT", "-PERP")
    }

    /// 为测试添加的辅助方法
    pub async fn get_recent_trades(&self, symbol: &str, limit: usize) -> Result<Vec<Trade>> {
        let hl_symbol = self.to_hyperliquid_symbol(symbol);
        let params = serde_json::json!({
            "type": "recentTrades",
            "coin": hl_symbol,
        });

        let response = self.public_request("/recentTrades", Some(&params)).await?;
        
        let mut trades = Vec::new();
        
        if let Some(trade_array) = response.as_array() {
            for (i, trade) in trade_array.iter().take(limit).enumerate() {
                trades.push(Trade {
                    trade_id: i.to_string(),
                    order_id: None,
                    symbol: symbol.to_string(),
                    price: trade
                        .get("px")
                        .and_then(|p| p.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0),
                    quantity: trade
                        .get("sz")
                        .and_then(|s| s.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0),
                    quote_quantity: 0.0,
                    commission: 0.0,
                    commission_asset: "USDC".to_string(),
                    side: if trade.get("side").and_then(|s| s.as_str()) == Some("B") {
                        OrderSide::Buy
                    } else {
                        OrderSide::Sell
                    },
                    is_buyer_maker: false,
                    timestamp: chrono::Utc::now(),
                });
            }
        }
        
        Ok(trades)
    }
}

#[async_trait]
impl Exchange for HyperliquidExchange {
    fn name(&self) -> &str {
        "hyperliquid"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn exchange_info(&self) -> Result<ExchangeInfo> {
        let response = self.public_request("/meta", None).await?;
        
        let symbols = if let Some(universe) = response.get("universe") {
            universe
                .as_array()
                .unwrap_or(&Vec::new())
                .iter()
                .filter_map(|item| {
                    let name = item.get("name")?.as_str()?;
                    Some(SymbolInfo {
                        symbol: self.normalize_symbol(name),
                        base_asset: name.replace("-PERP", ""),
                        quote_asset: "USDC".to_string(),
                        status: "TRADING".to_string(),
                        min_quantity: 0.001,
                        max_quantity: 1000000.0,
                        step_size: 0.001,
                        min_notional: 10.0,
                        tick_size: 0.01,
                        min_price: 0.01,
                        max_price: 1000000.0,
                    })
                })
                .collect()
        } else {
            Vec::new()
        };

        Ok(ExchangeInfo {
            timezone: "UTC".to_string(),
            server_time: chrono::Utc::now().timestamp_millis(),
            symbols,
        })
    }

    async fn get_balance(&self, _market_type: MarketType) -> Result<Vec<Balance>> {
        let action = serde_json::json!({
            "type": "clearinghouseState",
            "user": self.wallet_address,
        });

        let response = self.private_request(action).await?;
        
        let mut balances = Vec::new();
        
        if let Some(margin_summary) = response.get("marginSummary") {
            let account_value = margin_summary
                .get("accountValue")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
                
            let total_margin_used = margin_summary
                .get("totalMarginUsed")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);

            balances.push(Balance {
                asset: "USDC".to_string(),
                available: account_value - total_margin_used,
                locked: total_margin_used,
            });
        }

        Ok(balances)
    }

    async fn get_ticker(&self, symbol: &str, _market_type: MarketType) -> Result<Ticker> {
        let hl_symbol = self.to_hyperliquid_symbol(symbol);
        let params = serde_json::json!({
            "type": "l2Book",
            "coin": hl_symbol,
        });

        let response = self.public_request("/l2Book", Some(&params)).await?;
        
        let levels = response
            .get("levels")
            .ok_or_else(|| ExchangeError::ParseError("Missing levels".to_string()))?;

        let bids = levels
            .get(0)
            .and_then(|b| b.as_array())
            .ok_or_else(|| ExchangeError::ParseError("Missing bids".to_string()))?;
            
        let asks = levels
            .get(1)
            .and_then(|a| a.as_array())
            .ok_or_else(|| ExchangeError::ParseError("Missing asks".to_string()))?;

        let best_bid = bids.get(0);
        let best_ask = asks.get(0);

        let bid_price = best_bid
            .and_then(|b| b.get("px"))
            .and_then(|p| p.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
            
        let bid_size = best_bid
            .and_then(|b| b.get("sz"))
            .and_then(|s| s.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);

        let ask_price = best_ask
            .and_then(|a| a.get("px"))
            .and_then(|p| p.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
            
        let ask_size = best_ask
            .and_then(|a| a.get("sz"))
            .and_then(|s| s.as_str())
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);

        Ok(Ticker {
            symbol: symbol.to_string(),
            bid_price,
            bid_size,
            ask_price,
            ask_size,
            last_price: (bid_price + ask_price) / 2.0,
            volume: 0.0,
            quote_volume: 0.0,
            open_24h: 0.0,
            high_24h: 0.0,
            low_24h: 0.0,
            timestamp: chrono::Utc::now(),
        })
    }

    async fn get_all_tickers(&self, market_type: MarketType) -> Result<Vec<Ticker>> {
        // 获取所有交易对信息
        let info = self.exchange_info().await?;
        let mut tickers = Vec::new();
        
        // 逐个获取ticker（Hyperliquid可能没有批量接口）
        for symbol_info in info.symbols.iter().take(10) {
            match self.get_ticker(&symbol_info.symbol, market_type).await {
                Ok(ticker) => tickers.push(ticker),
                Err(e) => log::warn!("Failed to get ticker for {}: {}", symbol_info.symbol, e),
            }
        }
        
        Ok(tickers)
    }

    async fn get_orderbook(
        &self,
        symbol: &str,
        _market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<OrderBook> {
        let hl_symbol = self.to_hyperliquid_symbol(symbol);
        let params = serde_json::json!({
            "type": "l2Book",
            "coin": hl_symbol,
        });

        let response = self.public_request("/l2Book", Some(&params)).await?;
        
        let levels = response
            .get("levels")
            .ok_or_else(|| ExchangeError::ParseError("Missing levels".to_string()))?;

        let bid_levels = levels
            .get(0)
            .and_then(|b| b.as_array())
            .ok_or_else(|| ExchangeError::ParseError("Missing bids".to_string()))?;
            
        let ask_levels = levels
            .get(1)
            .and_then(|a| a.as_array())
            .ok_or_else(|| ExchangeError::ParseError("Missing asks".to_string()))?;

        let max_levels = limit.unwrap_or(20) as usize;

        let bids: Vec<OrderBookLevel> = bid_levels
            .iter()
            .take(max_levels)
            .filter_map(|level| {
                let price = level.get("px")?.as_str()?.parse::<f64>().ok()?;
                let quantity = level.get("sz")?.as_str()?.parse::<f64>().ok()?;
                Some(OrderBookLevel { price, quantity })
            })
            .collect();

        let asks: Vec<OrderBookLevel> = ask_levels
            .iter()
            .take(max_levels)
            .filter_map(|level| {
                let price = level.get("px")?.as_str()?.parse::<f64>().ok()?;
                let quantity = level.get("sz")?.as_str()?.parse::<f64>().ok()?;
                Some(OrderBookLevel { price, quantity })
            })
            .collect();

        Ok(OrderBook {
            symbol: symbol.to_string(),
            bids,
            asks,
            timestamp: chrono::Utc::now(),
        })
    }

    async fn create_order(&self, order_request: OrderRequest) -> Result<Order> {
        let _hl_symbol = self.to_hyperliquid_symbol(&order_request.symbol);
        
        let order_type = match order_request.order_type {
            OrderType::Limit => serde_json::json!({"limit": {"tif": "Gtc"}}),
            OrderType::Market => serde_json::json!({"market": {}}),
            _ => return Err(ExchangeError::NotSupported("Order type not supported".to_string())),
        };

        let is_buy = matches!(order_request.side, OrderSide::Buy);
        
        let action = serde_json::json!({
            "type": "order",
            "orders": [{
                "a": self.to_hyperliquid_symbol(&order_request.symbol).parse::<u32>().unwrap_or(0),
                "b": is_buy,
                "p": order_request.price.to_string(),
                "s": order_request.quantity.to_string(),
                "r": order_request.reduce_only.unwrap_or(false),
                "t": order_type,
            }],
            "grouping": "na",
        });

        let response = self.private_request(action).await?;
        
        // 解析响应获取订单ID
        let order_id = response
            .get("response")
            .and_then(|r| r.get("data"))
            .and_then(|d| d.get("statuses"))
            .and_then(|s| s.get(0))
            .and_then(|status| status.get("resting"))
            .and_then(|r| r.get("oid"))
            .and_then(|oid| oid.as_str())
            .unwrap_or("unknown")
            .to_string();

        Ok(Order {
            order_id,
            client_order_id: None,
            symbol: order_request.symbol,
            order_type: order_request.order_type,
            side: order_request.side,
            price: order_request.price,
            quantity: order_request.quantity,
            executed_qty: 0.0,
            status: OrderStatus::New,
            time_in_force: order_request.time_in_force,
            reduce_only: order_request.reduce_only,
            post_only: order_request.post_only,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
    }

    async fn cancel_order(
        &self,
        order_id: &str,
        symbol: &str,
        _market_type: MarketType,
    ) -> Result<Order> {
        let _hl_symbol = self.to_hyperliquid_symbol(symbol);
        
        let action = serde_json::json!({
            "type": "cancel",
            "cancels": [{
                "a": self.to_hyperliquid_symbol(symbol).parse::<u32>().unwrap_or(0),
                "o": order_id,
            }],
        });

        let _response = self.private_request(action).await?;

        // 返回一个基本的取消订单响应
        Ok(Order {
            order_id: order_id.to_string(),
            client_order_id: None,
            symbol: symbol.to_string(),
            order_type: OrderType::Limit,
            side: OrderSide::Buy,
            price: 0.0,
            quantity: 0.0,
            executed_qty: 0.0,
            status: OrderStatus::Cancelled,
            time_in_force: TimeInForce::GTC,
            reduce_only: None,
            post_only: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        })
    }

    async fn get_order(
        &self,
        order_id: &str,
        symbol: &str,
        _market_type: MarketType,
    ) -> Result<Order> {
        // Hyperliquid可能需要从开放订单列表中查找
        let orders = self.get_open_orders(Some(symbol), MarketType::LinearPerpetual).await?;
        
        orders
            .into_iter()
            .find(|o| o.order_id == order_id)
            .ok_or_else(|| ExchangeError::OrderNotFound(order_id.to_string()))
    }

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
        _market_type: MarketType,
    ) -> Result<Vec<Order>> {
        let action = serde_json::json!({
            "type": "openOrders",
            "user": self.wallet_address,
        });

        let response = self.private_request(action).await?;
        
        let mut orders = Vec::new();
        
        if let Some(open_orders) = response.as_array() {
            for order in open_orders {
                let order_symbol = order
                    .get("coin")
                    .and_then(|s| s.as_str())
                    .map(|s| self.normalize_symbol(s))
                    .unwrap_or_default();

                // 如果指定了symbol，只返回该symbol的订单
                if let Some(sym) = symbol {
                    if order_symbol != sym {
                        continue;
                    }
                }

                let order_obj = Order {
                    order_id: order
                        .get("oid")
                        .and_then(|o| o.as_str())
                        .unwrap_or("unknown")
                        .to_string(),
                    client_order_id: order
                        .get("cloid")
                        .and_then(|c| c.as_str())
                        .map(|s| s.to_string()),
                    symbol: order_symbol,
                    order_type: OrderType::Limit,
                    side: if order.get("side").and_then(|s| s.as_str()) == Some("B") {
                        OrderSide::Buy
                    } else {
                        OrderSide::Sell
                    },
                    price: order
                        .get("limitPx")
                        .and_then(|p| p.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0),
                    quantity: order
                        .get("sz")
                        .and_then(|s| s.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0),
                    executed_qty: 0.0,
                    status: OrderStatus::New,
                    time_in_force: TimeInForce::GTC,
                    reduce_only: order.get("reduceOnly").and_then(|r| r.as_bool()),
                    post_only: None,
                    created_at: chrono::Utc::now(),
                    updated_at: chrono::Utc::now(),
                };
                
                orders.push(order_obj);
            }
        }

        Ok(orders)
    }

    async fn get_order_history(
        &self,
        _symbol: Option<&str>,
        _market_type: MarketType,
        _limit: Option<u32>,
    ) -> Result<Vec<Order>> {
        // Hyperliquid可能没有直接的历史订单API
        Ok(Vec::new())
    }

    async fn get_trades(
        &self,
        _symbol: &str,
        _market_type: MarketType,
        _limit: Option<u32>,
    ) -> Result<Vec<Trade>> {
        // 获取最近成交
        Ok(Vec::new())
    }

    async fn get_my_trades(
        &self,
        _symbol: Option<&str>,
        _market_type: MarketType,
        _limit: Option<u32>,
    ) -> Result<Vec<Trade>> {
        // 获取我的成交记录
        Ok(Vec::new())
    }

    async fn get_klines(
        &self,
        _symbol: &str,
        _interval: Interval,
        _market_type: MarketType,
        _limit: Option<u32>,
    ) -> Result<Vec<Kline>> {
        // 获取K线数据
        Ok(Vec::new())
    }

    async fn get_24h_statistics(
        &self,
        _symbol: &str,
        _market_type: MarketType,
    ) -> Result<Statistics24h> {
        // 获取24小时统计
        Ok(Statistics24h {
            symbol: String::new(),
            price_change: 0.0,
            price_change_percent: 0.0,
            weighted_avg_price: 0.0,
            prev_close_price: 0.0,
            last_price: 0.0,
            last_qty: 0.0,
            bid_price: 0.0,
            bid_qty: 0.0,
            ask_price: 0.0,
            ask_qty: 0.0,
            open_price: 0.0,
            high_price: 0.0,
            low_price: 0.0,
            volume: 0.0,
            quote_volume: 0.0,
            open_time: chrono::Utc::now(),
            close_time: chrono::Utc::now(),
            count: Some(0),
        })
    }

    async fn get_all_24h_statistics(
        &self,
        _market_type: MarketType,
    ) -> Result<Vec<Statistics24h>> {
        Ok(Vec::new())
    }

    async fn get_trade_fee(
        &self,
        symbol: &str,
        _market_type: MarketType,
    ) -> Result<TradeFee> {
        // Hyperliquid标准费率
        Ok(TradeFee {
            symbol: symbol.to_string(),
            maker: 0.0002,
            taker: 0.0005,
            percentage: true,
            tier_based: false,
            maker_fee: Some(0.0002),
            taker_fee: Some(0.0005),
        })
    }

    async fn get_account_snapshot(
        &self,
        _market_type: MarketType,
    ) -> Result<AccountSnapshot> {
        let balances = self.get_balance(MarketType::LinearPerpetual).await?;
        let total_usdc = balances
            .iter()
            .filter(|b| b.asset == "USDC")
            .map(|b| b.available + b.locked)
            .sum::<f64>();

        Ok(AccountSnapshot {
            account_type: "FUTURES".to_string(),
            total_balance_btc: 0.0,
            total_balance_usdt: total_usdc,
        })
    }

    async fn get_positions(&self, symbol: Option<&str>) -> Result<Vec<Position>> {
        let action = serde_json::json!({
            "type": "clearinghouseState",
            "user": self.wallet_address,
        });

        let response = self.private_request(action).await?;
        
        let mut positions = Vec::new();
        
        if let Some(asset_positions) = response.get("assetPositions") {
            if let Some(pos_array) = asset_positions.as_array() {
                for position in pos_array {
                    let pos_symbol = position
                        .get("position")
                        .and_then(|p| p.get("coin"))
                        .and_then(|c| c.as_str())
                        .map(|s| self.normalize_symbol(s))
                        .unwrap_or_default();

                    // 如果指定了symbol，只返回该symbol的仓位
                    if let Some(sym) = symbol {
                        if pos_symbol != sym {
                            continue;
                        }
                    }

                    let pos_data = position.get("position").unwrap_or(position);
                    
                    let size = pos_data
                        .get("szi")
                        .and_then(|s| s.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);

                    if size.abs() < 0.000001 {
                        continue; // 跳过空仓位
                    }

                    let entry_price = pos_data
                        .get("entryPx")
                        .and_then(|e| e.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);

                    let mark_price = pos_data
                        .get("markPx")
                        .and_then(|m| m.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);

                    let unrealized_pnl = pos_data
                        .get("unrealizedPnl")
                        .and_then(|p| p.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);

                    let margin = pos_data
                        .get("marginUsed")
                        .and_then(|m| m.as_str())
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);

                    positions.push(Position {
                        symbol: pos_symbol,
                        side: if size > 0.0 { "LONG".to_string() } else { "SHORT".to_string() },
                        contracts: size.abs(),
                        contract_size: 1.0,
                        entry_price,
                        mark_price,
                        unrealized_pnl,
                        percentage: if entry_price > 0.0 {
                            (unrealized_pnl / (entry_price * size.abs())) * 100.0
                        } else {
                            0.0
                        },
                        margin,
                        margin_ratio: 0.0,
                        leverage: Some(10), // 默认杠杆
                        margin_type: Some("CROSS".to_string()),
                        size: size.abs(),
                        amount: size.abs() * mark_price,
                        timestamp: chrono::Utc::now(),
                    });
                }
            }
        }

        Ok(positions)
    }

    async fn set_leverage(&self, _symbol: &str, _leverage: u32) -> Result<()> {
        // Hyperliquid可能不支持动态设置杠杆
        Err(ExchangeError::NotSupported(
            "Leverage adjustment not supported".to_string(),
        ))
    }

    async fn cancel_all_orders(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
    ) -> Result<Vec<Order>> {
        let open_orders = self.get_open_orders(symbol, market_type).await?;
        let mut cancelled = Vec::new();
        
        for order in open_orders {
            if let Ok(cancelled_order) = self
                .cancel_order(&order.order_id, &order.symbol, market_type)
                .await
            {
                cancelled.push(cancelled_order);
            }
        }
        
        Ok(cancelled)
    }

    async fn get_server_time(&self) -> Result<DateTime<Utc>> {
        Ok(chrono::Utc::now())
    }

    async fn ping(&self) -> Result<()> {
        // 尝试获取交易所信息来测试连接
        self.exchange_info().await?;
        Ok(())
    }

    async fn create_batch_orders(&self, _batch_request: BatchOrderRequest) -> Result<BatchOrderResponse> {
        Err(ExchangeError::NotSupported(
            "Batch orders not supported".to_string(),
        ))
    }

    async fn get_all_spot_symbols(&self) -> Result<Vec<TradingPair>> {
        // Hyperliquid只有永续合约
        Ok(Vec::new())
    }

    async fn get_all_futures_symbols(&self) -> Result<Vec<TradingPair>> {
        let info = self.exchange_info().await?;
        Ok(info
            .symbols
            .into_iter()
            .map(|s| TradingPair {
                symbol: s.symbol,
                base_asset: s.base_asset,
                quote_asset: s.quote_asset,
                status: s.status,
                order_types: vec![OrderType::Limit, OrderType::Market],
                iceberg_allowed: false,
                oco_allowed: false,
                is_spot_trading_allowed: false,
                is_margin_trading_allowed: false,
                permissions: vec!["LEVERAGED".to_string()],
            })
            .collect())
    }

    async fn get_symbol_info(
        &self,
        symbol: &str,
        _market_type: MarketType,
    ) -> Result<TradingPair> {
        let info = self.exchange_info().await?;
        info.symbols
            .into_iter()
            .find(|s| s.symbol == symbol)
            .map(|s| TradingPair {
                symbol: s.symbol,
                base_asset: s.base_asset,
                quote_asset: s.quote_asset,
                status: s.status,
                order_types: vec![OrderType::Limit, OrderType::Market],
                iceberg_allowed: false,
                oco_allowed: false,
                is_spot_trading_allowed: false,
                is_margin_trading_allowed: false,
                permissions: vec!["LEVERAGED".to_string()],
            })
            .ok_or_else(|| ExchangeError::SymbolNotFound(symbol.to_string()))
    }

    async fn create_websocket_client(
        &self,
        _market_type: MarketType,
    ) -> Result<Box<dyn WebSocketClient>> {
        Err(ExchangeError::NotSupported(
            "WebSocket not implemented".to_string(),
        ))
    }
}