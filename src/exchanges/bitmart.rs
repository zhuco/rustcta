use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::warn;
use serde::Deserialize;
use std::collections::HashMap;

use crate::core::{
    config::{ApiKeys, Config},
    error::ExchangeError,
    exchange::{BaseExchange, Exchange},
    types::*,
};
use crate::utils::{SignatureHelper, SymbolConverter};

/// Bitmart交易所实现
pub struct BitmartExchange {
    base: BaseExchange,
    symbol_converter: SymbolConverter,
}

impl BitmartExchange {
    /// 创建Bitmart交易所实例
    pub fn new(config: Config, api_keys: ApiKeys) -> Self {
        let base = BaseExchange::new("bitmart".to_string(), config.clone(), api_keys);
        let symbol_converter = SymbolConverter::new(config);

        Self {
            base,
            symbol_converter,
        }
    }

    /// 发送认证请求到Bitmart
    async fn send_signed_request<T>(
        &self,
        method: &str,
        endpoint: &str,
        params: HashMap<String, String>,
    ) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        // 根据endpoint选择正确的API URL
        let base_url = if endpoint.contains("/contract/") {
            "https://api-cloud-v2.bitmart.com" // 期货V2 API
        } else {
            "https://api-cloud.bitmart.com" // 现货API
        };

        // 生成时间戳
        let timestamp = SignatureHelper::timestamp().to_string();

        // 构建请求体或查询参数
        let (url, body) = if method.to_uppercase() == "POST" {
            let body = if params.is_empty() {
                "{}".to_string()
            } else {
                serde_json::to_string(&params)?
            };
            (format!("{}{}", base_url, endpoint), body)
        } else {
            let query_string = if params.is_empty() {
                String::new()
            } else {
                format!("?{}", SignatureHelper::build_query_string(&params))
            };
            (
                format!("{}{}{}", base_url, endpoint, query_string),
                String::new(),
            )
        };

        // 生成签名（如果有memo则使用v3签名）
        let signature = if let Some(memo) = &self.base.api_keys.memo {
            // 使用v3签名（包含memo）
            SignatureHelper::bitmart_signature_v3(
                &self.base.api_keys.api_secret,
                &timestamp,
                memo,
                &body,
            )
        } else {
            // 使用旧版签名（不包含memo）
            SignatureHelper::bitmart_signature(&self.base.api_keys.api_secret, &timestamp, &body)
        };

        // 设置请求头
        let mut request = match method.to_uppercase().as_str() {
            "GET" => self.base.client.get(&url),
            "POST" => self.base.client.post(&url),
            "DELETE" => self.base.client.delete(&url),
            _ => return Err(ExchangeError::Other("不支持的HTTP方法".to_string())),
        };

        request = request
            .header("X-BM-KEY", &self.base.api_keys.api_key)
            .header("X-BM-SIGN", signature)
            .header("X-BM-TIMESTAMP", timestamp)
            .header("Content-Type", "application/json");

        // 添加Memo头如果存在
        if let Some(memo) = &self.base.api_keys.memo {
            request = request.header("X-BM-BROKER-ID", memo); // Bitmart使用X-BM-BROKER-ID作为memo头
        }

        // 如果是POST请求，添加请求体
        if method.to_uppercase() == "POST" && !body.is_empty() {
            request = request.body(body);
        }

        let response = request.send().await?;

        // 处理响应
        if response.status().is_success() {
            #[derive(Deserialize)]
            struct BitmartResponse<T> {
                code: i32,
                message: String,
                data: Option<T>,
            }

            let bitmart_response: BitmartResponse<T> = response.json().await?;

            if bitmart_response.code == 1000 {
                bitmart_response
                    .data
                    .ok_or_else(|| ExchangeError::ApiError {
                        code: bitmart_response.code,
                        message: "API返回空数据".to_string(),
                    })
            } else {
                Err(ExchangeError::ApiError {
                    code: bitmart_response.code,
                    message: bitmart_response.message,
                })
            }
        } else {
            let status_code = response.status().as_u16() as i32;
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "未知错误".to_string());
            Err(ExchangeError::ApiError {
                code: status_code,
                message: error_text,
            })
        }
    }

    /// 发送公共请求到Bitmart
    async fn send_public_request<T>(
        &self,
        endpoint: &str,
        params: Option<HashMap<String, String>>,
    ) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        // 根据endpoint选择正确的API URL
        let base_url = if endpoint.contains("/contract/") {
            "https://api-cloud-v2.bitmart.com" // 期货V2 API
        } else {
            "https://api-cloud.bitmart.com" // 现货API
        };

        let mut url = format!("{}{}", base_url, endpoint);

        // 添加查询参数
        if let Some(params) = params {
            if !params.is_empty() {
                let query_string = SignatureHelper::build_query_string(&params);
                url = format!("{}?{}", url, query_string);
            }
        }

        let response = self.base.client.get(&url).send().await?;

        if response.status().is_success() {
            #[derive(Deserialize)]
            struct BitmartResponse<T> {
                code: i32,
                message: String,
                data: Option<T>,
            }

            let bitmart_response: BitmartResponse<T> = response.json().await?;

            if bitmart_response.code == 1000 {
                bitmart_response
                    .data
                    .ok_or_else(|| ExchangeError::ApiError {
                        code: bitmart_response.code,
                        message: "API返回空数据".to_string(),
                    })
            } else {
                Err(ExchangeError::ApiError {
                    code: bitmart_response.code,
                    message: bitmart_response.message,
                })
            }
        } else {
            let status_code = response.status().as_u16() as i32;
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "未知错误".to_string());
            Err(ExchangeError::ApiError {
                code: status_code,
                message: error_text,
            })
        }
    }
}

#[async_trait]
impl Exchange for BitmartExchange {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.base.name
    }

    async fn exchange_info(&self) -> Result<ExchangeInfo> {
        #[derive(Deserialize)]
        struct BitmartSymbolsData {
            symbols: Vec<String>,
        }

        // 获取现货交易对信息
        let symbols_data: BitmartSymbolsData =
            self.send_public_request("/spot/v1/symbols", None).await?;

        let mut result_symbols = Vec::new();
        for symbol_str in symbols_data.symbols {
            // 尝试解析符号格式 "BASE_QUOTE"
            if let Ok(standard_symbol) =
                self.symbol_converter
                    .from_exchange_symbol(&symbol_str, "bitmart", MarketType::Spot)
            {
                // 从标准格式 "BASE/QUOTE" 解析出 base 和 quote
                let parts: Vec<&str> = standard_symbol.split('/').collect();
                if parts.len() == 2 {
                    result_symbols.push(Symbol {
                        base: parts[0].to_string(),
                        quote: parts[1].to_string(),
                        symbol: standard_symbol,
                        market_type: MarketType::Spot,
                        exchange_specific: Some(symbol_str),
                    });
                }
            }
        }

        Ok(ExchangeInfo {
            name: "Bitmart".to_string(),
            symbols: result_symbols,
            currencies: vec![],
            spot_enabled: true,
            futures_enabled: true, // Bitmart支持期货合约
        })
    }

    async fn get_balance(&self, market_type: MarketType) -> Result<Vec<Balance>> {
        let endpoint = match market_type {
            MarketType::Spot => "/spot/v1/wallet",
            MarketType::Futures => "/contract/private/assets-detail",
        };

        #[derive(Deserialize)]
        struct BitmartWallet {
            wallet: Vec<BitmartBalance>,
        }

        #[derive(Deserialize)]
        struct BitmartBalance {
            id: String,
            available: String,
            frozen: String,
        }

        // 现货余额查询
        if market_type == MarketType::Spot {
            let wallet: BitmartWallet = self
                .send_signed_request("GET", endpoint, HashMap::new())
                .await?;

            let mut balances = Vec::new();
            for balance in wallet.wallet {
                let free = balance.available.parse::<f64>().unwrap_or(0.0);
                let used = balance.frozen.parse::<f64>().unwrap_or(0.0);

                if free > 0.0 || used > 0.0 {
                    balances.push(Balance {
                        currency: balance.id,
                        total: free + used,
                        free,
                        used,
                        market_type: market_type.clone(),
                    });
                }
            }

            Ok(balances)
        } else {
            // 期货余额查询（需要不同的数据结构）
            #[derive(Deserialize)]
            struct BitmartFuturesBalance {
                #[serde(rename = "available_balance")]
                available_balance: String,
                #[serde(rename = "frozen_balance")]
                frozen_balance: String,
                #[serde(rename = "position_margin")]
                position_margin: Option<String>,
                #[serde(rename = "unrealized_pnl")]
                unrealized_pnl: Option<String>,
                #[serde(rename = "equity")]
                equity: Option<String>,
                currency: String,
            }

            // 尝试获取详细的期货账户信息
            match self
                .send_signed_request::<Vec<BitmartFuturesBalance>>("GET", endpoint, HashMap::new())
                .await
            {
                Ok(balances_data) => {
                    let mut balances = Vec::new();
                    for balance in balances_data {
                        let free = balance.available_balance.parse::<f64>().unwrap_or(0.0);
                        let used = balance.frozen_balance.parse::<f64>().unwrap_or(0.0);
                        let position_margin = balance
                            .position_margin
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0);

                        // 计算总余额，包括持仓保证金
                        let total = free + used + position_margin;

                        if total > 0.0 {
                            // 添加未实现盈亏信息到日志
                            if let Some(pnl) = &balance.unrealized_pnl {
                                if let Ok(pnl_value) = pnl.parse::<f64>() {
                                    if pnl_value != 0.0 {
                                        log::debug!(
                                            "{} 未实现盈亏: {}",
                                            balance.currency,
                                            pnl_value
                                        );
                                    }
                                }
                            }

                            balances.push(Balance {
                                currency: balance.currency,
                                total,
                                free,
                                used: used + position_margin, // 将持仓保证金算入已使用余额
                                market_type: market_type.clone(),
                            });
                        }
                    }
                    Ok(balances)
                }
                Err(_) => {
                    // 如果新API失败，尝试使用简单的账户查询
                    #[derive(Deserialize)]
                    struct SimpleFuturesBalance {
                        currency: String,
                        balance: String,
                        available: String,
                    }

                    let simple_endpoint = "/contract/private/account";
                    match self
                        .send_signed_request::<Vec<SimpleFuturesBalance>>(
                            "GET",
                            simple_endpoint,
                            HashMap::new(),
                        )
                        .await
                    {
                        Ok(simple_balances) => {
                            let mut balances = Vec::new();
                            for balance in simple_balances {
                                let total = balance.balance.parse::<f64>().unwrap_or(0.0);
                                let free = balance.available.parse::<f64>().unwrap_or(0.0);
                                let used = total - free;

                                if total > 0.0 {
                                    balances.push(Balance {
                                        currency: balance.currency,
                                        total,
                                        free,
                                        used,
                                        market_type: market_type.clone(),
                                    });
                                }
                            }
                            Ok(balances)
                        }
                        Err(e) => Err(e),
                    }
                }
            }
        }
    }

    async fn get_ticker(&self, symbol: &str, market_type: MarketType) -> Result<Ticker> {
        let exchange_symbol =
            self.symbol_converter
                .to_exchange_symbol(symbol, "bitmart", market_type)?;

        let endpoint = match market_type {
            MarketType::Spot => "/spot/v1/ticker_detail",
            MarketType::Futures => "/contract/public/details",
        };

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), exchange_symbol);

        #[derive(Deserialize)]
        struct BitmartTicker {
            symbol: String,
            high_24h: String,
            low_24h: String,
            #[serde(rename = "best_bid")]
            best_bid: String,
            #[serde(rename = "best_ask")]
            best_ask: String,
            last_price: String,
            base_volume_24h: String,
            timestamp: i64,
        }

        let ticker: BitmartTicker = self.send_public_request(endpoint, Some(params)).await?;

        Ok(Ticker {
            symbol: symbol.to_string(),
            high: ticker.high_24h.parse().unwrap_or(0.0),
            low: ticker.low_24h.parse().unwrap_or(0.0),
            bid: ticker.best_bid.parse().unwrap_or(0.0),
            ask: ticker.best_ask.parse().unwrap_or(0.0),
            last: ticker.last_price.parse().unwrap_or(0.0),
            volume: ticker.base_volume_24h.parse().unwrap_or(0.0),
            timestamp: DateTime::from_timestamp(ticker.timestamp / 1000, 0)
                .unwrap_or_else(|| Utc::now()),
        })
    }

    async fn get_all_tickers(&self, market_type: MarketType) -> Result<Vec<Ticker>> {
        let endpoint = match market_type {
            MarketType::Spot => "/spot/v1/ticker",
            MarketType::Futures => "/contract/public/details",
        };

        #[derive(Deserialize)]
        struct BitmartAllTicker {
            symbol: String,
            high_24h: String,
            low_24h: String,
            #[serde(rename = "best_bid")]
            best_bid: String,
            #[serde(rename = "best_ask")]
            best_ask: String,
            last_price: String,
            base_volume_24h: String,
            timestamp: i64,
        }

        let tickers: Vec<BitmartAllTicker> = self.send_public_request(endpoint, None).await?;

        let mut result = Vec::new();
        for ticker in tickers {
            // 转换交易对格式
            if let Ok(standard_symbol) =
                self.symbol_converter
                    .from_exchange_symbol(&ticker.symbol, "bitmart", market_type)
            {
                result.push(Ticker {
                    symbol: standard_symbol,
                    high: ticker.high_24h.parse().unwrap_or(0.0),
                    low: ticker.low_24h.parse().unwrap_or(0.0),
                    bid: ticker.best_bid.parse().unwrap_or(0.0),
                    ask: ticker.best_ask.parse().unwrap_or(0.0),
                    last: ticker.last_price.parse().unwrap_or(0.0),
                    volume: ticker.base_volume_24h.parse().unwrap_or(0.0),
                    timestamp: DateTime::from_timestamp(ticker.timestamp / 1000, 0)
                        .unwrap_or_else(|| Utc::now()),
                });
            }
        }

        Ok(result)
    }

    async fn get_orderbook(
        &self,
        symbol: &str,
        market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<OrderBook> {
        let exchange_symbol =
            self.symbol_converter
                .to_exchange_symbol(symbol, "bitmart", market_type)?;

        let endpoint = match market_type {
            MarketType::Spot => "/spot/v1/symbols/book",
            MarketType::Futures => "/contract/public/depth",
        };

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), exchange_symbol);
        if let Some(limit) = limit {
            params.insert("size".to_string(), limit.to_string());
        }

        #[derive(Deserialize)]
        struct BitmartOrderBook {
            buys: Vec<BitmartOrder>,
            sells: Vec<BitmartOrder>,
            timestamp: i64,
        }

        #[derive(Deserialize)]
        struct BitmartOrder {
            amount: String,
            total: String,
            price: String,
            count: String,
        }

        let orderbook: BitmartOrderBook = self.send_public_request(endpoint, Some(params)).await?;

        let bids: Vec<[f64; 2]> = orderbook
            .buys
            .iter()
            .map(|bid| {
                [
                    bid.price.parse().unwrap_or(0.0),
                    bid.amount.parse().unwrap_or(0.0),
                ]
            })
            .collect();

        let asks: Vec<[f64; 2]> = orderbook
            .sells
            .iter()
            .map(|ask| {
                [
                    ask.price.parse().unwrap_or(0.0),
                    ask.amount.parse().unwrap_or(0.0),
                ]
            })
            .collect();

        Ok(OrderBook {
            symbol: symbol.to_string(),
            bids,
            asks,
            timestamp: DateTime::from_timestamp(orderbook.timestamp / 1000, 0)
                .unwrap_or_else(|| Utc::now()),
        })
    }

    async fn create_order(&self, order_request: OrderRequest) -> Result<Order> {
        let exchange_symbol = self.symbol_converter.to_exchange_symbol(
            &order_request.symbol,
            "bitmart",
            order_request.market_type,
        )?;

        let endpoint = match order_request.market_type {
            MarketType::Spot => "/spot/v2/submit_order",
            MarketType::Futures => "/contract/private/submit-order",
        };

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), exchange_symbol);
        params.insert(
            "side".to_string(),
            match order_request.side {
                OrderSide::Buy => "buy".to_string(),
                OrderSide::Sell => "sell".to_string(),
            },
        );
        params.insert(
            "type".to_string(),
            match order_request.order_type {
                OrderType::Market => "market".to_string(),
                OrderType::Limit => "limit".to_string(),
                OrderType::StopLimit => "limit".to_string(), // Bitmart不直接支持止损限价单
                OrderType::StopMarket => "market".to_string(), // Bitmart不直接支持
                OrderType::TakeProfitLimit => "limit".to_string(), // Bitmart不直接支持
                OrderType::TakeProfitMarket => "market".to_string(), // Bitmart不直接支持
                OrderType::TrailingStop => "market".to_string(), // Bitmart不直接支持
            },
        );

        if order_request.order_type == OrderType::Market {
            if order_request.side == OrderSide::Buy {
                // 市价买入使用quote_quantity（计价货币数量）
                if let Some(price) = order_request.price {
                    params.insert(
                        "notional".to_string(),
                        (order_request.amount * price).to_string(),
                    );
                } else {
                    return Err(ExchangeError::OrderError(
                        "市价买单需要提供价格估算".to_string(),
                    ));
                }
            } else {
                // 市价卖出使用size（基础货币数量）
                params.insert("size".to_string(), order_request.amount.to_string());
            }
        } else {
            // 限价单
            params.insert("size".to_string(), order_request.amount.to_string());
            if let Some(price) = order_request.price {
                params.insert("price".to_string(), price.to_string());
            } else {
                return Err(ExchangeError::OrderError("限价单必须提供价格".to_string()));
            }
        }

        #[derive(Deserialize)]
        struct BitmartOrderResponse {
            order_id: i64,
        }

        let response: BitmartOrderResponse =
            self.send_signed_request("POST", endpoint, params).await?;

        Ok(Order {
            id: response.order_id.to_string(),
            symbol: order_request.symbol,
            side: order_request.side,
            order_type: order_request.order_type,
            amount: order_request.amount,
            price: order_request.price,
            filled: 0.0,
            remaining: order_request.amount,
            status: OrderStatus::Open,
            market_type: order_request.market_type,
            timestamp: Utc::now(),
            last_trade_timestamp: None,
            info: serde_json::json!({}),
        })
    }

    async fn cancel_order(
        &self,
        order_id: &str,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<Order> {
        let exchange_symbol =
            self.symbol_converter
                .to_exchange_symbol(symbol, "bitmart", market_type)?;

        let endpoint = match market_type {
            MarketType::Spot => "/spot/v3/cancel_order",
            MarketType::Futures => "/contract/private/cancel-order",
        };

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), exchange_symbol);
        params.insert("order_id".to_string(), order_id.to_string());

        #[derive(Deserialize)]
        struct BitmartCancelResponse {
            result: bool,
        }

        let _response: BitmartCancelResponse =
            self.send_signed_request("POST", endpoint, params).await?;

        // 获取订单详情
        self.get_order(order_id, symbol, market_type).await
    }

    async fn get_order(
        &self,
        order_id: &str,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<Order> {
        let exchange_symbol =
            self.symbol_converter
                .to_exchange_symbol(symbol, "bitmart", market_type)?;

        let endpoint = match market_type {
            MarketType::Spot => "/spot/v2/order_detail",
            MarketType::Futures => "/contract/private/order",
        };

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), exchange_symbol);
        params.insert("order_id".to_string(), order_id.to_string());

        #[derive(Deserialize)]
        struct BitmartOrder {
            order_id: String,
            symbol: String,
            side: String,
            #[serde(rename = "type")]
            order_type: String,
            size: String,
            price: String,
            filled_size: String,
            state: String,
            create_time: i64,
            update_time: i64,
        }

        let order: BitmartOrder = self.send_signed_request("GET", endpoint, params).await?;

        let filled = order.filled_size.parse::<f64>().unwrap_or(0.0);
        let amount = order.size.parse::<f64>().unwrap_or(0.0);

        Ok(Order {
            id: order.order_id,
            symbol: symbol.to_string(),
            side: match order.side.as_str() {
                "buy" => OrderSide::Buy,
                "sell" => OrderSide::Sell,
                _ => OrderSide::Buy,
            },
            order_type: match order.order_type.as_str() {
                "market" => OrderType::Market,
                "limit" => OrderType::Limit,
                _ => OrderType::Limit,
            },
            amount,
            price: if order.price != "0" {
                Some(order.price.parse().unwrap_or(0.0))
            } else {
                None
            },
            filled,
            remaining: amount - filled,
            status: match order.state.as_str() {
                "1" => OrderStatus::Open,     // 新建订单
                "2" => OrderStatus::Closed,   // 完全成交
                "3" => OrderStatus::Canceled, // 已取消
                "4" => OrderStatus::Open,     // 部分成交
                _ => OrderStatus::Pending,
            },
            market_type,
            timestamp: DateTime::from_timestamp(order.create_time / 1000, 0)
                .unwrap_or_else(|| Utc::now()),
            last_trade_timestamp: Some(
                DateTime::from_timestamp(order.update_time / 1000, 0).unwrap_or_else(|| Utc::now()),
            ),
            info: serde_json::json!({}),
        })
    }

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
    ) -> Result<Vec<Order>> {
        // 现货订单查询API已弃用，暂时返回空列表
        if market_type == MarketType::Spot {
            warn!("Bitmart现货订单查询API已弃用，返回空列表");
            return Ok(Vec::new());
        }

        let endpoint = "/contract/private/get-open-orders";
        let mut params = HashMap::new();

        if let Some(symbol) = symbol {
            let exchange_symbol =
                self.symbol_converter
                    .to_exchange_symbol(symbol, "bitmart", market_type)?;
            params.insert("symbol".to_string(), exchange_symbol);
        }

        #[derive(Deserialize)]
        struct BitmartOrder {
            order_id: String,
            symbol: String,
            side: String,
            #[serde(rename = "type")]
            order_type: String,
            size: String,
            price: String,
            filled_size: String,
            state: String,
            create_time: i64,
            update_time: i64,
        }

        let orders: Vec<BitmartOrder> = self.send_signed_request("GET", endpoint, params).await?;

        let mut result = Vec::new();
        for order in orders {
            // 只包含活跃订单
            if order.state == "1" || order.state == "4" {
                // 1=新建, 4=部分成交
                // 转换交易对格式
                if let Ok(standard_symbol) = self.symbol_converter.from_exchange_symbol(
                    &order.symbol,
                    "bitmart",
                    market_type,
                ) {
                    let filled = order.filled_size.parse::<f64>().unwrap_or(0.0);
                    let amount = order.size.parse::<f64>().unwrap_or(0.0);

                    result.push(Order {
                        id: order.order_id,
                        symbol: standard_symbol,
                        side: match order.side.as_str() {
                            "buy" => OrderSide::Buy,
                            "sell" => OrderSide::Sell,
                            _ => OrderSide::Buy,
                        },
                        order_type: match order.order_type.as_str() {
                            "market" => OrderType::Market,
                            "limit" => OrderType::Limit,
                            _ => OrderType::Limit,
                        },
                        amount,
                        price: if order.price != "0" {
                            Some(order.price.parse().unwrap_or(0.0))
                        } else {
                            None
                        },
                        filled,
                        remaining: amount - filled,
                        status: OrderStatus::Open,
                        market_type,
                        timestamp: DateTime::from_timestamp(order.create_time / 1000, 0)
                            .unwrap_or_else(|| Utc::now()),
                        last_trade_timestamp: Some(
                            DateTime::from_timestamp(order.update_time / 1000, 0)
                                .unwrap_or_else(|| Utc::now()),
                        ),
                        info: serde_json::json!({}),
                    });
                }
            }
        }

        Ok(result)
    }

    async fn get_order_history(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<Vec<Order>> {
        let endpoint = match market_type {
            MarketType::Spot => "/spot/v2/orders",
            MarketType::Futures => "/contract/private/order-history",
        };

        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            let exchange_symbol =
                self.symbol_converter
                    .to_exchange_symbol(symbol, "bitmart", market_type)?;
            params.insert("symbol".to_string(), exchange_symbol);
        }
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.to_string());
        }

        #[derive(Deserialize)]
        struct BitmartOrder {
            order_id: String,
            symbol: String,
            side: String,
            #[serde(rename = "type")]
            order_type: String,
            size: String,
            price: String,
            filled_size: String,
            state: String,
            create_time: i64,
            update_time: i64,
        }

        let orders: Vec<BitmartOrder> = self.send_signed_request("GET", endpoint, params).await?;

        let mut result = Vec::new();
        for order in orders {
            // 转换交易对格式
            if let Ok(standard_symbol) =
                self.symbol_converter
                    .from_exchange_symbol(&order.symbol, "bitmart", market_type)
            {
                let filled = order.filled_size.parse::<f64>().unwrap_or(0.0);
                let amount = order.size.parse::<f64>().unwrap_or(0.0);

                result.push(Order {
                    id: order.order_id,
                    symbol: standard_symbol,
                    side: match order.side.as_str() {
                        "buy" => OrderSide::Buy,
                        "sell" => OrderSide::Sell,
                        _ => OrderSide::Buy,
                    },
                    order_type: match order.order_type.as_str() {
                        "market" => OrderType::Market,
                        "limit" => OrderType::Limit,
                        _ => OrderType::Limit,
                    },
                    amount,
                    price: if order.price != "0" {
                        Some(order.price.parse().unwrap_or(0.0))
                    } else {
                        None
                    },
                    filled,
                    remaining: amount - filled,
                    status: match order.state.as_str() {
                        "1" => OrderStatus::Open,
                        "2" => OrderStatus::Closed,
                        "3" => OrderStatus::Canceled,
                        "4" => OrderStatus::Open,
                        _ => OrderStatus::Pending,
                    },
                    market_type,
                    timestamp: DateTime::from_timestamp(order.create_time / 1000, 0)
                        .unwrap_or_else(|| Utc::now()),
                    last_trade_timestamp: Some(
                        DateTime::from_timestamp(order.update_time / 1000, 0)
                            .unwrap_or_else(|| Utc::now()),
                    ),
                    info: serde_json::json!({}),
                });
            }
        }

        Ok(result)
    }

    async fn get_trades(
        &self,
        symbol: &str,
        market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<Vec<Trade>> {
        // Bitmart的trades接口已被弃用，使用订单簿数据作为替代
        // 注意：这不是真实的成交数据，仅用于演示
        let orderbook = self.get_orderbook(symbol, market_type, limit).await?;

        let mut result = Vec::new();
        let timestamp = Utc::now();

        // 使用买单和卖单的第一档作为模拟的最近成交
        for (index, bid) in orderbook.bids.iter().take(3).enumerate() {
            result.push(Trade {
                id: format!("sim_{}", index),
                symbol: symbol.to_string(),
                side: OrderSide::Buy,
                amount: bid[1],
                price: bid[0],
                timestamp,
                order_id: None,
                fee: None,
            });
        }

        for (index, ask) in orderbook.asks.iter().take(2).enumerate() {
            result.push(Trade {
                id: format!("sim_{}", index + 3),
                symbol: symbol.to_string(),
                side: OrderSide::Sell,
                amount: ask[1],
                price: ask[0],
                timestamp,
                order_id: None,
                fee: None,
            });
        }

        Ok(result)
    }

    async fn get_my_trades(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<Vec<Trade>> {
        let endpoint = match market_type {
            MarketType::Spot => "/spot/v2/trades",
            MarketType::Futures => "/contract/private/trades",
        };

        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            let exchange_symbol =
                self.symbol_converter
                    .to_exchange_symbol(symbol, "bitmart", market_type)?;
            params.insert("symbol".to_string(), exchange_symbol);
        }
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.to_string());
        }

        #[derive(Deserialize)]
        struct BitmartMyTrade {
            trade_id: String,
            order_id: String,
            symbol: String,
            side: String,
            size: String,
            price: String,
            fee: String,
            fee_coin_name: String,
            exec_time: i64,
        }

        let trades: Vec<BitmartMyTrade> = self.send_signed_request("GET", endpoint, params).await?;

        let mut result = Vec::new();
        for trade in trades {
            // 转换交易对格式
            if let Ok(standard_symbol) =
                self.symbol_converter
                    .from_exchange_symbol(&trade.symbol, "bitmart", market_type)
            {
                result.push(Trade {
                    id: trade.trade_id,
                    symbol: standard_symbol,
                    side: match trade.side.as_str() {
                        "buy" => OrderSide::Buy,
                        "sell" => OrderSide::Sell,
                        _ => OrderSide::Buy,
                    },
                    amount: trade.size.parse().unwrap_or(0.0),
                    price: trade.price.parse().unwrap_or(0.0),
                    timestamp: DateTime::from_timestamp(trade.exec_time / 1000, 0)
                        .unwrap_or_else(|| Utc::now()),
                    order_id: Some(trade.order_id),
                    fee: Some(Fee {
                        currency: trade.fee_coin_name,
                        cost: trade.fee.parse::<f64>().unwrap_or(0.0).abs(),
                        rate: None,
                    }),
                });
            }
        }

        Ok(result)
    }

    // === 新增高级功能实现 ===

    async fn get_klines(
        &self,
        symbol: &str,
        interval: Interval,
        market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<Vec<Kline>> {
        let exchange_symbol =
            self.symbol_converter
                .to_exchange_symbol(symbol, "bitmart", market_type)?;

        let (endpoint, params) = match market_type {
            MarketType::Spot => {
                let mut params = HashMap::new();
                params.insert("symbol".to_string(), exchange_symbol);
                params.insert("step".to_string(), interval.to_exchange_format("bitmart"));

                // 添加时间范围参数
                let now = chrono::Utc::now().timestamp();
                let before = now;
                let after = now - 3600 * 24; // 24小时前
                params.insert("before".to_string(), before.to_string());
                params.insert("after".to_string(), after.to_string());

                // Bitmart K线限制最多20条（减少以避免API限制）
                let actual_limit = limit.unwrap_or(20).min(20);
                params.insert("limit".to_string(), actual_limit.to_string());

                ("/spot/quotation/v3/klines", params)
            }
            MarketType::Futures => {
                // 期货K线API
                let mut params = HashMap::new();
                params.insert("symbol".to_string(), exchange_symbol);

                // 期货使用step参数，格式: 1, 5, 15, 30, 60, 120, 240, 360, 720, 1440
                let step = match interval {
                    Interval::OneMinute => "1",
                    Interval::FiveMinutes => "5",
                    Interval::FifteenMinutes => "15",
                    Interval::ThirtyMinutes => "30",
                    Interval::OneHour => "60",
                    Interval::TwoHours => "120",
                    Interval::FourHours => "240",
                    Interval::SixHours => "360",
                    Interval::TwelveHours => "720",
                    Interval::OneDay => "1440",
                    _ => "60", // 默认1小时
                };
                params.insert("step".to_string(), step.to_string());

                // 期货API使用start_time和end_time
                let now = chrono::Utc::now().timestamp();
                let end_time = now;
                let start_time = now - 3600 * 24; // 24小时前
                params.insert("start_time".to_string(), start_time.to_string());
                params.insert("end_time".to_string(), end_time.to_string());

                ("/contract/public/kline", params)
            }
        };

        #[derive(Deserialize)]
        struct BitmartKline {
            timestamp: i64,
            open: String,
            high: String,
            low: String,
            close: String,
            volume: String,
            quote_volume: String,
        }

        #[derive(Deserialize)]
        struct FuturesKline {
            timestamp: i64,
            o: String, // open
            h: String, // high
            l: String, // low
            c: String, // close
            v: String, // volume
        }

        if market_type == MarketType::Futures {
            // 期货K线响应格式不同
            let klines: Vec<FuturesKline> =
                self.send_public_request(endpoint, Some(params)).await?;

            let mut result = Vec::new();
            for kline in klines {
                result.push(Kline {
                    symbol: symbol.to_string(),
                    interval: interval.to_string(),
                    open_time: DateTime::from_timestamp(kline.timestamp, 0)
                        .unwrap_or_else(|| chrono::Utc::now()),
                    close_time: DateTime::from_timestamp(kline.timestamp, 0)
                        .unwrap_or_else(|| chrono::Utc::now()),
                    open: kline.o.parse().unwrap_or(0.0),
                    high: kline.h.parse().unwrap_or(0.0),
                    low: kline.l.parse().unwrap_or(0.0),
                    close: kline.c.parse().unwrap_or(0.0),
                    volume: kline.v.parse().unwrap_or(0.0),
                    quote_volume: 0.0, // 期货API不提供quote_volume
                    trade_count: 0,
                });
            }
            Ok(result)
        } else {
            let klines: Vec<BitmartKline> =
                self.send_public_request(endpoint, Some(params)).await?;

            let mut result = Vec::new();
            for kline in klines {
                result.push(Kline {
                    symbol: symbol.to_string(),
                    interval: interval.to_string(),
                    open_time: DateTime::from_timestamp(kline.timestamp / 1000, 0)
                        .unwrap_or_else(|| chrono::Utc::now()),
                    close_time: DateTime::from_timestamp(kline.timestamp / 1000, 0)
                        .unwrap_or_else(|| chrono::Utc::now()),
                    open: kline.open.parse().unwrap_or(0.0),
                    high: kline.high.parse().unwrap_or(0.0),
                    low: kline.low.parse().unwrap_or(0.0),
                    close: kline.close.parse().unwrap_or(0.0),
                    volume: kline.volume.parse().unwrap_or(0.0),
                    quote_volume: kline.quote_volume.parse().unwrap_or(0.0),
                    trade_count: 0,
                });
            }

            Ok(result)
        }
    }

    async fn get_24h_statistics(
        &self,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<Statistics24h> {
        let exchange_symbol =
            self.symbol_converter
                .to_exchange_symbol(symbol, "bitmart", market_type)?;

        let endpoint = match market_type {
            MarketType::Spot => "/spot/v1/ticker_detail",
            MarketType::Futures => "/contract/public/details",
        };

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), exchange_symbol);

        #[derive(Deserialize)]
        struct BitmartStats {
            symbol: String,
            open_24h: String,
            high_24h: String,
            low_24h: String,
            close_24h: String,
            last_price: String,
            base_volume_24h: String,
            quote_volume_24h: String,
            fluctuation: String, // 涨跌幅
        }

        let stats: BitmartStats = self.send_public_request(endpoint, Some(params)).await?;

        let price_change = stats.last_price.parse::<f64>().unwrap_or(0.0)
            - stats.open_24h.parse::<f64>().unwrap_or(0.0);
        let price_change_percent = stats.fluctuation.parse::<f64>().unwrap_or(0.0) * 100.0;

        Ok(Statistics24h {
            symbol: symbol.to_string(),
            open: stats.open_24h.parse().unwrap_or(0.0),
            high: stats.high_24h.parse().unwrap_or(0.0),
            low: stats.low_24h.parse().unwrap_or(0.0),
            close: stats.close_24h.parse().unwrap_or(0.0),
            volume: stats.base_volume_24h.parse().unwrap_or(0.0),
            quote_volume: stats.quote_volume_24h.parse().unwrap_or(0.0),
            change: price_change,
            change_percent: price_change_percent,
            timestamp: chrono::Utc::now(),
            // 兼容字段
            price_change: Some(price_change),
            price_change_percent: Some(price_change_percent),
            weighted_avg_price: Some(stats.last_price.parse().unwrap_or(0.0)),
            open_price: Some(stats.open_24h.parse().unwrap_or(0.0)),
            high_price: Some(stats.high_24h.parse().unwrap_or(0.0)),
            low_price: Some(stats.low_24h.parse().unwrap_or(0.0)),
            close_price: Some(stats.close_24h.parse().unwrap_or(0.0)),
            count: Some(0),
        })
    }

    async fn get_all_24h_statistics(&self, market_type: MarketType) -> Result<Vec<Statistics24h>> {
        let endpoint = match market_type {
            MarketType::Spot => "/spot/v1/ticker",
            MarketType::Futures => "/contract/public/details",
        };

        #[derive(Deserialize)]
        struct BitmartStats {
            symbol: String,
            open_24h: String,
            high_24h: String,
            low_24h: String,
            close_24h: String,
            last_price: String,
            base_volume_24h: String,
            quote_volume_24h: String,
            #[serde(rename = "rise_fall_rate")]
            rise_fall_rate: String,
            #[serde(rename = "rise_fall_value")]
            rise_fall_value: String,
        }

        let all_stats: Vec<BitmartStats> = self.send_public_request(endpoint, None).await?;

        let mut result = Vec::new();
        for stats in all_stats {
            if let Ok(standard_symbol) =
                self.symbol_converter
                    .from_exchange_symbol(&stats.symbol, "bitmart", market_type)
            {
                let price_change = stats.rise_fall_value.parse().unwrap_or(0.0);
                let price_change_percent =
                    stats.rise_fall_rate.parse::<f64>().unwrap_or(0.0) * 100.0;

                result.push(Statistics24h {
                    symbol: standard_symbol,
                    open: stats.open_24h.parse().unwrap_or(0.0),
                    high: stats.high_24h.parse().unwrap_or(0.0),
                    low: stats.low_24h.parse().unwrap_or(0.0),
                    close: stats.close_24h.parse().unwrap_or(0.0),
                    volume: stats.base_volume_24h.parse().unwrap_or(0.0),
                    quote_volume: stats.quote_volume_24h.parse().unwrap_or(0.0),
                    change: price_change,
                    change_percent: price_change_percent,
                    timestamp: chrono::Utc::now(),
                    // 兼容字段
                    price_change: Some(price_change),
                    price_change_percent: Some(price_change_percent),
                    weighted_avg_price: Some(stats.last_price.parse().unwrap_or(0.0)),
                    open_price: Some(stats.open_24h.parse().unwrap_or(0.0)),
                    high_price: Some(stats.high_24h.parse().unwrap_or(0.0)),
                    low_price: Some(stats.low_24h.parse().unwrap_or(0.0)),
                    close_price: Some(stats.close_24h.parse().unwrap_or(0.0)),
                    count: Some(0),
                });
            }
        }

        Ok(result)
    }

    async fn get_trade_fee(&self, symbol: &str, market_type: MarketType) -> Result<TradeFee> {
        let exchange_symbol =
            self.symbol_converter
                .to_exchange_symbol(symbol, "bitmart", market_type)?;

        let endpoint = match market_type {
            MarketType::Spot => "/spot/v1/trade_fee",
            MarketType::Futures => "/contract/private/trade-fee",
        };

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), exchange_symbol);

        #[derive(Deserialize)]
        struct SpotFeeResponse {
            taker_fee_rate: String,
            maker_fee_rate: String,
        }

        #[derive(Deserialize)]
        struct FuturesFeeResponse {
            taker_fee: String,
            maker_fee: String,
        }

        match market_type {
            MarketType::Spot => {
                match self
                    .send_signed_request::<SpotFeeResponse>("GET", endpoint, params)
                    .await
                {
                    Ok(response) => {
                        let maker = response.maker_fee_rate.parse().unwrap_or(0.0025);
                        let taker = response.taker_fee_rate.parse().unwrap_or(0.0025);
                        Ok(TradeFee {
                            symbol: symbol.to_string(),
                            maker,
                            taker,
                            percentage: true,
                            tier_based: false,
                            maker_fee: Some(maker),
                            taker_fee: Some(taker),
                        })
                    }
                    Err(_) => {
                        // 如果API请求失败，返回默认费率
                        Ok(TradeFee {
                            symbol: symbol.to_string(),
                            maker: 0.0025, // 0.25%
                            taker: 0.0025, // 0.25%
                            percentage: true,
                            tier_based: false,
                            maker_fee: Some(0.0025),
                            taker_fee: Some(0.0025),
                        })
                    }
                }
            }
            MarketType::Futures => {
                match self
                    .send_signed_request::<FuturesFeeResponse>("GET", endpoint, params)
                    .await
                {
                    Ok(response) => {
                        let maker = response.maker_fee.parse().unwrap_or(0.0002);
                        let taker = response.taker_fee.parse().unwrap_or(0.0006);
                        Ok(TradeFee {
                            symbol: symbol.to_string(),
                            maker,
                            taker,
                            percentage: true,
                            tier_based: false,
                            maker_fee: Some(maker),
                            taker_fee: Some(taker),
                        })
                    }
                    Err(_) => {
                        // 如果API请求失败，返回默认费率
                        Ok(TradeFee {
                            symbol: symbol.to_string(),
                            maker: 0.0002, // 0.02%
                            taker: 0.0006, // 0.06%
                            percentage: true,
                            tier_based: false,
                            maker_fee: Some(0.0002),
                            taker_fee: Some(0.0006),
                        })
                    }
                }
            }
        }
    }

    async fn get_account_snapshot(&self, market_type: MarketType) -> Result<AccountSnapshot> {
        let balances = self.get_balance(market_type).await?;

        let mut total_balance_usdt = 0.0;
        let mut total_balance_btc = 0.0;

        for balance in &balances {
            if balance.currency == "USDT" {
                total_balance_usdt += balance.total;
            } else if balance.currency == "BTC" {
                total_balance_btc += balance.total;
                // 获取BTC价格计算等值USDT
                if let Ok(ticker) = self.get_ticker("BTC/USDT", MarketType::Spot).await {
                    total_balance_usdt += balance.total * ticker.last;
                }
            }
        }

        Ok(AccountSnapshot {
            account_type: "spot".to_string(),
            balances,
            timestamp: chrono::Utc::now(),
            // 兼容字段
            total_balance_btc: Some(total_balance_btc),
            total_balance_usdt: Some(total_balance_usdt),
        })
    }

    async fn get_positions(&self, symbol: Option<&str>) -> Result<Vec<Position>> {
        // Bitmart期货持仓查询
        let endpoint = "/contract/private/position";

        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            let exchange_symbol =
                self.symbol_converter
                    .to_exchange_symbol(symbol, "bitmart", MarketType::Futures)?;
            params.insert("symbol".to_string(), exchange_symbol);
        }

        #[derive(Deserialize)]
        struct BitmartPosition {
            symbol: String,
            side: String,
            leverage: String,
            open_avg_price: String,
            mark_price: String,
            position_value: String,
            position: String,
            unrealized_pnl: String,
            realized_pnl: String,
            margin: String,
            #[serde(rename = "maint_margin_rate")]
            maint_margin_rate: String,
            timestamp: i64,
        }

        let positions: Vec<BitmartPosition> =
            self.send_signed_request("GET", endpoint, params).await?;

        let mut result = Vec::new();
        for pos in positions {
            let size = pos.position.parse::<f64>().unwrap_or(0.0);
            if size.abs() > 0.0 {
                if let Ok(standard_symbol) = self.symbol_converter.from_exchange_symbol(
                    &pos.symbol,
                    "bitmart",
                    MarketType::Futures,
                ) {
                    let unrealized_pnl = pos.unrealized_pnl.parse::<f64>().unwrap_or(0.0);
                    let entry_price = pos.open_avg_price.parse::<f64>().unwrap_or(0.0);
                    let percentage = if entry_price > 0.0 {
                        (unrealized_pnl / (size.abs() * entry_price)) * 100.0
                    } else {
                        0.0
                    };

                    result.push(Position {
                        symbol: standard_symbol,
                        side: pos.side,
                        contracts: size.abs(),
                        contract_size: 1.0, // USDT本位合约
                        size: size.abs(),
                        entry_price,
                        mark_price: pos.mark_price.parse().unwrap_or(0.0),
                        unrealized_pnl,
                        percentage,
                        margin: pos.margin.parse().unwrap_or(0.0),
                        margin_ratio: pos.maint_margin_rate.parse().unwrap_or(0.0),
                        leverage: pos.leverage.parse::<u32>().ok(),
                        margin_type: None, // Bitmart doesn't provide this in response
                        amount: size,
                        timestamp: DateTime::from_timestamp(pos.timestamp / 1000, 0)
                            .unwrap_or_else(|| chrono::Utc::now()),
                    });
                }
            }
        }

        Ok(result)
    }

    async fn set_leverage(&self, symbol: &str, leverage: u32) -> Result<()> {
        let exchange_symbol =
            self.symbol_converter
                .to_exchange_symbol(symbol, "bitmart", MarketType::Futures)?;

        let endpoint = "/contract/private/leverage";
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), exchange_symbol);
        params.insert("leverage".to_string(), leverage.to_string());
        params.insert("side".to_string(), "1".to_string()); // 1=多仓，2=空仓

        #[derive(Deserialize)]
        struct BitmartLeverageResponse {
            result: bool,
        }

        let _response: BitmartLeverageResponse =
            self.send_signed_request("POST", endpoint, params).await?;

        Ok(())
    }

    async fn cancel_all_orders(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
    ) -> Result<Vec<Order>> {
        let endpoint = match market_type {
            MarketType::Spot => "/spot/v3/cancel_orders",
            MarketType::Futures => "/contract/private/cancel-orders",
        };

        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            let exchange_symbol =
                self.symbol_converter
                    .to_exchange_symbol(symbol, "bitmart", market_type)?;
            params.insert("symbol".to_string(), exchange_symbol);
        }

        #[derive(Deserialize)]
        struct BitmartCancelResponse {
            order_ids: Vec<String>,
        }

        let response: BitmartCancelResponse =
            self.send_signed_request("POST", endpoint, params).await?;

        let mut result = Vec::new();
        for order_id in response.order_ids {
            result.push(Order {
                id: order_id,
                symbol: symbol.unwrap_or("").to_string(),
                side: OrderSide::Buy,
                order_type: OrderType::Limit,
                amount: 0.0,
                price: None,
                filled: 0.0,
                remaining: 0.0,
                status: OrderStatus::Canceled,
                market_type,
                timestamp: chrono::Utc::now(),
                last_trade_timestamp: None,
                info: serde_json::Value::Null,
            });
        }

        Ok(result)
    }

    async fn get_server_time(&self) -> Result<chrono::DateTime<chrono::Utc>> {
        #[derive(Deserialize)]
        struct ServerTime {
            server_time: i64,
        }

        let time: ServerTime = self.send_public_request("/system/time", None).await?;

        Ok(DateTime::from_timestamp(time.server_time / 1000, 0)
            .unwrap_or_else(|| chrono::Utc::now()))
    }

    async fn ping(&self) -> Result<()> {
        let exchange_config = self.base.get_exchange_config()?;

        let response = self
            .base
            .client
            .get(&format!("{}/system/service", exchange_config.base_url))
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(ExchangeError::NetworkError(reqwest::Error::from(
                response.error_for_status().unwrap_err(),
            )))
        }
    }

    async fn create_batch_orders(
        &self,
        batch_request: BatchOrderRequest,
    ) -> Result<BatchOrderResponse> {
        // 检查是否为期货订单且数量合适
        if !batch_request.orders.is_empty()
            && batch_request.orders[0].market_type == MarketType::Futures
            && batch_request.orders.len() <= 10
        {
            // Bitmart期货支持批量下单（最多10个订单）
            let mut order_data = Vec::new();

            for order in &batch_request.orders {
                let exchange_symbol = self.symbol_converter.to_exchange_symbol(
                    &order.symbol,
                    "bitmart",
                    order.market_type,
                )?;

                let mut params = serde_json::Map::new();
                params.insert(
                    "symbol".to_string(),
                    serde_json::Value::String(exchange_symbol),
                );
                params.insert(
                    "side".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(match order.side {
                        OrderSide::Buy => 1,
                        OrderSide::Sell => 2,
                    })),
                );
                params.insert(
                    "type".to_string(),
                    serde_json::Value::String(
                        match order.order_type {
                            OrderType::Limit => "limit",
                            OrderType::Market => "market",
                            _ => "limit",
                        }
                        .to_string(),
                    ),
                );
                params.insert(
                    "size".to_string(),
                    serde_json::Value::Number(serde_json::Number::from_f64(order.amount).unwrap()),
                );

                if let OrderType::Limit = order.order_type {
                    if let Some(price) = order.price {
                        params.insert(
                            "price".to_string(),
                            serde_json::Value::Number(serde_json::Number::from_f64(price).unwrap()),
                        );
                    }
                }

                order_data.push(serde_json::Value::Object(params));
            }

            let mut request_params = HashMap::new();
            request_params.insert(
                "order_data".to_string(),
                serde_json::to_string(&order_data)?,
            );

            // 发送批量订单请求
            let endpoint = "/contract/private/submit-plan-order";

            #[derive(Deserialize)]
            struct BatchResponse {
                order_ids: Vec<String>,
            }

            match self
                .send_signed_request::<BatchResponse>("POST", endpoint, request_params)
                .await
            {
                Ok(response) => {
                    let mut successful_orders = Vec::new();
                    for (i, order_id) in response.order_ids.iter().enumerate() {
                        if let Some(order_request) = batch_request.orders.get(i) {
                            successful_orders.push(Order {
                                id: order_id.clone(),
                                symbol: order_request.symbol.clone(),
                                order_type: order_request.order_type.clone(),
                                side: order_request.side.clone(),
                                price: order_request.price,
                                amount: order_request.amount,
                                status: OrderStatus::Open,
                                filled: 0.0,
                                remaining: order_request.amount,
                                market_type: order_request.market_type.clone(),
                                timestamp: chrono::Utc::now(),
                                last_trade_timestamp: None,
                                info: serde_json::Value::Null,
                            });
                        }
                    }

                    Ok(BatchOrderResponse {
                        successful_orders,
                        failed_orders: Vec::new(),
                    })
                }
                Err(_) => {
                    // 如果批量请求失败，回退到逐个下单
                    let mut successful_orders = Vec::new();
                    let mut failed_orders = Vec::new();

                    for order_request in batch_request.orders {
                        match self.create_order(order_request.clone()).await {
                            Ok(order) => successful_orders.push(order),
                            Err(e) => {
                                failed_orders.push(BatchOrderError {
                                    order_request,
                                    error_message: e.to_string(),
                                    error_code: match e {
                                        ExchangeError::ApiError { code, .. } => {
                                            Some(code.to_string())
                                        }
                                        _ => None,
                                    },
                                });
                            }
                        }
                    }

                    Ok(BatchOrderResponse {
                        successful_orders,
                        failed_orders,
                    })
                }
            }
        } else {
            // 现货或订单数量超过10个，逐个下单
            let mut successful_orders = Vec::new();
            let mut failed_orders = Vec::new();

            for order_request in batch_request.orders {
                match self.create_order(order_request.clone()).await {
                    Ok(order) => successful_orders.push(order),
                    Err(e) => {
                        failed_orders.push(BatchOrderError {
                            order_request,
                            error_message: e.to_string(),
                            error_code: match e {
                                ExchangeError::ApiError { code, .. } => Some(code.to_string()),
                                _ => None,
                            },
                        });
                    }
                }
            }

            Ok(BatchOrderResponse {
                successful_orders,
                failed_orders,
            })
        }
    }

    async fn get_all_spot_symbols(&self) -> Result<Vec<TradingPair>> {
        #[derive(Deserialize)]
        struct BitmartSymbols {
            symbols: Vec<BitmartSymbol>,
        }

        #[derive(Deserialize)]
        struct BitmartSymbol {
            symbol: String,
            base_currency: String,
            quote_currency: String,
            status: String,
            base_min_size: String,
            base_max_size: String,
            price_min_precision: i32,
            price_max_precision: i32,
            size_min_precision: i32,
            size_max_precision: i32,
        }

        // 使用公开API获取交易对详情
        let symbols_data: BitmartSymbols = self
            .send_public_request("/spot/v1/symbols/details", None)
            .await?;

        let mut result = Vec::new();
        for symbol in symbols_data.symbols {
            if symbol.status == "online" {
                let tick_size = 10_f64.powi(-symbol.price_max_precision);
                let step_size = 10_f64.powi(-symbol.size_max_precision);

                result.push(TradingPair {
                    symbol: format!("{}/{}", symbol.base_currency, symbol.quote_currency),
                    base_asset: symbol.base_currency,
                    quote_asset: symbol.quote_currency,
                    status: symbol.status,
                    min_order_size: symbol.base_min_size.parse().unwrap_or(0.0),
                    max_order_size: symbol.base_max_size.parse().unwrap_or(0.0),
                    tick_size,
                    step_size,
                    min_notional: None,
                    is_trading: true,
                    market_type: MarketType::Spot,
                });
            }
        }
        Ok(result)
    }

    async fn get_all_futures_symbols(&self) -> Result<Vec<TradingPair>> {
        #[derive(Deserialize)]
        struct BitmartContractsResponse {
            code: i32,
            message: String,
            data: Option<BitmartContracts>,
        }

        #[derive(Deserialize)]
        struct BitmartContracts {
            symbols: Vec<BitmartContract>,
        }

        #[derive(Deserialize)]
        struct BitmartContract {
            symbol: String,
            base_currency: String,
            quote_currency: String,
            is_trading: i32,
            min_volume: String,
            max_volume: String,
            tick_size: String,
            volume_precision: i32,
        }

        let response: BitmartContractsResponse = self
            .send_public_request("/contract/public/details", None)
            .await?;

        if response.code == 1000 {
            if let Some(data) = response.data {
                let mut result = Vec::new();
                for contract in data.symbols {
                    if contract.is_trading == 1 {
                        let step_size = 10_f64.powi(-contract.volume_precision);

                        result.push(TradingPair {
                            symbol: format!(
                                "{}/{}",
                                contract.base_currency, contract.quote_currency
                            ),
                            base_asset: contract.base_currency,
                            quote_asset: contract.quote_currency,
                            status: if contract.is_trading == 1 {
                                "online".to_string()
                            } else {
                                "offline".to_string()
                            },
                            min_order_size: contract.min_volume.parse().unwrap_or(0.0),
                            max_order_size: contract.max_volume.parse().unwrap_or(0.0),
                            tick_size: contract.tick_size.parse().unwrap_or(0.0),
                            step_size,
                            min_notional: None,
                            is_trading: contract.is_trading == 1,
                            market_type: MarketType::Futures,
                        });
                    }
                }
                Ok(result)
            } else {
                Ok(Vec::new())
            }
        } else {
            Err(ExchangeError::ApiError {
                code: response.code,
                message: response.message,
            })
        }
    }

    async fn get_symbol_info(&self, symbol: &str, market_type: MarketType) -> Result<TradingPair> {
        // 获取所有交易对然后查找特定的交易对
        let all_symbols = match market_type {
            MarketType::Spot => self.get_all_spot_symbols().await?,
            MarketType::Futures => self.get_all_futures_symbols().await?,
        };

        all_symbols
            .into_iter()
            .find(|s| s.symbol == symbol)
            .ok_or_else(|| ExchangeError::SymbolNotFound {
                symbol: symbol.to_string(),
                market_type,
            })
    }

    async fn create_websocket_client(
        &self,
        market_type: MarketType,
    ) -> Result<Box<dyn crate::core::websocket::WebSocketClient>> {
        use crate::core::websocket::WebSocketClient;

        let ws_url = self.get_websocket_url(market_type);
        let client =
            crate::core::websocket::BaseWebSocketClient::new(ws_url, "bitmart".to_string());

        Ok(Box::new(client))
    }

    fn get_websocket_url(&self, market_type: MarketType) -> String {
        match market_type {
            MarketType::Spot => {
                "wss://ws-manager-compress.bitmart.com/api?protocol=1.1".to_string()
            }
            MarketType::Futures => "wss://openapi-ws-v2.bitmart.com/api?protocol=1.1".to_string(),
        }
    }
}
