use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::core::{
    config::{ApiKeys, Config},
    error::ExchangeError,
    exchange::{BaseExchange, Exchange},
    types::*,
    websocket::{ConnectionState, MessageHandler, WebSocketClient},
};
use crate::utils::{SignatureHelper, SymbolConverter};

/// OKX交易所实现
pub struct OkxExchange {
    base: BaseExchange,
    symbol_converter: SymbolConverter,
}

impl OkxExchange {
    /// 创建OKX交易所实例
    pub fn new(config: Config, api_keys: ApiKeys) -> Self {
        let base = BaseExchange::new("okx".to_string(), config.clone(), api_keys);
        let symbol_converter = SymbolConverter::new(config);

        Self {
            base,
            symbol_converter,
        }
    }

    /// 发送认证请求到OKX
    async fn send_signed_request<T>(&self, method: &str, endpoint: &str, body: &str) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let exchange_config = self.base.get_exchange_config()?;

        // 生成时间戳
        let timestamp = chrono::Utc::now()
            .format("%Y-%m-%dT%H:%M:%S.%3fZ")
            .to_string();

        // 生成签名
        let signature = SignatureHelper::okx_signature(
            &self.base.api_keys.api_secret,
            &timestamp,
            method,
            endpoint,
            body,
        );

        // 构建完整URL
        let url = format!("{}{}", exchange_config.base_url, endpoint);

        // 设置请求头
        let mut request = match method.to_uppercase().as_str() {
            "GET" => self.base.client.get(&url),
            "POST" => self.base.client.post(&url),
            "DELETE" => self.base.client.delete(&url),
            _ => return Err(ExchangeError::Other("不支持的HTTP方法".to_string())),
        };

        // 添加OKX必需的请求头
        request = request
            .header("OK-ACCESS-KEY", &self.base.api_keys.api_key)
            .header("OK-ACCESS-SIGN", signature)
            .header("OK-ACCESS-TIMESTAMP", timestamp)
            .header(
                "OK-ACCESS-PASSPHRASE",
                self.base
                    .api_keys
                    .passphrase
                    .as_ref()
                    .ok_or_else(|| ExchangeError::AuthError("OKX需要PASSPHRASE".to_string()))?,
            )
            .header("Content-Type", "application/json");

        // 如果是POST请求，添加请求体
        if method.to_uppercase() == "POST" && !body.is_empty() {
            request = request.body(body.to_string());
        }

        let response = request.send().await?;

        // 处理响应
        if response.status().is_success() {
            #[derive(Deserialize)]
            struct OkxResponse<T> {
                code: String,
                msg: String,
                data: Option<T>,
            }

            let okx_response: OkxResponse<T> = response.json().await?;

            if okx_response.code == "0" {
                okx_response.data.ok_or_else(|| ExchangeError::ApiError {
                    code: 0,
                    message: "API返回空数据".to_string(),
                })
            } else {
                Err(ExchangeError::ApiError {
                    code: okx_response.code.parse().unwrap_or(-1),
                    message: okx_response.msg,
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

    /// 发送公共请求到OKX
    async fn send_public_request<T>(
        &self,
        endpoint: &str,
        params: Option<HashMap<String, String>>,
    ) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let exchange_config = self.base.get_exchange_config()?;

        let mut url = format!("{}{}", exchange_config.base_url, endpoint);

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
            struct OkxResponse<T> {
                code: String,
                msg: String,
                data: Option<T>,
            }

            let okx_response: OkxResponse<T> = response.json().await?;

            if okx_response.code == "0" {
                okx_response.data.ok_or_else(|| ExchangeError::ApiError {
                    code: 0,
                    message: "API返回空数据".to_string(),
                })
            } else {
                Err(ExchangeError::ApiError {
                    code: okx_response.code.parse().unwrap_or(-1),
                    message: okx_response.msg,
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
impl Exchange for OkxExchange {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.base.name
    }

    async fn exchange_info(&self) -> Result<ExchangeInfo> {
        #[derive(Deserialize)]
        struct OkxInstrument {
            #[serde(rename = "instId")]
            inst_id: String,
            #[serde(rename = "baseCcy")]
            base_ccy: String,
            #[serde(rename = "quoteCcy")]
            quote_ccy: String,
            #[serde(rename = "instType")]
            inst_type: String,
            state: String,
        }

        // 获取现货交易对信息
        let spot_instruments: Vec<OkxInstrument> = self
            .send_public_request("/api/v5/public/instruments?instType=SPOT", None)
            .await?;

        let mut symbols = Vec::new();
        for instrument in spot_instruments {
            if instrument.state == "live" {
                symbols.push(Symbol {
                    base: instrument.base_ccy.clone(),
                    quote: instrument.quote_ccy.clone(),
                    symbol: format!("{}/{}", instrument.base_ccy, instrument.quote_ccy),
                    market_type: MarketType::Spot,
                    exchange_specific: Some(instrument.inst_id),
                });
            }
        }

        // 获取永续合约信息
        let swap_instruments: Vec<OkxInstrument> = self
            .send_public_request("/api/v5/public/instruments?instType=SWAP", None)
            .await?;

        for instrument in swap_instruments {
            if instrument.state == "live" {
                symbols.push(Symbol {
                    base: instrument.base_ccy.clone(),
                    quote: instrument.quote_ccy.clone(),
                    symbol: format!("{}/{}", instrument.base_ccy, instrument.quote_ccy),
                    market_type: MarketType::Futures,
                    exchange_specific: Some(instrument.inst_id),
                });
            }
        }

        Ok(ExchangeInfo {
            name: "OKX".to_string(),
            symbols,
            currencies: vec![], // 可以后续实现
            spot_enabled: true,
            futures_enabled: true,
        })
    }

    async fn get_balance(&self, market_type: MarketType) -> Result<Vec<Balance>> {
        #[derive(Deserialize)]
        struct OkxBalanceData {
            details: Vec<OkxBalance>,
        }

        #[derive(Deserialize)]
        struct OkxBalance {
            ccy: String,
            #[serde(rename = "availBal")]
            avail_bal: Option<String>,
            #[serde(rename = "frozenBal")]
            frozen_bal: Option<String>,
            #[serde(rename = "cashBal")]
            cash_bal: Option<String>,
        }

        let balance_data: Vec<OkxBalanceData> = self
            .send_signed_request("GET", "/api/v5/account/balance", "")
            .await?;

        let mut balances = Vec::new();
        for data in balance_data {
            for balance in data.details {
                let free = balance
                    .avail_bal
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let used = balance
                    .frozen_bal
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let total = balance
                    .cash_bal
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(free + used);

                if total > 0.0 {
                    balances.push(Balance {
                        currency: balance.ccy,
                        total,
                        free,
                        used,
                        market_type: market_type.clone(),
                    });
                }
            }
        }

        Ok(balances)
    }

    async fn get_ticker(&self, symbol: &str, market_type: MarketType) -> Result<Ticker> {
        let exchange_symbol =
            self.symbol_converter
                .to_exchange_symbol(symbol, "okx", market_type)?;

        let mut params = HashMap::new();
        params.insert("instId".to_string(), exchange_symbol);

        #[derive(Deserialize)]
        struct OkxTicker {
            #[serde(rename = "instId")]
            inst_id: String,
            high24h: String,
            low24h: String,
            #[serde(rename = "bidPx")]
            bid_px: String,
            #[serde(rename = "askPx")]
            ask_px: String,
            last: String,
            #[serde(rename = "vol24h")]
            vol_24h: String,
            ts: String,
        }

        let tickers: Vec<OkxTicker> = self
            .send_public_request("/api/v5/market/ticker", Some(params))
            .await?;

        if let Some(ticker) = tickers.first() {
            Ok(Ticker {
                symbol: symbol.to_string(),
                high: ticker.high24h.parse().unwrap_or(0.0),
                low: ticker.low24h.parse().unwrap_or(0.0),
                bid: ticker.bid_px.parse().unwrap_or(0.0),
                ask: ticker.ask_px.parse().unwrap_or(0.0),
                last: ticker.last.parse().unwrap_or(0.0),
                volume: ticker.vol_24h.parse().unwrap_or(0.0),
                timestamp: DateTime::from_timestamp(
                    ticker.ts.parse::<i64>().unwrap_or(0) / 1000,
                    0,
                )
                .unwrap_or_else(|| Utc::now()),
            })
        } else {
            Err(ExchangeError::SymbolError(format!(
                "未找到交易对: {}",
                symbol
            )))
        }
    }

    async fn get_all_tickers(&self, market_type: MarketType) -> Result<Vec<Ticker>> {
        let inst_type = match market_type {
            MarketType::Spot => "SPOT",
            MarketType::Futures => "SWAP",
        };

        let mut params = HashMap::new();
        params.insert("instType".to_string(), inst_type.to_string());

        #[derive(Deserialize)]
        struct OkxTicker {
            #[serde(rename = "instId")]
            inst_id: String,
            high24h: String,
            low24h: String,
            #[serde(rename = "bidPx")]
            bid_px: String,
            #[serde(rename = "askPx")]
            ask_px: String,
            last: String,
            #[serde(rename = "vol24h")]
            vol_24h: String,
            ts: String,
        }

        let tickers: Vec<OkxTicker> = self
            .send_public_request("/api/v5/market/tickers", Some(params))
            .await?;

        let mut result = Vec::new();
        for ticker in tickers {
            // 转换交易对格式
            if let Ok(standard_symbol) =
                self.symbol_converter
                    .from_exchange_symbol(&ticker.inst_id, "okx", market_type)
            {
                result.push(Ticker {
                    symbol: standard_symbol,
                    high: ticker.high24h.parse().unwrap_or(0.0),
                    low: ticker.low24h.parse().unwrap_or(0.0),
                    bid: ticker.bid_px.parse().unwrap_or(0.0),
                    ask: ticker.ask_px.parse().unwrap_or(0.0),
                    last: ticker.last.parse().unwrap_or(0.0),
                    volume: ticker.vol_24h.parse().unwrap_or(0.0),
                    timestamp: DateTime::from_timestamp(
                        ticker.ts.parse::<i64>().unwrap_or(0) / 1000,
                        0,
                    )
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
                .to_exchange_symbol(symbol, "okx", market_type)?;

        let mut params = HashMap::new();
        params.insert("instId".to_string(), exchange_symbol);
        if let Some(limit) = limit {
            params.insert("sz".to_string(), limit.to_string());
        }

        #[derive(Deserialize)]
        struct OkxOrderBook {
            bids: Vec<[String; 4]>,
            asks: Vec<[String; 4]>,
            ts: String,
        }

        let orderbooks: Vec<OkxOrderBook> = self
            .send_public_request("/api/v5/market/books", Some(params))
            .await?;

        if let Some(orderbook) = orderbooks.first() {
            let bids: Vec<[f64; 2]> = orderbook
                .bids
                .iter()
                .map(|bid| [bid[0].parse().unwrap_or(0.0), bid[1].parse().unwrap_or(0.0)])
                .collect();

            let asks: Vec<[f64; 2]> = orderbook
                .asks
                .iter()
                .map(|ask| [ask[0].parse().unwrap_or(0.0), ask[1].parse().unwrap_or(0.0)])
                .collect();

            Ok(OrderBook {
                symbol: symbol.to_string(),
                bids,
                asks,
                timestamp: DateTime::from_timestamp(
                    orderbook.ts.parse::<i64>().unwrap_or(0) / 1000,
                    0,
                )
                .unwrap_or_else(|| Utc::now()),
            })
        } else {
            Err(ExchangeError::SymbolError(format!(
                "未找到交易对的订单簿: {}",
                symbol
            )))
        }
    }

    async fn create_order(&self, order_request: OrderRequest) -> Result<Order> {
        let exchange_symbol = self.symbol_converter.to_exchange_symbol(
            &order_request.symbol,
            "okx",
            order_request.market_type,
        )?;

        #[derive(Serialize)]
        struct OkxOrderRequest {
            #[serde(rename = "instId")]
            inst_id: String,
            #[serde(rename = "tdMode")]
            td_mode: String,
            side: String,
            #[serde(rename = "ordType")]
            ord_type: String,
            sz: String,
            px: Option<String>,
        }

        let request_body = OkxOrderRequest {
            inst_id: exchange_symbol,
            td_mode: match order_request.market_type {
                MarketType::Spot => "cash".to_string(),
                MarketType::Futures => "isolated".to_string(), // 或 "cross"
            },
            side: match order_request.side {
                OrderSide::Buy => "buy".to_string(),
                OrderSide::Sell => "sell".to_string(),
            },
            ord_type: match order_request.order_type {
                OrderType::Market => "market".to_string(),
                OrderType::Limit => "limit".to_string(),
                OrderType::StopLimit => "conditional".to_string(),
                OrderType::StopMarket => "market".to_string(), // OKX使用条件单
                OrderType::TakeProfitLimit => "limit".to_string(), // OKX使用条件单
                OrderType::TakeProfitMarket => "market".to_string(), // OKX使用条件单
                OrderType::TrailingStop => "market".to_string(), // OKX使用移动止损
            },
            sz: order_request.amount.to_string(),
            px: order_request.price.map(|p| p.to_string()),
        };

        let body = serde_json::to_string(&[request_body])?;

        #[derive(Deserialize)]
        struct OkxOrderResponse {
            #[serde(rename = "ordId")]
            ord_id: String,
            #[serde(rename = "sCode")]
            s_code: String,
            #[serde(rename = "sMsg")]
            s_msg: String,
        }

        let responses: Vec<OkxOrderResponse> = self
            .send_signed_request("POST", "/api/v5/trade/order", &body)
            .await?;

        if let Some(response) = responses.first() {
            if response.s_code == "0" {
                Ok(Order {
                    id: response.ord_id.clone(),
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
            } else {
                Err(ExchangeError::OrderError(response.s_msg.clone()))
            }
        } else {
            Err(ExchangeError::OrderError(
                "创建订单失败，无响应数据".to_string(),
            ))
        }
    }

    async fn cancel_order(
        &self,
        order_id: &str,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<Order> {
        let exchange_symbol =
            self.symbol_converter
                .to_exchange_symbol(symbol, "okx", market_type)?;

        #[derive(Serialize)]
        struct OkxCancelRequest {
            #[serde(rename = "instId")]
            inst_id: String,
            #[serde(rename = "ordId")]
            ord_id: String,
        }

        let request_body = OkxCancelRequest {
            inst_id: exchange_symbol,
            ord_id: order_id.to_string(),
        };

        let body = serde_json::to_string(&[request_body])?;

        #[derive(Deserialize)]
        struct OkxCancelResponse {
            #[serde(rename = "ordId")]
            ord_id: String,
            #[serde(rename = "sCode")]
            s_code: String,
            #[serde(rename = "sMsg")]
            s_msg: String,
        }

        let responses: Vec<OkxCancelResponse> = self
            .send_signed_request("POST", "/api/v5/trade/cancel-order", &body)
            .await?;

        if let Some(response) = responses.first() {
            if response.s_code == "0" {
                // 获取订单详情
                self.get_order(order_id, symbol, market_type).await
            } else {
                Err(ExchangeError::OrderError(response.s_msg.clone()))
            }
        } else {
            Err(ExchangeError::OrderError(
                "取消订单失败，无响应数据".to_string(),
            ))
        }
    }

    async fn get_order(
        &self,
        order_id: &str,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<Order> {
        let exchange_symbol =
            self.symbol_converter
                .to_exchange_symbol(symbol, "okx", market_type)?;

        let mut params = HashMap::new();
        params.insert("instId".to_string(), exchange_symbol);
        params.insert("ordId".to_string(), order_id.to_string());

        #[derive(Deserialize)]
        struct OkxOrder {
            #[serde(rename = "ordId")]
            ord_id: String,
            #[serde(rename = "instId")]
            inst_id: String,
            side: String,
            #[serde(rename = "ordType")]
            ord_type: String,
            sz: String,
            px: String,
            #[serde(rename = "fillSz")]
            fill_sz: String,
            state: String,
            #[serde(rename = "cTime")]
            c_time: String,
            #[serde(rename = "uTime")]
            u_time: String,
        }

        let orders: Vec<OkxOrder> = self
            .send_signed_request("GET", "/api/v5/trade/order", "")
            .await?;

        if let Some(order) = orders.first() {
            let filled = order.fill_sz.parse::<f64>().unwrap_or(0.0);
            let amount = order.sz.parse::<f64>().unwrap_or(0.0);

            Ok(Order {
                id: order.ord_id.clone(),
                symbol: symbol.to_string(),
                side: match order.side.as_str() {
                    "buy" => OrderSide::Buy,
                    "sell" => OrderSide::Sell,
                    _ => OrderSide::Buy,
                },
                order_type: match order.ord_type.as_str() {
                    "market" => OrderType::Market,
                    "limit" => OrderType::Limit,
                    _ => OrderType::Limit,
                },
                amount,
                price: if order.px != "" {
                    Some(order.px.parse().unwrap_or(0.0))
                } else {
                    None
                },
                filled,
                remaining: amount - filled,
                status: match order.state.as_str() {
                    "live" => OrderStatus::Open,
                    "filled" => OrderStatus::Closed,
                    "canceled" => OrderStatus::Canceled,
                    _ => OrderStatus::Pending,
                },
                market_type,
                timestamp: DateTime::from_timestamp(
                    order.c_time.parse::<i64>().unwrap_or(0) / 1000,
                    0,
                )
                .unwrap_or_else(|| Utc::now()),
                last_trade_timestamp: Some(
                    DateTime::from_timestamp(order.u_time.parse::<i64>().unwrap_or(0) / 1000, 0)
                        .unwrap_or_else(|| Utc::now()),
                ),
                info: serde_json::json!({}),
            })
        } else {
            Err(ExchangeError::OrderError(format!(
                "未找到订单: {}",
                order_id
            )))
        }
    }

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
    ) -> Result<Vec<Order>> {
        let inst_type = match market_type {
            MarketType::Spot => "SPOT",
            MarketType::Futures => "SWAP",
        };

        let mut params = HashMap::new();
        params.insert("instType".to_string(), inst_type.to_string());
        if let Some(symbol) = symbol {
            let exchange_symbol =
                self.symbol_converter
                    .to_exchange_symbol(symbol, "okx", market_type)?;
            params.insert("instId".to_string(), exchange_symbol);
        }

        #[derive(Deserialize)]
        struct OkxOrder {
            #[serde(rename = "ordId")]
            ord_id: String,
            #[serde(rename = "instId")]
            inst_id: String,
            side: String,
            #[serde(rename = "ordType")]
            ord_type: String,
            sz: String,
            px: String,
            #[serde(rename = "fillSz")]
            fill_sz: String,
            state: String,
            #[serde(rename = "cTime")]
            c_time: String,
            #[serde(rename = "uTime")]
            u_time: String,
        }

        let orders: Vec<OkxOrder> = self
            .send_signed_request("GET", "/api/v5/trade/orders-pending", "")
            .await?;

        let mut result = Vec::new();
        for order in orders {
            // 转换交易对格式
            if let Ok(standard_symbol) =
                self.symbol_converter
                    .from_exchange_symbol(&order.inst_id, "okx", market_type)
            {
                let filled = order.fill_sz.parse::<f64>().unwrap_or(0.0);
                let amount = order.sz.parse::<f64>().unwrap_or(0.0);

                result.push(Order {
                    id: order.ord_id,
                    symbol: standard_symbol,
                    side: match order.side.as_str() {
                        "buy" => OrderSide::Buy,
                        "sell" => OrderSide::Sell,
                        _ => OrderSide::Buy,
                    },
                    order_type: match order.ord_type.as_str() {
                        "market" => OrderType::Market,
                        "limit" => OrderType::Limit,
                        _ => OrderType::Limit,
                    },
                    amount,
                    price: if order.px != "" {
                        Some(order.px.parse().unwrap_or(0.0))
                    } else {
                        None
                    },
                    filled,
                    remaining: amount - filled,
                    status: OrderStatus::Open,
                    market_type,
                    timestamp: DateTime::from_timestamp(
                        order.c_time.parse::<i64>().unwrap_or(0) / 1000,
                        0,
                    )
                    .unwrap_or_else(|| Utc::now()),
                    last_trade_timestamp: Some(
                        DateTime::from_timestamp(
                            order.u_time.parse::<i64>().unwrap_or(0) / 1000,
                            0,
                        )
                        .unwrap_or_else(|| Utc::now()),
                    ),
                    info: serde_json::json!({}),
                });
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
        let inst_type = match market_type {
            MarketType::Spot => "SPOT",
            MarketType::Futures => "SWAP",
        };

        let mut params = HashMap::new();
        params.insert("instType".to_string(), inst_type.to_string());
        if let Some(symbol) = symbol {
            let exchange_symbol =
                self.symbol_converter
                    .to_exchange_symbol(symbol, "okx", market_type)?;
            params.insert("instId".to_string(), exchange_symbol);
        }
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.to_string());
        }

        #[derive(Deserialize)]
        struct OkxOrder {
            #[serde(rename = "ordId")]
            ord_id: String,
            #[serde(rename = "instId")]
            inst_id: String,
            side: String,
            #[serde(rename = "ordType")]
            ord_type: String,
            sz: String,
            px: String,
            #[serde(rename = "fillSz")]
            fill_sz: String,
            state: String,
            #[serde(rename = "cTime")]
            c_time: String,
            #[serde(rename = "uTime")]
            u_time: String,
        }

        let orders: Vec<OkxOrder> = self
            .send_signed_request("GET", "/api/v5/trade/orders-history", "")
            .await?;

        let mut result = Vec::new();
        for order in orders {
            // 转换交易对格式
            if let Ok(standard_symbol) =
                self.symbol_converter
                    .from_exchange_symbol(&order.inst_id, "okx", market_type)
            {
                let filled = order.fill_sz.parse::<f64>().unwrap_or(0.0);
                let amount = order.sz.parse::<f64>().unwrap_or(0.0);

                result.push(Order {
                    id: order.ord_id,
                    symbol: standard_symbol,
                    side: match order.side.as_str() {
                        "buy" => OrderSide::Buy,
                        "sell" => OrderSide::Sell,
                        _ => OrderSide::Buy,
                    },
                    order_type: match order.ord_type.as_str() {
                        "market" => OrderType::Market,
                        "limit" => OrderType::Limit,
                        _ => OrderType::Limit,
                    },
                    amount,
                    price: if order.px != "" {
                        Some(order.px.parse().unwrap_or(0.0))
                    } else {
                        None
                    },
                    filled,
                    remaining: amount - filled,
                    status: match order.state.as_str() {
                        "live" => OrderStatus::Open,
                        "filled" => OrderStatus::Closed,
                        "canceled" => OrderStatus::Canceled,
                        _ => OrderStatus::Pending,
                    },
                    market_type,
                    timestamp: DateTime::from_timestamp(
                        order.c_time.parse::<i64>().unwrap_or(0) / 1000,
                        0,
                    )
                    .unwrap_or_else(|| Utc::now()),
                    last_trade_timestamp: Some(
                        DateTime::from_timestamp(
                            order.u_time.parse::<i64>().unwrap_or(0) / 1000,
                            0,
                        )
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
        let exchange_symbol =
            self.symbol_converter
                .to_exchange_symbol(symbol, "okx", market_type)?;

        let mut params = HashMap::new();
        params.insert("instId".to_string(), exchange_symbol);
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.to_string());
        }

        #[derive(Deserialize)]
        struct OkxTrade {
            #[serde(rename = "tradeId")]
            trade_id: String,
            px: String,
            sz: String,
            side: String,
            ts: String,
        }

        let trades: Vec<OkxTrade> = self
            .send_public_request("/api/v5/market/trades", Some(params))
            .await?;

        let mut result = Vec::new();
        for trade in trades {
            result.push(Trade {
                id: trade.trade_id,
                symbol: symbol.to_string(),
                side: match trade.side.as_str() {
                    "buy" => OrderSide::Buy,
                    "sell" => OrderSide::Sell,
                    _ => OrderSide::Buy,
                },
                amount: trade.sz.parse().unwrap_or(0.0),
                price: trade.px.parse().unwrap_or(0.0),
                timestamp: DateTime::from_timestamp(trade.ts.parse::<i64>().unwrap_or(0) / 1000, 0)
                    .unwrap_or_else(|| Utc::now()),
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
        let inst_type = match market_type {
            MarketType::Spot => "SPOT",
            MarketType::Futures => "SWAP",
        };

        let mut params = HashMap::new();
        params.insert("instType".to_string(), inst_type.to_string());
        if let Some(symbol) = symbol {
            let exchange_symbol =
                self.symbol_converter
                    .to_exchange_symbol(symbol, "okx", market_type)?;
            params.insert("instId".to_string(), exchange_symbol);
        }
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.to_string());
        }

        #[derive(Deserialize)]
        struct OkxMyTrade {
            #[serde(rename = "tradeId")]
            trade_id: String,
            #[serde(rename = "ordId")]
            ord_id: String,
            #[serde(rename = "instId")]
            inst_id: String,
            side: String,
            #[serde(rename = "fillSz")]
            fill_sz: String,
            #[serde(rename = "fillPx")]
            fill_px: String,
            fee: String,
            #[serde(rename = "feeCcy")]
            fee_ccy: String,
            ts: String,
        }

        let trades: Vec<OkxMyTrade> = self
            .send_signed_request("GET", "/api/v5/trade/fills", "")
            .await?;

        let mut result = Vec::new();
        for trade in trades {
            // 转换交易对格式
            if let Ok(standard_symbol) =
                self.symbol_converter
                    .from_exchange_symbol(&trade.inst_id, "okx", market_type)
            {
                result.push(Trade {
                    id: trade.trade_id,
                    symbol: standard_symbol,
                    side: match trade.side.as_str() {
                        "buy" => OrderSide::Buy,
                        "sell" => OrderSide::Sell,
                        _ => OrderSide::Buy,
                    },
                    amount: trade.fill_sz.parse().unwrap_or(0.0),
                    price: trade.fill_px.parse().unwrap_or(0.0),
                    timestamp: DateTime::from_timestamp(
                        trade.ts.parse::<i64>().unwrap_or(0) / 1000,
                        0,
                    )
                    .unwrap_or_else(|| Utc::now()),
                    order_id: Some(trade.ord_id),
                    fee: Some(Fee {
                        currency: trade.fee_ccy,
                        cost: trade.fee.parse::<f64>().unwrap_or(0.0).abs(), // OKX返回负数，取绝对值
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
                .to_exchange_symbol(symbol, "okx", market_type)?;

        let mut params = HashMap::new();
        params.insert("instId".to_string(), exchange_symbol);
        params.insert("bar".to_string(), interval.to_exchange_format("okx"));
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.to_string());
        }

        // OKX K线数据格式: [时间戳, 开盘价, 最高价, 最低价, 收盘价, 成交量, 成交额, 确认状态]
        let klines_data: Vec<Vec<String>> = self
            .send_public_request("/api/v5/market/candles", Some(params))
            .await?;

        let mut result = Vec::new();
        for kline_data in klines_data {
            if kline_data.len() >= 7 {
                result.push(Kline {
                    symbol: symbol.to_string(),
                    interval: interval.to_string(),
                    open_time: DateTime::from_timestamp(
                        kline_data[0].parse::<i64>().unwrap_or(0) / 1000,
                        0,
                    )
                    .unwrap_or_else(|| chrono::Utc::now()),
                    close_time: DateTime::from_timestamp(
                        kline_data[0].parse::<i64>().unwrap_or(0) / 1000,
                        0,
                    )
                    .unwrap_or_else(|| chrono::Utc::now()),
                    open: kline_data[1].parse().unwrap_or(0.0),
                    high: kline_data[2].parse().unwrap_or(0.0),
                    low: kline_data[3].parse().unwrap_or(0.0),
                    close: kline_data[4].parse().unwrap_or(0.0),
                    volume: kline_data[5].parse().unwrap_or(0.0),
                    quote_volume: kline_data[6].parse().unwrap_or(0.0),
                    trade_count: 0, // OKX不提供成交次数
                });
            }
        }

        Ok(result)
    }

    async fn get_24h_statistics(
        &self,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<Statistics24h> {
        let exchange_symbol =
            self.symbol_converter
                .to_exchange_symbol(symbol, "okx", market_type)?;

        let mut params = HashMap::new();
        params.insert("instId".to_string(), exchange_symbol);

        #[derive(Deserialize)]
        struct OkxStats {
            #[serde(rename = "instId")]
            inst_id: String,
            #[serde(rename = "open24h")]
            open_24h: String,
            #[serde(rename = "high24h")]
            high_24h: String,
            #[serde(rename = "low24h")]
            low_24h: String,
            #[serde(rename = "last")]
            last: String,
            #[serde(rename = "vol24h")]
            vol_24h: String,
            #[serde(rename = "volCcy24h")]
            vol_ccy_24h: String,
            ts: String,
        }

        let stats_list: Vec<OkxStats> = self
            .send_public_request("/api/v5/market/ticker", Some(params))
            .await?;

        if let Some(stats) = stats_list.first() {
            let open_price = stats.open_24h.parse().unwrap_or(0.0);
            let close_price = stats.last.parse().unwrap_or(0.0);
            let price_change = close_price - open_price;
            let price_change_percent = if open_price > 0.0 {
                (price_change / open_price) * 100.0
            } else {
                0.0
            };

            Ok(Statistics24h {
                symbol: symbol.to_string(),
                open: open_price,
                high: stats.high_24h.parse().unwrap_or(0.0),
                low: stats.low_24h.parse().unwrap_or(0.0),
                close: close_price,
                volume: stats.vol_24h.parse().unwrap_or(0.0),
                quote_volume: stats.vol_ccy_24h.parse().unwrap_or(0.0),
                change: price_change,
                change_percent: price_change_percent,
                timestamp: DateTime::from_timestamp(stats.ts.parse::<i64>().unwrap_or(0) / 1000, 0)
                    .unwrap_or_else(|| chrono::Utc::now()),
                // 兼容字段
                price_change: Some(price_change),
                price_change_percent: Some(price_change_percent),
                weighted_avg_price: Some(close_price), // OKX不提供加权平均价，用收盘价代替
                open_price: Some(open_price),
                high_price: Some(stats.high_24h.parse().unwrap_or(0.0)),
                low_price: Some(stats.low_24h.parse().unwrap_or(0.0)),
                close_price: Some(close_price),
                count: Some(0), // OKX不提供成交次数
            })
        } else {
            Err(ExchangeError::SymbolError(format!(
                "未找到交易对统计数据: {}",
                symbol
            )))
        }
    }

    async fn get_all_24h_statistics(&self, market_type: MarketType) -> Result<Vec<Statistics24h>> {
        let inst_type = match market_type {
            MarketType::Spot => "SPOT",
            MarketType::Futures => "SWAP",
        };

        let mut params = HashMap::new();
        params.insert("instType".to_string(), inst_type.to_string());

        #[derive(Deserialize)]
        struct OkxStats {
            #[serde(rename = "instId")]
            inst_id: String,
            #[serde(rename = "open24h")]
            open_24h: String,
            #[serde(rename = "high24h")]
            high_24h: String,
            #[serde(rename = "low24h")]
            low_24h: String,
            #[serde(rename = "last")]
            last: String,
            #[serde(rename = "vol24h")]
            vol_24h: String,
            #[serde(rename = "volCcy24h")]
            vol_ccy_24h: String,
            ts: String,
        }

        let all_stats: Vec<OkxStats> = self
            .send_public_request("/api/v5/market/tickers", Some(params))
            .await?;

        let mut result = Vec::new();
        for stats in all_stats {
            if let Ok(standard_symbol) =
                self.symbol_converter
                    .from_exchange_symbol(&stats.inst_id, "okx", market_type)
            {
                let open_price = stats.open_24h.parse().unwrap_or(0.0);
                let close_price = stats.last.parse().unwrap_or(0.0);
                let price_change = close_price - open_price;
                let price_change_percent = if open_price > 0.0 {
                    (price_change / open_price) * 100.0
                } else {
                    0.0
                };

                result.push(Statistics24h {
                    symbol: standard_symbol,
                    open: open_price,
                    high: stats.high_24h.parse().unwrap_or(0.0),
                    low: stats.low_24h.parse().unwrap_or(0.0),
                    close: close_price,
                    volume: stats.vol_24h.parse().unwrap_or(0.0),
                    quote_volume: stats.vol_ccy_24h.parse().unwrap_or(0.0),
                    change: price_change,
                    change_percent: price_change_percent,
                    timestamp: DateTime::from_timestamp(
                        stats.ts.parse::<i64>().unwrap_or(0) / 1000,
                        0,
                    )
                    .unwrap_or_else(|| chrono::Utc::now()),
                    // 兼容字段
                    price_change: Some(price_change),
                    price_change_percent: Some(price_change_percent),
                    weighted_avg_price: Some(close_price),
                    open_price: Some(open_price),
                    high_price: Some(stats.high_24h.parse().unwrap_or(0.0)),
                    low_price: Some(stats.low_24h.parse().unwrap_or(0.0)),
                    close_price: Some(close_price),
                    count: Some(0),
                });
            }
        }

        Ok(result)
    }

    async fn get_trade_fee(&self, symbol: &str, market_type: MarketType) -> Result<TradeFee> {
        let inst_type = match market_type {
            MarketType::Spot => "SPOT",
            MarketType::Futures => "SWAP",
        };

        let mut params = HashMap::new();
        params.insert("instType".to_string(), inst_type.to_string());

        #[derive(Deserialize)]
        struct OkxTradeFee {
            #[serde(rename = "instType")]
            inst_type: String,
            level: String,
            #[serde(rename = "makerU")]
            maker_u: String,
            #[serde(rename = "takerU")]
            taker_u: String,
        }

        // 对于GET请求，参数需要作为查询字符串
        let query_string = SignatureHelper::build_query_string(&params);
        let endpoint_with_params = format!("/api/v5/account/trade-fee?{}", query_string);

        let fees: Vec<OkxTradeFee> = self
            .send_signed_request("GET", &endpoint_with_params, "")
            .await?;

        if let Some(fee) = fees.first() {
            Ok(TradeFee {
                symbol: symbol.to_string(),
                maker: fee.maker_u.parse().unwrap_or(0.001),
                taker: fee.taker_u.parse().unwrap_or(0.001),
                percentage: true,
                tier_based: false,
                // 兼容字段
                maker_fee: Some(fee.maker_u.parse().unwrap_or(0.001)),
                taker_fee: Some(fee.taker_u.parse().unwrap_or(0.001)),
            })
        } else {
            // 默认手续费率
            Ok(TradeFee {
                symbol: symbol.to_string(),
                maker: 0.001, // 0.1%
                taker: 0.001, // 0.1%
                percentage: true,
                tier_based: false,
                // 兼容字段
                maker_fee: Some(0.001),
                taker_fee: Some(0.001),
            })
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
            account_type: "spot".to_string(), // OKX不区分市场类型
            balances,
            timestamp: chrono::Utc::now(),
            // 兼容字段
            total_balance_btc: Some(total_balance_btc),
            total_balance_usdt: Some(total_balance_usdt),
        })
    }

    async fn get_positions(&self, symbol: Option<&str>) -> Result<Vec<Position>> {
        let inst_type = "SWAP";
        let mut query_params = HashMap::new();
        query_params.insert("instType".to_string(), inst_type.to_string());

        if let Some(symbol) = symbol {
            let exchange_symbol =
                self.symbol_converter
                    .to_exchange_symbol(symbol, "okx", MarketType::Futures)?;
            query_params.insert("instId".to_string(), exchange_symbol);
        }

        #[derive(Deserialize)]
        struct OkxPosition {
            #[serde(rename = "instId")]
            inst_id: String,
            #[serde(rename = "posSide")]
            pos_side: String,
            pos: String,
            #[serde(rename = "avgPx")]
            avg_px: String,
            #[serde(rename = "markPx")]
            mark_px: String,
            upl: String,
            #[serde(rename = "uplRatio")]
            upl_ratio: String,
            margin: String,
            #[serde(rename = "mgnRatio")]
            mgn_ratio: String,
            lever: String,
            #[serde(rename = "uTime")]
            u_time: String,
        }

        let positions: Vec<OkxPosition> = self
            .send_signed_request("GET", "/api/v5/account/positions", "")
            .await?;

        let mut result = Vec::new();
        for pos in positions {
            let size = pos.pos.parse::<f64>().unwrap_or(0.0);
            if size.abs() > 0.0 {
                if let Ok(standard_symbol) = self.symbol_converter.from_exchange_symbol(
                    &pos.inst_id,
                    "okx",
                    MarketType::Futures,
                ) {
                    result.push(Position {
                        symbol: standard_symbol,
                        side: pos.pos_side,
                        contracts: size.abs(),
                        contract_size: 1.0, // USDT本位合约
                        size: size.abs(),
                        entry_price: pos.avg_px.parse().unwrap_or(0.0),
                        mark_price: pos.mark_px.parse().unwrap_or(0.0),
                        unrealized_pnl: pos.upl.parse().unwrap_or(0.0),
                        percentage: pos.upl_ratio.parse::<f64>().unwrap_or(0.0) * 100.0,
                        margin: pos.margin.parse().unwrap_or(0.0),
                        margin_ratio: pos.mgn_ratio.parse().unwrap_or(0.0),
                        leverage: pos.lever.parse::<u32>().ok(),
                        margin_type: None, // OKX doesn't provide this in positions response
                        amount: size,
                        timestamp: DateTime::from_timestamp(
                            pos.u_time.parse::<i64>().unwrap_or(0) / 1000,
                            0,
                        )
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
                .to_exchange_symbol(symbol, "okx", MarketType::Futures)?;

        #[derive(Serialize)]
        struct OkxLeverageRequest {
            #[serde(rename = "instId")]
            inst_id: String,
            lever: String,
            #[serde(rename = "mgnMode")]
            mgn_mode: String, // isolated 或 cross
        }

        let request_body = OkxLeverageRequest {
            inst_id: exchange_symbol,
            lever: leverage.to_string(),
            mgn_mode: "isolated".to_string(), // 默认逐仓模式
        };

        let body = serde_json::to_string(&[request_body])?;

        #[derive(Deserialize)]
        struct OkxLeverageResponse {
            #[serde(rename = "instId")]
            inst_id: String,
            lever: String,
            #[serde(rename = "mgnMode")]
            mgn_mode: String,
        }

        let _responses: Vec<OkxLeverageResponse> = self
            .send_signed_request("POST", "/api/v5/account/set-leverage", &body)
            .await?;

        Ok(())
    }

    async fn cancel_all_orders(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
    ) -> Result<Vec<Order>> {
        let inst_type = match market_type {
            MarketType::Spot => "SPOT",
            MarketType::Futures => "SWAP",
        };

        #[derive(Serialize)]
        struct OkxCancelAllRequest {
            #[serde(rename = "instType")]
            inst_type: String,
            #[serde(rename = "instId")]
            inst_id: Option<String>,
        }

        let request_body = OkxCancelAllRequest {
            inst_type: inst_type.to_string(),
            inst_id: if let Some(symbol) = symbol {
                Some(
                    self.symbol_converter
                        .to_exchange_symbol(symbol, "okx", market_type)?,
                )
            } else {
                None
            },
        };

        let body = serde_json::to_string(&[request_body])?;

        #[derive(Deserialize)]
        struct OkxCancelResponse {
            #[serde(rename = "ordId")]
            ord_id: String,
            #[serde(rename = "instId")]
            inst_id: String,
        }

        let responses: Vec<OkxCancelResponse> = self
            .send_signed_request("POST", "/api/v5/trade/cancel-batch-orders", &body)
            .await?;

        let mut result = Vec::new();
        for response in responses {
            if let Ok(standard_symbol) =
                self.symbol_converter
                    .from_exchange_symbol(&response.inst_id, "okx", market_type)
            {
                result.push(Order {
                    id: response.ord_id,
                    symbol: standard_symbol,
                    side: OrderSide::Buy,         // 占位符
                    order_type: OrderType::Limit, // 占位符
                    amount: 0.0,
                    price: None,
                    filled: 0.0,
                    remaining: 0.0,
                    status: OrderStatus::Canceled,
                    market_type,
                    timestamp: chrono::Utc::now(),
                    last_trade_timestamp: None,
                    info: serde_json::json!({}),
                });
            }
        }

        Ok(result)
    }

    async fn get_server_time(&self) -> Result<DateTime<chrono::Utc>> {
        #[derive(Deserialize)]
        struct ServerTime {
            ts: String,
        }

        let time_data: Vec<ServerTime> = self
            .send_public_request("/api/v5/public/time", None)
            .await?;

        if let Some(time) = time_data.first() {
            Ok(
                DateTime::from_timestamp(time.ts.parse::<i64>().unwrap_or(0) / 1000, 0)
                    .unwrap_or_else(|| chrono::Utc::now()),
            )
        } else {
            Err(ExchangeError::ApiError {
                code: 0,
                message: "无法获取服务器时间".to_string(),
            })
        }
    }

    async fn ping(&self) -> Result<()> {
        // OKX没有专门的ping接口，使用获取服务器时间来测试连接
        self.get_server_time().await?;
        Ok(())
    }

    async fn create_batch_orders(
        &self,
        batch_request: BatchOrderRequest,
    ) -> Result<BatchOrderResponse> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct OkxBatchOrderRequest {
            inst_id: String,
            td_mode: String,
            side: String,
            ord_type: String,
            sz: String,
            px: Option<String>,
        }

        #[derive(Deserialize)]
        struct OkxBatchOrderResponse {
            code: String,
            msg: String,
            data: Vec<OkxOrderData>,
        }

        #[derive(Deserialize, Serialize)]
        #[serde(rename_all = "camelCase")]
        struct OkxOrderData {
            ord_id: String,
            cl_ord_id: String,
            s_code: String,
            s_msg: String,
        }

        let mut successful_orders = Vec::new();
        let mut failed_orders = Vec::new();

        // OKX支持批量下单，最多20个订单
        let chunks: Vec<_> = batch_request.orders.chunks(20).collect();

        for chunk in chunks {
            let mut okx_orders = Vec::new();

            for order_request in chunk {
                let exchange_symbol = self.symbol_converter.to_exchange_symbol(
                    &order_request.symbol,
                    "okx",
                    batch_request.market_type,
                )?;

                okx_orders.push(OkxBatchOrderRequest {
                    inst_id: exchange_symbol,
                    td_mode: "cash".to_string(), // 现货模式
                    side: match order_request.side {
                        OrderSide::Buy => "buy".to_string(),
                        OrderSide::Sell => "sell".to_string(),
                    },
                    ord_type: match order_request.order_type {
                        OrderType::Market => "market".to_string(),
                        OrderType::Limit => "limit".to_string(),
                        _ => "limit".to_string(),
                    },
                    sz: order_request.amount.to_string(),
                    px: order_request.price.map(|p| p.to_string()),
                });
            }

            let body = serde_json::to_string(&okx_orders)?;

            match self
                .send_signed_request::<OkxBatchOrderResponse>(
                    "POST",
                    "/api/v5/trade/batch-orders",
                    &body,
                )
                .await
            {
                Ok(response) => {
                    if response.code == "0" {
                        for (i, order_data) in response.data.iter().enumerate() {
                            if order_data.s_code == "0" {
                                // 成功的订单
                                let order_request = &chunk[i];
                                successful_orders.push(Order {
                                    id: order_data.ord_id.clone(),
                                    symbol: order_request.symbol.clone(),
                                    side: order_request.side.clone(),
                                    order_type: order_request.order_type.clone(),
                                    amount: order_request.amount,
                                    price: order_request.price,
                                    filled: 0.0,
                                    remaining: order_request.amount,
                                    status: OrderStatus::Open,
                                    market_type: batch_request.market_type,
                                    timestamp: chrono::Utc::now(),
                                    last_trade_timestamp: None,
                                    info: serde_json::json!(order_data),
                                });
                            } else {
                                // 失败的订单
                                failed_orders.push(BatchOrderError {
                                    order_request: chunk[i].clone(),
                                    error_message: order_data.s_msg.clone(),
                                    error_code: Some(order_data.s_code.clone()),
                                });
                            }
                        }
                    } else {
                        // 整个批量请求失败
                        for order_request in chunk {
                            failed_orders.push(BatchOrderError {
                                order_request: order_request.clone(),
                                error_message: response.msg.clone(),
                                error_code: Some(response.code.clone()),
                            });
                        }
                    }
                }
                Err(e) => {
                    // 请求失败，所有订单标记为失败
                    for order_request in chunk {
                        failed_orders.push(BatchOrderError {
                            order_request: order_request.clone(),
                            error_message: e.to_string(),
                            error_code: None,
                        });
                    }
                }
            }
        }

        Ok(BatchOrderResponse {
            successful_orders,
            failed_orders,
        })
    }

    async fn get_all_spot_symbols(&self) -> Result<Vec<TradingPair>> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct OkxInstrument {
            inst_id: String,
            base_ccy: String,
            quote_ccy: String,
            state: String,
            min_sz: String,
            max_sz: Option<String>, // 有些交易对可能没有最大限制
            tick_sz: String,
            lot_sz: String,
            min_order_sz: Option<String>,
        }

        let mut params = HashMap::new();
        params.insert("instType".to_string(), "SPOT".to_string());

        let instruments: Vec<OkxInstrument> = self
            .send_public_request("/api/v5/public/instruments", Some(params))
            .await?;

        let mut trading_pairs = Vec::new();

        for instrument in instruments {
            let standardized_symbol = self.symbol_converter.from_exchange_symbol(
                &instrument.inst_id,
                "okx",
                MarketType::Spot,
            )?;

            trading_pairs.push(TradingPair {
                symbol: standardized_symbol,
                base_asset: instrument.base_ccy,
                quote_asset: instrument.quote_ccy,
                status: instrument.state.clone(),
                min_order_size: instrument.min_sz.parse().unwrap_or(0.0),
                max_order_size: instrument
                    .max_sz
                    .unwrap_or_else(|| "999999999".to_string())
                    .parse()
                    .unwrap_or(999999999.0),
                tick_size: instrument.tick_sz.parse().unwrap_or(0.0),
                step_size: instrument.lot_sz.parse().unwrap_or(0.0),
                min_notional: instrument.min_order_sz.and_then(|s| s.parse().ok()),
                is_trading: instrument.state == "live",
                market_type: MarketType::Spot,
            });
        }

        Ok(trading_pairs)
    }

    async fn get_all_futures_symbols(&self) -> Result<Vec<TradingPair>> {
        #[derive(Deserialize)]
        struct OkxInstrumentsResponse {
            code: String,
            msg: String,
            data: Vec<OkxFuturesInstrument>,
        }

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct OkxFuturesInstrument {
            inst_id: String,
            uly: String,        // 标的指数
            settle_ccy: String, // 结算币种
            state: String,
            min_sz: String,
            max_sz: String,
            tick_sz: String,
            lot_sz: String,
            ct_val: String, // 合约面值
        }

        let exchange_config = self.base.get_exchange_config()?;
        let url = format!(
            "{}/api/v5/public/instruments?instType=SWAP",
            exchange_config.base_url
        );

        let response: OkxInstrumentsResponse =
            self.base.client.get(&url).send().await?.json().await?;

        if response.code != "0" {
            return Err(ExchangeError::ApiError {
                code: response.code.parse().unwrap_or(-1),
                message: response.msg,
            });
        }

        let mut trading_pairs = Vec::new();

        for instrument in response.data {
            // 解析标的资产和结算资产
            let parts: Vec<&str> = instrument.uly.split('-').collect();
            let base_asset = parts.get(0).unwrap_or(&"").to_string();

            let standardized_symbol = self.symbol_converter.from_exchange_symbol(
                &instrument.inst_id,
                "okx",
                MarketType::Futures,
            )?;

            trading_pairs.push(TradingPair {
                symbol: standardized_symbol,
                base_asset,
                quote_asset: instrument.settle_ccy,
                status: instrument.state.clone(),
                min_order_size: instrument.min_sz.parse().unwrap_or(0.0),
                max_order_size: instrument.max_sz.parse().unwrap_or(0.0),
                tick_size: instrument.tick_sz.parse().unwrap_or(0.0),
                step_size: instrument.lot_sz.parse().unwrap_or(0.0),
                min_notional: Some(instrument.ct_val.parse().unwrap_or(0.0)),
                is_trading: instrument.state == "live",
                market_type: MarketType::Futures,
            });
        }

        Ok(trading_pairs)
    }

    async fn get_symbol_info(&self, symbol: &str, market_type: MarketType) -> Result<TradingPair> {
        let all_symbols = match market_type {
            MarketType::Spot => self.get_all_spot_symbols().await?,
            MarketType::Futures => self.get_all_futures_symbols().await?,
        };

        all_symbols
            .into_iter()
            .find(|tp| tp.symbol == symbol)
            .ok_or_else(|| ExchangeError::SymbolNotFound {
                symbol: symbol.to_string(),
                market_type,
            })
    }

    // === WebSocket 功能实现 ===

    async fn create_websocket_client(
        &self,
        market_type: MarketType,
    ) -> Result<Box<dyn WebSocketClient>> {
        let ws_url = self.get_websocket_url(market_type);
        let client = OkxWebSocketClient::new(ws_url, market_type);
        Ok(Box::new(client))
    }

    fn get_websocket_url(&self, market_type: MarketType) -> String {
        match market_type {
            MarketType::Spot => "wss://ws.okx.com:8443/ws/v5/public".to_string(),
            MarketType::Futures => "wss://ws.okx.com:8443/ws/v5/public".to_string(),
        }
    }
}

/// OKX WebSocket客户端实现
pub struct OkxWebSocketClient {
    base: Box<dyn WebSocketClient>,
    market_type: MarketType,
}

impl OkxWebSocketClient {
    pub fn new(url: String, market_type: MarketType) -> Self {
        Self {
            base: Box::new(crate::core::websocket::BaseWebSocketClient::new(
                url,
                "okx".to_string(),
            )),
            market_type,
        }
    }
}

#[async_trait]
impl WebSocketClient for OkxWebSocketClient {
    async fn connect(&mut self) -> Result<()> {
        self.base.connect().await
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.base.disconnect().await
    }

    async fn send(&mut self, message: String) -> Result<()> {
        self.base.send(message).await
    }

    async fn receive(&mut self) -> Result<Option<String>> {
        self.base.receive().await
    }

    async fn ping(&self) -> Result<()> {
        self.base.ping().await
    }

    fn get_state(&self) -> ConnectionState {
        self.base.get_state()
    }
}

/// OKX消息处理器包装器
struct OkxMessageHandler {
    inner_handler: Box<dyn MessageHandler>,
    market_type: MarketType,
}

impl OkxMessageHandler {
    fn new(handler: Box<dyn MessageHandler>, market_type: MarketType) -> Self {
        Self {
            inner_handler: handler,
            market_type,
        }
    }

    fn parse_message(&self, text: &str) -> Result<WsMessage> {
        let json: serde_json::Value = serde_json::from_str(text)?;

        // OKX WebSocket消息格式: {"event":"...", "data":...} 或 {"arg":..., "data":...}
        if let Some(data_arr) = json.get("data").and_then(|v| v.as_array()) {
            if let Some(arg) = json.get("arg") {
                if let Some(channel) = arg.get("channel").and_then(|v| v.as_str()) {
                    if !data_arr.is_empty() {
                        let data = &data_arr[0];
                        match channel {
                            "tickers" => self.parse_ticker(data),
                            "books" => self.parse_orderbook(data, arg),
                            "trades" => self.parse_trade(data),
                            _ => Ok(WsMessage::Error(format!("Unknown channel: {}", channel))),
                        }
                    } else {
                        Ok(WsMessage::Error("Empty data array".to_string()))
                    }
                } else {
                    Ok(WsMessage::Error("Missing channel in arg".to_string()))
                }
            } else {
                Ok(WsMessage::Error("Missing arg field".to_string()))
            }
        } else {
            Ok(WsMessage::Error("Invalid message format".to_string()))
        }
    }

    fn parse_ticker(&self, data: &serde_json::Value) -> Result<WsMessage> {
        #[derive(Deserialize)]
        struct OkxWsTicker {
            #[serde(rename = "instId")]
            inst_id: String,
            last: String,
            #[serde(rename = "bidPx")]
            bid_px: String,
            #[serde(rename = "askPx")]
            ask_px: String,
            #[serde(rename = "high24h")]
            high_24h: String,
            #[serde(rename = "low24h")]
            low_24h: String,
            #[serde(rename = "vol24h")]
            vol_24h: String,
            ts: String,
        }

        let ticker: OkxWsTicker = serde_json::from_value(data.clone())?;

        Ok(WsMessage::Ticker(Ticker {
            symbol: ticker.inst_id,
            high: ticker.high_24h.parse().unwrap_or(0.0),
            low: ticker.low_24h.parse().unwrap_or(0.0),
            bid: ticker.bid_px.parse().unwrap_or(0.0),
            ask: ticker.ask_px.parse().unwrap_or(0.0),
            last: ticker.last.parse().unwrap_or(0.0),
            volume: ticker.vol_24h.parse().unwrap_or(0.0),
            timestamp: DateTime::from_timestamp(ticker.ts.parse::<i64>().unwrap_or(0) / 1000, 0)
                .unwrap_or_else(|| Utc::now()),
        }))
    }

    fn parse_orderbook(
        &self,
        data: &serde_json::Value,
        arg: &serde_json::Value,
    ) -> Result<WsMessage> {
        #[derive(Deserialize)]
        struct OkxWsOrderBook {
            bids: Vec<Vec<String>>,
            asks: Vec<Vec<String>>,
            ts: String,
        }

        let orderbook: OkxWsOrderBook = serde_json::from_value(data.clone())?;
        let symbol = arg.get("instId").and_then(|v| v.as_str()).unwrap_or("");

        let bids: Vec<[f64; 2]> = orderbook
            .bids
            .iter()
            .filter_map(|bid| {
                if bid.len() >= 2 {
                    let price = bid[0].parse::<f64>().ok()?;
                    let amount = bid[1].parse::<f64>().ok()?;
                    Some([price, amount])
                } else {
                    None
                }
            })
            .collect();

        let asks: Vec<[f64; 2]> = orderbook
            .asks
            .iter()
            .filter_map(|ask| {
                if ask.len() >= 2 {
                    let price = ask[0].parse::<f64>().ok()?;
                    let amount = ask[1].parse::<f64>().ok()?;
                    Some([price, amount])
                } else {
                    None
                }
            })
            .collect();

        Ok(WsMessage::OrderBook(OrderBook {
            symbol: symbol.to_string(),
            bids,
            asks,
            timestamp: DateTime::from_timestamp(orderbook.ts.parse::<i64>().unwrap_or(0) / 1000, 0)
                .unwrap_or_else(|| Utc::now()),
        }))
    }

    fn parse_trade(&self, data: &serde_json::Value) -> Result<WsMessage> {
        #[derive(Deserialize)]
        struct OkxWsTrade {
            #[serde(rename = "instId")]
            inst_id: String,
            #[serde(rename = "tradeId")]
            trade_id: String,
            px: String,
            sz: String,
            side: String,
            ts: String,
        }

        let trade: OkxWsTrade = serde_json::from_value(data.clone())?;

        Ok(WsMessage::Trade(Trade {
            id: trade.trade_id,
            symbol: trade.inst_id,
            side: match trade.side.as_str() {
                "buy" => OrderSide::Buy,
                "sell" => OrderSide::Sell,
                _ => OrderSide::Buy,
            },
            amount: trade.sz.parse().unwrap_or(0.0),
            price: trade.px.parse().unwrap_or(0.0),
            timestamp: DateTime::from_timestamp(trade.ts.parse::<i64>().unwrap_or(0) / 1000, 0)
                .unwrap_or_else(|| Utc::now()),
            order_id: None,
            fee: None,
        }))
    }
}

#[async_trait]
impl MessageHandler for OkxMessageHandler {
    async fn handle_message(&self, message: WsMessage) -> Result<()> {
        self.inner_handler.handle_message(message).await
    }

    // handle_state_change方法已从 trait 中移除

    async fn handle_error(&self, error: ExchangeError) -> Result<()> {
        self.inner_handler.handle_error(error).await
    }
}
