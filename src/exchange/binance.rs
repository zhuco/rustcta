use super::traits::Exchange;
use crate::config::endpoints::{BinanceEndpoints, WsConnectionConfig, WsConnectionStatus};
use crate::error::AppError;
use crate::exchange::binance_model::{AccountInfo, ExchangeInfo, FundingRate, Kline, Order, WsEvent};
use crate::utils::order_id::generate_order_id;
use crate::utils::symbol::{MarketType, Symbol};
use crate::utils::time::{get_adjusted_timestamp, sync_time_with_server};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use native_tls::TlsConnector;
use reqwest::Client;
use reqwest::Method;
use rustls::{ClientConfig, RootCertStore};
use serde::Deserialize;
use serde_json::Value;
use sha2::Sha256;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio_tungstenite::{
    connect_async, connect_async_tls_with_config, tungstenite::protocol::Message, Connector,
};
use url::Url;

pub struct Binance {
    api_key: String,
    secret_key: String,
    client: Client,
    listen_key: std::sync::Arc<tokio::sync::Mutex<Option<String>>>,
    endpoints: BinanceEndpoints,
    ws_config: WsConnectionConfig,
    ws_status: Arc<tokio::sync::Mutex<WsConnectionStatus>>,
}

#[derive(Debug, Deserialize)]
pub struct ServerTime {
    #[serde(rename = "serverTime")]
    pub server_time: i64,
}

impl Binance {
    pub fn new(api_key: String, secret_key: String) -> Self {
        Binance {
            api_key,
            secret_key,
            client: Client::new(),
            listen_key: std::sync::Arc::new(tokio::sync::Mutex::new(None)),
            endpoints: BinanceEndpoints::new(),
            ws_config: WsConnectionConfig::default(),
            ws_status: Arc::new(tokio::sync::Mutex::new(WsConnectionStatus::Disconnected)),
        }
    }
}

impl Exchange for Binance {
    fn get_price<'a>(
        &'a self,
        symbol: &'a Symbol,
    ) -> Pin<Box<dyn Future<Output = Result<f64, AppError>> + Send + 'a>> {
        Box::pin(async move {
            let endpoint = self.endpoints.get_price_endpoint(symbol.market_type);
            let url = format!(
                "{}{}?symbol={}",
                self.endpoints.get_base_url(symbol.market_type),
                endpoint,
                symbol.to_binance()
            );
            let response = self.client.get(&url).send().await?;
            let status = response.status();
            let response_text = response.text().await?;

            if status.is_success() {
                let response_json: Value = serde_json::from_str(&response_text)?;
                let price_str = response_json["price"].as_str().unwrap_or("0");
                price_str
                    .parse::<f64>()
                    .map_err(|_| AppError::Other("Failed to parse price".to_string()))
            } else {
                let error_json: Value = serde_json::from_str(&response_text)?;
                let code = error_json
                    .get("code")
                    .and_then(|c| c.as_i64())
                    .unwrap_or(-1);
                let msg = error_json
                    .get("msg")
                    .and_then(|m| m.as_str())
                    .unwrap_or("Unknown error")
                    .to_string();
                Err(AppError::BinanceError { code, msg })
            }
        })
    }

    fn place_order<'a>(
        &'a self,
        symbol: &'a Symbol,
        side: &'a str,
        order_type: &'a str,
        quantity: f64,
        price: Option<f64>,
    ) -> Pin<Box<dyn Future<Output = Result<Order, AppError>> + Send + 'a>> {
        Box::pin(async move {
            let new_order_id = generate_order_id(16);
            let mut params = format!(
                "symbol={}&side={}&type={}&quantity={}&newClientOrderId={}",
                symbol.to_binance(),
                side,
                order_type,
                quantity,
                new_order_id
            );

            if let Some(p) = price {
                if order_type == "LIMIT" {
                    params.push_str(&format!("&price={p}&timeInForce=GTC"));
                } else if order_type == "STOP" || order_type == "TAKE_PROFIT" {
                    params.push_str(&format!("&stopPrice={p}"));
                }
            }

            let endpoint = self.endpoints.get_order_endpoint(symbol.market_type);
            let value = self
                .send_signed_request(Method::POST, endpoint, &params)
                .await?;
            let order = match serde_json::from_value::<Order>(value.clone()) {
                Ok(order) => order,
                Err(e) => {
                    eprintln!(
                        "place_order JSON解析失败，原始响应: {}",
                        serde_json::to_string_pretty(&value)
                            .unwrap_or_else(|_| format!("{:?}", value))
                    );
                    return Err(AppError::SerdeJson(e));
                }
            };
            Ok(order)
        })
    }

    fn place_batch_orders<'a>(
        &'a self,
        symbol: &'a Symbol,
        orders: Vec<(String, String, f64, Option<f64>)>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Order>, AppError>> + Send + 'a>> {
        Box::pin(async move {
            if orders.is_empty() {
                return Ok(Vec::new());
            }

            // 限制每批最多5个订单
            if orders.len() > 5 {
                return Err(AppError::Other("批量下单每批最多支持5个订单".to_string()));
            }

            let mut batch_orders = Vec::new();
            for (side, order_type, quantity, price) in &orders {
                let new_order_id = generate_order_id(16);
                let mut order_params = serde_json::json!({
                    "symbol": symbol.to_binance(),
                    "side": side,
                    "type": order_type,
                    "quantity": quantity.to_string(),
                    "newClientOrderId": new_order_id
                });

                if let Some(p) = price {
                    if order_type == "LIMIT" {
                        order_params["price"] = serde_json::Value::from(p.to_string());
                        order_params["timeInForce"] = serde_json::Value::from("GTC");
                    } else if order_type == "STOP" || order_type == "TAKE_PROFIT" {
                        order_params["stopPrice"] = serde_json::Value::from(p.to_string());
                    }
                }

                batch_orders.push(order_params);
            }

            let batch_orders_json = serde_json::to_string(&batch_orders)?;
            let params = format!("batchOrders={}", urlencoding::encode(&batch_orders_json));

            let endpoint = self.endpoints.get_batch_orders_endpoint(symbol.market_type);

            // 对于现货市场，由于不支持批量下单，我们逐个下单
            if symbol.market_type == MarketType::Spot {
                let mut results = Vec::new();
                for (side, order_type, quantity, price) in &orders {
                    match self
                        .place_order(symbol, side, order_type, *quantity, *price)
                        .await
                    {
                        Ok(order) => results.push(order),
                        Err(e) => return Err(e),
                    }
                }
                return Ok(results);
            }

            // 期货市场支持批量下单
            let value = self
                .send_signed_request(Method::POST, endpoint, &params)
                .await?;

            match value {
                serde_json::Value::Array(orders_array) => {
                    let mut result_orders = Vec::new();
                    for order_value in orders_array {
                        match serde_json::from_value::<Order>(order_value.clone()) {
                            Ok(order) => result_orders.push(order),
                            Err(e) => {
                                eprintln!(
                                    "批量下单JSON解析失败，原始响应: {}",
                                    serde_json::to_string_pretty(&order_value)
                                        .unwrap_or_else(|_| format!("{:?}", order_value))
                                );
                                return Err(AppError::SerdeJson(e));
                            }
                        }
                    }
                    Ok(result_orders)
                }
                _ => {
                    eprintln!(
                        "批量下单响应格式错误: {}",
                        serde_json::to_string_pretty(&value)
                            .unwrap_or_else(|_| format!("{:?}", value))
                    );
                    Err(AppError::Other("批量下单响应格式错误".to_string()))
                }
            }
        })
    }

    fn cancel_all_orders<'a>(
        &'a self,
        symbol: &'a Symbol,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + 'a>> {
        Box::pin(async move {
            let endpoint = self
                .endpoints
                .get_cancel_all_orders_endpoint(symbol.market_type);
            let params = format!("symbol={}", symbol.to_binance());
            self.send_signed_request(Method::DELETE, endpoint, &params)
                .await?;
            Ok(())
        })
    }

    fn get_klines<'a>(
        &'a self,
        symbol: &'a Symbol,
        interval: &'a str,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<i32>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Kline>, AppError>> + Send + 'a>> {
        Box::pin(async move {
            let mut params = format!("symbol={}&interval={}", symbol.to_binance(), interval);
            if let Some(st) = start_time {
                params.push_str(&format!("&startTime={st}"));
            }
            if let Some(et) = end_time {
                params.push_str(&format!("&endTime={et}"));
            }
            if let Some(l) = limit {
                params.push_str(&format!("&limit={l}"));
            }

            let endpoint = self.endpoints.get_klines_endpoint(symbol.market_type);
            let url = format!(
                "{}{}?{}",
                self.endpoints.get_base_url(symbol.market_type),
                endpoint,
                params
            );
            let response = self.client.get(&url).send().await?;
            let status = response.status();
            let response_text = response.text().await?;

            if status.is_success() {
                let klines: Vec<Vec<Value>> = serde_json::from_str(&response_text)?;
                let result = klines
                    .into_iter()
                    .map(|k| Kline {
                        open_time: k[0].as_i64().unwrap_or_default(),
                        open: k[1].as_str().unwrap_or_default().to_string(),
                        high: k[2].as_str().unwrap_or_default().to_string(),
                        low: k[3].as_str().unwrap_or_default().to_string(),
                        close: k[4].as_str().unwrap_or_default().to_string(),
                        volume: k[5].as_str().unwrap_or_default().to_string(),
                        close_time: k[6].as_i64().unwrap_or_default(),
                        quote_asset_volume: k[7].as_str().unwrap_or_default().to_string(),
                        number_of_trades: k[8].as_i64().unwrap_or_default(),
                        taker_buy_base_asset_volume: k[9].as_str().unwrap_or_default().to_string(),
                        taker_buy_quote_asset_volume: k[10]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                        ignore: k[11].as_str().unwrap_or_default().to_string(),
                    })
                    .collect();
                Ok(result)
            } else {
                let error_json: Value = serde_json::from_str(&response_text)?;
                let code = error_json
                    .get("code")
                    .and_then(|c| c.as_i64())
                    .unwrap_or(-1);
                let msg = error_json
                    .get("msg")
                    .and_then(|m| m.as_str())
                    .unwrap_or("Unknown error")
                    .to_string();
                Err(AppError::BinanceError { code, msg })
            }
        })
    }

    fn get_exchange_info<'a>(
        &'a self,
        market_type: MarketType,
    ) -> Pin<Box<dyn Future<Output = Result<ExchangeInfo, AppError>> + Send + 'a>> {
        Box::pin(async move {
            let endpoint = self.endpoints.get_exchange_info_endpoint(market_type);
            let value = self.send_signed_request(Method::GET, endpoint, "").await?;
            match serde_json::from_value::<ExchangeInfo>(value.clone()) {
                Ok(info) => Ok(info),
                Err(e) => {
                    eprintln!(
                        "JSON解析失败，原始响应: {}",
                        serde_json::to_string_pretty(&value)
                            .unwrap_or_else(|_| format!("{:?}", value))
                    );
                    Err(AppError::SerdeJson(e))
                }
            }
        })
    }

    fn get_order<'a>(
        &'a self,
        symbol: &'a Symbol,
        order_id: i64,
    ) -> Pin<Box<dyn Future<Output = Result<Order, AppError>> + Send + 'a>> {
        Box::pin(async move {
            let endpoint = self.endpoints.get_order_endpoint(symbol.market_type);
            let params = format!("symbol={}&orderId={}", symbol.to_binance(), order_id);
            let value = self
                .send_signed_request(Method::GET, endpoint, &params)
                .await?;
            let order = match serde_json::from_value::<Order>(value.clone()) {
                Ok(order) => order,
                Err(e) => {
                    eprintln!(
                        "get_order JSON解析失败，原始响应: {}",
                        serde_json::to_string_pretty(&value)
                            .unwrap_or_else(|_| format!("{:?}", value))
                    );
                    return Err(AppError::SerdeJson(e));
                }
            };
            Ok(order)
        })
    }

    fn cancel_order<'a>(
        &'a self,
        symbol: &'a Symbol,
        order_id: i64,
    ) -> Pin<Box<dyn Future<Output = Result<Order, AppError>> + Send + 'a>> {
        Box::pin(async move {
            let endpoint = self.endpoints.get_order_endpoint(symbol.market_type);
            let params = format!("symbol={}&orderId={}", symbol.to_binance(), order_id);
            let value = self
                .send_signed_request(Method::DELETE, endpoint, &params)
                .await?;
            let order = match serde_json::from_value::<Order>(value.clone()) {
                Ok(order) => order,
                Err(e) => {
                    eprintln!(
                        "cancel_order JSON解析失败，原始响应: {}",
                        serde_json::to_string_pretty(&value)
                            .unwrap_or_else(|_| format!("{:?}", value))
                    );
                    return Err(AppError::SerdeJson(e));
                }
            };
            Ok(order)
        })
    }

    fn get_account_info<'a>(
        &'a self,
        market_type: MarketType,
    ) -> Pin<Box<dyn Future<Output = Result<AccountInfo, AppError>> + Send + 'a>> {
        Box::pin(async move {
            let endpoint = self.endpoints.get_account_info_endpoint(market_type);
            let value = self.send_signed_request(Method::GET, endpoint, "").await?;
            let account_info: AccountInfo = serde_json::from_value(value)?;
            Ok(account_info)
        })
    }

    fn connect_ws<'a>(
        &'a self,
        market_type: MarketType,
        mut on_message: Box<
            dyn FnMut(WsEvent) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
        >,
    ) -> Pin<Box<dyn Future<Output = Result<(), AppError>> + Send + 'a>> {
        Box::pin(async move {
            let listen_key_val = self.get_listen_key(market_type).await?;
            *self.listen_key.lock().await = Some(listen_key_val.clone());
            let listen_key = listen_key_val;
            let ws_url = self.endpoints.get_ws_url(market_type, &listen_key);
            *self.ws_status.lock().await = WsConnectionStatus::Connecting;
            let url = Url::parse(&ws_url).map_err(|e| {
                let error_msg = format!("URL解析失败: {} - URL: {}", e, ws_url);
                eprintln!("❌ {}", error_msg);
                AppError::Other(error_msg)
            })?;
            // 配置TLS连接器以解决TLS错误
            let connector = TlsConnector::builder()
                .danger_accept_invalid_certs(false)
                .danger_accept_invalid_hostnames(false)
                .build()
                .map_err(|e| {
                    let error_msg = format!("TLS配置失败: {}", e);
                    eprintln!("❌ {}", error_msg);
                    AppError::Other(error_msg)
                })?;
            let tls_connector = Connector::NativeTls(connector);
            let connection_result = match url.scheme() {
                "wss" => {
                    connect_async_tls_with_config(url.clone(), None, Some(tls_connector)).await
                }
                "ws" => {
                    connect_async(url.clone()).await
                }
                _ => {
                    let error_msg = format!("不支持的WebSocket协议: {}", url.scheme());
                    eprintln!("❌ {}", error_msg);
                    return Err(AppError::Other(error_msg));
                }
            };

            let (ws_stream, _response) = match connection_result {
                Ok(result) => {
                    result
                }
                Err(e) => {
                    let error_msg = format!("WebSocket连接失败: {} - URL: {}", e, ws_url);
                    eprintln!("❌ {}", error_msg);
                    eprintln!("连接失败详情:");
                    eprintln!("  - 错误类型: {}", std::any::type_name_of_val(&e));
                    eprintln!("  - 错误信息: {}", e);
                    eprintln!("  - 建议检查:");
                    eprintln!("    1. 网络连接是否正常");
                    eprintln!("    2. API Key是否有效");
                    eprintln!("    3. Listen Key是否已过期");
                    eprintln!("    4. 防火墙是否阻止连接");
                    eprintln!("    5. 币安服务器是否可访问");
                    *self.ws_status.lock().await = WsConnectionStatus::Error(error_msg.clone());
                    return Err(AppError::Other(error_msg));
                }
            };

            *self.ws_status.lock().await = WsConnectionStatus::Connected;
            let (mut write, mut read) = ws_stream.split();

            // Keep-alive task
            let binance_clone = self.clone();
            tokio::spawn(async move {
                binance_clone.keep_alive_listen_key(market_type).await;
            });

            while let Some(message) = read.next().await {
                match message {
                    Ok(Message::Text(text)) => {
                        // println!("收到WebSocket消息: {}", text);
                        match serde_json::from_str::<WsEvent>(&text) {
                            Ok(event) => {
                                // 只处理订单交易更新事件
                                match &event {
                                    WsEvent::OrderTradeUpdate { .. } => {
                                        // println!("收到订单交易更新事件: {:?}", event);
                                        on_message(event).await;
                                    }
                                    _ => {
                                        // 忽略其他事件类型（如账户更新、K线等）
                                        // println!("忽略非订单交易事件");
                                    }
                                }
                            }
                            Err(e) => {
                                println!("WebSocket消息解析失败: {} - 原始消息: {}", e, text);
                                // 尝试解析为通用JSON以查看结构
                                if let Ok(json_value) =
                                    serde_json::from_str::<serde_json::Value>(&text)
                                {
                                    println!("消息JSON结构: {:#}", json_value);
                                }
                            }
                        }
                    }
                    Ok(Message::Ping(ping)) => {
                        if write.send(Message::Pong(ping)).await.is_err() {
                            break;
                        }
                    }
                    Ok(Message::Close(_)) => {
                        *self.ws_status.lock().await = WsConnectionStatus::Disconnected;
                        println!("WebSocket connection closed.");
                        break;
                    }
                    Err(e) => {
                        *self.ws_status.lock().await = WsConnectionStatus::Error(e.to_string());
                        eprintln!("WebSocket error: {e}");
                        break;
                    }
                    _ => {}
                }
            }

            Ok(())
        })
    }
}

impl Clone for Binance {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            api_key: self.api_key.clone(),
            secret_key: self.secret_key.clone(),
            listen_key: self.listen_key.clone(),
            endpoints: self.endpoints.clone(),
            ws_config: self.ws_config.clone(),
            ws_status: self.ws_status.clone(),
        }
    }
}

impl Binance {
    async fn keep_alive_listen_key(&self, market_type: MarketType) {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(30 * 60)).await; // 30 minutes
            let listen_key_guard = self.listen_key.lock().await;
            if let Some(listen_key) = listen_key_guard.as_ref() {
                let endpoint = self.endpoints.get_listen_key_endpoint(market_type);
                let url = format!("{}{}", self.endpoints.get_base_url(market_type), endpoint);
                let request_builder = self
                    .client
                    .put(&url)
                    .header("X-MBX-APIKEY", &self.api_key)
                    .query(&[("listenKey", listen_key.as_str())]);
                match request_builder.send().await {
                    Ok(response) => {
                        if response.status().is_success() {
                            println!("Successfully kept listenKey alive");
                        } else {
                            println!(
                                "Failed to keep listenKey alive: {}",
                                response.text().await.unwrap_or_default()
                            );
                        }
                    }
                    Err(e) => {
                        println!("Error keeping listenKey alive: {e}");
                    }
                }
            }
        }
    }

    pub async fn get_server_time(&self, market_type: MarketType) -> Result<i64, AppError> {
        let endpoint = self.endpoints.get_server_time_endpoint(market_type);
        let url = format!("{}{}", self.endpoints.get_base_url(market_type), endpoint);
        let resp = self
            .client
            .get(&url)
            .send()
            .await?
            .json::<ServerTime>()
            .await?;
        Ok(resp.server_time)
    }

    async fn send_signed_request(
        &self,
        method: Method,
        endpoint: &str,
        params: &str,
    ) -> Result<Value, AppError> {
        // This needs to be adapted based on the endpoint's market type
        // For now, we'll assume futures for signed requests.
        // A better approach would be to pass the market_type to send_signed_request
        let market_type = if endpoint.starts_with("/fapi") {
            MarketType::UsdFutures
        } else {
            MarketType::Spot
        };
        // 使用调整后的时间戳（本地时间减去时间差异）
        let timestamp = get_adjusted_timestamp();
        let query_string = if params.is_empty() {
            format!("timestamp={timestamp}")
        } else {
            format!("{params}&timestamp={timestamp}")
        };

        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(self.secret_key.as_bytes())?;
        mac.update(query_string.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        let base_url = self.endpoints.get_base_url(market_type);

        let url = format!(
            "{}{}{}{}&signature={}",
            base_url,
            endpoint,
            if query_string.is_empty() { "" } else { "?" },
            query_string,
            signature
        );

        let request_builder = self
            .client
            .request(method, &url)
            .header("X-MBX-APIKEY", &self.api_key);
        let response = request_builder.send().await?;
        let status = response.status();
        let response_text = response.text().await?;

        if status.is_success() {
            let response_json: Value = serde_json::from_str(&response_text)?;
            Ok(response_json)
        } else {
            let error_json: Value = serde_json::from_str(&response_text)?;
            let code = error_json
                .get("code")
                .and_then(|c| c.as_i64())
                .unwrap_or(-1);
            let msg = error_json
                .get("msg")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown error")
                .to_string();
            Err(AppError::BinanceError { code, msg })
        }
    }

    pub async fn get_listen_key(&self, market_type: MarketType) -> Result<String, AppError> {
        let endpoint = self.endpoints.get_listen_key_endpoint(market_type);
        let value = self.send_signed_request(Method::POST, endpoint, "").await?;
        value["listenKey"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| AppError::Other("listenKey not found".to_string()))
    }

    /// 与交易所同步时间
    pub async fn sync_time(&self, market_type: MarketType) -> Result<(), AppError> {
        let get_server_time_fn = || async { self.get_server_time(market_type).await };
        sync_time_with_server(get_server_time_fn).await
    }

    /// 获取所有永续合约的资金费率
    pub async fn get_funding_rates(&self) -> Result<Vec<FundingRate>, AppError> {
        let endpoint = self.endpoints.get_funding_rate_endpoint(MarketType::UsdFutures);
        let url = format!(
            "{}{}",
            self.endpoints.get_base_url(MarketType::UsdFutures),
            endpoint
        );
        let response = self.client.get(&url).send().await?;
        let status = response.status();
        let response_text = response.text().await?;

        if status.is_success() {
            let funding_rates: Vec<FundingRate> = serde_json::from_str(&response_text)?;
            Ok(funding_rates)
        } else {
            Err(AppError::Other(format!(
                "获取资金费率失败: {}",
                response_text
            )))
        }
    }

    /// 获取单个交易对的资金费率
    pub async fn get_funding_rate(&self, symbol: &str) -> Result<FundingRate, AppError> {
        let endpoint = self.endpoints.get_funding_rate_endpoint(MarketType::UsdFutures);
        let url = format!(
            "{}{}?symbol={}",
            self.endpoints.get_base_url(MarketType::UsdFutures),
            endpoint,
            symbol
        );
        let response = self.client.get(&url).send().await?;
        let status = response.status();
        let response_text = response.text().await?;

        if status.is_success() {
            let funding_rate: FundingRate = serde_json::from_str(&response_text)?;
            Ok(funding_rate)
        } else {
            Err(AppError::Other(format!(
                "获取{}资金费率失败: {}",
                symbol, response_text
            )))
        }
    }

    // ... More REST and WebSocket methods to be added here
}
