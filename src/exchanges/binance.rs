use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::time::{interval, Duration};

use crate::core::{
    config::{ApiKeys, Config},
    error::ExchangeError,
    exchange::{BaseExchange, Exchange},
    types::*,
    websocket::{ConnectionState, MessageHandler, WebSocketClient},
};
use crate::utils::{get_synced_timestamp, get_time_sync, SignatureHelper, SymbolConverter};
use tokio_tungstenite::tungstenite::Message;

/// ListenKey管理器，负责自动续期和重连
#[derive(Debug, Clone)]
pub struct ListenKeyManager {
    /// 当前的ListenKey
    pub listen_key: Option<String>,
    /// 市场类型
    pub market_type: MarketType,
    /// 是否正在运行续期任务
    pub is_running: bool,
    /// 最后续期时间
    pub last_keepalive: Option<DateTime<Utc>>,
}

impl ListenKeyManager {
    pub fn new(market_type: MarketType) -> Self {
        Self {
            listen_key: None,
            market_type,
            is_running: false,
            last_keepalive: None,
        }
    }

    /// 设置新的ListenKey
    pub fn set_listen_key(&mut self, key: String) {
        self.listen_key = Some(key);
        self.last_keepalive = Some(Utc::now());
    }

    /// 获取当前ListenKey
    pub fn get_listen_key(&self) -> Option<&String> {
        self.listen_key.as_ref()
    }

    /// 重置管理器状态
    pub fn reset(&mut self) {
        self.listen_key = None;
        self.is_running = false;
        self.last_keepalive = None;
    }

    /// 检查是否需要续期（超过25分钟）
    pub fn needs_keepalive(&self) -> bool {
        if let Some(last_time) = self.last_keepalive {
            let elapsed = Utc::now().signed_duration_since(last_time);
            elapsed.num_minutes() >= 25 // 提前5分钟续期
        } else {
            true
        }
    }
}

/// 币安交易所实现
pub struct BinanceExchange {
    base: BaseExchange,
    symbol_converter: SymbolConverter,
    /// 现货市场ListenKey管理器
    spot_listen_key_manager: Arc<Mutex<ListenKeyManager>>,
    /// 期货市场ListenKey管理器
    futures_listen_key_manager: Arc<Mutex<ListenKeyManager>>,
    /// 账户持仓模式缓存 (true: 双向持仓, false: 单向持仓)
    position_mode_cache: Arc<Mutex<Option<bool>>>,
    /// 时间偏移量（毫秒），用于校正本地时间与服务器时间的差异
    time_offset: Arc<Mutex<i64>>,
}

impl BinanceExchange {
    /// 创建币安交易所实例
    pub fn new(config: Config, api_keys: ApiKeys) -> Self {
        log::info!("🔍🔍 创建 BinanceExchange 实例");
        let base = BaseExchange::new("binance".to_string(), config.clone(), api_keys);
        let symbol_converter = SymbolConverter::new(config);

        let exchange = Self {
            base,
            symbol_converter,
            spot_listen_key_manager: Arc::new(Mutex::new(ListenKeyManager::new(MarketType::Spot))),
            futures_listen_key_manager: Arc::new(Mutex::new(ListenKeyManager::new(
                MarketType::Futures,
            ))),
            position_mode_cache: Arc::new(Mutex::new(None)),
            time_offset: Arc::new(Mutex::new(0)),
        };

        log::info!(
            "🔍🔍 BinanceExchange 实例创建完成，类型: {}",
            std::any::type_name::<Self>()
        );
        exchange
    }
}

impl Clone for BinanceExchange {
    fn clone(&self) -> Self {
        Self {
            base: self.base.clone(),
            symbol_converter: self.symbol_converter.clone(),
            spot_listen_key_manager: self.spot_listen_key_manager.clone(),
            futures_listen_key_manager: self.futures_listen_key_manager.clone(),
            position_mode_cache: self.position_mode_cache.clone(),
            time_offset: self.time_offset.clone(),
        }
    }
}

impl BinanceExchange {
    /// 同步服务器时间，计算本地时间与服务器时间的偏移（增加重试机制）
    pub async fn sync_server_time(&self) -> Result<()> {
        // 使用静态变量记录是否已经同步过（进程级别）
        static SYNCED: std::sync::Once = std::sync::Once::new();
        static mut GLOBAL_OFFSET: i64 = 0;

        let mut need_sync = false;
        SYNCED.call_once(|| {
            need_sync = true;
        });

        if need_sync {
            #[derive(Deserialize)]
            struct ServerTime {
                #[serde(rename = "serverTime")]
                server_time: i64,
            }

            // 获取Binance服务器时间
            let time: ServerTime = self
                .base
                .client
                .get("https://api.binance.com/api/v3/time")
                .send()
                .await?
                .json()
                .await?;

            // 计算时间偏移（服务器时间 - 本地时间）
            let local_time = chrono::Utc::now().timestamp_millis();
            let offset = time.server_time - local_time;

            // 存储到静态变量和实例变量
            unsafe {
                GLOBAL_OFFSET = offset;
            }
            *self.time_offset.lock().unwrap() = offset;

            log::info!(
                "✅ Binance进程级时间同步成功，时间偏移: {}ms（本次进程首次同步）",
                offset
            );
        } else {
            // 使用已同步的偏移值
            let offset = unsafe { GLOBAL_OFFSET };
            *self.time_offset.lock().unwrap() = offset;
            log::debug!("使用已同步的时间偏移: {}ms", offset);
        }

        Ok(())
    }

    /// 获取校正后的时间戳（毫秒）
    pub async fn get_corrected_timestamp(&self) -> u64 {
        // 优先使用全局时间同步
        if let Some(time_sync) = get_time_sync() {
            let timestamp = time_sync.get_adjusted_timestamp().await;
            timestamp as u64
        } else {
            // 使用本地时间偏移
            let local_time = chrono::Utc::now().timestamp_millis();
            let offset = *self.time_offset.lock().unwrap();
            (local_time + offset) as u64
        }
    }

    /// 获取账户持仓模式 (true: 双向持仓, false: 单向持仓)
    pub async fn get_position_mode(&self) -> Result<bool> {
        // 先检查缓存
        if let Some(cached) = *self.position_mode_cache.lock().unwrap() {
            return Ok(cached);
        }

        // 调用API获取持仓模式
        #[derive(Deserialize)]
        struct PositionModeResponse {
            #[serde(rename = "dualSidePosition")]
            dual_side_position: bool,
        }

        let response: PositionModeResponse = self
            .send_signed_request(
                "GET",
                "/fapi/v1/positionSide/dual",
                HashMap::new(),
                MarketType::Futures,
            )
            .await?;

        // 缓存结果
        *self.position_mode_cache.lock().unwrap() = Some(response.dual_side_position);

        log::info!(
            "Binance账户持仓模式: {}",
            if response.dual_side_position {
                "双向持仓"
            } else {
                "单向持仓"
            }
        );

        Ok(response.dual_side_position)
    }

    // 辅助方法：逐个创建订单
    async fn create_orders_one_by_one(
        &self,
        batch_request: BatchOrderRequest,
    ) -> Result<BatchOrderResponse> {
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

    /// 创建UserDataStream的listenKey
    pub async fn create_listen_key(&self, market_type: MarketType) -> Result<String> {
        let (base_url, endpoint) = match market_type {
            MarketType::Spot => ("https://api.binance.com", "/api/v3/userDataStream"),
            MarketType::Futures => ("https://fapi.binance.com", "/fapi/v1/listenKey"),
        };

        let url = format!("{}{}", base_url, endpoint);

        let response = self
            .base
            .client
            .post(&url)
            .header("X-MBX-APIKEY", &self.base.api_keys.api_key)
            .send()
            .await?;

        if response.status().is_success() {
            #[derive(Deserialize)]
            struct ListenKeyResponse {
                #[serde(rename = "listenKey")]
                listen_key: String,
            }

            let resp: ListenKeyResponse = response
                .json()
                .await
                .map_err(|e| ExchangeError::ParseError(e.to_string()))?;

            log::info!("✅ Created Binance listenKey for UserDataStream");
            Ok(resp.listen_key)
        } else {
            let status_code = response.status().as_u16() as i32;
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            Err(ExchangeError::ApiError {
                code: status_code,
                message: error_text,
            })
        }
    }

    /// 延长listenKey有效期
    pub async fn keepalive_listen_key(
        &self,
        listen_key: &str,
        market_type: MarketType,
    ) -> Result<()> {
        let (base_url, endpoint) = match market_type {
            MarketType::Spot => ("https://api.binance.com", "/api/v3/userDataStream"),
            MarketType::Futures => ("https://fapi.binance.com", "/fapi/v1/listenKey"),
        };

        let url = format!("{}{}?listenKey={}", base_url, endpoint, listen_key);

        let response = self
            .base
            .client
            .put(&url)
            .header("X-MBX-APIKEY", &self.base.api_keys.api_key)
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            let status_code = response.status().as_u16() as i32;
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            Err(ExchangeError::ApiError {
                code: status_code,
                message: error_text,
            })
        }
    }

    /// 创建期货ListenKey
    pub async fn create_futures_listen_key(&self) -> Result<String> {
        self.create_listen_key(MarketType::Futures).await
    }

    /// 创建现货ListenKey
    pub async fn create_spot_listen_key(&self) -> Result<String> {
        self.create_listen_key(MarketType::Spot).await
    }

    /// 延长期货ListenKey
    pub async fn extend_futures_listen_key(&self, listen_key: &str) -> Result<()> {
        self.keepalive_listen_key(listen_key, MarketType::Futures)
            .await
    }

    /// 延长现货ListenKey
    pub async fn extend_spot_listen_key(&self, listen_key: &str) -> Result<()> {
        self.keepalive_listen_key(listen_key, MarketType::Spot)
            .await
    }

    /// 创建ListenKey并启动自动续期任务
    pub async fn create_listen_key_with_auto_renewal(
        &self,
        market_type: MarketType,
    ) -> Result<String> {
        // 创建新的ListenKey
        let listen_key = self.create_listen_key(market_type).await?;

        // 更新管理器状态
        let manager = match market_type {
            MarketType::Spot => &self.spot_listen_key_manager,
            MarketType::Futures => &self.futures_listen_key_manager,
        };

        {
            let mut mgr = manager.lock().expect("Lock poisoned");
            mgr.set_listen_key(listen_key.clone());
        }

        // 启动自动续期任务
        self.start_keepalive_task(market_type).await;

        log::info!(
            "✅ 创建ListenKey并启动自动续期任务: {} (市场类型: {:?})",
            listen_key,
            market_type
        );
        Ok(listen_key)
    }

    /// 延长ListenKey有效期
    pub async fn extend_listen_key(&self, listen_key: &str, market_type: MarketType) -> Result<()> {
        let (base_url, endpoint) = match market_type {
            MarketType::Spot => ("https://api.binance.com", "/api/v3/userDataStream"),
            MarketType::Futures => ("https://fapi.binance.com", "/fapi/v1/listenKey"),
        };

        let url = format!("{}{}", base_url, endpoint);

        let mut params = HashMap::new();
        params.insert("listenKey".to_string(), listen_key.to_string());

        let response = self
            .base
            .client
            .put(&url)
            .header("X-MBX-APIKEY", &self.base.api_keys.api_key)
            .form(&params)
            .send()
            .await?;

        if response.status().is_success() {
            log::info!(
                "✅ 成功延长ListenKey有效期: {} (市场类型: {:?})",
                listen_key,
                market_type
            );
            Ok(())
        } else {
            let status_code = response.status().as_u16() as i32;
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            Err(ExchangeError::ApiError {
                code: status_code,
                message: format!("延长ListenKey失败: {}", error_text),
            })
        }
    }

    /// 启动ListenKey自动续期任务
    pub async fn start_keepalive_task(&self, market_type: MarketType) {
        let manager = match market_type {
            MarketType::Spot => self.spot_listen_key_manager.clone(),
            MarketType::Futures => self.futures_listen_key_manager.clone(),
        };

        // 检查是否已经在运行续期任务
        {
            let mut mgr = manager.lock().expect("Lock poisoned");
            if mgr.is_running {
                log::info!(
                    "📝 ListenKey续期任务已在运行中，跳过启动 (市场类型: {:?})",
                    market_type
                );
                return;
            }
            mgr.is_running = true;
        }

        let exchange_clone = self.clone();
        let manager_clone = manager.clone();

        // 创建异步任务进行定期续期
        tokio::spawn(async move {
            let mut interval_timer = interval(Duration::from_secs(25 * 60)); // 每25分钟检查一次

            log::info!("🔄 启动ListenKey自动续期任务 (市场类型: {:?})", market_type);

            loop {
                interval_timer.tick().await;

                let should_keepalive = {
                    let mgr = manager_clone.lock().expect("Lock poisoned");
                    mgr.needs_keepalive() && mgr.listen_key.is_some()
                };

                if should_keepalive {
                    let listen_key = {
                        let mgr = manager_clone.lock().expect("Lock poisoned");
                        mgr.listen_key.clone()
                    };

                    if let Some(key) = listen_key {
                        log::info!(
                            "🔄 执行ListenKey续期: {} (市场类型: {:?})",
                            key,
                            market_type
                        );

                        match exchange_clone.keepalive_listen_key(&key, market_type).await {
                            Ok(()) => {
                                let mut mgr = manager_clone.lock().expect("Lock poisoned");
                                mgr.last_keepalive = Some(Utc::now());
                                log::info!(
                                    "✅ ListenKey续期成功: {} (市场类型: {:?})",
                                    key,
                                    market_type
                                );
                            }
                            Err(e) => {
                                log::error!(
                                    "❌ ListenKey续期失败: {} (市场类型: {:?}), 错误: {}",
                                    key.as_str(),
                                    market_type,
                                    e
                                );

                                // 续期失败时，尝试重新创建ListenKey
                                log::info!(
                                    "🔄 尝试重新创建ListenKey (市场类型: {:?})",
                                    market_type
                                );
                                match exchange_clone.create_listen_key(market_type).await {
                                    Ok(new_key) => {
                                        let mut mgr = manager_clone.lock().expect("Lock poisoned");
                                        mgr.set_listen_key(new_key.clone());
                                        log::info!(
                                            "✅ 重新创建ListenKey成功: {} (市场类型: {:?})",
                                            new_key,
                                            market_type
                                        );
                                    }
                                    Err(e) => {
                                        log::error!(
                                            "❌ 重新创建ListenKey失败 (市场类型: {:?}): {}",
                                            market_type,
                                            e
                                        );
                                        // 重新创建失败，停止续期任务
                                        let mut mgr = manager_clone.lock().expect("Lock poisoned");
                                        mgr.reset();
                                        break;
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // 检查管理器是否仍然有效
                    let has_key = {
                        let mgr = manager_clone.lock().expect("Lock poisoned");
                        mgr.listen_key.is_some()
                    };

                    if !has_key {
                        log::info!(
                            "⏹️ ListenKey已清空，停止续期任务 (市场类型: {:?})",
                            market_type
                        );
                        let mut mgr = manager_clone.lock().expect("Lock poisoned");
                        mgr.is_running = false;
                        break;
                    }
                }
            }

            log::info!(
                "⏹️ ListenKey自动续期任务已停止 (市场类型: {:?})",
                market_type
            );
        });
    }

    /// 停止ListenKey自动续期任务
    pub fn stop_keepalive_task(&self, market_type: MarketType) {
        let manager = match market_type {
            MarketType::Spot => &self.spot_listen_key_manager,
            MarketType::Futures => &self.futures_listen_key_manager,
        };

        let mut mgr = manager.lock().expect("Lock poisoned");
        mgr.reset();
        log::info!("⏹️ 停止ListenKey自动续期任务 (市场类型: {:?})", market_type);
    }

    /// 获取当前的ListenKey
    pub fn get_current_listen_key(&self, market_type: MarketType) -> Option<String> {
        let manager = match market_type {
            MarketType::Spot => &self.spot_listen_key_manager,
            MarketType::Futures => &self.futures_listen_key_manager,
        };

        let mgr = manager.lock().expect("Lock poisoned");
        mgr.listen_key.clone()
    }

    /// 处理ListenKey过期事件
    pub async fn handle_listen_key_expired(&self, market_type: MarketType) -> Result<String> {
        log::error!("🔄 处理ListenKey过期事件，市场类型: {:?}", market_type);

        // 停止当前的续期任务
        self.stop_keepalive_task(market_type);

        // 重新创建ListenKey并启动续期任务
        let new_listen_key = self
            .create_listen_key_with_auto_renewal(market_type)
            .await?;

        log::info!(
            "✅ ListenKey过期处理完成，新的ListenKey: {} (市场类型: {:?})",
            new_listen_key,
            market_type
        );

        Ok(new_listen_key)
    }

    /// 发送认证请求
    async fn send_signed_request<T>(
        &self,
        method: &str,
        endpoint: &str,
        mut params: HashMap<String, String>,
        market_type: MarketType,
    ) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        // 如果时间偏移为0，先同步一次时间（增加重试机制）
        if *self.time_offset.lock().unwrap() == 0 {
            let mut retry_count = 0;
            while retry_count < 3 {
                if let Err(e) = self.sync_server_time().await {
                    log::warn!("第{}次同步Binance服务器时间失败: {}", retry_count + 1, e);
                    retry_count += 1;
                    if retry_count < 3 {
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }
                } else {
                    break;
                }
            }
        }

        // 添加时间戳和recvWindow（增加到60秒，避免时间不同步问题）
        // 使用校正后的时间戳
        let timestamp = self.get_corrected_timestamp().await.to_string();
        params.insert("timestamp".to_string(), timestamp.clone());
        params.insert("recvWindow".to_string(), "60000".to_string());

        // 按字母顺序排序参数以生成签名
        let mut sorted_params: Vec<(&String, &String)> = params.iter().collect();
        sorted_params.sort_by_key(|&(k, _)| k);

        // 构建查询字符串用于签名
        let query_string: Vec<String> = sorted_params
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();
        let query_string = query_string.join("&");

        // 生成签名
        let signature =
            SignatureHelper::binance_signature(&self.base.api_keys.api_secret, &query_string);

        // 构建最终的查询字符串（包含签名）
        let final_query = format!("{}&signature={}", query_string, signature);

        // 构建完整URL
        let base_url = match market_type {
            MarketType::Spot => "https://api.binance.com",
            MarketType::Futures => "https://fapi.binance.com", // 币安期货API
        };
        let url = format!("{}{}?{}", base_url, endpoint, final_query);

        // 设置请求头
        let response = match method.to_uppercase().as_str() {
            "GET" => {
                self.base
                    .client
                    .get(&url)
                    .header("X-MBX-APIKEY", &self.base.api_keys.api_key)
                    .send()
                    .await?
            }
            "POST" => {
                self.base
                    .client
                    .post(&url)
                    .header("X-MBX-APIKEY", &self.base.api_keys.api_key)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .send()
                    .await?
            }
            "DELETE" => {
                self.base
                    .client
                    .delete(&url)
                    .header("X-MBX-APIKEY", &self.base.api_keys.api_key)
                    .send()
                    .await?
            }
            _ => return Err(ExchangeError::Other("不支持的HTTP方法".to_string())),
        };

        // 处理响应
        if response.status().is_success() {
            let data = response.json::<T>().await?;
            Ok(data)
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

    /// 发送公共请求
    async fn send_public_request<T>(
        &self,
        endpoint: &str,
        params: Option<HashMap<String, String>>,
        market_type: MarketType,
    ) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let base_url = match market_type {
            MarketType::Spot => "https://api.binance.com",
            MarketType::Futures => "https://fapi.binance.com", // 币安期货API
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
            let data = response.json::<T>().await?;
            Ok(data)
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
impl Exchange for BinanceExchange {
    fn name(&self) -> &str {
        &self.base.name
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn exchange_info(&self) -> Result<ExchangeInfo> {
        #[derive(Deserialize)]
        struct BinanceExchangeInfo {
            symbols: Vec<BinanceSymbol>,
        }

        #[derive(Deserialize)]
        struct BinanceSymbol {
            symbol: String,
            #[serde(rename = "baseAsset")]
            base_asset: String,
            #[serde(rename = "quoteAsset")]
            quote_asset: String,
            status: String,
        }

        // 获取现货交易对信息
        let spot_info: BinanceExchangeInfo = self
            .send_public_request("/api/v3/exchangeInfo", None, MarketType::Spot)
            .await?;

        let mut symbols = Vec::new();
        for symbol_info in spot_info.symbols {
            if symbol_info.status == "TRADING" {
                symbols.push(Symbol {
                    base: symbol_info.base_asset.clone(),
                    quote: symbol_info.quote_asset.clone(),
                    symbol: format!("{}/{}", symbol_info.base_asset, symbol_info.quote_asset),
                    market_type: MarketType::Spot,
                    exchange_specific: Some(symbol_info.symbol),
                });
            }
        }

        Ok(ExchangeInfo {
            name: "Binance".to_string(),
            symbols,
            currencies: vec![], // 可以后续实现
            spot_enabled: true,
            futures_enabled: true,
        })
    }

    async fn get_balance(&self, market_type: MarketType) -> Result<Vec<Balance>> {
        fn default_zero_string() -> String {
            "0".to_string()
        }
        let endpoint = match market_type {
            MarketType::Spot => "/api/v3/account",
            MarketType::Futures => "/fapi/v2/account",
        };

        #[derive(Deserialize, Debug)]
        struct BinanceBalance {
            balances: Option<Vec<BinanceAsset>>, // 现货
            assets: Option<Vec<BinanceAsset>>,   // 期货
        }

        #[derive(Deserialize, Debug)]
        struct BinanceAsset {
            asset: String,
            #[serde(alias = "free", alias = "availableBalance")]
            free: Option<String>,
            #[serde(alias = "locked", alias = "crossUnPnl")]
            locked: Option<String>,
            #[serde(alias = "walletBalance")]
            wallet_balance: Option<String>,
        }

        // 先获取原始响应来调试
        let response: serde_json::Value = self
            .send_signed_request("GET", endpoint, HashMap::new(), market_type)
            .await?;

        // 尝试解析为结构体
        let balance: BinanceBalance = serde_json::from_value(response.clone()).map_err(|e| {
            eprintln!(
                "Binance balance parse error: {:?}, response: {}",
                e, response
            );
            ExchangeError::ParseError(format!("Failed to parse balance: {}", e))
        })?;

        let assets = balance.balances.or(balance.assets).unwrap_or_default();

        let mut balances = Vec::new();
        for asset in assets {
            // 期货账户使用availableBalance和walletBalance
            let free = asset
                .free
                .as_ref()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let total = asset
                .wallet_balance
                .as_ref()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(free);
            let used = asset
                .locked
                .as_ref()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(total - free);

            if total > 0.0 || free > 0.0 {
                balances.push(Balance {
                    currency: asset.asset,
                    total,
                    free,
                    used,
                    market_type: market_type.clone(),
                });
            }
        }

        Ok(balances)
    }

    async fn get_ticker(&self, symbol: &str, market_type: MarketType) -> Result<Ticker> {
        let exchange_symbol =
            self.symbol_converter
                .to_exchange_symbol(symbol, "binance", market_type)?;

        match market_type {
            MarketType::Spot => {
                #[derive(Deserialize)]
                struct BinanceSpotTicker {
                    symbol: String,
                    #[serde(rename = "highPrice")]
                    high_price: String,
                    #[serde(rename = "lowPrice")]
                    low_price: String,
                    #[serde(rename = "bidPrice")]
                    bid_price: String,
                    #[serde(rename = "askPrice")]
                    ask_price: String,
                    #[serde(rename = "lastPrice")]
                    last_price: String,
                    volume: String,
                    #[serde(rename = "closeTime")]
                    close_time: i64,
                }

                let mut params = HashMap::new();
                params.insert("symbol".to_string(), exchange_symbol);

                let ticker: BinanceSpotTicker = self
                    .send_public_request("/api/v3/ticker/24hr", Some(params), market_type)
                    .await?;

                Ok(Ticker {
                    symbol: symbol.to_string(),
                    high: ticker.high_price.parse().unwrap_or(0.0),
                    low: ticker.low_price.parse().unwrap_or(0.0),
                    bid: ticker.bid_price.parse().unwrap_or(0.0),
                    ask: ticker.ask_price.parse().unwrap_or(0.0),
                    last: ticker.last_price.parse().unwrap_or(0.0),
                    volume: ticker.volume.parse().unwrap_or(0.0),
                    timestamp: DateTime::from_timestamp(ticker.close_time / 1000, 0)
                        .unwrap_or_else(|| Utc::now()),
                })
            }
            MarketType::Futures => {
                #[derive(Deserialize)]
                struct BinanceFuturesTicker {
                    symbol: String,
                    #[serde(rename = "highPrice")]
                    high_price: String,
                    #[serde(rename = "lowPrice")]
                    low_price: String,
                    #[serde(rename = "lastPrice")]
                    last_price: String,
                    volume: String,
                    #[serde(rename = "closeTime")]
                    close_time: i64,
                }

                let mut params = HashMap::new();
                params.insert("symbol".to_string(), exchange_symbol.clone());

                let ticker: BinanceFuturesTicker = self
                    .send_public_request("/fapi/v1/ticker/24hr", Some(params), market_type)
                    .await?;

                // 对于期货，我们需要单独获取盘口数据来得到买卖价
                #[derive(Deserialize)]
                struct BinanceBookTicker {
                    #[serde(rename = "bidPrice")]
                    bid_price: String,
                    #[serde(rename = "askPrice")]
                    ask_price: String,
                }

                let mut book_params = HashMap::new();
                book_params.insert("symbol".to_string(), exchange_symbol);

                let book_ticker: BinanceBookTicker = self
                    .send_public_request(
                        "/fapi/v1/ticker/bookTicker",
                        Some(book_params),
                        market_type,
                    )
                    .await?;

                Ok(Ticker {
                    symbol: symbol.to_string(),
                    high: ticker.high_price.parse().unwrap_or(0.0),
                    low: ticker.low_price.parse().unwrap_or(0.0),
                    bid: book_ticker.bid_price.parse().unwrap_or(0.0),
                    ask: book_ticker.ask_price.parse().unwrap_or(0.0),
                    last: ticker.last_price.parse().unwrap_or(0.0),
                    volume: ticker.volume.parse().unwrap_or(0.0),
                    timestamp: DateTime::from_timestamp(ticker.close_time / 1000, 0)
                        .unwrap_or_else(|| Utc::now()),
                })
            }
        }
    }

    async fn get_all_tickers(&self, market_type: MarketType) -> Result<Vec<Ticker>> {
        let endpoint = match market_type {
            MarketType::Spot => "/api/v3/ticker/24hr",
            MarketType::Futures => "/fapi/v1/ticker/24hr",
        };

        #[derive(Deserialize)]
        struct BinanceTicker {
            symbol: String,
            #[serde(rename = "highPrice")]
            high_price: String,
            #[serde(rename = "lowPrice")]
            low_price: String,
            #[serde(rename = "bidPrice")]
            bid_price: String,
            #[serde(rename = "askPrice")]
            ask_price: String,
            #[serde(rename = "lastPrice")]
            last_price: String,
            volume: String,
            #[serde(rename = "closeTime")]
            close_time: i64,
        }

        let tickers: Vec<BinanceTicker> = self
            .send_public_request(endpoint, None, market_type)
            .await?;

        let mut result = Vec::new();
        for ticker in tickers {
            // 转换交易对格式
            if let Ok(standard_symbol) =
                self.symbol_converter
                    .from_exchange_symbol(&ticker.symbol, "binance", market_type)
            {
                result.push(Ticker {
                    symbol: standard_symbol,
                    high: ticker.high_price.parse().unwrap_or(0.0),
                    low: ticker.low_price.parse().unwrap_or(0.0),
                    bid: ticker.bid_price.parse().unwrap_or(0.0),
                    ask: ticker.ask_price.parse().unwrap_or(0.0),
                    last: ticker.last_price.parse().unwrap_or(0.0),
                    volume: ticker.volume.parse().unwrap_or(0.0),
                    timestamp: DateTime::from_timestamp(ticker.close_time / 1000, 0)
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
                .to_exchange_symbol(symbol, "binance", market_type)?;

        let endpoint = match market_type {
            MarketType::Spot => "/api/v3/depth",
            MarketType::Futures => "/fapi/v1/depth",
        };

        #[derive(Deserialize)]
        struct BinanceOrderBook {
            #[serde(rename = "lastUpdateId")]
            last_update_id: i64,
            bids: Vec<[String; 2]>,
            asks: Vec<[String; 2]>,
        }

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), exchange_symbol);
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.to_string());
        }

        let orderbook: BinanceOrderBook = self
            .send_public_request(endpoint, Some(params), market_type)
            .await?;

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
            timestamp: Utc::now(),
        })
    }

    async fn create_order(&self, order_request: OrderRequest) -> Result<Order> {
        let exchange_symbol = self.symbol_converter.to_exchange_symbol(
            &order_request.symbol,
            "binance",
            order_request.market_type,
        )?;

        let endpoint = match order_request.market_type {
            MarketType::Spot => "/api/v3/order",
            MarketType::Futures => "/fapi/v1/order",
        };

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), exchange_symbol);
        params.insert(
            "side".to_string(),
            match order_request.side {
                OrderSide::Buy => "BUY".to_string(),
                OrderSide::Sell => "SELL".to_string(),
            },
        );
        params.insert(
            "type".to_string(),
            match order_request.order_type {
                OrderType::Market => "MARKET".to_string(),
                OrderType::Limit => "LIMIT".to_string(),
                OrderType::StopLimit => "STOP_LOSS_LIMIT".to_string(),
                OrderType::StopMarket => "STOP_LOSS".to_string(),
                OrderType::TakeProfitLimit => "TAKE_PROFIT_LIMIT".to_string(),
                OrderType::TakeProfitMarket => "TAKE_PROFIT".to_string(),
                OrderType::TrailingStop => "TRAILING_STOP_MARKET".to_string(),
            },
        );
        params.insert("quantity".to_string(), order_request.amount.to_string());

        if let Some(price) = order_request.price {
            params.insert("price".to_string(), price.to_string());
        }

        if order_request.order_type == OrderType::Limit {
            params.insert("timeInForce".to_string(), "GTC".to_string());
        }

        // 添加自定义订单ID（用于策略识别）
        if let Some(client_order_id) = &order_request.client_order_id {
            params.insert("newClientOrderId".to_string(), client_order_id.clone());
        }

        // 对于期货订单，添加额外参数
        if order_request.market_type == MarketType::Futures {
            // 如果有额外参数，添加它们
            if let Some(extra_params) = &order_request.params {
                for (key, value) in extra_params {
                    params.insert(key.clone(), value.clone());
                }
            }

            // 自动处理双向持仓模式的positionSide参数
            // 如果没有手动设置positionSide，自动检测并设置
            if !params.contains_key("positionSide") {
                // 检查是否是双向持仓模式
                if let Ok(is_dual_mode) = self.get_position_mode().await {
                    if is_dual_mode {
                        // 双向持仓模式下，需要设置positionSide
                        // 如果是reduceOnly订单，需要获取当前持仓方向
                        if params
                            .get("reduceOnly")
                            .map(|v| v == "true")
                            .unwrap_or(false)
                        {
                            // 获取当前持仓来确定positionSide
                            if let Ok(positions) = self.get_positions(None).await {
                                for pos in positions {
                                    if pos.symbol == order_request.symbol && pos.contracts > 0.0 {
                                        // 找到对应持仓，设置正确的positionSide
                                        let position_side =
                                            if pos.side == "LONG" { "LONG" } else { "SHORT" };
                                        params.insert(
                                            "positionSide".to_string(),
                                            position_side.to_string(),
                                        );
                                        log::debug!(
                                            "自动设置平仓positionSide: {} for {}",
                                            position_side,
                                            order_request.symbol
                                        );
                                        break;
                                    }
                                }
                            }
                        } else {
                            // 开仓订单，根据方向设置positionSide
                            let position_side = match order_request.side {
                                OrderSide::Buy => "LONG",   // 买入开多
                                OrderSide::Sell => "SHORT", // 卖出开空
                            };
                            params.insert("positionSide".to_string(), position_side.to_string());
                            log::debug!(
                                "自动设置开仓positionSide: {} for {}",
                                position_side,
                                order_request.symbol
                            );
                        }
                    }
                }
            }
        }

        #[derive(Deserialize)]
        struct BinanceOrderResponse {
            #[serde(rename = "orderId")]
            order_id: i64,
            symbol: String,
            side: String,
            #[serde(rename = "type")]
            order_type: String,
            #[serde(rename = "origQty")]
            orig_qty: String,
            price: Option<String>,
            #[serde(rename = "executedQty")]
            executed_qty: String,
            status: String,
            #[serde(rename = "transactTime")]
            transact_time: Option<i64>,
            #[serde(rename = "updateTime")]
            update_time: Option<i64>,
        }

        let response: BinanceOrderResponse = self
            .send_signed_request("POST", endpoint, params, order_request.market_type)
            .await?;

        Ok(Order {
            id: response.order_id.to_string(),
            symbol: order_request.symbol,
            side: order_request.side,
            order_type: order_request.order_type,
            amount: response.orig_qty.parse().unwrap_or(0.0),
            price: response.price.as_ref().and_then(|p| {
                if p != "0.00000000" {
                    Some(p.parse().unwrap_or(0.0))
                } else {
                    None
                }
            }),
            filled: response.executed_qty.parse().unwrap_or(0.0),
            remaining: response.orig_qty.parse().unwrap_or(0.0)
                - response.executed_qty.parse().unwrap_or(0.0),
            status: match response.status.as_str() {
                "NEW" => OrderStatus::Open,
                "FILLED" => OrderStatus::Closed,
                "CANCELED" => OrderStatus::Canceled,
                "EXPIRED" => OrderStatus::Expired,
                "REJECTED" => OrderStatus::Rejected,
                _ => OrderStatus::Pending,
            },
            market_type: order_request.market_type,
            timestamp: response
                .transact_time
                .or(response.update_time)
                .and_then(|t| DateTime::from_timestamp(t / 1000, 0))
                .unwrap_or_else(|| Utc::now()),
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
                .to_exchange_symbol(symbol, "binance", market_type)?;

        let endpoint = match market_type {
            MarketType::Spot => "/api/v3/order",
            MarketType::Futures => "/fapi/v1/order",
        };

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), exchange_symbol);
        params.insert("orderId".to_string(), order_id.to_string());

        #[derive(Deserialize)]
        struct BinanceOrderResponse {
            #[serde(rename = "orderId")]
            order_id: i64,
            symbol: String,
            side: String,
            #[serde(rename = "type")]
            order_type: String,
            #[serde(rename = "origQty")]
            orig_qty: String,
            price: Option<String>,
            #[serde(rename = "executedQty")]
            executed_qty: String,
            status: String,
        }

        let response: BinanceOrderResponse = self
            .send_signed_request("DELETE", endpoint, params, market_type)
            .await?;

        Ok(Order {
            id: response.order_id.to_string(),
            symbol: symbol.to_string(),
            side: match response.side.as_str() {
                "BUY" => OrderSide::Buy,
                "SELL" => OrderSide::Sell,
                _ => OrderSide::Buy,
            },
            order_type: match response.order_type.as_str() {
                "MARKET" => OrderType::Market,
                "LIMIT" => OrderType::Limit,
                _ => OrderType::Limit,
            },
            amount: response.orig_qty.parse().unwrap_or(0.0),
            price: response.price.as_ref().and_then(|p| {
                if p != "0.00000000" {
                    Some(p.parse().unwrap_or(0.0))
                } else {
                    None
                }
            }),
            filled: response.executed_qty.parse().unwrap_or(0.0),
            remaining: response.orig_qty.parse().unwrap_or(0.0)
                - response.executed_qty.parse().unwrap_or(0.0),
            status: OrderStatus::Canceled,
            market_type,
            timestamp: Utc::now(),
            last_trade_timestamp: None,
            info: serde_json::json!({}),
        })
    }

    async fn get_order(
        &self,
        order_id: &str,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<Order> {
        let exchange_symbol =
            self.symbol_converter
                .to_exchange_symbol(symbol, "binance", market_type)?;

        let endpoint = match market_type {
            MarketType::Spot => "/api/v3/order",
            MarketType::Futures => "/fapi/v1/order",
        };

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), exchange_symbol);
        params.insert("orderId".to_string(), order_id.to_string());

        #[derive(Deserialize)]
        struct BinanceOrderResponse {
            #[serde(rename = "orderId")]
            order_id: i64,
            symbol: String,
            side: String,
            #[serde(rename = "type")]
            order_type: String,
            #[serde(rename = "origQty")]
            orig_qty: String,
            price: Option<String>,
            #[serde(rename = "executedQty")]
            executed_qty: String,
            status: String,
            time: i64,
            #[serde(rename = "updateTime")]
            update_time: i64,
        }

        let response: BinanceOrderResponse = self
            .send_signed_request("GET", endpoint, params, market_type)
            .await?;

        Ok(Order {
            id: response.order_id.to_string(),
            symbol: symbol.to_string(),
            side: match response.side.as_str() {
                "BUY" => OrderSide::Buy,
                "SELL" => OrderSide::Sell,
                _ => OrderSide::Buy,
            },
            order_type: match response.order_type.as_str() {
                "MARKET" => OrderType::Market,
                "LIMIT" => OrderType::Limit,
                _ => OrderType::Limit,
            },
            amount: response.orig_qty.parse().unwrap_or(0.0),
            price: response.price.as_ref().and_then(|p| {
                if p != "0.00000000" {
                    Some(p.parse().unwrap_or(0.0))
                } else {
                    None
                }
            }),
            filled: response.executed_qty.parse().unwrap_or(0.0),
            remaining: response.orig_qty.parse().unwrap_or(0.0)
                - response.executed_qty.parse().unwrap_or(0.0),
            status: match response.status.as_str() {
                "NEW" => OrderStatus::Open,
                "FILLED" => OrderStatus::Closed,
                "CANCELED" => OrderStatus::Canceled,
                "EXPIRED" => OrderStatus::Expired,
                "REJECTED" => OrderStatus::Rejected,
                _ => OrderStatus::Pending,
            },
            market_type,
            timestamp: DateTime::from_timestamp(response.time / 1000, 0)
                .unwrap_or_else(|| Utc::now()),
            last_trade_timestamp: Some(
                DateTime::from_timestamp(response.update_time / 1000, 0)
                    .unwrap_or_else(|| Utc::now()),
            ),
            info: serde_json::json!({}),
        })
    }

    async fn get_open_orders(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
    ) -> Result<Vec<Order>> {
        let endpoint = match market_type {
            MarketType::Spot => "/api/v3/openOrders",
            MarketType::Futures => "/fapi/v1/openOrders",
        };

        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            let exchange_symbol =
                self.symbol_converter
                    .to_exchange_symbol(symbol, "binance", market_type)?;
            params.insert("symbol".to_string(), exchange_symbol);
        }

        #[derive(Deserialize)]
        struct BinanceOrderResponse {
            #[serde(rename = "orderId")]
            order_id: i64,
            symbol: String,
            side: String,
            #[serde(rename = "type")]
            order_type: String,
            #[serde(rename = "origQty")]
            orig_qty: String,
            price: Option<String>,
            #[serde(rename = "executedQty")]
            executed_qty: String,
            status: String,
            time: i64,
            #[serde(rename = "updateTime")]
            update_time: i64,
        }

        let orders: Vec<BinanceOrderResponse> = self
            .send_signed_request("GET", endpoint, params, market_type)
            .await?;

        let mut result = Vec::new();
        for order in orders {
            // 转换交易对格式
            if let Ok(standard_symbol) =
                self.symbol_converter
                    .from_exchange_symbol(&order.symbol, "binance", market_type)
            {
                result.push(Order {
                    id: order.order_id.to_string(),
                    symbol: standard_symbol,
                    side: match order.side.as_str() {
                        "BUY" => OrderSide::Buy,
                        "SELL" => OrderSide::Sell,
                        _ => OrderSide::Buy,
                    },
                    order_type: match order.order_type.as_str() {
                        "MARKET" => OrderType::Market,
                        "LIMIT" => OrderType::Limit,
                        _ => OrderType::Limit,
                    },
                    amount: order.orig_qty.parse().unwrap_or(0.0),
                    price: order.price.as_ref().and_then(|p| {
                        if p != "0.00000000" {
                            Some(p.parse().unwrap_or(0.0))
                        } else {
                            None
                        }
                    }),
                    filled: order.executed_qty.parse().unwrap_or(0.0),
                    remaining: order.orig_qty.parse().unwrap_or(0.0)
                        - order.executed_qty.parse().unwrap_or(0.0),
                    status: match order.status.as_str() {
                        "NEW" => OrderStatus::Open,
                        "FILLED" => OrderStatus::Closed,
                        "CANCELED" => OrderStatus::Canceled,
                        "EXPIRED" => OrderStatus::Expired,
                        "REJECTED" => OrderStatus::Rejected,
                        _ => OrderStatus::Pending,
                    },
                    market_type,
                    timestamp: DateTime::from_timestamp(order.time / 1000, 0)
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

    async fn get_order_history(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
        limit: Option<u32>,
    ) -> Result<Vec<Order>> {
        let endpoint = match market_type {
            MarketType::Spot => "/api/v3/allOrders",
            MarketType::Futures => "/fapi/v1/allOrders",
        };

        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            let exchange_symbol =
                self.symbol_converter
                    .to_exchange_symbol(symbol, "binance", market_type)?;
            params.insert("symbol".to_string(), exchange_symbol);
        }
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.to_string());
        }

        #[derive(Deserialize)]
        struct BinanceOrderResponse {
            #[serde(rename = "orderId")]
            order_id: i64,
            symbol: String,
            side: String,
            #[serde(rename = "type")]
            order_type: String,
            #[serde(rename = "origQty")]
            orig_qty: String,
            price: Option<String>,
            #[serde(rename = "executedQty")]
            executed_qty: String,
            status: String,
            time: i64,
            #[serde(rename = "updateTime")]
            update_time: i64,
        }

        let orders: Vec<BinanceOrderResponse> = self
            .send_signed_request("GET", endpoint, params, market_type)
            .await?;

        let mut result = Vec::new();
        for order in orders {
            // 转换交易对格式
            if let Ok(standard_symbol) =
                self.symbol_converter
                    .from_exchange_symbol(&order.symbol, "binance", market_type)
            {
                result.push(Order {
                    id: order.order_id.to_string(),
                    symbol: standard_symbol,
                    side: match order.side.as_str() {
                        "BUY" => OrderSide::Buy,
                        "SELL" => OrderSide::Sell,
                        _ => OrderSide::Buy,
                    },
                    order_type: match order.order_type.as_str() {
                        "MARKET" => OrderType::Market,
                        "LIMIT" => OrderType::Limit,
                        _ => OrderType::Limit,
                    },
                    amount: order.orig_qty.parse().unwrap_or(0.0),
                    price: order.price.as_ref().and_then(|p| {
                        if p != "0.00000000" {
                            Some(p.parse().unwrap_or(0.0))
                        } else {
                            None
                        }
                    }),
                    filled: order.executed_qty.parse().unwrap_or(0.0),
                    remaining: order.orig_qty.parse().unwrap_or(0.0)
                        - order.executed_qty.parse().unwrap_or(0.0),
                    status: match order.status.as_str() {
                        "NEW" => OrderStatus::Open,
                        "FILLED" => OrderStatus::Closed,
                        "CANCELED" => OrderStatus::Canceled,
                        "EXPIRED" => OrderStatus::Expired,
                        "REJECTED" => OrderStatus::Rejected,
                        _ => OrderStatus::Pending,
                    },
                    market_type,
                    timestamp: DateTime::from_timestamp(order.time / 1000, 0)
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
        let exchange_symbol =
            self.symbol_converter
                .to_exchange_symbol(symbol, "binance", market_type)?;

        let endpoint = match market_type {
            MarketType::Spot => "/api/v3/trades",
            MarketType::Futures => "/fapi/v1/trades",
        };

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), exchange_symbol);
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.to_string());
        }

        #[derive(Deserialize)]
        struct BinanceTrade {
            id: i64,
            price: String,
            qty: String,
            #[serde(rename = "quoteQty")]
            quote_qty: String,
            time: i64,
            #[serde(rename = "isBuyerMaker")]
            is_buyer_maker: bool,
        }

        let trades: Vec<BinanceTrade> = self
            .send_public_request(endpoint, Some(params), market_type)
            .await?;

        let mut result = Vec::new();
        for trade in trades {
            result.push(Trade {
                id: trade.id.to_string(),
                symbol: symbol.to_string(),
                side: if trade.is_buyer_maker {
                    OrderSide::Sell
                } else {
                    OrderSide::Buy
                },
                amount: trade.qty.parse().unwrap_or(0.0),
                price: trade.price.parse().unwrap_or(0.0),
                timestamp: DateTime::from_timestamp(trade.time / 1000, 0)
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
        let endpoint = match market_type {
            MarketType::Spot => "/api/v3/myTrades",
            MarketType::Futures => "/fapi/v1/userTrades",
        };

        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            let exchange_symbol =
                self.symbol_converter
                    .to_exchange_symbol(symbol, "binance", market_type)?;
            params.insert("symbol".to_string(), exchange_symbol);
        }
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.to_string());
        }

        #[derive(Deserialize)]
        struct BinanceMyTrade {
            id: i64,
            #[serde(rename = "orderId")]
            order_id: i64,
            symbol: String,
            #[serde(default)]
            side: String,
            qty: String,
            price: String,
            commission: String,
            #[serde(rename = "commissionAsset")]
            commission_asset: String,
            time: i64,
            // Spot API uses 'isBuyer', Futures API uses 'buyer'
            #[serde(rename = "isBuyer")]
            #[serde(alias = "buyer")]
            is_buyer: bool,
        }

        let trades: Vec<BinanceMyTrade> = self
            .send_signed_request("GET", endpoint, params, market_type)
            .await?;

        let mut result = Vec::new();
        for trade in trades {
            // 转换交易对格式
            if let Ok(standard_symbol) =
                self.symbol_converter
                    .from_exchange_symbol(&trade.symbol, "binance", market_type)
            {
                result.push(Trade {
                    id: trade.id.to_string(),
                    symbol: standard_symbol,
                    side: if trade.is_buyer {
                        OrderSide::Buy
                    } else {
                        OrderSide::Sell
                    },
                    amount: trade.qty.parse().unwrap_or(0.0),
                    price: trade.price.parse().unwrap_or(0.0),
                    timestamp: DateTime::from_timestamp(trade.time / 1000, 0)
                        .unwrap_or_else(|| Utc::now()),
                    order_id: Some(trade.order_id.to_string()),
                    fee: Some(Fee {
                        currency: trade.commission_asset,
                        cost: trade.commission.parse().unwrap_or(0.0),
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
                .to_exchange_symbol(symbol, "binance", market_type)?;

        let endpoint = match market_type {
            MarketType::Spot => "/api/v3/klines",
            MarketType::Futures => "/fapi/v1/klines",
        };

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), exchange_symbol);
        params.insert(
            "interval".to_string(),
            interval.to_exchange_format("binance"),
        );
        if let Some(limit) = limit {
            params.insert("limit".to_string(), limit.to_string());
        }

        // 币安K线数据格式: [开盘时间, 开盘价, 最高价, 最低价, 收盘价, 成交量, 收盘时间, 成交额, 成交次数, ...]
        let klines_data: Vec<Vec<serde_json::Value>> = self
            .send_public_request(endpoint, Some(params), market_type)
            .await?;

        let mut result = Vec::new();
        for kline_data in klines_data {
            if kline_data.len() >= 9 {
                result.push(Kline {
                    symbol: symbol.to_string(),
                    interval: interval.to_string(),
                    open_time: DateTime::from_timestamp(
                        kline_data[0].as_i64().unwrap_or(0) / 1000,
                        0,
                    )
                    .unwrap_or_else(|| chrono::Utc::now()),
                    close_time: DateTime::from_timestamp(
                        kline_data[6].as_i64().unwrap_or(0) / 1000,
                        0,
                    )
                    .unwrap_or_else(|| chrono::Utc::now()),
                    open: kline_data[1].as_str().unwrap_or("0").parse().unwrap_or(0.0),
                    high: kline_data[2].as_str().unwrap_or("0").parse().unwrap_or(0.0),
                    low: kline_data[3].as_str().unwrap_or("0").parse().unwrap_or(0.0),
                    close: kline_data[4].as_str().unwrap_or("0").parse().unwrap_or(0.0),
                    volume: kline_data[5].as_str().unwrap_or("0").parse().unwrap_or(0.0),
                    quote_volume: kline_data[7].as_str().unwrap_or("0").parse().unwrap_or(0.0),
                    trade_count: kline_data[8].as_u64().unwrap_or(0),
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
                .to_exchange_symbol(symbol, "binance", market_type)?;

        let endpoint = match market_type {
            MarketType::Spot => "/api/v3/ticker/24hr",
            MarketType::Futures => "/fapi/v1/ticker/24hr",
        };

        let mut params = HashMap::new();
        params.insert("symbol".to_string(), exchange_symbol);

        #[derive(Deserialize)]
        struct BinanceStats {
            symbol: String,
            #[serde(rename = "priceChange")]
            price_change: String,
            #[serde(rename = "priceChangePercent")]
            price_change_percent: String,
            #[serde(rename = "weightedAvgPrice")]
            weighted_avg_price: String,
            #[serde(rename = "openPrice")]
            open_price: String,
            #[serde(rename = "highPrice")]
            high_price: String,
            #[serde(rename = "lowPrice")]
            low_price: String,
            #[serde(rename = "lastPrice")]
            last_price: String,
            volume: String,
            #[serde(rename = "quoteVolume")]
            quote_volume: String,
            count: u64,
            #[serde(rename = "closeTime")]
            close_time: i64,
        }

        let stats: BinanceStats = self
            .send_public_request(endpoint, Some(params), market_type)
            .await?;

        Ok(Statistics24h {
            symbol: symbol.to_string(),
            open: stats.open_price.parse().unwrap_or(0.0),
            high: stats.high_price.parse().unwrap_or(0.0),
            low: stats.low_price.parse().unwrap_or(0.0),
            close: stats.last_price.parse().unwrap_or(0.0),
            volume: stats.volume.parse().unwrap_or(0.0),
            quote_volume: stats.quote_volume.parse().unwrap_or(0.0),
            change: stats.price_change.parse().unwrap_or(0.0),
            change_percent: stats.price_change_percent.parse().unwrap_or(0.0),
            timestamp: DateTime::from_timestamp(stats.close_time / 1000, 0)
                .unwrap_or_else(|| chrono::Utc::now()),
            // 兼容字段
            price_change: Some(stats.price_change.parse().unwrap_or(0.0)),
            price_change_percent: Some(stats.price_change_percent.parse().unwrap_or(0.0)),
            weighted_avg_price: Some(stats.weighted_avg_price.parse().unwrap_or(0.0)),
            open_price: Some(stats.open_price.parse().unwrap_or(0.0)),
            high_price: Some(stats.high_price.parse().unwrap_or(0.0)),
            low_price: Some(stats.low_price.parse().unwrap_or(0.0)),
            close_price: Some(stats.last_price.parse().unwrap_or(0.0)),
            count: Some(stats.count),
        })
    }

    async fn get_all_24h_statistics(&self, market_type: MarketType) -> Result<Vec<Statistics24h>> {
        let endpoint = match market_type {
            MarketType::Spot => "/api/v3/ticker/24hr",
            MarketType::Futures => "/fapi/v1/ticker/24hr",
        };

        #[derive(Deserialize)]
        struct BinanceStats {
            symbol: String,
            #[serde(rename = "priceChange")]
            price_change: String,
            #[serde(rename = "priceChangePercent")]
            price_change_percent: String,
            #[serde(rename = "weightedAvgPrice")]
            weighted_avg_price: String,
            #[serde(rename = "openPrice")]
            open_price: String,
            #[serde(rename = "highPrice")]
            high_price: String,
            #[serde(rename = "lowPrice")]
            low_price: String,
            #[serde(rename = "lastPrice")]
            last_price: String,
            volume: String,
            #[serde(rename = "quoteVolume")]
            quote_volume: String,
            count: u64,
            #[serde(rename = "closeTime")]
            close_time: i64,
        }

        let all_stats: Vec<BinanceStats> = self
            .send_public_request(endpoint, None, market_type)
            .await?;

        let mut result = Vec::new();
        for stats in all_stats {
            if let Ok(standard_symbol) =
                self.symbol_converter
                    .from_exchange_symbol(&stats.symbol, "binance", market_type)
            {
                result.push(Statistics24h {
                    symbol: standard_symbol,
                    open: stats.open_price.parse().unwrap_or(0.0),
                    high: stats.high_price.parse().unwrap_or(0.0),
                    low: stats.low_price.parse().unwrap_or(0.0),
                    close: stats.last_price.parse().unwrap_or(0.0),
                    volume: stats.volume.parse().unwrap_or(0.0),
                    quote_volume: stats.quote_volume.parse().unwrap_or(0.0),
                    change: stats.price_change.parse().unwrap_or(0.0),
                    change_percent: stats.price_change_percent.parse().unwrap_or(0.0),
                    timestamp: DateTime::from_timestamp(stats.close_time / 1000, 0)
                        .unwrap_or_else(|| chrono::Utc::now()),
                    // 兼容字段
                    price_change: Some(stats.price_change.parse().unwrap_or(0.0)),
                    price_change_percent: Some(stats.price_change_percent.parse().unwrap_or(0.0)),
                    weighted_avg_price: Some(stats.weighted_avg_price.parse().unwrap_or(0.0)),
                    open_price: Some(stats.open_price.parse().unwrap_or(0.0)),
                    high_price: Some(stats.high_price.parse().unwrap_or(0.0)),
                    low_price: Some(stats.low_price.parse().unwrap_or(0.0)),
                    close_price: Some(stats.last_price.parse().unwrap_or(0.0)),
                    count: Some(stats.count),
                });
            }
        }

        Ok(result)
    }

    async fn get_trade_fee(&self, symbol: &str, market_type: MarketType) -> Result<TradeFee> {
        let _exchange_symbol =
            self.symbol_converter
                .to_exchange_symbol(symbol, "binance", market_type)?;

        let endpoint = match market_type {
            MarketType::Spot => "/api/v3/account",
            MarketType::Futures => "/fapi/v2/account",
        };

        #[derive(Deserialize)]
        struct BinanceAccount {
            #[serde(rename = "makerCommission")]
            maker_commission: Option<u32>,
            #[serde(rename = "takerCommission")]
            taker_commission: Option<u32>,
        }

        let account: BinanceAccount = self
            .send_signed_request("GET", endpoint, HashMap::new(), market_type)
            .await?;

        // 币安的手续费以万分之一为单位，需要转换为小数
        let maker_fee = account.maker_commission.unwrap_or(10) as f64 / 10000.0;
        let taker_fee = account.taker_commission.unwrap_or(10) as f64 / 10000.0;

        Ok(TradeFee {
            symbol: symbol.to_string(),
            maker: maker_fee,
            taker: taker_fee,
            percentage: true,
            tier_based: false,
            // 兼容字段
            maker_fee: Some(maker_fee),
            taker_fee: Some(taker_fee),
        })
    }

    async fn get_account_snapshot(&self, market_type: MarketType) -> Result<AccountSnapshot> {
        // 获取余额
        let balances = self.get_balance(market_type).await?;

        // 计算总资产 (简化版本，实际应该根据实时汇率计算)
        let mut total_balance_usdt = 0.0;
        let mut total_balance_btc = 0.0;

        for balance in &balances {
            if balance.currency == "USDT" {
                total_balance_usdt += balance.total;
            } else if balance.currency == "BTC" {
                total_balance_btc += balance.total;
                // 假设BTC价格45000 USDT (实际应该获取实时价格)
                total_balance_usdt += balance.total * 45000.0;
            }
        }

        Ok(AccountSnapshot {
            account_type: match market_type {
                MarketType::Spot => "spot".to_string(),
                MarketType::Futures => "futures".to_string(),
            },
            balances,
            timestamp: chrono::Utc::now(),
            // 兼容字段
            total_balance_btc: Some(total_balance_btc),
            total_balance_usdt: Some(total_balance_usdt),
        })
    }

    async fn get_positions(&self, symbol: Option<&str>) -> Result<Vec<Position>> {
        fn default_zero_string() -> String {
            "0".to_string()
        }
        // 持仓查询只适用于期货市场
        // 注：这里不需要检查base_url，因为这是期货API调用

        let endpoint = "/fapi/v2/positionRisk";
        let mut params = HashMap::new();

        if let Some(symbol) = symbol {
            let exchange_symbol =
                self.symbol_converter
                    .to_exchange_symbol(symbol, "binance", MarketType::Futures)?;
            params.insert("symbol".to_string(), exchange_symbol);
        }

        #[derive(Deserialize)]
        struct BinancePosition {
            symbol: String,
            #[serde(rename = "positionSide")]
            position_side: String,
            #[serde(rename = "positionAmt")]
            position_amt: String,
            #[serde(rename = "entryPrice")]
            entry_price: String,
            #[serde(rename = "markPrice")]
            mark_price: String,
            #[serde(rename = "unRealizedProfit")]
            unrealized_profit: String,
            #[serde(default = "default_zero_string")]
            percentage: String,
            #[serde(rename = "isolatedMargin")]
            isolated_margin: String,
            leverage: String,
            #[serde(rename = "updateTime")]
            update_time: i64,
        }

        let positions: Vec<BinancePosition> = self
            .send_signed_request("GET", endpoint, params, MarketType::Futures)
            .await?;

        let mut result = Vec::new();
        for pos in positions {
            let size = pos.position_amt.parse::<f64>().unwrap_or(0.0);
            if size.abs() > 0.0 {
                // 只返回有持仓的
                if let Ok(standard_symbol) = self.symbol_converter.from_exchange_symbol(
                    &pos.symbol,
                    "binance",
                    MarketType::Futures,
                ) {
                    result.push(Position {
                        symbol: standard_symbol,
                        side: pos.position_side,
                        contracts: size.abs(),
                        contract_size: 1.0, // USDT本位合约
                        size: size.abs(),
                        entry_price: pos.entry_price.parse().unwrap_or(0.0),
                        mark_price: pos.mark_price.parse().unwrap_or(0.0),
                        unrealized_pnl: pos.unrealized_profit.parse().unwrap_or(0.0),
                        percentage: pos.percentage.parse().unwrap_or(0.0),
                        margin: pos.isolated_margin.parse().unwrap_or(0.0),
                        margin_ratio: 0.0, // 币安API不直接提供
                        leverage: pos.leverage.parse().ok(),
                        margin_type: None, // BinancePosition没有这个字段
                        amount: size,
                        timestamp: DateTime::from_timestamp(pos.update_time / 1000, 0)
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
                .to_exchange_symbol(symbol, "binance", MarketType::Futures)?;

        let endpoint = "/fapi/v1/leverage";
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), exchange_symbol);
        params.insert("leverage".to_string(), leverage.to_string());

        #[derive(Deserialize)]
        struct BinanceLeverageResponse {
            leverage: u32,
            #[serde(rename = "maxNotionalValue")]
            max_notional_value: String,
            symbol: String,
        }

        let _response: BinanceLeverageResponse = self
            .send_signed_request("POST", endpoint, params, MarketType::Futures)
            .await?;

        Ok(())
    }

    async fn cancel_all_orders(
        &self,
        symbol: Option<&str>,
        market_type: MarketType,
    ) -> Result<Vec<Order>> {
        let endpoint = match market_type {
            MarketType::Spot => "/api/v3/openOrders",
            MarketType::Futures => "/fapi/v1/allOpenOrders",
        };

        let mut params = HashMap::new();
        if let Some(symbol) = symbol {
            let exchange_symbol =
                self.symbol_converter
                    .to_exchange_symbol(symbol, "binance", market_type)?;
            params.insert("symbol".to_string(), exchange_symbol);
        }

        #[derive(Deserialize)]
        struct BinanceCancelResponse {
            #[serde(rename = "orderId")]
            order_id: i64,
            symbol: String,
            #[serde(rename = "origClientOrderId")]
            orig_client_order_id: String,
        }

        // 先获取原始响应
        let response: serde_json::Value = self
            .send_signed_request("DELETE", endpoint, params, market_type)
            .await?;

        // 根据响应类型处理
        let responses: Vec<BinanceCancelResponse> = if response.is_array() {
            // 正常情况：返回订单数组
            serde_json::from_value(response)?
        } else if response.is_object() {
            // 检查是否是成功响应（code: 200）
            if let Some(code) = response.get("code").and_then(|v| v.as_i64()) {
                if code == 200 {
                    // 成功但没有订单需要取消
                    // 获取实际被取消的订单数量（通过获取当前挂单）
                    match self.get_open_orders(symbol, market_type).await {
                        Ok(orders) => {
                            log::info!("取消所有订单成功，当前挂单数: {}", orders.len());
                            return Ok(Vec::new()); // 返回空数组表示没有订单被取消
                        }
                        Err(_) => {
                            return Ok(Vec::new());
                        }
                    }
                }
            }
            // 检查是否是错误响应
            if response.get("code").is_some() && response.get("msg").is_some() {
                log::debug!("Binance cancel orders response: {}", response);
                Vec::new()
            } else {
                // 尝试作为单个订单处理
                match serde_json::from_value(response) {
                    Ok(order) => vec![order],
                    Err(_) => Vec::new(),
                }
            }
        } else {
            Vec::new()
        };

        let mut result = Vec::new();
        for response in responses {
            if let Ok(standard_symbol) =
                self.symbol_converter
                    .from_exchange_symbol(&response.symbol, "binance", market_type)
            {
                result.push(Order {
                    id: response.order_id.to_string(),
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
            #[serde(rename = "serverTime")]
            server_time: i64,
        }

        let time: ServerTime = self
            .base
            .client
            .get(&format!("{}/api/v3/time", "https://api.binance.com"))
            .send()
            .await?
            .json()
            .await?;

        Ok(DateTime::from_timestamp(time.server_time / 1000, 0)
            .unwrap_or_else(|| chrono::Utc::now()))
    }

    async fn ping(&self) -> Result<()> {
        let response = self
            .base
            .client
            .get(&format!("{}/api/v3/ping", "https://api.binance.com"))
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
        // Binance期货支持批量下单 (最多5个订单)
        if batch_request.market_type == MarketType::Futures && batch_request.orders.len() <= 5 {
            // 构建批量订单参数
            let mut batch_orders = Vec::new();
            for order in &batch_request.orders {
                let exchange_symbol = self.symbol_converter.to_exchange_symbol(
                    &order.symbol,
                    "binance",
                    batch_request.market_type,
                )?;

                let order_type = match order.order_type {
                    OrderType::Limit => "LIMIT",
                    OrderType::Market => "MARKET",
                    OrderType::StopLimit => "STOP",
                    OrderType::StopMarket => "STOP_MARKET",
                    OrderType::TakeProfitLimit => "TAKE_PROFIT",
                    OrderType::TakeProfitMarket => "TAKE_PROFIT_MARKET",
                    OrderType::TrailingStop => "TRAILING_STOP_MARKET",
                    _ => "LIMIT",
                };

                let mut params = serde_json::Map::new();
                params.insert(
                    "symbol".to_string(),
                    serde_json::Value::String(exchange_symbol),
                );
                params.insert(
                    "side".to_string(),
                    serde_json::Value::String(match order.side {
                        OrderSide::Buy => "BUY".to_string(),
                        OrderSide::Sell => "SELL".to_string(),
                    }),
                );
                params.insert(
                    "type".to_string(),
                    serde_json::Value::String(order_type.to_string()),
                );
                params.insert(
                    "quantity".to_string(),
                    serde_json::Value::String(order.amount.to_string()),
                );

                match order.order_type {
                    OrderType::Limit | OrderType::StopLimit | OrderType::TakeProfitLimit => {
                        let mut tif = order
                            .time_in_force
                            .clone()
                            .unwrap_or_else(|| "GTC".to_string());
                        if order.post_only.unwrap_or(false) && tif.to_uppercase() != "GTX" {
                            tif = "GTX".to_string();
                        }
                        params.insert("timeInForce".to_string(), serde_json::Value::String(tif));

                        if let Some(price) = order.price {
                            params.insert(
                                "price".to_string(),
                                serde_json::Value::String(price.to_string()),
                            );
                        }
                    }
                    OrderType::Market
                    | OrderType::StopMarket
                    | OrderType::TakeProfitMarket
                    | OrderType::TrailingStop => {
                        if let Some(tif) = order.time_in_force.clone() {
                            params
                                .insert("timeInForce".to_string(), serde_json::Value::String(tif));
                        }

                        if let Some(price) = order.price {
                            params.insert(
                                "price".to_string(),
                                serde_json::Value::String(price.to_string()),
                            );
                        }
                    }
                    _ => {}
                }

                if let Some(client_id) = &order.client_order_id {
                    params.insert(
                        "newClientOrderId".to_string(),
                        serde_json::Value::String(client_id.clone()),
                    );
                }

                if let Some(reduce_only) = order.reduce_only {
                    params.insert(
                        "reduceOnly".to_string(),
                        serde_json::Value::Bool(reduce_only),
                    );
                }

                batch_orders.push(serde_json::Value::Object(params));
            }

            // 对batchOrders进行URL编码
            let params_str = serde_json::to_string(&batch_orders)?;
            let mut params = HashMap::new();
            params.insert(
                "batchOrders".to_string(),
                urlencoding::encode(&params_str).to_string(),
            );

            // 发送批量订单请求
            let endpoint = "/fapi/v1/batchOrders";
            match self
                .send_signed_request::<serde_json::Value>(
                    "POST",
                    endpoint,
                    params,
                    MarketType::Futures,
                )
                .await
            {
                Ok(response) => {
                    // 解析批量响应
                    let mut successful_orders = Vec::new();
                    let mut failed_orders = Vec::new();

                    if let Some(arr) = response.as_array() {
                        for (i, result) in arr.iter().enumerate() {
                            if let Some(order_id) = result.get("orderId") {
                                // 成功的订单
                                let order = Order {
                                    id: order_id.as_i64().unwrap_or(0).to_string(),
                                    symbol: batch_request.orders[i].symbol.clone(),
                                    side: batch_request.orders[i].side.clone(),
                                    order_type: OrderType::Limit,
                                    amount: batch_request.orders[i].amount,
                                    price: batch_request.orders[i].price,
                                    filled: 0.0,
                                    remaining: batch_request.orders[i].amount,
                                    status: OrderStatus::Open,
                                    market_type: MarketType::Futures,
                                    timestamp: Utc::now(),
                                    last_trade_timestamp: None,
                                    info: result.clone(),
                                };
                                successful_orders.push(order);
                            } else if let Some(msg) = result.get("msg") {
                                // 失败的订单
                                failed_orders.push(BatchOrderError {
                                    order_request: batch_request.orders[i].clone(),
                                    error_message: msg
                                        .as_str()
                                        .unwrap_or("Unknown error")
                                        .to_string(),
                                    error_code: result
                                        .get("code")
                                        .and_then(|c| c.as_i64())
                                        .map(|c| c.to_string()),
                                });
                            }
                        }
                    }

                    Ok(BatchOrderResponse {
                        successful_orders,
                        failed_orders,
                    })
                }
                Err(e) => {
                    // 批量下单失败，回退到逐个下单
                    log::warn!("批量下单失败，回退到逐个下单: {}", e);
                    self.create_orders_one_by_one(batch_request).await
                }
            }
        } else {
            // 现货或超过5个订单，逐个下单
            self.create_orders_one_by_one(batch_request).await
        }
    }

    async fn get_all_spot_symbols(&self) -> Result<Vec<TradingPair>> {
        #[derive(Deserialize)]
        struct BinanceExchangeInfo {
            symbols: Vec<BinanceSymbolInfo>,
        }

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct BinanceSymbolInfo {
            symbol: String,
            base_asset: String,
            quote_asset: String,
            status: String,
            filters: Vec<serde_json::Value>,
        }

        let url = format!("{}/api/v3/exchangeInfo", "https://api.binance.com");

        let response: BinanceExchangeInfo = self.base.client.get(&url).send().await?.json().await?;

        let mut trading_pairs = Vec::new();

        for symbol_info in response.symbols {
            let mut min_order_size = 0.0;
            let mut max_order_size = 0.0;
            let mut tick_size = 0.0;
            let mut step_size = 0.0;
            let mut min_notional = None;

            // 解析过滤器
            for filter in &symbol_info.filters {
                if let Some(filter_type) = filter.get("filterType").and_then(|v| v.as_str()) {
                    match filter_type {
                        "LOT_SIZE" => {
                            if let Some(min_qty) = filter.get("minQty").and_then(|v| v.as_str()) {
                                min_order_size = min_qty.parse().unwrap_or(0.0);
                            }
                            if let Some(max_qty) = filter.get("maxQty").and_then(|v| v.as_str()) {
                                max_order_size = max_qty.parse().unwrap_or(0.0);
                            }
                            if let Some(step) = filter.get("stepSize").and_then(|v| v.as_str()) {
                                step_size = step.parse().unwrap_or(0.0);
                            }
                        }
                        "PRICE_FILTER" => {
                            if let Some(tick) = filter.get("tickSize").and_then(|v| v.as_str()) {
                                tick_size = tick.parse().unwrap_or(0.0);
                            }
                        }
                        "MIN_NOTIONAL" | "NOTIONAL" => {
                            if let Some(min_val) =
                                filter.get("minNotional").and_then(|v| v.as_str())
                            {
                                min_notional = Some(min_val.parse().unwrap_or(0.0));
                            }
                        }
                        _ => {}
                    }
                }
            }

            // 尝试解析符号，如果失败则跳过
            let standardized_symbol = match self.symbol_converter.from_exchange_symbol(
                &symbol_info.symbol,
                "binance",
                MarketType::Spot,
            ) {
                Ok(symbol) => symbol,
                Err(e) => {
                    // 记录但跳过无法解析的符号
                    log::debug!("跳过无法解析的交易对 {}: {}", symbol_info.symbol, e);
                    continue;
                }
            };

            trading_pairs.push(TradingPair {
                symbol: standardized_symbol,
                base_asset: symbol_info.base_asset,
                quote_asset: symbol_info.quote_asset,
                status: symbol_info.status.clone(),
                min_order_size,
                max_order_size,
                tick_size,
                step_size,
                min_notional,
                is_trading: symbol_info.status == "TRADING",
                market_type: MarketType::Spot,
            });
        }

        Ok(trading_pairs)
    }

    async fn get_all_futures_symbols(&self) -> Result<Vec<TradingPair>> {
        #[derive(Deserialize)]
        struct BinanceFuturesExchangeInfo {
            symbols: Vec<BinanceFuturesSymbolInfo>,
        }

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct BinanceFuturesSymbolInfo {
            symbol: String,
            base_asset: String,
            quote_asset: String,
            status: String,
            filters: Vec<serde_json::Value>,
        }

        let url = "https://fapi.binance.com/fapi/v1/exchangeInfo";

        let response: BinanceFuturesExchangeInfo =
            self.base.client.get(url).send().await?.json().await?;

        let mut trading_pairs = Vec::new();

        for symbol_info in response.symbols {
            // 跳过有到期日的期货合约（包含下划线的），只处理永续合约
            if symbol_info.symbol.contains('_') {
                continue;
            }

            let mut min_order_size = 0.0;
            let mut max_order_size = 0.0;
            let mut tick_size = 0.0;
            let mut step_size = 0.0;
            let mut min_notional = None;

            // 解析过滤器
            for filter in &symbol_info.filters {
                if let Some(filter_type) = filter.get("filterType").and_then(|v| v.as_str()) {
                    match filter_type {
                        "LOT_SIZE" => {
                            if let Some(min_qty) = filter.get("minQty").and_then(|v| v.as_str()) {
                                min_order_size = min_qty.parse().unwrap_or(0.0);
                            }
                            if let Some(max_qty) = filter.get("maxQty").and_then(|v| v.as_str()) {
                                max_order_size = max_qty.parse().unwrap_or(0.0);
                            }
                            if let Some(step) = filter.get("stepSize").and_then(|v| v.as_str()) {
                                step_size = step.parse().unwrap_or(0.0);
                            }
                        }
                        "PRICE_FILTER" => {
                            if let Some(tick) = filter.get("tickSize").and_then(|v| v.as_str()) {
                                tick_size = tick.parse().unwrap_or(0.0);
                            }
                        }
                        "MIN_NOTIONAL" => {
                            if let Some(notional) = filter.get("notional").and_then(|v| v.as_str())
                            {
                                min_notional = Some(notional.parse().unwrap_or(0.0));
                            }
                        }
                        _ => {}
                    }
                }
            }

            // 尝试解析符号，如果失败则跳过
            let standardized_symbol = match self.symbol_converter.from_exchange_symbol(
                &symbol_info.symbol,
                "binance",
                MarketType::Futures,
            ) {
                Ok(symbol) => symbol,
                Err(_) => continue, // 跳过无法解析的符号
            };

            trading_pairs.push(TradingPair {
                symbol: standardized_symbol,
                base_asset: symbol_info.base_asset,
                quote_asset: symbol_info.quote_asset,
                status: symbol_info.status.clone(),
                min_order_size,
                max_order_size,
                tick_size,
                step_size,
                min_notional,
                is_trading: symbol_info.status == "TRADING",
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

        // 标准化符号格式：移除斜杠
        let normalized_symbol = symbol.replace("/", "");

        all_symbols
            .into_iter()
            .find(|tp| {
                // 比较时忽略斜杠
                tp.symbol == symbol
                    || tp.symbol == normalized_symbol
                    || tp.symbol.replace("/", "") == normalized_symbol
            })
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
        let client = BinanceWebSocketClient::new(ws_url, market_type);
        Ok(Box::new(client))
    }

    fn get_websocket_url(&self, market_type: MarketType) -> String {
        match market_type {
            MarketType::Spot => "wss://stream.binance.com:9443/ws".to_string(),
            MarketType::Futures => "wss://fstream.binance.com/ws".to_string(),
        }
    }

    async fn create_user_data_stream(&self, market_type: MarketType) -> Result<String> {
        log::info!(
            "🔍 BinanceExchange::create_user_data_stream 被调用了! market_type: {:?}",
            market_type
        );

        let endpoint = match market_type {
            MarketType::Spot => "/api/v3/userDataStream",
            MarketType::Futures => "/fapi/v1/listenKey",
        };

        #[derive(Deserialize)]
        struct ListenKeyResponse {
            #[serde(rename = "listenKey")]
            listen_key: String,
        }

        log::info!("🔍 准备调用 send_signed_request，endpoint: {}", endpoint);
        let response: ListenKeyResponse = self
            .send_signed_request("POST", endpoint, HashMap::new(), market_type)
            .await?;

        // 更新管理器
        let manager = match market_type {
            MarketType::Spot => &self.spot_listen_key_manager,
            MarketType::Futures => &self.futures_listen_key_manager,
        };

        manager
            .lock()
            .unwrap()
            .set_listen_key(response.listen_key.clone());

        Ok(response.listen_key)
    }

    async fn keepalive_user_data_stream(
        &self,
        listen_key: &str,
        market_type: MarketType,
    ) -> Result<()> {
        let endpoint = match market_type {
            MarketType::Spot => "/api/v3/userDataStream",
            MarketType::Futures => "/fapi/v1/listenKey",
        };

        let mut params = HashMap::new();
        params.insert("listenKey".to_string(), listen_key.to_string());

        let _: serde_json::Value = self
            .send_signed_request("PUT", endpoint, params, market_type)
            .await?;

        // 更新管理器的续期时间
        let manager = match market_type {
            MarketType::Spot => &self.spot_listen_key_manager,
            MarketType::Futures => &self.futures_listen_key_manager,
        };

        if let Some(mut mgr) = manager.lock().ok() {
            mgr.last_keepalive = Some(Utc::now());
        }

        Ok(())
    }

    async fn close_user_data_stream(
        &self,
        listen_key: &str,
        market_type: MarketType,
    ) -> Result<()> {
        let endpoint = match market_type {
            MarketType::Spot => "/api/v3/userDataStream",
            MarketType::Futures => "/fapi/v1/listenKey",
        };

        let mut params = HashMap::new();
        params.insert("listenKey".to_string(), listen_key.to_string());

        let _: serde_json::Value = self
            .send_signed_request("DELETE", endpoint, params, market_type)
            .await?;

        // 清理管理器
        let manager = match market_type {
            MarketType::Spot => &self.spot_listen_key_manager,
            MarketType::Futures => &self.futures_listen_key_manager,
        };

        if let Some(mut mgr) = manager.lock().ok() {
            mgr.reset();
        }

        Ok(())
    }
}

impl BinanceExchange {
    /// 创建带有ListenKey自动续期功能的WebSocket客户端
    pub async fn create_websocket_client_with_auto_renewal(
        &self,
        market_type: MarketType,
    ) -> Result<BinanceWebSocketClientWithRenewal> {
        let ws_url = self.get_websocket_url(market_type);
        let client = BinanceWebSocketClientWithRenewal::new(ws_url, market_type, self.clone());
        Ok(client)
    }
}

/// Binance WebSocket客户端实现
pub struct BinanceWebSocketClient {
    base: Box<dyn WebSocketClient>,
    market_type: MarketType,
}

impl BinanceWebSocketClient {
    pub fn new(url: String, market_type: MarketType) -> Self {
        Self {
            base: Box::new(crate::core::websocket::BaseWebSocketClient::new(
                url,
                "binance".to_string(),
            )),
            market_type,
        }
    }

    /// 解析Binance WebSocket消息
    pub fn parse_binance_message(&self, text: &str) -> Result<WsMessage> {
        let json: serde_json::Value = serde_json::from_str(text)?;

        // 检查是否是事件消息
        if let Some(event_type) = json.get("e").and_then(|v| v.as_str()) {
            match event_type {
                "24hrTicker" => self.parse_ticker_message(&json),
                "depthUpdate" => self.parse_orderbook_message(&json),
                "trade" => self.parse_trade_message(&json),
                "kline" => self.parse_kline_message(&json),
                "executionReport" => self.parse_execution_report(&json), // 现货用户订单执行报告
                "ORDER_TRADE_UPDATE" => self.parse_order_trade_update(&json), // 期货用户订单更新
                "outboundAccountPosition" => self.parse_account_update(&json), // 账户余额更新
                _ => {
                    log::debug!("Received event type: {}", event_type);
                    Ok(WsMessage::Error(format!(
                        "Unknown event type: {}",
                        event_type
                    )))
                }
            }
        } else {
            // 可能是订阅确认或错误消息
            Ok(WsMessage::Error("Non-event message received".to_string()))
        }
    }

    /// 解析executionReport事件（订单执行报告）
    fn parse_execution_report(&self, json: &serde_json::Value) -> Result<WsMessage> {
        #[derive(Deserialize)]
        struct BinanceExecutionReport {
            #[serde(rename = "s")]
            symbol: String,
            #[serde(rename = "i")]
            order_id: i64,
            #[serde(rename = "c")]
            client_order_id: String,
            #[serde(rename = "S")]
            side: String,
            #[serde(rename = "o")]
            order_type: String,
            #[serde(rename = "X")]
            status: String,
            #[serde(rename = "p")]
            price: String,
            #[serde(rename = "q")]
            amount: String,
            #[serde(rename = "z")]
            executed_amount: String,
            #[serde(rename = "Z")]
            executed_quote: String,
            #[serde(rename = "n")]
            commission: String,
            #[serde(rename = "N")]
            commission_asset: Option<String>,
            #[serde(rename = "T")]
            trade_time: i64,
            #[serde(rename = "m")]
            is_maker: bool,
        }

        let report: BinanceExecutionReport = serde_json::from_value(json.clone())?;

        // 记录详细的订单执行信息
        log::info!("📊 现货订单执行报告详情:");
        log::info!("  - Symbol: {}", report.symbol);
        log::info!("  - OrderId: {}", report.order_id);
        log::info!("  - Status: {}", report.status);
        log::info!("  - Side: {}", report.side);
        log::info!("  - Type: {}", report.order_type);
        log::info!("  - Price: {}", report.price);
        log::info!("  - Amount: {}", report.amount);
        log::info!("  - ExecutedAmount: {}", report.executed_amount);
        log::info!("  - ExecutedQuote: {}", report.executed_quote);
        log::info!(
            "  - Commission: {} {}",
            report.commission,
            report
                .commission_asset
                .as_ref()
                .unwrap_or(&"N/A".to_string())
        );
        log::info!("  - IsMaker: {}", report.is_maker);

        // 转换订单状态
        let status = match report.status.as_str() {
            "NEW" => OrderStatus::Open,
            "FILLED" => OrderStatus::Closed,
            "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
            "CANCELED" => OrderStatus::Canceled,
            "EXPIRED" => OrderStatus::Expired,
            "REJECTED" => OrderStatus::Rejected,
            _ => OrderStatus::Pending,
        };

        // 转换订单方向
        let side = match report.side.as_str() {
            "BUY" => OrderSide::Buy,
            "SELL" => OrderSide::Sell,
            _ => {
                return Err(ExchangeError::ParseError(format!(
                    "Unknown side: {}",
                    report.side
                )))
            }
        };

        // 转换订单类型
        let order_type = match report.order_type.as_str() {
            "MARKET" => OrderType::Market,
            "LIMIT" => OrderType::Limit,
            "STOP_MARKET" => OrderType::StopMarket,
            "STOP_LIMIT" => OrderType::StopLimit,
            _ => OrderType::Limit,
        };

        let executed_amount = report.executed_amount.parse::<f64>().unwrap_or(0.0);
        let executed_price = if executed_amount > 0.0 {
            report.executed_quote.parse::<f64>().unwrap_or(0.0) / executed_amount
        } else {
            report.price.parse::<f64>().unwrap_or(0.0)
        };

        Ok(WsMessage::ExecutionReport(ExecutionReport {
            symbol: report.symbol,
            order_id: report.order_id.to_string(),
            client_order_id: Some(report.client_order_id),
            side,
            order_type,
            status,
            price: report.price.parse().unwrap_or(0.0),
            amount: report.amount.parse().unwrap_or(0.0),
            executed_amount,
            executed_price,
            commission: report.commission.parse().unwrap_or(0.0),
            commission_asset: report
                .commission_asset
                .unwrap_or_else(|| "USDT".to_string()),
            timestamp: DateTime::from_timestamp(report.trade_time / 1000, 0)
                .unwrap_or_else(|| Utc::now()),
            is_maker: report.is_maker,
        }))
    }

    /// 解析ORDER_TRADE_UPDATE事件（期货订单更新）
    fn parse_order_trade_update(&self, json: &serde_json::Value) -> Result<WsMessage> {
        log::debug!("🔍 开始解析ORDER_TRADE_UPDATE");

        // 获取订单对象
        let order_obj = json.get("o").ok_or_else(|| {
            ExchangeError::ParseError("Missing 'o' field in ORDER_TRADE_UPDATE".to_string())
        })?;

        #[derive(Deserialize)]
        struct FuturesOrderUpdate {
            #[serde(rename = "s")]
            symbol: String,
            #[serde(rename = "i")]
            order_id: i64,
            #[serde(rename = "c")]
            client_order_id: String,
            #[serde(rename = "S")]
            side: String,
            #[serde(rename = "o")]
            order_type: String,
            #[serde(rename = "X")]
            status: String,
            #[serde(rename = "p")]
            price: String,
            #[serde(rename = "q")]
            amount: String,
            #[serde(rename = "z")]
            executed_qty: String,
            #[serde(rename = "ap")]
            avg_price: String,
            #[serde(rename = "n")]
            commission: String,
            #[serde(rename = "N")]
            commission_asset: Option<String>,
            #[serde(rename = "T")]
            trade_time: i64,
            #[serde(rename = "m")]
            is_maker: bool,
        }

        let order: FuturesOrderUpdate = serde_json::from_value(order_obj.clone())?;

        // 只记录debug级别的详细信息
        log::debug!(
            "订单更新: {} {} {} @ {}",
            order.symbol,
            order.side,
            order.status,
            order.price
        );

        // 只有成交时才记录INFO级别日志
        if order.status == "FILLED" || order.status == "PARTIALLY_FILLED" {
            let now = chrono::Local::now();
            let side_str = if order.side == "BUY" { "BUY" } else { "SELL" };
            let trade_value = order.executed_qty.parse::<f64>().unwrap_or(0.0)
                * order.avg_price.parse::<f64>().unwrap_or(0.0);
            log::info!(
                "[成交] {} {} {} @ {} | {:.2} USDT",
                now.format("%H:%M:%S"),
                order.symbol,
                side_str,
                order.avg_price,
                trade_value
            );
        }

        // 转换订单状态
        let status = match order.status.as_str() {
            "NEW" => OrderStatus::Open,
            "FILLED" => OrderStatus::Closed,
            "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
            "CANCELED" => OrderStatus::Canceled,
            "EXPIRED" => OrderStatus::Expired,
            "REJECTED" => OrderStatus::Rejected,
            _ => OrderStatus::Pending,
        };

        // 转换订单方向
        let side = match order.side.as_str() {
            "BUY" => OrderSide::Buy,
            "SELL" => OrderSide::Sell,
            _ => {
                return Err(ExchangeError::ParseError(format!(
                    "Unknown side: {}",
                    order.side
                )))
            }
        };

        // 转换订单类型
        let order_type = match order.order_type.as_str() {
            "MARKET" => OrderType::Market,
            "LIMIT" => OrderType::Limit,
            "STOP_MARKET" => OrderType::StopMarket,
            "STOP" | "STOP_LIMIT" => OrderType::StopLimit,
            _ => OrderType::Limit,
        };

        let executed_amount = order.executed_qty.parse::<f64>().unwrap_or(0.0);
        let executed_price = order.avg_price.parse::<f64>().unwrap_or(0.0);

        let report = ExecutionReport {
            symbol: order.symbol.clone(),
            order_id: order.order_id.to_string(),
            client_order_id: Some(order.client_order_id.clone()),
            side,
            order_type,
            status,
            price: order.price.parse().unwrap_or(0.0),
            amount: order.amount.parse().unwrap_or(0.0),
            executed_amount,
            executed_price,
            commission: order.commission.parse().unwrap_or(0.0),
            commission_asset: order
                .commission_asset
                .clone()
                .unwrap_or_else(|| "USDC".to_string()),
            timestamp: DateTime::from_timestamp(order.trade_time / 1000, 0)
                .unwrap_or_else(|| Utc::now()),
            is_maker: order.is_maker,
        };

        log::debug!(
            "订单解析成功: ID={}, 状态={:?}",
            report.order_id,
            report.status
        );

        Ok(WsMessage::ExecutionReport(report))
    }

    /// 解析账户余额更新
    fn parse_account_update(&self, json: &serde_json::Value) -> Result<WsMessage> {
        // 简单返回错误，可以后续实现
        Ok(WsMessage::Error(
            "Account update not implemented".to_string(),
        ))
    }

    fn parse_ticker_message(&self, json: &serde_json::Value) -> Result<WsMessage> {
        #[derive(Deserialize)]
        struct BinanceWsTicker {
            #[serde(rename = "s")]
            symbol: String,
            #[serde(rename = "c")]
            close: String,
            #[serde(rename = "h")]
            high: String,
            #[serde(rename = "l")]
            low: String,
            #[serde(rename = "b")]
            bid: String,
            #[serde(rename = "a")]
            ask: String,
            #[serde(rename = "v")]
            volume: String,
            #[serde(rename = "E")]
            event_time: i64,
        }

        let ticker: BinanceWsTicker = serde_json::from_value(json.clone())?;

        Ok(WsMessage::Ticker(Ticker {
            symbol: ticker.symbol,
            high: ticker.high.parse().unwrap_or(0.0),
            low: ticker.low.parse().unwrap_or(0.0),
            bid: ticker.bid.parse().unwrap_or(0.0),
            ask: ticker.ask.parse().unwrap_or(0.0),
            last: ticker.close.parse().unwrap_or(0.0),
            volume: ticker.volume.parse().unwrap_or(0.0),
            timestamp: DateTime::from_timestamp(ticker.event_time / 1000, 0)
                .unwrap_or_else(|| Utc::now()),
        }))
    }

    fn parse_orderbook_message(&self, json: &serde_json::Value) -> Result<WsMessage> {
        #[derive(Deserialize)]
        struct BinanceWsOrderBook {
            #[serde(rename = "s")]
            symbol: String,
            #[serde(rename = "b")]
            bids: Vec<Vec<String>>,
            #[serde(rename = "a")]
            asks: Vec<Vec<String>>,
            #[serde(rename = "E")]
            event_time: i64,
        }

        let orderbook: BinanceWsOrderBook = serde_json::from_value(json.clone())?;

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
            symbol: orderbook.symbol,
            bids,
            asks,
            timestamp: DateTime::from_timestamp(orderbook.event_time / 1000, 0)
                .unwrap_or_else(|| Utc::now()),
        }))
    }

    fn parse_trade_message(&self, json: &serde_json::Value) -> Result<WsMessage> {
        #[derive(Deserialize)]
        struct BinanceWsTrade {
            #[serde(rename = "s")]
            symbol: String,
            #[serde(rename = "t")]
            trade_id: i64,
            #[serde(rename = "p")]
            price: String,
            #[serde(rename = "q")]
            quantity: String,
            #[serde(rename = "T")]
            trade_time: i64,
            #[serde(rename = "m")]
            is_buyer_maker: bool,
        }

        let trade: BinanceWsTrade = serde_json::from_value(json.clone())?;

        // 记录收到的成交
        log::info!(
            "Binance WebSocket收到成交: {} - 价格: {}, 数量: {}",
            trade.symbol,
            trade.price,
            trade.quantity
        );

        Ok(WsMessage::Trade(Trade {
            id: trade.trade_id.to_string(),
            symbol: trade.symbol,
            side: if trade.is_buyer_maker {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            },
            amount: trade.quantity.parse().unwrap_or(0.0),
            price: trade.price.parse().unwrap_or(0.0),
            timestamp: DateTime::from_timestamp(trade.trade_time / 1000, 0)
                .unwrap_or_else(|| Utc::now()),
            order_id: None,
            fee: None,
        }))
    }

    fn parse_kline_message(&self, json: &serde_json::Value) -> Result<WsMessage> {
        #[derive(Deserialize)]
        struct BinanceWsKline {
            #[serde(rename = "s")]
            symbol: String,
            #[serde(rename = "k")]
            kline: BinanceKlineData,
        }

        #[derive(Deserialize)]
        struct BinanceKlineData {
            #[serde(rename = "t")]
            open_time: i64,
            #[serde(rename = "T")]
            close_time: i64,
            #[serde(rename = "o")]
            open: String,
            #[serde(rename = "h")]
            high: String,
            #[serde(rename = "l")]
            low: String,
            #[serde(rename = "c")]
            close: String,
            #[serde(rename = "v")]
            volume: String,
            #[serde(rename = "q")]
            quote_volume: String,
            #[serde(rename = "n")]
            trade_count: u64,
            #[serde(rename = "i")]
            interval: String,
        }

        let kline_msg: BinanceWsKline = serde_json::from_value(json.clone())?;
        let k = &kline_msg.kline;

        Ok(WsMessage::Kline(Kline {
            symbol: kline_msg.symbol,
            interval: k.interval.clone(),
            open_time: DateTime::from_timestamp(k.open_time / 1000, 0)
                .unwrap_or_else(|| Utc::now()),
            close_time: DateTime::from_timestamp(k.close_time / 1000, 0)
                .unwrap_or_else(|| Utc::now()),
            open: k.open.parse().unwrap_or(0.0),
            high: k.high.parse().unwrap_or(0.0),
            low: k.low.parse().unwrap_or(0.0),
            close: k.close.parse().unwrap_or(0.0),
            volume: k.volume.parse().unwrap_or(0.0),
            quote_volume: k.quote_volume.parse().unwrap_or(0.0),
            trade_count: k.trade_count,
        }))
    }
}

#[async_trait]
impl WebSocketClient for BinanceWebSocketClient {
    async fn connect(&mut self) -> Result<()> {
        self.base.connect().await
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.base.disconnect().await
    }

    async fn receive(&mut self) -> Result<Option<String>> {
        // 调用基础实现的receive方法
        self.base.receive().await
    }

    async fn send(&mut self, message: String) -> Result<()> {
        self.base.send(message).await
    }

    async fn ping(&self) -> Result<()> {
        self.base.ping().await
    }

    fn get_state(&self) -> ConnectionState {
        self.base.get_state()
    }
}

/// Binance消息处理器包装器
pub struct BinanceMessageHandler {
    inner_handler: Box<dyn MessageHandler>,
    market_type: MarketType,
    exchange: Option<BinanceExchange>,
}

impl BinanceMessageHandler {
    pub fn new(handler: Box<dyn MessageHandler>, market_type: MarketType) -> Self {
        Self {
            inner_handler: handler,
            market_type,
            exchange: None,
        }
    }

    /// 创建带有Exchange引用的处理器，用于处理ListenKey过期事件
    pub fn with_exchange(
        handler: Box<dyn MessageHandler>,
        market_type: MarketType,
        exchange: BinanceExchange,
    ) -> Self {
        Self {
            inner_handler: handler,
            market_type,
            exchange: Some(exchange),
        }
    }

    /// 解析executionReport事件（现货订单执行报告）
    fn parse_execution_report(&self, json: &serde_json::Value) -> Result<WsMessage> {
        // 使用BinanceWebSocketClient的同样逻辑
        BinanceWebSocketClient::new("".to_string(), MarketType::Spot).parse_execution_report(json)
    }

    /// 解析ORDER_TRADE_UPDATE事件（期货订单更新）
    fn parse_order_trade_update(&self, json: &serde_json::Value) -> Result<WsMessage> {
        // 使用BinanceWebSocketClient的同样逻辑
        BinanceWebSocketClient::new("".to_string(), MarketType::Futures)
            .parse_order_trade_update(json)
    }

    /// 解析账户更新
    fn parse_account_update(&self, json: &serde_json::Value) -> Result<WsMessage> {
        // 暂时返回错误消息
        Ok(WsMessage::Error(
            "Account update not implemented".to_string(),
        ))
    }

    /// 解析TRADE_LITE事件（期货轻量级成交事件）
    fn parse_trade_lite(&self, json: &serde_json::Value) -> Result<WsMessage> {
        // TRADE_LITE 是期货成交的轻量级事件，包含基本成交信息
        // 从日志看：订单成交: 9 Sell @ 0.8100
        // 说明字段有: s(symbol), q(quantity), p(price), S(side)

        let symbol = json
            .get("s")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let quantity = json
            .get("q")
            .and_then(|v| v.as_str())
            .unwrap_or("0")
            .parse::<f64>()
            .unwrap_or(0.0);

        let price = json
            .get("p")
            .and_then(|v| v.as_str())
            .unwrap_or("0")
            .parse::<f64>()
            .unwrap_or(0.0);

        let side = json.get("S").and_then(|v| v.as_str()).unwrap_or("BUY");

        let trade_time = json
            .get("T")
            .and_then(|v| v.as_i64())
            .unwrap_or_else(|| Utc::now().timestamp_millis());

        // 转换为Trade消息
        Ok(WsMessage::Trade(Trade {
            id: format!("trade_{}", trade_time),
            symbol,
            side: match side {
                "SELL" => OrderSide::Sell,
                _ => OrderSide::Buy,
            },
            amount: quantity,
            price,
            timestamp: DateTime::from_timestamp(trade_time / 1000, 0).unwrap_or_else(|| Utc::now()),
            order_id: json
                .get("c")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            fee: None,
        }))
    }

    fn parse_message(&self, text: &str) -> Result<WsMessage> {
        // 记录所有接收到的原始消息（用于调试订单成交问题）
        log::debug!("📨 Binance WebSocket原始消息: {}", text);

        let json: serde_json::Value = serde_json::from_str(text)?;

        // 检查是否是事件消息
        if let Some(event_type) = json.get("e").and_then(|v| v.as_str()) {
            log::info!("🔔 处理WebSocket事件类型: {}", event_type);

            let result = match event_type {
                "24hrTicker" => self.parse_ticker(&json),
                "depthUpdate" => self.parse_orderbook(&json),
                "trade" => self.parse_trade(&json),
                "kline" => self.parse_kline(&json),
                "executionReport" => {
                    log::info!("📋 收到现货订单执行报告事件");
                    self.parse_execution_report(&json)
                }
                "ORDER_TRADE_UPDATE" => {
                    log::info!("📋 收到期货订单更新事件");
                    self.parse_order_trade_update(&json)
                }
                "TRADE_LITE" => {
                    log::info!("💱 收到期货轻量级成交事件");
                    self.parse_trade_lite(&json)
                }
                "ACCOUNT_UPDATE" => self.parse_account_update(&json), // 账户更新
                "listenKeyExpired" => {
                    log::error!("❌ ListenKey已过期，需要重新创建");
                    // 返回特殊的ListenKey过期消息，让上层处理器处理
                    Ok(WsMessage::Error("ListenKeyExpired".to_string()))
                }
                _ => {
                    log::warn!(
                        "⚠️ 未知的WebSocket事件类型: {}, 完整消息: {}",
                        event_type,
                        json
                    );
                    Ok(WsMessage::Error(format!(
                        "Unknown event type: {}",
                        event_type
                    )))
                }
            };

            // 记录解析结果
            match &result {
                Ok(msg) => log::debug!("✅ 成功解析WebSocket消息: {:?}", msg),
                Err(e) => log::error!("❌ 解析WebSocket消息失败: {}", e),
            }

            result
        } else {
            // 非事件消息，可能是订阅确认等
            log::debug!("非事件消息: {}", text);
            Ok(WsMessage::Error("Non-event message".to_string()))
        }
    }

    fn parse_ticker(&self, json: &serde_json::Value) -> Result<WsMessage> {
        // 重复ticker解析逻辑
        #[derive(Deserialize)]
        struct BinanceWsTicker {
            #[serde(rename = "s")]
            symbol: String,
            #[serde(rename = "c")]
            close: String,
            #[serde(rename = "h")]
            high: String,
            #[serde(rename = "l")]
            low: String,
            #[serde(rename = "b")]
            bid: String,
            #[serde(rename = "a")]
            ask: String,
            #[serde(rename = "v")]
            volume: String,
            #[serde(rename = "E")]
            event_time: i64,
        }

        let ticker: BinanceWsTicker = serde_json::from_value(json.clone())?;

        Ok(WsMessage::Ticker(Ticker {
            symbol: ticker.symbol,
            high: ticker.high.parse().unwrap_or(0.0),
            low: ticker.low.parse().unwrap_or(0.0),
            bid: ticker.bid.parse().unwrap_or(0.0),
            ask: ticker.ask.parse().unwrap_or(0.0),
            last: ticker.close.parse().unwrap_or(0.0),
            volume: ticker.volume.parse().unwrap_or(0.0),
            timestamp: DateTime::from_timestamp(ticker.event_time / 1000, 0)
                .unwrap_or_else(|| Utc::now()),
        }))
    }

    fn parse_orderbook(&self, json: &serde_json::Value) -> Result<WsMessage> {
        #[derive(Deserialize)]
        struct BinanceWsOrderBook {
            #[serde(rename = "s")]
            symbol: String,
            #[serde(rename = "b")]
            bids: Vec<Vec<String>>,
            #[serde(rename = "a")]
            asks: Vec<Vec<String>>,
            #[serde(rename = "E")]
            event_time: i64,
        }

        let orderbook: BinanceWsOrderBook = serde_json::from_value(json.clone())?;

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
            symbol: orderbook.symbol,
            bids,
            asks,
            timestamp: DateTime::from_timestamp(orderbook.event_time / 1000, 0)
                .unwrap_or_else(|| Utc::now()),
        }))
    }

    fn parse_trade(&self, json: &serde_json::Value) -> Result<WsMessage> {
        #[derive(Deserialize)]
        struct BinanceWsTrade {
            #[serde(rename = "s")]
            symbol: String,
            #[serde(rename = "t")]
            trade_id: i64,
            #[serde(rename = "p")]
            price: String,
            #[serde(rename = "q")]
            quantity: String,
            #[serde(rename = "T")]
            trade_time: i64,
            #[serde(rename = "m")]
            is_buyer_maker: bool,
        }

        let trade: BinanceWsTrade = serde_json::from_value(json.clone())?;

        Ok(WsMessage::Trade(Trade {
            id: trade.trade_id.to_string(),
            symbol: trade.symbol,
            side: if trade.is_buyer_maker {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            },
            amount: trade.quantity.parse().unwrap_or(0.0),
            price: trade.price.parse().unwrap_or(0.0),
            timestamp: DateTime::from_timestamp(trade.trade_time / 1000, 0)
                .unwrap_or_else(|| Utc::now()),
            order_id: None,
            fee: None,
        }))
    }

    fn parse_kline(&self, json: &serde_json::Value) -> Result<WsMessage> {
        #[derive(Deserialize)]
        struct BinanceWsKline {
            #[serde(rename = "s")]
            symbol: String,
            #[serde(rename = "k")]
            kline: BinanceKlineData,
        }

        #[derive(Deserialize)]
        struct BinanceKlineData {
            #[serde(rename = "t")]
            open_time: i64,
            #[serde(rename = "T")]
            close_time: i64,
            #[serde(rename = "o")]
            open: String,
            #[serde(rename = "h")]
            high: String,
            #[serde(rename = "l")]
            low: String,
            #[serde(rename = "c")]
            close: String,
            #[serde(rename = "v")]
            volume: String,
            #[serde(rename = "q")]
            quote_volume: String,
            #[serde(rename = "n")]
            trade_count: u64,
            #[serde(rename = "i")]
            interval: String,
        }

        let kline_msg: BinanceWsKline = serde_json::from_value(json.clone())?;
        let k = &kline_msg.kline;

        Ok(WsMessage::Kline(Kline {
            symbol: kline_msg.symbol,
            interval: k.interval.clone(),
            open_time: DateTime::from_timestamp(k.open_time / 1000, 0)
                .unwrap_or_else(|| Utc::now()),
            close_time: DateTime::from_timestamp(k.close_time / 1000, 0)
                .unwrap_or_else(|| Utc::now()),
            open: k.open.parse().unwrap_or(0.0),
            high: k.high.parse().unwrap_or(0.0),
            low: k.low.parse().unwrap_or(0.0),
            close: k.close.parse().unwrap_or(0.0),
            volume: k.volume.parse().unwrap_or(0.0),
            quote_volume: k.quote_volume.parse().unwrap_or(0.0),
            trade_count: k.trade_count,
        }))
    }
}

#[async_trait]
impl MessageHandler for BinanceMessageHandler {
    async fn handle_message(&self, message: WsMessage) -> Result<()> {
        // 检查是否是ListenKey过期错误
        if let WsMessage::Error(ref error_msg) = message {
            if error_msg == "ListenKeyExpired" {
                log::error!("🔄 检测到ListenKey过期事件");

                // 如果有Exchange引用，则自动处理ListenKey过期
                if let Some(ref exchange) = self.exchange {
                    match exchange.handle_listen_key_expired(self.market_type).await {
                        Ok(new_listen_key) => {
                            log::info!("✅ 成功处理ListenKey过期，新ListenKey: {}", new_listen_key);
                            // 通知上层处理器ListenKey已重新创建
                            let renewal_message =
                                WsMessage::Error(format!("ListenKeyRenewed:{}", new_listen_key));
                            return self.inner_handler.handle_message(renewal_message).await;
                        }
                        Err(e) => {
                            log::error!("❌ 处理ListenKey过期失败: {}", e);
                            // 继续传递错误消息
                        }
                    }
                }
            }
        }

        // 转发消息到内部处理器
        self.inner_handler.handle_message(message).await
    }

    // 移除不存在于trait中的方法

    async fn handle_error(&self, error: ExchangeError) -> Result<()> {
        self.inner_handler.handle_error(error).await
    }
}

/// 带有ListenKey自动续期功能的Binance WebSocket客户端
pub struct BinanceWebSocketClientWithRenewal {
    base_client: BinanceWebSocketClient,
    exchange: BinanceExchange,
    reconnect_attempts: u32,
    max_reconnect_attempts: u32,
    reconnect_delay: Duration,
}

impl BinanceWebSocketClientWithRenewal {
    pub fn new(url: String, market_type: MarketType, exchange: BinanceExchange) -> Self {
        Self {
            base_client: BinanceWebSocketClient::new(url, market_type),
            exchange,
            reconnect_attempts: 0,
            max_reconnect_attempts: 5,
            reconnect_delay: Duration::from_secs(5),
        }
    }

    /// 设置重连参数
    pub fn with_reconnect_config(mut self, max_attempts: u32, delay: Duration) -> Self {
        self.max_reconnect_attempts = max_attempts;
        self.reconnect_delay = delay;
        self
    }

    /// 执行自动重连
    async fn try_reconnect(&mut self) -> Result<()> {
        log::info!(
            "🔄 开始WebSocket重连尝试 {} / {}",
            self.reconnect_attempts + 1,
            self.max_reconnect_attempts
        );

        for attempt in 1..=self.max_reconnect_attempts {
            self.reconnect_attempts = attempt;

            // 等待重连延迟
            if attempt > 1 {
                let delay = self.reconnect_delay * attempt; // 递增延迟
                log::info!("⏳ 等待 {:?} 后进行第 {} 次重连尝试", delay, attempt);
                tokio::time::sleep(delay).await;
            }

            // 尝试重连
            match self.base_client.connect().await {
                Ok(()) => {
                    log::info!("✅ WebSocket重连成功（第 {} 次尝试）", attempt);
                    self.reconnect_attempts = 0;
                    return Ok(());
                }
                Err(e) => {
                    log::warn!("⚠️ WebSocket重连失败（第 {} 次尝试）: {}", attempt, e);
                    if attempt == self.max_reconnect_attempts {
                        log::error!("❌ WebSocket重连彻底失败，已达到最大重试次数");
                        return Err(e);
                    }
                }
            }
        }

        Err(ExchangeError::Other("WebSocket重连失败".to_string()))
    }
}

#[async_trait]
impl WebSocketClient for BinanceWebSocketClientWithRenewal {
    async fn connect(&mut self) -> Result<()> {
        match self.base_client.connect().await {
            Ok(()) => {
                self.reconnect_attempts = 0;
                Ok(())
            }
            Err(e) => {
                log::warn!("❌ 初始WebSocket连接失败: {}", e);
                self.try_reconnect().await
            }
        }
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.base_client.disconnect().await
    }

    async fn receive(&mut self) -> Result<Option<String>> {
        self.base_client.receive().await
    }

    async fn send(&mut self, message: String) -> Result<()> {
        self.base_client.send(message).await
    }

    async fn ping(&self) -> Result<()> {
        self.base_client.ping().await
    }

    fn get_state(&self) -> ConnectionState {
        self.base_client.get_state()
    }
}

// 移除的方法移到扩展功能中
impl BinanceWebSocketClientWithRenewal {
    async fn start_with_renewal(&mut self, handler: Box<dyn MessageHandler>) -> Result<()> {
        // 创建带有自动重连功能的消息处理器
        let reconnect_handler = ReconnectMessageHandler::new(
            handler,
            self.base_client.market_type,
            self.exchange.clone(),
        );

        loop {
            log::info!("🔗 开始WebSocket消息接收循环");

            // 创建带有Exchange引用的消息处理器
            let binance_handler = BinanceMessageHandler::with_exchange(
                Box::new(reconnect_handler.clone()),
                self.base_client.market_type,
                self.exchange.clone(),
            );

            // TODO: 需要重新设计消息接收机制
            match self.base_client.receive().await {
                Ok(Some(msg)) => {
                    log::info!("📡 收到WebSocket消息: {}", msg);
                    // 处理消息
                }
                Ok(None) => {
                    log::info!("📡 WebSocket消息接收正常结束");
                    break;
                }
                Err(e) => {
                    log::error!("❌ WebSocket连接错误: {}", e);

                    // 尝试重连
                    match self.try_reconnect().await {
                        Ok(()) => {
                            log::info!("✅ WebSocket重连成功，继续消息接收");
                            continue;
                        }
                        Err(reconnect_error) => {
                            log::error!("❌ WebSocket重连失败: {}", reconnect_error);
                            return Err(reconnect_error);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn get_state(&self) -> ConnectionState {
        self.base_client.get_state()
    }

    async fn send_ping(&mut self) -> Result<()> {
        self.base_client.ping().await
    }
}

/// 支持重连的消息处理器
#[derive(Clone)]
pub struct ReconnectMessageHandler {
    inner_handler: std::sync::Arc<tokio::sync::Mutex<Box<dyn MessageHandler>>>,
    market_type: MarketType,
    exchange: BinanceExchange,
}

impl ReconnectMessageHandler {
    pub fn new(
        handler: Box<dyn MessageHandler>,
        market_type: MarketType,
        exchange: BinanceExchange,
    ) -> Self {
        Self {
            inner_handler: std::sync::Arc::new(tokio::sync::Mutex::new(handler)),
            market_type,
            exchange,
        }
    }
}

#[async_trait]
impl MessageHandler for ReconnectMessageHandler {
    async fn handle_message(&self, message: WsMessage) -> Result<()> {
        let handler = self.inner_handler.lock().await;
        handler.handle_message(message).await
    }

    // handle_raw_message和handle_state_change方法已从 trait 中移除

    async fn handle_error(&self, error: ExchangeError) -> Result<()> {
        log::error!("❌ WebSocket错误: {}", error);
        let handler = self.inner_handler.lock().await;
        handler.handle_error(error).await
    }
}
