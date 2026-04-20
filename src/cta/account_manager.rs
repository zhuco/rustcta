use crate::core::{
    config::{ApiKeys, Config},
    error::ExchangeError,
    types::*,
};
use crate::exchanges::*;
use crate::Exchange;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// 账户配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountConfig {
    pub id: String,
    pub exchange: String,
    pub enabled: bool,
    pub api_key_env: String,
    pub max_positions: u32,
    pub max_orders_per_symbol: u32,
}

/// 账户信息
#[derive(Clone)]
pub struct AccountInfo {
    pub id: String,
    pub exchange_name: String,
    pub exchange: Arc<Box<dyn Exchange>>,
    pub config: AccountConfig,
    pub balance: Arc<RwLock<Vec<Balance>>>,
    pub positions: Arc<RwLock<Vec<Position>>>,
    pub open_orders: Arc<RwLock<HashMap<String, Vec<Order>>>>,
}

impl AccountInfo {}

/// 账户管理器
pub struct AccountManager {
    accounts: HashMap<String, Arc<AccountInfo>>,
    exchange_config: Config,
    offline_mode: bool,
}

impl AccountManager {
    /// 创建账户管理器
    pub fn new(exchange_config: Config) -> Self {
        let offline_mode = std::env::var("RUSTCTA_OFFLINE")
            .map(|val| {
                let normalized = val.trim().to_ascii_lowercase();
                matches!(normalized.as_str(), "1" | "true" | "yes" | "on")
            })
            .unwrap_or(false);

        if offline_mode {
            log::warn!("⚠️ AccountManager: 检测到离线模式，交易所请求将使用 MockExchange 并跳过真实网络访问");
        }

        Self {
            accounts: HashMap::new(),
            exchange_config,
            offline_mode,
        }
    }

    /// 添加账户
    pub async fn add_account(&mut self, account_config: AccountConfig) -> Result<()> {
        if !account_config.enabled {
            log::info!("账户 {} 未启用，跳过", account_config.id);
            return Ok(());
        }

        if self.offline_mode {
            let exchange: Box<dyn Exchange> = Box::new(crate::exchanges::MockExchange::new(
                &account_config.exchange,
            ));

            let account_info = Arc::new(AccountInfo {
                id: account_config.id.clone(),
                exchange_name: account_config.exchange.clone(),
                exchange: Arc::new(exchange),
                config: account_config.clone(),
                balance: Arc::new(RwLock::new(Vec::new())),
                positions: Arc::new(RwLock::new(Vec::new())),
                open_orders: Arc::new(RwLock::new(HashMap::new())),
            });

            self.accounts
                .insert(account_config.id.clone(), account_info);
            log::info!(
                "✅ 离线模式: 已使用 MockExchange 添加账户 {} ({})",
                account_config.id,
                account_config.exchange
            );
            return Ok(());
        }

        // 从环境变量加载API密钥
        let api_keys = ApiKeys::from_env(&account_config.api_key_env)?;

        // 创建交易所实例
        let exchange: Box<dyn Exchange> = match account_config.exchange.as_str() {
            "binance" => {
                log::info!("🔍🔍 AccountManager: 创建 Binance 交易所实例");
                let binance = BinanceExchange::new(self.exchange_config.clone(), api_keys);
                log::info!("🔍🔍 AccountManager: 将 BinanceExchange 装箱为 Box<dyn Exchange>");
                Box::new(binance)
            }
            "okx" => Box::new(OkxExchange::new(self.exchange_config.clone(), api_keys)),
            "bitmart" => Box::new(BitmartExchange::new(self.exchange_config.clone(), api_keys)),
            "hyperliquid" => Box::new(HyperliquidExchange::new(
                self.exchange_config.clone(),
                api_keys,
            )),
            _ => {
                return Err(ExchangeError::UnsupportedExchange(
                    account_config.exchange.clone(),
                ));
            }
        };

        // 创建账户信息
        let account_info = Arc::new(AccountInfo {
            id: account_config.id.clone(),
            exchange_name: account_config.exchange.clone(),
            exchange: Arc::new(exchange),
            config: account_config.clone(),
            balance: Arc::new(RwLock::new(Vec::new())),
            positions: Arc::new(RwLock::new(Vec::new())),
            open_orders: Arc::new(RwLock::new(HashMap::new())),
        });

        // 初始化账户数据 - 暂时关闭以减少API请求
        // self.update_account_data(&account_info).await?;
        log::info!("跳过账户初始化数据获取，减少启动时API请求");

        self.accounts
            .insert(account_config.id.clone(), account_info);
        log::info!(
            "成功添加账户: {} ({})",
            account_config.id,
            account_config.exchange
        );

        Ok(())
    }

    /// 获取账户
    pub fn get_account(&self, account_id: &str) -> Option<Arc<AccountInfo>> {
        self.accounts.get(account_id).cloned()
    }

    /// 直接添加交易所实例（用于从统一账户管理器迁移）
    pub fn add_exchange_instance(&mut self, account_id: &str, exchange: Arc<Box<dyn Exchange>>) {
        let account_info = Arc::new(AccountInfo {
            id: account_id.to_string(),
            exchange_name: "unknown".to_string(), // 将从exchange实例推断
            exchange: exchange.clone(),
            config: AccountConfig {
                id: account_id.to_string(),
                exchange: "unknown".to_string(),
                api_key_env: "".to_string(),
                enabled: true,
                max_positions: 10,
                max_orders_per_symbol: 20,
            },
            balance: Arc::new(RwLock::new(Vec::new())),
            positions: Arc::new(RwLock::new(Vec::new())),
            open_orders: Arc::new(RwLock::new(HashMap::new())),
        });

        self.accounts.insert(account_id.to_string(), account_info);
    }

    /// 根据交易所名称获取第一个账户
    pub fn get_account_by_exchange(&self, exchange_name: &str) -> Option<Arc<AccountInfo>> {
        self.accounts
            .values()
            .find(|acc| acc.exchange_name.to_lowercase() == exchange_name.to_lowercase())
            .cloned()
    }

    /// 获取所有账户
    pub fn get_all_accounts(&self) -> Vec<Arc<AccountInfo>> {
        self.accounts.values().cloned().collect()
    }

    /// 获取指定交易所的所有账户
    pub fn get_exchange_accounts(&self, exchange: &str) -> Vec<Arc<AccountInfo>> {
        self.accounts
            .values()
            .filter(|acc| acc.exchange_name == exchange)
            .cloned()
            .collect()
    }

    /// 当前是否处于离线模式
    pub fn is_offline(&self) -> bool {
        self.offline_mode
    }

    /// 更新账户数据
    pub async fn update_account_data(&self, account: &Arc<AccountInfo>) -> Result<()> {
        // 更新余额
        match account.exchange.get_balance(MarketType::Futures).await {
            Ok(balance) => {
                let mut bal = account.balance.write().await;
                *bal = balance;
            }
            Err(e) => {
                log::error!("更新账户 {} 余额失败: {}", account.id, e);
            }
        }

        // 更新持仓
        match account.exchange.get_positions(None).await {
            Ok(positions) => {
                let mut pos = account.positions.write().await;
                *pos = positions;
            }
            Err(e) => {
                log::error!("更新账户 {} 持仓失败: {}", account.id, e);
            }
        }

        // 更新挂单
        match account
            .exchange
            .get_open_orders(None, MarketType::Futures)
            .await
        {
            Ok(orders) => {
                let mut open_orders = account.open_orders.write().await;
                open_orders.clear();
                for order in orders {
                    open_orders
                        .entry(order.symbol.clone())
                        .or_insert_with(Vec::new)
                        .push(order);
                }
            }
            Err(e) => {
                log::error!("更新账户 {} 挂单失败: {}", account.id, e);
            }
        }

        Ok(())
    }

    /// 更新所有账户数据
    pub async fn update_all_accounts(&self) -> Result<()> {
        let mut tasks = Vec::new();

        for account in self.accounts.values() {
            let account_clone = account.clone();
            tasks.push(tokio::spawn(async move {
                if let Err(e) = Self::update_account_data_static(&account_clone).await {
                    log::error!("更新账户 {} 失败: {}", account_clone.id, e);
                }
            }));
        }

        // 等待所有更新完成
        for task in tasks {
            let _ = task.await;
        }

        Ok(())
    }

    /// 静态方法用于异步更新
    async fn update_account_data_static(account: &Arc<AccountInfo>) -> Result<()> {
        // 更新余额
        log::debug!(
            "开始更新账户 {} ({}) 的余额",
            account.id,
            account.exchange_name
        );
        match account.exchange.get_balance(MarketType::Futures).await {
            Ok(balance) => {
                let mut bal = account.balance.write().await;
                *bal = balance;
                log::debug!("账户 {} 余额更新成功", account.id);
            }
            Err(e) => {
                log::error!("更新账户 {} 余额失败: {}", account.id, e);
                log::error!("  - 交易所类型: {}", account.exchange_name);
            }
        }

        Ok(())
    }

    /// 取消账户的所有订单
    pub async fn cancel_all_orders(
        &self,
        account_id: &str,
        symbol: Option<&str>,
    ) -> Result<Vec<Order>> {
        let account = self
            .get_account(account_id)
            .ok_or_else(|| ExchangeError::Other(format!("账户 {} 不存在", account_id)))?;

        account
            .exchange
            .cancel_all_orders(symbol, MarketType::Futures)
            .await
    }

    /// 批量下单
    pub async fn create_batch_orders(
        &self,
        account_id: &str,
        orders: Vec<OrderRequest>,
    ) -> Result<BatchOrderResponse> {
        let account = self
            .get_account(account_id)
            .ok_or_else(|| ExchangeError::Other(format!("账户 {} 不存在", account_id)))?;

        let batch_request = BatchOrderRequest {
            orders,
            market_type: MarketType::Futures,
        };

        account.exchange.create_batch_orders(batch_request).await
    }

    /// 获取账户余额
    pub async fn get_balance(&self, account_id: &str) -> Result<Vec<Balance>> {
        let account = self
            .get_account(account_id)
            .ok_or_else(|| ExchangeError::Other(format!("账户 {} 不存在", account_id)))?;

        let balance = account.balance.read().await;
        Ok(balance.clone())
    }

    /// 获取账户持仓
    pub async fn get_positions(&self, account_id: &str) -> Result<Vec<Position>> {
        let account = self
            .get_account(account_id)
            .ok_or_else(|| ExchangeError::Other(format!("账户 {} 不存在", account_id)))?;

        let positions = account.positions.read().await;
        Ok(positions.clone())
    }

    /// 获取账户挂单
    pub async fn get_open_orders(
        &self,
        account_id: &str,
        symbol: Option<&str>,
    ) -> Result<Vec<Order>> {
        let account = self
            .get_account(account_id)
            .ok_or_else(|| ExchangeError::Other(format!("账户 {} 不存在", account_id)))?;

        let open_orders = account.open_orders.read().await;

        if let Some(sym) = symbol {
            Ok(open_orders.get(sym).cloned().unwrap_or_default())
        } else {
            Ok(open_orders.values().flatten().cloned().collect())
        }
    }
}
