use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// 交易数据模型（精简版），供策略在无重型分析管线时依然共享结构
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeData {
    pub trade_time: DateTime<Utc>,
    pub strategy_name: String,
    pub account_id: String,
    pub exchange: String,
    pub symbol: String,
    pub side: String,
    pub order_type: Option<String>,
    pub price: Decimal,
    pub amount: Decimal,
    pub value: Option<Decimal>,
    pub fee: Option<Decimal>,
    pub fee_currency: Option<String>,
    pub position_side: Option<String>,
    pub realized_pnl: Option<Decimal>,
    pub pnl_percentage: Option<Decimal>,
    pub order_id: String,
    pub parent_order_id: Option<String>,
    pub metadata: Option<HashMap<String, Value>>,
}

/// 最小化的交易收集器实现：保留接口，默认不做任何持久化
#[derive(Clone, Default)]
pub struct TradeCollector {
    _client: Option<clickhouse::Client>,
}

impl TradeCollector {
    /// 创建一个空收集器，占位但不执行任何写入
    pub fn new() -> Self {
        Self { _client: None }
    }

    /// 使用 ClickHouse URL 初始化，便于后续扩展；当前实现仍为空操作
    pub fn with_clickhouse(url: &str) -> Self {
        let client = clickhouse::Client::default().with_url(url);
        Self {
            _client: Some(client),
        }
    }

    /// 记录交易数据。精简版本中直接返回 Ok，保留异步接口兼容策略逻辑
    pub async fn record_trade(
        &self,
        _trade: TradeData,
    ) -> crate::core::types::Result<()> {
        Ok(())
    }
}
