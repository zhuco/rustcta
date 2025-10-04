use crate::analysis::{TradeCollector, TradeData};
use async_trait::async_trait;
use std::sync::Arc;

/// 交易记录trait，所有策略都应该实现这个trait来自动记录交易
#[async_trait]
pub trait TradeRecording: Send + Sync {
    /// 获取数据收集器
    fn get_collector(&self) -> Option<&Arc<TradeCollector>>;

    /// 设置数据收集器
    fn set_collector(&mut self, collector: Option<Arc<TradeCollector>>);

    /// 记录交易（内部使用）
    async fn record_trade_internal(
        &self,
        trade_data: TradeData,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(collector) = self.get_collector() {
            collector
                .record_trade(trade_data)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        } else {
            Ok(()) // 如果没有collector，静默跳过
        }
    }

    /// 记录策略启动事件
    async fn record_strategy_start(
        &self,
        strategy_name: &str,
        account_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(collector) = self.get_collector() {
            use chrono::Utc;
            use rust_decimal::Decimal;
            use std::collections::HashMap;

            let mut metadata = HashMap::new();
            metadata.insert(
                "event".to_string(),
                serde_json::Value::String("strategy_start".to_string()),
            );

            let trade_data = TradeData {
                trade_time: Utc::now(),
                strategy_name: strategy_name.to_string(),
                account_id: account_id.to_string(),
                exchange: "SYSTEM".to_string(),
                symbol: "SYSTEM".to_string(),
                side: "INFO".to_string(),
                order_type: Some("SYSTEM".to_string()),
                price: Decimal::ZERO,
                amount: Decimal::ZERO,
                value: Some(Decimal::ZERO),
                fee: Some(Decimal::ZERO),
                fee_currency: Some("".to_string()),
                position_side: None,
                realized_pnl: None,
                pnl_percentage: None,
                order_id: format!("START_{}_{}", strategy_name, Utc::now().timestamp()),
                parent_order_id: None,
                metadata: Some(metadata),
            };

            collector
                .record_trade(trade_data)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        } else {
            Ok(())
        }
    }

    /// 记录策略停止事件
    async fn record_strategy_stop(
        &self,
        strategy_name: &str,
        account_id: &str,
        reason: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(collector) = self.get_collector() {
            use chrono::Utc;
            use rust_decimal::Decimal;
            use std::collections::HashMap;

            let mut metadata = HashMap::new();
            metadata.insert(
                "event".to_string(),
                serde_json::Value::String("strategy_stop".to_string()),
            );
            metadata.insert(
                "reason".to_string(),
                serde_json::Value::String(reason.to_string()),
            );

            let trade_data = TradeData {
                trade_time: Utc::now(),
                strategy_name: strategy_name.to_string(),
                account_id: account_id.to_string(),
                exchange: "SYSTEM".to_string(),
                symbol: "SYSTEM".to_string(),
                side: "INFO".to_string(),
                order_type: Some("SYSTEM".to_string()),
                price: Decimal::ZERO,
                amount: Decimal::ZERO,
                value: Some(Decimal::ZERO),
                fee: Some(Decimal::ZERO),
                fee_currency: Some("".to_string()),
                position_side: None,
                realized_pnl: None,
                pnl_percentage: None,
                order_id: format!("STOP_{}_{}", strategy_name, Utc::now().timestamp()),
                parent_order_id: None,
                metadata: Some(metadata),
            };

            collector
                .record_trade(trade_data)
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        } else {
            Ok(())
        }
    }
}
