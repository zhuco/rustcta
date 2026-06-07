//! 订单缓存模块
//! 用于减少对交易所的查询请求

use crate::core::types::{MarketType, Order};
use dashmap::DashMap;
use log::{debug, info};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// 缓存项
#[derive(Clone, Debug)]
struct CacheItem<T> {
    data: T,
    timestamp: Instant,
    ttl: Duration,
}

impl<T> CacheItem<T> {
    fn new(data: T, ttl: Duration) -> Self {
        Self {
            data,
            timestamp: Instant::now(),
            ttl,
        }
    }

    fn is_expired(&self) -> bool {
        self.timestamp.elapsed() > self.ttl
    }
}

/// 订单缓存管理器
pub struct OrderCache {
    /// 活跃订单缓存 (symbol -> orders)
    open_orders: Arc<DashMap<String, CacheItem<Vec<Order>>>>,
    /// 单个订单缓存 (order_id -> order)
    order_by_id: Arc<DashMap<String, CacheItem<Order>>>,
    /// 默认TTL
    default_ttl: Duration,
    /// 缓存命中统计
    hit_count: Arc<AtomicU64>,
    /// 缓存未命中统计
    miss_count: Arc<AtomicU64>,
}

impl OrderCache {
    /// 创建新的订单缓存
    pub fn new(ttl_seconds: u64) -> Self {
        Self {
            open_orders: Arc::new(DashMap::new()),
            order_by_id: Arc::new(DashMap::new()),
            default_ttl: Duration::from_secs(ttl_seconds),
            hit_count: Arc::new(AtomicU64::new(0)),
            miss_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// 获取缓存的活跃订单
    pub async fn get_open_orders(&self, symbol: &str) -> Option<Vec<Order>> {
        if let Some(item) = self.open_orders.get(symbol) {
            if !item.is_expired() {
                self.hit_count.fetch_add(1, Ordering::Relaxed);
                debug!("缓存命中: {} 活跃订单", symbol);
                return Some(item.data.clone());
            }
        }

        self.miss_count.fetch_add(1, Ordering::Relaxed);
        debug!("缓存未命中: {} 活跃订单", symbol);
        None
    }

    /// 缓存活跃订单
    pub async fn set_open_orders(&self, symbol: String, orders: Vec<Order>) {
        self.open_orders.insert(
            symbol.clone(),
            CacheItem::new(orders.clone(), self.default_ttl),
        );
        debug!("缓存更新: {} 活跃订单 ({} 个)", symbol, orders.len());
    }

    /// 获取单个订单缓存
    pub async fn get_order(&self, order_id: &str) -> Option<Order> {
        if let Some(item) = self.order_by_id.get(order_id) {
            if !item.is_expired() {
                self.hit_count.fetch_add(1, Ordering::Relaxed);
                return Some(item.data.clone());
            }
        }

        self.miss_count.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// 缓存单个订单
    pub async fn set_order(&self, order: Order) {
        self.order_by_id
            .insert(order.id.clone(), CacheItem::new(order, self.default_ttl));
    }

    /// 通过WebSocket更新订单状态
    pub async fn update_order_from_ws(&self, order: Order, symbol: &str) {
        // 更新单个订单缓存
        self.set_order(order.clone()).await;

        // 更新活跃订单列表
        if let Some(mut item) = self.open_orders.get_mut(symbol) {
            // 查找并更新订单
            let mut found = false;
            for cached_order in &mut item.data {
                if cached_order.id == order.id {
                    *cached_order = order.clone();
                    found = true;
                    break;
                }
            }

            // 如果是新订单，添加到列表
            if !found && order.status == crate::core::types::OrderStatus::Open {
                item.data.push(order.clone());
            }

            // 如果订单已完成或取消，从列表移除
            if order.status == crate::core::types::OrderStatus::Closed
                || order.status == crate::core::types::OrderStatus::Canceled
            {
                let order_id = order.id.clone();
                item.data.retain(|o| o.id != order_id);
            }
        }
    }

    /// 清理过期缓存
    pub async fn cleanup(&self) {
        let before_open = self.open_orders.len();
        let before_single = self.order_by_id.len();

        self.open_orders.retain(|_, item| !item.is_expired());
        self.order_by_id.retain(|_, item| !item.is_expired());

        let removed_open = before_open - self.open_orders.len();
        let removed_single = before_single - self.order_by_id.len();

        if removed_open > 0 || removed_single > 0 {
            info!(
                "清理过期缓存: {} 个交易对, {} 个订单",
                removed_open, removed_single
            );
        }
    }

    /// 获取缓存统计
    pub async fn get_stats(&self) -> (u64, u64, f64) {
        let hits = self.hit_count.load(Ordering::Relaxed);
        let misses = self.miss_count.load(Ordering::Relaxed);
        let total = hits + misses;
        let hit_rate = if total > 0 {
            (hits as f64 / total as f64) * 100.0
        } else {
            0.0
        };

        (hits, misses, hit_rate)
    }

    /// 使特定订单的缓存失效
    pub async fn invalidate_order(&self, order_id: &str) {
        if self.order_by_id.remove(order_id).is_some() {
            debug!("订单缓存失效: {}", order_id);
        }

        // 也从活跃订单缓存中移除
        for mut item in self.open_orders.iter_mut() {
            let symbol = item.key().clone();
            let original_len = item.data.len();
            item.data.retain(|o| o.id != order_id);
            if item.data.len() != original_len {
                debug!("从 {} 的活跃订单缓存中移除订单: {}", symbol, order_id);
                break;
            }
        }
    }

    /// 使特定交易对的活跃订单缓存失效
    pub async fn invalidate_open_orders(&self, symbol: &str) {
        if self.open_orders.remove(symbol).is_some() {
            debug!("活跃订单缓存失效: {}", symbol);
        }
    }

    /// 清空所有缓存
    pub async fn clear(&self) {
        self.open_orders.clear();
        self.order_by_id.clear();

        info!("所有订单缓存已清空");
    }
}
