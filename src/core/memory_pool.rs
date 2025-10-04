use crossbeam::queue::ArrayQueue;
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::fmt::Debug;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// 对象池 trait
pub trait Poolable: Default + Debug + Send + Sync {
    /// 重置对象到初始状态
    fn reset(&mut self);
}

/// 通用对象池 - 支持无锁操作
pub struct ObjectPool<T: Poolable> {
    pool: Arc<Mutex<VecDeque<T>>>,
    fast_pool: Option<Arc<ArrayQueue<T>>>, // 无锁队列
    max_size: usize,
    created_count: Arc<AtomicUsize>,
    hits: Arc<AtomicUsize>,
    misses: Arc<AtomicUsize>,
}

impl<T: Poolable> ObjectPool<T>
where
    T: Send + Sync,
{
    /// 创建新的对象池
    pub fn new(max_size: usize) -> Self {
        Self {
            pool: Arc::new(Mutex::new(VecDeque::with_capacity(max_size))),
            fast_pool: None,
            max_size,
            created_count: Arc::new(AtomicUsize::new(0)),
            hits: Arc::new(AtomicUsize::new(0)),
            misses: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// 创建高性能对象池（无锁）
    pub fn new_fast(max_size: usize) -> Self {
        Self {
            pool: Arc::new(Mutex::new(VecDeque::with_capacity(max_size))),
            fast_pool: Some(Arc::new(ArrayQueue::new(max_size))),
            max_size,
            created_count: Arc::new(AtomicUsize::new(0)),
            hits: Arc::new(AtomicUsize::new(0)),
            misses: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// 预分配对象
    pub fn preallocate(&self, count: usize) {
        if let Some(ref fast_pool) = self.fast_pool {
            // 无锁队列预分配
            for _ in 0..count.min(self.max_size) {
                let obj = T::default();
                let _ = fast_pool.push(obj);
                self.created_count.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            // 使用锁的版本
            let mut pool = self.pool.lock();
            for _ in 0..count.min(self.max_size) {
                pool.push_back(T::default());
            }
            self.created_count.store(pool.len(), Ordering::Relaxed);
        }
    }

    /// 获取对象
    pub fn get(&self) -> PooledObject<T> {
        // 优先尝试无锁队列
        if let Some(ref fast_pool) = self.fast_pool {
            if let Some(mut obj) = fast_pool.pop() {
                self.hits.fetch_add(1, Ordering::Relaxed);
                obj.reset();
                return PooledObject::new_fast(obj, fast_pool.clone(), self.max_size);
            }
        }

        // 回退到有锁版本
        let mut pool = self.pool.lock();
        let obj = pool.pop_front().unwrap_or_else(|| {
            self.misses.fetch_add(1, Ordering::Relaxed);
            self.created_count.fetch_add(1, Ordering::Relaxed);
            T::default()
        });
        PooledObject::new(obj, self.pool.clone(), self.max_size)
    }

    /// 获取命中率
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed) as f64;
        let total = hits + self.misses.load(Ordering::Relaxed) as f64;
        if total > 0.0 {
            hits / total * 100.0
        } else {
            0.0
        }
    }

    /// 获取池中对象数量
    pub fn size(&self) -> usize {
        self.pool.lock().len()
    }

    /// 获取创建的对象总数
    pub fn created_count(&self) -> usize {
        self.created_count.load(Ordering::Relaxed)
    }

    /// 清空对象池
    pub fn clear(&self) {
        self.pool.lock().clear();
    }
}

/// 池化对象包装器
pub struct PooledObject<T: Poolable> {
    object: Option<T>,
    pool: Option<Arc<Mutex<VecDeque<T>>>>,
    fast_pool: Option<Arc<ArrayQueue<T>>>,
    max_size: usize,
}

impl<T: Poolable> PooledObject<T>
where
    T: Send + Sync,
{
    fn new(object: T, pool: Arc<Mutex<VecDeque<T>>>, max_size: usize) -> Self {
        Self {
            object: Some(object),
            pool: Some(pool),
            fast_pool: None,
            max_size,
        }
    }

    fn new_fast(object: T, fast_pool: Arc<ArrayQueue<T>>, max_size: usize) -> Self {
        Self {
            object: Some(object),
            pool: None,
            fast_pool: Some(fast_pool),
            max_size,
        }
    }

    /// 获取对象的可变引用
    pub fn get_mut(&mut self) -> &mut T {
        self.object
            .as_mut()
            .expect("Object already returned to pool")
    }

    /// 获取对象的不可变引用
    pub fn get(&self) -> &T {
        self.object
            .as_ref()
            .expect("Object already returned to pool")
    }

    /// 手动返回对象到池中
    pub fn return_to_pool(mut self) {
        if let Some(mut obj) = self.object.take() {
            obj.reset();
            if let Some(ref fast_pool) = self.fast_pool {
                let _ = fast_pool.push(obj);
            } else if let Some(ref pool) = self.pool {
                let mut pool = pool.lock();
                if pool.len() < self.max_size {
                    pool.push_back(obj);
                }
            }
        }
    }
}

impl<T: Poolable> Drop for PooledObject<T>
where
    T: Send + Sync,
{
    fn drop(&mut self) {
        if let Some(mut obj) = self.object.take() {
            obj.reset();
            if let Some(ref fast_pool) = self.fast_pool {
                let _ = fast_pool.push(obj);
            } else if let Some(ref pool) = self.pool {
                let mut pool = pool.lock();
                if pool.len() < self.max_size {
                    pool.push_back(obj);
                }
            }
        }
    }
}

impl<T: Poolable> std::ops::Deref for PooledObject<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.get()
    }
}

impl<T: Poolable> std::ops::DerefMut for PooledObject<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.get_mut()
    }
}

/// 优化的订单簿数据结构
#[derive(Clone, Debug)]
pub struct OptimizedOrderBook {
    /// 使用固定大小的数组避免动态分配
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
    /// 使用位图标记有效数据
    valid_bids: u64,
    valid_asks: u64,
    symbol: Arc<str>, // 使用Arc<str>减少字符串复制
    timestamp: i64,
}

impl OptimizedOrderBook {
    pub fn new(symbol: String, capacity: usize) -> Self {
        Self {
            bids: Vec::with_capacity(capacity),
            asks: Vec::with_capacity(capacity),
            valid_bids: 0,
            valid_asks: 0,
            symbol: Arc::from(symbol.as_str()),
            timestamp: 0,
        }
    }

    /// 批量更新买单
    pub fn update_bids(&mut self, bids: Vec<(f64, f64)>) {
        self.bids.clear();
        self.bids.extend_from_slice(&bids);
        self.valid_bids = (1 << bids.len()) - 1;
    }

    /// 批量更新卖单
    pub fn update_asks(&mut self, asks: Vec<(f64, f64)>) {
        self.asks.clear();
        self.asks.extend_from_slice(&asks);
        self.valid_asks = (1 << asks.len()) - 1;
    }

    /// 获取最优买价
    pub fn best_bid(&self) -> Option<(f64, f64)> {
        if self.valid_bids > 0 {
            self.bids.first().copied()
        } else {
            None
        }
    }

    /// 获取最优卖价
    pub fn best_ask(&self) -> Option<(f64, f64)> {
        if self.valid_asks > 0 {
            self.asks.first().copied()
        } else {
            None
        }
    }

    /// 获取价差
    pub fn spread(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some((bid, _)), Some((ask, _))) => Some(ask - bid),
            _ => None,
        }
    }

    /// 获取中间价
    pub fn mid_price(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some((bid, _)), Some((ask, _))) => Some((bid + ask) / 2.0),
            _ => None,
        }
    }
}

impl Default for OptimizedOrderBook {
    fn default() -> Self {
        Self::new(String::new(), 20)
    }
}

impl Poolable for OptimizedOrderBook {
    fn reset(&mut self) {
        self.bids.clear();
        self.asks.clear();
        self.valid_bids = 0;
        self.valid_asks = 0;
        self.timestamp = 0;
    }
}

/// 环形缓冲区 - 用于存储历史数据
pub struct RingBuffer<T> {
    buffer: Vec<T>,
    capacity: usize,
    head: usize,
    tail: usize,
    size: usize,
}

impl<T: Clone> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            capacity,
            head: 0,
            tail: 0,
            size: 0,
        }
    }

    /// 添加元素
    pub fn push(&mut self, item: T) {
        if self.size < self.capacity {
            self.buffer.push(item);
            self.size += 1;
            self.tail = self.size;
        } else {
            self.buffer[self.tail % self.capacity] = item;
            self.tail = (self.tail + 1) % self.capacity;
            self.head = (self.head + 1) % self.capacity;
        }
    }

    /// 获取最新的N个元素
    pub fn get_last_n(&self, n: usize) -> Vec<T> {
        let count = n.min(self.size);
        let mut result = Vec::with_capacity(count);

        for i in 0..count {
            let idx = (self.tail + self.capacity - count + i) % self.capacity;
            if idx < self.buffer.len() {
                result.push(self.buffer[idx].clone());
            }
        }

        result
    }

    /// 获取所有元素
    pub fn to_vec(&self) -> Vec<T> {
        self.get_last_n(self.size)
    }

    /// 清空缓冲区
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.head = 0;
        self.tail = 0;
        self.size = 0;
    }

    /// 获取缓冲区大小
    pub fn len(&self) -> usize {
        self.size
    }

    /// 检查是否为空
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
}

/// 优化的交易数据结构
#[derive(Clone, Debug)]
pub struct OptimizedTrade {
    pub id: u64,        // 使用u64代替String
    pub symbol_id: u32, // 使用ID代替字符串
    pub price: f64,
    pub amount: f64,
    pub timestamp: i64,
    pub side: u8, // 0=Buy, 1=Sell
    pub order_id: Option<u64>,
}

impl Default for OptimizedTrade {
    fn default() -> Self {
        Self {
            id: 0,
            symbol_id: 0,
            price: 0.0,
            amount: 0.0,
            timestamp: 0,
            side: 0,
            order_id: None,
        }
    }
}

impl Poolable for OptimizedTrade {
    fn reset(&mut self) {
        self.id = 0;
        self.symbol_id = 0;
        self.price = 0.0;
        self.amount = 0.0;
        self.timestamp = 0;
        self.side = 0;
        self.order_id = None;
    }
}

/// 符号ID映射器 - 减少字符串使用
pub struct SymbolIdMapper {
    symbol_to_id: Arc<RwLock<HashMap<String, u32>>>,
    id_to_symbol: Arc<RwLock<HashMap<u32, String>>>,
    next_id: Arc<RwLock<u32>>,
}

use std::collections::HashMap;

impl SymbolIdMapper {
    pub fn new() -> Self {
        Self {
            symbol_to_id: Arc::new(RwLock::new(HashMap::new())),
            id_to_symbol: Arc::new(RwLock::new(HashMap::new())),
            next_id: Arc::new(RwLock::new(1)),
        }
    }

    /// 获取或创建符号ID
    pub fn get_or_create_id(&self, symbol: &str) -> u32 {
        {
            let map = self.symbol_to_id.read();
            if let Some(&id) = map.get(symbol) {
                return id;
            }
        }

        let mut symbol_map = self.symbol_to_id.write();
        let mut id_map = self.id_to_symbol.write();
        let mut next_id = self.next_id.write();

        let id = *next_id;
        symbol_map.insert(symbol.to_string(), id);
        id_map.insert(id, symbol.to_string());
        *next_id += 1;

        id
    }

    /// 通过ID获取符号
    pub fn get_symbol(&self, id: u32) -> Option<String> {
        self.id_to_symbol.read().get(&id).cloned()
    }

    /// 通过符号获取ID
    pub fn get_id(&self, symbol: &str) -> Option<u32> {
        self.symbol_to_id.read().get(symbol).copied()
    }
}

/// 内存使用统计
pub struct MemoryStats {
    pub total_allocated: usize,
    pub pool_objects: usize,
    pub cache_entries: usize,
    pub active_connections: usize,
}

impl MemoryStats {
    pub fn estimate_memory_usage(&self) -> usize {
        let object_size = mem::size_of::<OptimizedOrderBook>() * self.pool_objects;
        let cache_size = self.cache_entries * 1024; // 假设平均每个缓存条目1KB
        let connection_size = self.active_connections * 8192; // 每个连接约8KB

        object_size + cache_size + connection_size + self.total_allocated
    }

    pub fn print_stats(&self) {
        println!("=== 内存使用统计 ===");
        println!("总分配: {} MB", self.total_allocated / 1_048_576);
        println!("池化对象: {}", self.pool_objects);
        println!("缓存条目: {}", self.cache_entries);
        println!("活跃连接: {}", self.active_connections);
        println!(
            "预估总内存: {} MB",
            self.estimate_memory_usage() / 1_048_576
        );
    }
}
