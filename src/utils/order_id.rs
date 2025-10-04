/// 订单ID生成器
///
/// 为每个策略生成唯一且可识别的订单ID
/// 支持不同交易所的格式要求
use chrono::{DateTime, Utc};
use std::sync::atomic::{AtomicU32, Ordering};

/// 交易所订单ID规则
#[derive(Debug, Clone)]
pub struct ExchangeOrderIdRules {
    pub max_length: usize,
    pub allow_letters: bool,
    pub allow_numbers: bool,
    pub allow_underscore: bool,
    pub allow_dash: bool,
    pub prefix_allowed: bool,
    pub case_sensitive: bool,
}

impl ExchangeOrderIdRules {
    /// Binance规则
    pub fn binance() -> Self {
        Self {
            max_length: 36,          // 最大36个字符
            allow_letters: true,     // 允许字母
            allow_numbers: true,     // 允许数字
            allow_underscore: false, // 不允许下划线
            allow_dash: false,       // 不允许横线
            prefix_allowed: true,    // 允许前缀
            case_sensitive: true,    // 区分大小写
        }
    }

    /// OKX规则
    pub fn okx() -> Self {
        Self {
            max_length: 32,         // 最大32个字符
            allow_letters: true,    // 允许字母
            allow_numbers: true,    // 允许数字
            allow_underscore: true, // 允许下划线
            allow_dash: true,       // 允许横线
            prefix_allowed: true,   // 允许前缀
            case_sensitive: false,  // 不区分大小写
        }
    }

    /// Bitmart规则
    pub fn bitmart() -> Self {
        Self {
            max_length: 30,         // 最大30个字符
            allow_letters: true,    // 允许字母
            allow_numbers: true,    // 允许数字
            allow_underscore: true, // 允许下划线
            allow_dash: false,      // 不允许横线
            prefix_allowed: true,   // 允许前缀
            case_sensitive: true,   // 区分大小写
        }
    }

    /// Bybit规则
    pub fn bybit() -> Self {
        Self {
            max_length: 36,         // 最大36个字符
            allow_letters: true,    // 允许字母
            allow_numbers: true,    // 允许数字
            allow_underscore: true, // 允许下划线
            allow_dash: true,       // 允许横线
            prefix_allowed: true,   // 允许前缀
            case_sensitive: true,   // 区分大小写
        }
    }
}

/// 订单ID生成器
pub struct OrderIdGenerator {
    strategy_code: String,
    sequence: AtomicU32,
    rules: ExchangeOrderIdRules,
}

impl OrderIdGenerator {
    /// 创建新的订单ID生成器
    pub fn new(strategy_name: &str, exchange: &str) -> Self {
        // 生成策略代码（缩写）
        let strategy_code = Self::generate_strategy_code(strategy_name);

        // 获取交易所规则
        let rules = match exchange.to_lowercase().as_str() {
            "binance" => ExchangeOrderIdRules::binance(),
            "okx" => ExchangeOrderIdRules::okx(),
            "bitmart" => ExchangeOrderIdRules::bitmart(),
            "bybit" => ExchangeOrderIdRules::bybit(),
            _ => ExchangeOrderIdRules::binance(), // 默认使用最严格的规则
        };

        Self {
            strategy_code,
            sequence: AtomicU32::new(0),
            rules,
        }
    }

    /// 生成订单ID
    pub fn generate(&self) -> String {
        // 获取序列号
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);

        // 生成时间戳部分（紧凑格式）
        let timestamp = Self::generate_timestamp();

        // 组合订单ID
        let mut order_id = String::new();

        // 策略代码（3-4个字符）
        order_id.push_str(&self.strategy_code);

        // 分隔符（根据规则）
        if self.rules.allow_underscore {
            order_id.push('_');
        } else if self.rules.allow_dash {
            order_id.push('-');
        }

        // 时间戳（8-10个字符）
        order_id.push_str(&timestamp);

        // 序列号（4-6个字符）
        order_id.push_str(&format!("{:04}", seq % 10000));

        // 确保不超过最大长度
        if order_id.len() > self.rules.max_length {
            order_id.truncate(self.rules.max_length);
        }

        // 根据大小写规则转换
        if !self.rules.case_sensitive {
            order_id = order_id.to_uppercase();
        }

        order_id
    }

    /// 生成带自定义标签的订单ID
    pub fn generate_with_tag(&self, tag: &str) -> String {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);
        let timestamp = Self::generate_timestamp_short();

        // 清理标签（只保留字母数字）
        let clean_tag: String = tag
            .chars()
            .filter(|c| c.is_alphanumeric())
            .take(4)
            .collect();

        let mut order_id = format!(
            "{}{}{}{:03}",
            self.strategy_code,
            clean_tag,
            timestamp,
            seq % 1000
        );

        // 确保符合规则
        if !self.rules.allow_underscore {
            order_id = order_id.replace('_', "");
        }
        if !self.rules.allow_dash {
            order_id = order_id.replace('-', "");
        }

        // 截断到最大长度
        if order_id.len() > self.rules.max_length {
            order_id.truncate(self.rules.max_length);
        }

        order_id
    }

    /// 解析订单ID获取策略信息
    pub fn parse_order_id(order_id: &str) -> Option<OrderIdInfo> {
        // 尝试解析订单ID格式
        // 格式: [策略代码][分隔符][时间戳][序列号]

        if order_id.len() < 10 {
            return None;
        }

        // 提取策略代码（前3-4个字符）
        let strategy_code = if order_id
            .chars()
            .nth(3)
            .map_or(false, |c| !c.is_alphanumeric())
        {
            &order_id[..3]
        } else if order_id
            .chars()
            .nth(4)
            .map_or(false, |c| !c.is_alphanumeric())
        {
            &order_id[..4]
        } else {
            &order_id[..3]
        };

        // 识别策略名称
        let strategy_name = Self::decode_strategy_code(strategy_code);

        Some(OrderIdInfo {
            strategy_code: strategy_code.to_string(),
            strategy_name,
            order_id: order_id.to_string(),
        })
    }

    /// 生成策略代码
    fn generate_strategy_code(strategy_name: &str) -> String {
        match strategy_name.to_lowercase().as_str() {
            "trend_grid" | "trend_grid_v2" => "TG2".to_string(),
            "funding_rate_arbitrage" => "FRA".to_string(),
            "poisson_market_maker" => "PMM".to_string(),
            "copy_trading" => "CPY".to_string(),
            "cross_exchange_arbitrage" => "CEA".to_string(),
            _ => {
                // 生成缩写：取每个单词的首字母
                strategy_name
                    .split('_')
                    .map(|w| w.chars().next().unwrap_or('X'))
                    .collect::<String>()
                    .to_uppercase()
                    .chars()
                    .take(3)
                    .collect::<String>()
            }
        }
    }

    /// 解码策略代码
    fn decode_strategy_code(code: &str) -> String {
        match code {
            "TG2" => "trend_grid_v2".to_string(),
            "FRA" => "funding_rate_arbitrage".to_string(),
            "PMM" => "poisson_market_maker".to_string(),
            "CPY" => "copy_trading".to_string(),
            "CEA" => "cross_exchange_arbitrage".to_string(),
            _ => format!("unknown_{}", code),
        }
    }

    /// 生成时间戳（紧凑格式）
    fn generate_timestamp() -> String {
        let now = Utc::now();
        // 格式: MMDDHHMM (月日时分)
        format!(
            "{:02}{:02}{:02}{:02}",
            now.format("%m").to_string().parse::<u32>().unwrap_or(1),
            now.format("%d").to_string().parse::<u32>().unwrap_or(1),
            now.format("%H").to_string().parse::<u32>().unwrap_or(0),
            now.format("%M").to_string().parse::<u32>().unwrap_or(0)
        )
    }

    /// 生成短时间戳
    fn generate_timestamp_short() -> String {
        let now = Utc::now();
        // 格式: DDHHMM (日时分)
        format!(
            "{:02}{:02}{:02}",
            now.format("%d").to_string().parse::<u32>().unwrap_or(1),
            now.format("%H").to_string().parse::<u32>().unwrap_or(0),
            now.format("%M").to_string().parse::<u32>().unwrap_or(0)
        )
    }
}

/// 订单ID信息
#[derive(Debug, Clone)]
pub struct OrderIdInfo {
    pub strategy_code: String,
    pub strategy_name: String,
    pub order_id: String,
}

/// 订单ID管理器（全局）
pub struct OrderIdManager {
    generators: std::collections::HashMap<String, OrderIdGenerator>,
}

impl OrderIdManager {
    pub fn new() -> Self {
        Self {
            generators: std::collections::HashMap::new(),
        }
    }

    /// 获取或创建生成器
    pub fn get_or_create(&mut self, strategy_name: &str, exchange: &str) -> &OrderIdGenerator {
        let key = format!("{}_{}", strategy_name, exchange);
        self.generators
            .entry(key)
            .or_insert_with(|| OrderIdGenerator::new(strategy_name, exchange))
    }

    /// 生成订单ID
    pub fn generate(&mut self, strategy_name: &str, exchange: &str) -> String {
        self.get_or_create(strategy_name, exchange).generate()
    }

    /// 生成带标签的订单ID
    pub fn generate_with_tag(&mut self, strategy_name: &str, exchange: &str, tag: &str) -> String {
        self.get_or_create(strategy_name, exchange)
            .generate_with_tag(tag)
    }
}

// 全局订单ID管理器
lazy_static::lazy_static! {
    pub static ref ORDER_ID_MANAGER: std::sync::Mutex<OrderIdManager> =
        std::sync::Mutex::new(OrderIdManager::new());
}

/// 便捷函数：生成订单ID
pub fn generate_order_id(strategy_name: &str, exchange: &str) -> String {
    ORDER_ID_MANAGER
        .lock()
        .expect("Lock poisoned")
        .generate(strategy_name, exchange)
}

/// 便捷函数：生成带标签的订单ID
pub fn generate_order_id_with_tag(strategy_name: &str, exchange: &str, tag: &str) -> String {
    ORDER_ID_MANAGER
        .lock()
        .expect("Lock poisoned")
        .generate_with_tag(strategy_name, exchange, tag)
}

/// 便捷函数：解析订单ID
pub fn parse_order_id(order_id: &str) -> Option<OrderIdInfo> {
    OrderIdGenerator::parse_order_id(order_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_id_generation() {
        let gen = OrderIdGenerator::new("trend_grid_v2", "binance");
        let id1 = gen.generate();
        let id2 = gen.generate();

        println!("Generated ID 1: {}", id1);
        println!("Generated ID 2: {}", id2);

        assert_ne!(id1, id2);
        assert!(id1.len() <= 36);
        assert!(id1.starts_with("TG2"));
    }

    #[test]
    fn test_order_id_parsing() {
        let gen = OrderIdGenerator::new("funding_rate_arbitrage", "okx");
        let id = gen.generate();

        println!("Generated ID: {}", id);

        if let Some(info) = OrderIdGenerator::parse_order_id(&id) {
            println!("Parsed: {:?}", info);
            assert_eq!(info.strategy_code, "FRA");
            assert_eq!(info.strategy_name, "funding_rate_arbitrage");
        }
    }

    #[test]
    fn test_exchange_rules() {
        // Binance: 不允许下划线
        let gen_binance = OrderIdGenerator::new("test_strategy", "binance");
        let id_binance = gen_binance.generate();
        assert!(!id_binance.contains('_'));

        // OKX: 允许下划线
        let gen_okx = OrderIdGenerator::new("test_strategy", "okx");
        let id_okx = gen_okx.generate();
        println!("OKX ID: {}", id_okx);
    }
}
