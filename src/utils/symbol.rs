use crate::core::{config::Config, error::ExchangeError, types::MarketType};
use once_cell::sync::Lazy;
use std::collections::HashMap;

/// 交易所符号格式配置
#[derive(Clone)]
struct ExchangeSymbolFormat {
    /// 现货分隔符
    spot_separator: &'static str,
    /// 期货分隔符
    futures_separator: &'static str,
    /// 期货后缀
    futures_suffix: &'static str,
    /// 是否大写
    uppercase: bool,
    /// 特殊映射
    special_mappings: HashMap<String, (String, String)>,
}

/// 全局交易所配置
static EXCHANGE_FORMATS: Lazy<HashMap<&'static str, ExchangeSymbolFormat>> = Lazy::new(|| {
    let mut formats = HashMap::new();

    // Binance: BTCUSDT 格式
    formats.insert(
        "binance",
        ExchangeSymbolFormat {
            spot_separator: "",
            futures_separator: "",
            futures_suffix: "",
            uppercase: true,
            special_mappings: create_binance_mappings(),
        },
    );

    // OKX: BTC-USDT 格式，期货加 -SWAP
    formats.insert(
        "okx",
        ExchangeSymbolFormat {
            spot_separator: "-",
            futures_separator: "-",
            futures_suffix: "-SWAP",
            uppercase: true,
            special_mappings: HashMap::new(),
        },
    );

    // Bitmart: BTC_USDT 格式
    formats.insert(
        "bitmart",
        ExchangeSymbolFormat {
            spot_separator: "_",
            futures_separator: "_",
            futures_suffix: "",
            uppercase: true,
            special_mappings: HashMap::new(),
        },
    );

    // Bybit: BTCUSDT 格式
    formats.insert(
        "bybit",
        ExchangeSymbolFormat {
            spot_separator: "",
            futures_separator: "",
            futures_suffix: "",
            uppercase: true,
            special_mappings: HashMap::new(),
        },
    );

    // HTX (Huobi): btcusdt 格式（小写）
    formats.insert(
        "htx",
        ExchangeSymbolFormat {
            spot_separator: "",
            futures_separator: "-",
            futures_suffix: "",
            uppercase: false,
            special_mappings: HashMap::new(),
        },
    );

    // Hyperliquid: 特殊处理
    formats.insert(
        "hyperliquid",
        ExchangeSymbolFormat {
            spot_separator: "/",
            futures_separator: "",
            futures_suffix: "",
            uppercase: true,
            special_mappings: HashMap::new(),
        },
    );

    formats
});

/// 常见的报价货币列表（按优先级排序）
static QUOTE_CURRENCIES: Lazy<Vec<&'static str>> = Lazy::new(|| {
    vec![
        // 稳定币（优先级最高）
        "USDT", "USDC", "BUSD", "TUSD", "FDUSD", "USDS", "DAI", "PAX", "USDP", "UST", "HUSD",
        // 主要加密货币
        "BTC", "ETH", "BNB", "HT", "OKB", // 法币
        "EUR", "GBP", "AUD", "TRY", "RUB", "UAH", "NGN", "VAI", "BVND", // 其他
        "TRX", "XRP",
    ]
});

/// 创建 Binance 特殊映射
fn create_binance_mappings() -> HashMap<String, (String, String)> {
    let mut mappings = HashMap::new();

    // 添加已知的特殊交易对
    mappings.insert(
        "QTUMETH".to_string(),
        ("QTUM".to_string(), "ETH".to_string()),
    );
    mappings.insert("ADAETH".to_string(), ("ADA".to_string(), "ETH".to_string()));
    mappings.insert("NEOETH".to_string(), ("NEO".to_string(), "ETH".to_string()));
    mappings.insert("XLMETH".to_string(), ("XLM".to_string(), "ETH".to_string()));
    mappings.insert("TRXETH".to_string(), ("TRX".to_string(), "ETH".to_string()));
    mappings.insert("TRXXRP".to_string(), ("TRX".to_string(), "XRP".to_string()));
    mappings.insert("XZCXRP".to_string(), ("XZC".to_string(), "XRP".to_string()));
    mappings.insert(
        "BNBUSDS".to_string(),
        ("BNB".to_string(), "USDS".to_string()),
    );

    mappings
}

/// 交易对格式转换器
#[derive(Clone)]
pub struct SymbolConverter {
    config: Config,
    /// 自定义映射表（可以在运行时添加）
    custom_mappings: HashMap<String, HashMap<String, String>>,
    /// 反向映射缓存
    reverse_cache: HashMap<String, String>,
}

impl SymbolConverter {
    /// 创建新的符号转换器
    pub fn new(config: Config) -> Self {
        Self {
            config,
            custom_mappings: HashMap::new(),
            reverse_cache: HashMap::new(),
        }
    }

    /// 添加自定义映射
    pub fn add_custom_mapping(&mut self, exchange: &str, standard: &str, exchange_specific: &str) {
        let exchange_mappings = self
            .custom_mappings
            .entry(exchange.to_string())
            .or_insert_with(HashMap::new);
        exchange_mappings.insert(standard.to_string(), exchange_specific.to_string());

        // 同时更新反向缓存
        let cache_key = format!("{}:{}", exchange, exchange_specific);
        self.reverse_cache.insert(cache_key, standard.to_string());
    }

    /// 将标准格式(BASE/QUOTE)转换为交易所特定格式
    pub fn to_exchange_symbol(
        &self,
        symbol: &str,
        exchange: &str,
        market_type: MarketType,
    ) -> Result<String, ExchangeError> {
        // 解析标准格式 "BTC/USDT"
        let parts: Vec<&str> = symbol.split('/').collect();
        if parts.len() != 2 {
            return Err(ExchangeError::SymbolError(format!(
                "无效的交易对格式: {}，应为 BASE/QUOTE 格式",
                symbol
            )));
        }

        let base = parts[0];
        let quote = parts[1];

        // 获取交易所配置（可选）
        let _exchange_config = self.config.get_exchange_config(exchange).ok();

        // 根据交易所和市场类型转换格式
        let exchange_lower = exchange.to_lowercase();
        log::debug!(
            "Symbol converter: exchange='{}', lower='{}', symbol='{}', market={:?}",
            exchange,
            exchange_lower,
            symbol,
            market_type
        );

        let converted = match exchange_lower.as_str() {
            "binance" => self.convert_binance_symbol(base, quote, market_type),
            "okx" => self.convert_okx_symbol(base, quote, market_type),
            "bitmart" => self.convert_bitmart_symbol(base, quote, market_type),
            "bybit" => self.convert_bybit_symbol(base, quote, market_type),
            "hyperliquid" => self.convert_hyperliquid_symbol(base, quote, market_type),
            "htx" => self.convert_htx_symbol(base, quote, market_type),
            _ => {
                log::error!("Unsupported exchange in symbol converter: '{}'", exchange);
                Err(ExchangeError::UnsupportedExchange(exchange.to_string()))
            }
        }?;

        Ok(converted)
    }

    /// 将交易所特定格式转换为标准格式(BASE/QUOTE)
    pub fn from_exchange_symbol(
        &self,
        symbol: &str,
        exchange: &str,
        market_type: MarketType,
    ) -> Result<String, ExchangeError> {
        let (base, quote) = match exchange.to_lowercase().as_str() {
            "binance" => self.parse_binance_symbol(symbol, market_type)?,
            "okx" => self.parse_okx_symbol(symbol, market_type)?,
            "bitmart" => self.parse_bitmart_symbol(symbol, market_type)?,
            "bybit" => self.parse_bybit_symbol(symbol, market_type)?,
            "hyperliquid" => self.parse_hyperliquid_symbol(symbol, market_type)?,
            "htx" => self.parse_htx_symbol(symbol, market_type)?,
            _ => return Err(ExchangeError::UnsupportedExchange(exchange.to_string())),
        };

        Ok(format!("{}/{}", base, quote))
    }

    /// 币安交易对转换
    fn convert_binance_symbol(
        &self,
        base: &str,
        quote: &str,
        market_type: MarketType,
    ) -> Result<String, ExchangeError> {
        match market_type {
            MarketType::Spot => Ok(format!("{}{}", base.to_uppercase(), quote.to_uppercase())),
            MarketType::Futures => {
                // 币安期货通常使用 BTCUSDT 格式
                if quote.to_uppercase() == "USDT" {
                    Ok(format!("{}USDT", base.to_uppercase()))
                } else {
                    Ok(format!("{}{}", base.to_uppercase(), quote.to_uppercase()))
                }
            }
        }
    }

    /// OKX交易对转换
    fn convert_okx_symbol(
        &self,
        base: &str,
        quote: &str,
        market_type: MarketType,
    ) -> Result<String, ExchangeError> {
        match market_type {
            MarketType::Spot => Ok(format!("{}-{}", base.to_uppercase(), quote.to_uppercase())),
            MarketType::Futures => {
                // OKX永续合约格式: BTC-USDT-SWAP
                Ok(format!(
                    "{}-{}-SWAP",
                    base.to_uppercase(),
                    quote.to_uppercase()
                ))
            }
        }
    }

    /// Bitmart交易对转换
    fn convert_bitmart_symbol(
        &self,
        base: &str,
        quote: &str,
        market_type: MarketType,
    ) -> Result<String, ExchangeError> {
        match market_type {
            MarketType::Spot => Ok(format!("{}_{}", base.to_uppercase(), quote.to_uppercase())),
            MarketType::Futures => {
                // Bitmart期货格式: BTC_USDT
                Ok(format!("{}_{}", base.to_uppercase(), quote.to_uppercase()))
            }
        }
    }

    /// Bybit交易对转换
    fn convert_bybit_symbol(
        &self,
        base: &str,
        quote: &str,
        market_type: MarketType,
    ) -> Result<String, ExchangeError> {
        match market_type {
            MarketType::Spot => Ok(format!("{}{}", base.to_uppercase(), quote.to_uppercase())),
            MarketType::Futures => {
                // Bybit永续合约格式: BTCUSDT
                Ok(format!("{}{}", base.to_uppercase(), quote.to_uppercase()))
            }
        }
    }

    /// 解析币安交易对
    fn parse_binance_symbol(
        &self,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<(String, String), ExchangeError> {
        match market_type {
            MarketType::Spot | MarketType::Futures => {
                // 处理币安期货合约中的日期后缀（如BTCUSDT_250926）
                // 对于有到期日的期货合约，我们暂时跳过
                let clean_symbol = if symbol.contains('_') {
                    // 如果包含下划线，可能是有到期日的期货合约
                    // 暂时只处理永续合约（不含下划线的）
                    return Err(ExchangeError::SymbolError(format!(
                        "跳过有到期日的期货合约: {}",
                        symbol
                    )));
                } else {
                    symbol
                };

                // 常见的报价货币，按长度从长到短排序
                let quote_currencies = vec![
                    "USDT", "BUSD", "USDC", "TUSD", "FDUSD", "USDS", // 稳定币
                    "BTC", "ETH", "BNB", // 主要加密货币
                    "EUR", "GBP", "AUD", "TRY", "RUB", "UAH", "NGN", "VAI", "BVND", // 法币
                    "DAI", "PAX", "USDP", "UST", // 其他稳定币
                ];

                for quote in &quote_currencies {
                    if clean_symbol.ends_with(quote) {
                        let base = &clean_symbol[..clean_symbol.len() - quote.len()];
                        if !base.is_empty() {
                            return Ok((base.to_string(), quote.to_string()));
                        }
                    }
                }

                // 如果没有匹配到常见的报价货币，尝试其他可能的分割方式
                // 对于一些特殊情况，我们可以添加硬编码的映射
                let special_mappings = vec![
                    ("QTUMETH", ("QTUM", "ETH")),
                    ("ADAETH", ("ADA", "ETH")),
                    ("NEOETH", ("NEO", "ETH")),
                    ("XLMETH", ("XLM", "ETH")),
                    ("TRXETH", ("TRX", "ETH")),
                    ("TRXXRP", ("TRX", "XRP")),   // TRX对XRP
                    ("XZCXRP", ("XZC", "XRP")),   // XZC对XRP
                    ("BNBUSDS", ("BNB", "USDS")), // BNB对USDS
                ];

                for (sym, (base, quote)) in &special_mappings {
                    if symbol == *sym {
                        return Ok((base.to_string(), quote.to_string()));
                    }
                }

                Err(ExchangeError::SymbolError(format!(
                    "无法解析币安交易对: {}，请添加对应的映射规则",
                    symbol
                )))
            }
        }
    }

    /// 解析OKX交易对
    fn parse_okx_symbol(
        &self,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<(String, String), ExchangeError> {
        match market_type {
            MarketType::Spot => {
                let parts: Vec<&str> = symbol.split('-').collect();
                if parts.len() >= 2 {
                    Ok((parts[0].to_string(), parts[1].to_string()))
                } else {
                    Err(ExchangeError::SymbolError(format!(
                        "无法解析OKX现货交易对: {}",
                        symbol
                    )))
                }
            }
            MarketType::Futures => {
                // 处理 BTC-USDT-SWAP 格式
                let parts: Vec<&str> = symbol.split('-').collect();
                if parts.len() >= 3 && parts[2] == "SWAP" {
                    Ok((parts[0].to_string(), parts[1].to_string()))
                } else {
                    Err(ExchangeError::SymbolError(format!(
                        "无法解析OKX期货交易对: {}",
                        symbol
                    )))
                }
            }
        }
    }

    /// 解析Bitmart交易对
    fn parse_bitmart_symbol(
        &self,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<(String, String), ExchangeError> {
        let parts: Vec<&str> = symbol.split('_').collect();
        if parts.len() >= 2 {
            Ok((parts[0].to_string(), parts[1].to_string()))
        } else {
            Err(ExchangeError::SymbolError(format!(
                "无法解析Bitmart交易对: {}",
                symbol
            )))
        }
    }

    /// 解析Bybit交易对
    fn parse_bybit_symbol(
        &self,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<(String, String), ExchangeError> {
        match market_type {
            MarketType::Spot | MarketType::Futures => {
                // 类似币安的处理方式
                if symbol.ends_with("USDT") {
                    let base = &symbol[..symbol.len() - 4];
                    Ok((base.to_string(), "USDT".to_string()))
                } else if symbol.ends_with("BTC") {
                    let base = &symbol[..symbol.len() - 3];
                    Ok((base.to_string(), "BTC".to_string()))
                } else {
                    Err(ExchangeError::SymbolError(format!(
                        "无法解析Bybit交易对: {}",
                        symbol
                    )))
                }
            }
        }
    }

    /// Hyperliquid交易对转换
    fn convert_hyperliquid_symbol(
        &self,
        base: &str,
        quote: &str,
        market_type: MarketType,
    ) -> Result<String, ExchangeError> {
        match market_type {
            MarketType::Spot => {
                // Hyperliquid 现货使用 BASE/QUOTE 格式，如 PURR/USDC
                // 但有些也只用 BASE 名称
                if quote.to_uppercase() == "USDC" {
                    // 对于 USDC 交易对，使用 BASE/USDC 格式
                    Ok(format!("{}/USDC", base.to_uppercase()))
                } else {
                    // 其他情况只用币种名称
                    Ok(base.to_uppercase())
                }
            }
            MarketType::Futures => {
                // Hyperliquid 期货使用 BASE/USD 格式，如 BTC/USD, ETH/USD, CRV/USD
                // 即使输入是 USDT，也转换为 USD
                Ok(format!("{}/USD", base.to_uppercase()))
            }
        }
    }

    /// 解析Hyperliquid交易对
    fn parse_hyperliquid_symbol(
        &self,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<(String, String), ExchangeError> {
        match market_type {
            MarketType::Spot => {
                // 现货可能是 BASE/USDC 或只是 BASE
                if symbol.contains('/') {
                    let parts: Vec<&str> = symbol.split('/').collect();
                    if parts.len() == 2 {
                        Ok((parts[0].to_string(), parts[1].to_string()))
                    } else {
                        Err(ExchangeError::SymbolError(format!(
                            "无效的现货交易对格式: {}",
                            symbol
                        )))
                    }
                } else {
                    // 没有分隔符，默认对 USDC
                    Ok((symbol.to_string(), "USDC".to_string()))
                }
            }
            MarketType::Futures => {
                // 期货格式是 BASE/USD
                if symbol.contains('/') {
                    let parts: Vec<&str> = symbol.split('/').collect();
                    if parts.len() == 2 {
                        Ok((parts[0].to_string(), parts[1].to_string()))
                    } else {
                        Err(ExchangeError::SymbolError(format!(
                            "无效的期货交易对格式: {}",
                            symbol
                        )))
                    }
                } else {
                    // 没有分隔符，默认对 USD
                    Ok((symbol.to_string(), "USD".to_string()))
                }
            }
        }
    }

    /// HTX交易对转换
    fn convert_htx_symbol(
        &self,
        base: &str,
        quote: &str,
        market_type: MarketType,
    ) -> Result<String, ExchangeError> {
        match market_type {
            MarketType::Spot => Ok(format!("{}{}", base.to_lowercase(), quote.to_lowercase())),
            MarketType::Futures => {
                // HTX期货通常使用 BTC-USD 格式
                Ok(format!("{}-{}", base.to_uppercase(), quote.to_uppercase()))
            }
        }
    }

    /// 解析HTX交易对
    fn parse_htx_symbol(
        &self,
        symbol: &str,
        market_type: MarketType,
    ) -> Result<(String, String), ExchangeError> {
        match market_type {
            MarketType::Spot => {
                // HTX现货使用btcusdt格式
                let symbol_upper = symbol.to_uppercase();

                // 常见的报价货币
                let quote_currencies = vec!["USDT", "HUSD", "BTC", "ETH", "HT", "USDC", "TRX"];

                for quote in &quote_currencies {
                    if symbol_upper.ends_with(quote) {
                        let base = &symbol_upper[..symbol_upper.len() - quote.len()];
                        if !base.is_empty() {
                            return Ok((base.to_string(), quote.to_string()));
                        }
                    }
                }

                Err(ExchangeError::SymbolError(format!(
                    "无法解析HTX交易对: {}",
                    symbol
                )))
            }
            MarketType::Futures => {
                // 处理 BTC-USD 格式
                let parts: Vec<&str> = symbol.split('-').collect();
                if parts.len() >= 2 {
                    Ok((parts[0].to_string(), parts[1].to_string()))
                } else {
                    Err(ExchangeError::SymbolError(format!(
                        "无法解析HTX期货交易对: {}",
                        symbol
                    )))
                }
            }
        }
    }

    /// 批量转换交易对
    pub fn convert_symbols(
        &self,
        symbols: &[String],
        exchange: &str,
        market_type: MarketType,
    ) -> Result<HashMap<String, String>, ExchangeError> {
        let mut result = HashMap::new();

        for symbol in symbols {
            let converted = self.to_exchange_symbol(symbol, exchange, market_type)?;
            result.insert(symbol.clone(), converted);
        }

        Ok(result)
    }

    /// 批量转换（支持错误处理）
    pub fn batch_convert(
        &self,
        symbols: &[String],
        exchange: &str,
        market_type: MarketType,
        to_exchange: bool,
    ) -> HashMap<String, Result<String, String>> {
        let mut results = HashMap::new();

        for symbol in symbols {
            let result = if to_exchange {
                match self.to_exchange_symbol(symbol, exchange, market_type) {
                    Ok(converted) => Ok(converted),
                    Err(e) => Err(e.to_string()),
                }
            } else {
                match self.from_exchange_symbol(symbol, exchange, market_type) {
                    Ok(converted) => Ok(converted),
                    Err(e) => Err(e.to_string()),
                }
            };

            results.insert(symbol.clone(), result);
        }

        results
    }

    /// 验证交易对格式是否正确
    pub fn validate_symbol(&self, symbol: &str, exchange: &str, market_type: MarketType) -> bool {
        // 尝试转换，如果成功则格式正确
        self.to_exchange_symbol(symbol, exchange, market_type)
            .is_ok()
    }

    /// 获取交易所支持的所有报价货币
    pub fn get_quote_currencies() -> Vec<&'static str> {
        QUOTE_CURRENCIES.clone()
    }

    /// 智能识别交易对格式并返回标准格式
    pub fn normalize_symbol(&self, symbol: &str) -> Result<String, ExchangeError> {
        // 如果已经是标准格式，直接返回
        if symbol.contains('/') {
            let parts: Vec<&str> = symbol.split('/').collect();
            if parts.len() == 2 {
                return Ok(symbol.to_string());
            }
        }

        // 尝试各种解析方式
        if let Ok((base, quote)) = self.parse_standard_symbol(symbol) {
            return Ok(format!("{}/{}", base, quote));
        }

        Err(ExchangeError::SymbolError(format!(
            "无法规范化交易对: {}",
            symbol
        )))
    }

    /// 解析标准符号格式
    fn parse_standard_symbol(&self, symbol: &str) -> Result<(String, String), ExchangeError> {
        // 支持多种分隔符
        let separators = vec!["/", "-", "_"];

        for separator in separators {
            if symbol.contains(separator) {
                let parts: Vec<&str> = symbol.split(separator).collect();
                if parts.len() == 2 && !parts[0].is_empty() && !parts[1].is_empty() {
                    return Ok((parts[0].to_string(), parts[1].to_string()));
                }
            }
        }

        // 如果没有分隔符，尝试智能解析
        self.parse_no_separator_symbol(symbol, MarketType::Spot)
    }

    /// 解析无分隔符的交易对（如 BTCUSDT）
    fn parse_no_separator_symbol(
        &self,
        symbol: &str,
        _market_type: MarketType,
    ) -> Result<(String, String), ExchangeError> {
        let symbol_upper = symbol.to_uppercase();

        // 尝试从后往前匹配报价货币
        for quote in QUOTE_CURRENCIES.iter() {
            if symbol_upper.ends_with(quote) {
                let base = &symbol_upper[..symbol_upper.len() - quote.len()];
                if !base.is_empty() {
                    return Ok((base.to_string(), quote.to_string()));
                }
            }
        }

        Err(ExchangeError::SymbolError(format!(
            "无法解析交易对: {}",
            symbol
        )))
    }
}
