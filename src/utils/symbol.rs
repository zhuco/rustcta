// src/utils/symbol.rs

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

/// 市场类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarketType {
    Spot,       // 现货
    UsdFutures, // U本位永续合约
}

/// 统一的交易对符号
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct Symbol {
    pub base: String,            // 交易货币
    pub quote: String,           // 计价货币
    pub market_type: MarketType, // 市场类型
}

impl Symbol {
    /// 创建一个新的Symbol
    pub fn new(base: &str, quote: &str, market_type: MarketType) -> Self {
        Self {
            base: base.to_uppercase(),
            quote: quote.to_uppercase(),
            market_type,
        }
    }

    /// 将Symbol转换为币安格式的字符串
    pub fn to_binance(&self) -> String {
        match self.market_type {
            MarketType::Spot => format!("{}{}", self.base, self.quote),
            MarketType::UsdFutures => format!("{}{}", self.base, self.quote),
        }
    }

    /// 从字符串解析Symbol
    pub fn from_str(s: &str, market_type: MarketType) -> Result<Self, String> {
        if let Some(base) = s.strip_suffix("USDC") {
            Ok(Symbol::new(base, "USDC", market_type))
        } else if let Some(base) = s.strip_suffix("USDT") {
            Ok(Symbol::new(base, "USDT", market_type))
        } else {
            Err(format!("Unsupported symbol format: {s}"))
        }
    }
}

struct SymbolVisitor;

impl<'de> Visitor<'de> for SymbolVisitor {
    type Value = Symbol;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string representing a trading symbol")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        // 支持多种格式：
        // 1. "OMNI/USDT:USDT" -> base: "OMNI", quote: "USDT"
        // 2. "OMNIUSDT" -> base: "OMNI", quote: "USDT"

        // 首先尝试解析 "BASE/QUOTE:QUOTE" 格式
        if value.contains('/') && value.contains(':') {
            let parts: Vec<&str> = value.split('/').collect();
            if parts.len() == 2 {
                let base = parts[0];
                let quote_part: Vec<&str> = parts[1].split(':').collect();
                if quote_part.len() == 2 && quote_part[0] == quote_part[1] {
                    let quote = quote_part[0];
                    return Ok(Symbol::new(base, quote, MarketType::UsdFutures));
                }
            }
        }

        // 然后尝试解析简单格式 "BASEUSDC" 或 "BASEUSDT"
        if let Some(base) = value.strip_suffix("USDC") {
            Ok(Symbol::new(base, "USDC", MarketType::UsdFutures))
        } else if let Some(base) = value.strip_suffix("USDT") {
            Ok(Symbol::new(base, "USDT", MarketType::UsdFutures))
        } else {
            Err(de::Error::custom(format!(
                "Unsupported symbol format: {value}"
            )))
        }
    }
}

impl<'de> Deserialize<'de> for Symbol {
    fn deserialize<D>(deserializer: D) -> Result<Symbol, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(SymbolVisitor)
    }
}
