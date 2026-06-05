use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::Utc;
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::exchanges::unified::MarketType;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientOrderId(String);

impl ClientOrderId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for ClientOrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientOrderIdPolicy {
    pub exchange: String,
    pub market_type: MarketType,
    pub exchange_prefix: &'static str,
    pub market_prefix: &'static str,
    pub max_len: usize,
    pub min_len: usize,
    pub allowed_chars: ClientOrderIdAllowedChars,
    pub uppercase_only: bool,
    pub allow_cancel_lookup: bool,
    pub duplicate_policy: ClientOrderIdDuplicatePolicy,
    pub notes: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientOrderIdAllowedChars {
    UppercaseAlnum,
    AlnumUnderscore,
    AlnumHyphenUnderscore,
}

impl ClientOrderIdAllowedChars {
    fn accepts(self, ch: char) -> bool {
        match self {
            Self::UppercaseAlnum => ch.is_ascii_uppercase() || ch.is_ascii_digit(),
            Self::AlnumUnderscore => ch.is_ascii_alphanumeric() || ch == '_',
            Self::AlnumHyphenUnderscore => ch.is_ascii_alphanumeric() || ch == '_' || ch == '-',
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientOrderIdDuplicatePolicy {
    RejectsDuplicates,
    UnknownConservative,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ClientOrderIdValidationError {
    #[error("client_order_id must not be empty")]
    Empty,
    #[error("client_order_id length {actual} exceeds max {max}")]
    TooLong { actual: usize, max: usize },
    #[error("client_order_id length {actual} is below min {min}")]
    TooShort { actual: usize, min: usize },
    #[error("client_order_id contains invalid character '{0}'")]
    InvalidCharacter(char),
    #[error("client_order_id must be uppercase for this exchange")]
    NotUppercase,
}

#[derive(Debug, Default)]
pub struct ClientOrderIdGenerator {
    counter: AtomicU64,
}

impl ClientOrderIdGenerator {
    pub const fn new() -> Self {
        Self {
            counter: AtomicU64::new(1),
        }
    }

    pub fn generate(
        &self,
        exchange: &str,
        market_type: MarketType,
        strategy_prefix: &str,
    ) -> ClientOrderId {
        let policy = policy_for(exchange, market_type);
        let strategy = sanitize_prefix(strategy_prefix);
        let counter = self.counter.fetch_add(1, Ordering::Relaxed);
        let suffix = random_suffix(4);
        let timestamp = Utc::now().timestamp_millis();
        let preferred = format!(
            "{}_{}_{}_{}_{}{}",
            policy.exchange_prefix, policy.market_prefix, strategy, timestamp, counter, suffix
        );
        let value = if preferred.len() <= policy.max_len {
            preferred
        } else {
            compact_id(&policy, &strategy, timestamp, counter, &suffix)
        };
        ClientOrderId(value)
    }
}

pub static GLOBAL_CLIENT_ORDER_ID_GENERATOR: ClientOrderIdGenerator = ClientOrderIdGenerator::new();

pub fn generate_client_order_id(
    exchange: &str,
    market_type: MarketType,
    strategy_prefix: &str,
) -> ClientOrderId {
    GLOBAL_CLIENT_ORDER_ID_GENERATOR.generate(exchange, market_type, strategy_prefix)
}

pub fn validate_client_order_id(
    exchange: &str,
    market_type: MarketType,
    client_order_id: &str,
) -> Result<(), ClientOrderIdValidationError> {
    policy_for(exchange, market_type).validate(client_order_id)
}

impl ClientOrderIdPolicy {
    pub fn validate(&self, client_order_id: &str) -> Result<(), ClientOrderIdValidationError> {
        let value = client_order_id.trim();
        if value.is_empty() {
            return Err(ClientOrderIdValidationError::Empty);
        }
        if value.len() < self.min_len {
            return Err(ClientOrderIdValidationError::TooShort {
                actual: value.len(),
                min: self.min_len,
            });
        }
        if value.len() > self.max_len {
            return Err(ClientOrderIdValidationError::TooLong {
                actual: value.len(),
                max: self.max_len,
            });
        }
        if self.uppercase_only && value != value.to_ascii_uppercase() {
            return Err(ClientOrderIdValidationError::NotUppercase);
        }
        for ch in value.chars() {
            if !self.allowed_chars.accepts(ch) {
                return Err(ClientOrderIdValidationError::InvalidCharacter(ch));
            }
        }
        Ok(())
    }
}

pub fn policy_for(exchange: &str, market_type: MarketType) -> ClientOrderIdPolicy {
    let normalized = exchange.trim().to_ascii_lowercase();
    match (normalized.as_str(), market_type) {
        ("paper", MarketType::Spot) => paper_policy("paper", market_type, "PP", "SPT", 64),
        ("paper", MarketType::Perpetual) => paper_policy("paper", market_type, "PP", "PERP", 64),
        ("mexc" | "mxc", MarketType::Spot) => policy("mexc", market_type, "MX", "SPT", 32),
        ("coinex" | "coin_ex", MarketType::Spot) => policy("coinex", market_type, "CX", "SPT", 32),
        ("binance", MarketType::Spot) => policy("binance", market_type, "BN", "SPT", 36),
        ("binance", MarketType::Perpetual) => policy("binance", market_type, "BN", "PERP", 36),
        ("bitget", MarketType::Spot) => policy("bitget", market_type, "BG", "SPT", 32),
        ("okx", MarketType::Spot) => policy("okx", market_type, "OKX", "SPT", 32),
        ("okx", MarketType::Perpetual) => policy("okx", market_type, "OKX", "PERP", 32),
        ("bitmart", MarketType::Spot) => policy("bitmart", market_type, "BM", "SPT", 32),
        ("bitmart", MarketType::Perpetual) => policy("bitmart", market_type, "BM", "PERP", 32),
        ("hyperliquid", MarketType::Perpetual) => {
            policy("hyperliquid", market_type, "HL", "PERP", 32)
        }
        ("gate" | "gateio" | "gate.io", MarketType::Spot) => {
            policy("gateio", market_type, "GT", "SPT", 32)
        }
        ("kucoin", MarketType::Spot) => policy("kucoin", market_type, "KC", "SPT", 32),
        ("bybit", MarketType::Perpetual) => policy("bybit", market_type, "BB", "PERP", 32),
        ("htx" | "huobi", MarketType::Perpetual) => policy("htx", market_type, "HTX", "PERP", 32),
        _ => policy("other", market_type, "EX", market_prefix(market_type), 32),
    }
}

fn paper_policy(
    exchange: &'static str,
    market_type: MarketType,
    exchange_prefix: &'static str,
    market_prefix: &'static str,
    max_len: usize,
) -> ClientOrderIdPolicy {
    ClientOrderIdPolicy {
        exchange: exchange.to_string(),
        market_type,
        exchange_prefix,
        market_prefix,
        max_len,
        min_len: 4,
        allowed_chars: ClientOrderIdAllowedChars::AlnumHyphenUnderscore,
        uppercase_only: false,
        allow_cancel_lookup: true,
        duplicate_policy: ClientOrderIdDuplicatePolicy::RejectsDuplicates,
        notes: "Paper policy accepts existing RustCTA deterministic IDs such as crossarb-ls-mk-1-deadbeef while still enforcing length and character safety.",
    }
}

fn policy(
    exchange: &'static str,
    market_type: MarketType,
    exchange_prefix: &'static str,
    market_prefix: &'static str,
    max_len: usize,
) -> ClientOrderIdPolicy {
    ClientOrderIdPolicy {
        exchange: exchange.to_string(),
        market_type,
        exchange_prefix,
        market_prefix,
        max_len,
        min_len: 4,
        allowed_chars: ClientOrderIdAllowedChars::AlnumUnderscore,
        uppercase_only: true,
        allow_cancel_lookup: true,
        duplicate_policy: ClientOrderIdDuplicatePolicy::UnknownConservative,
        notes: "Conservative RustCTA policy. Tighten with official venue-specific rules when adding live order paths.",
    }
}

fn market_prefix(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "SPT",
        MarketType::Perpetual => "PERP",
    }
}

pub fn sanitize_prefix(prefix: &str) -> String {
    let sanitized = prefix
        .trim()
        .to_ascii_uppercase()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .take(8)
        .collect::<String>();
    if sanitized.is_empty() {
        "GEN".to_string()
    } else {
        sanitized
    }
}

pub fn truncate_safely(input: &str, max_len: usize) -> String {
    input.chars().take(max_len).collect()
}

pub fn extract_metadata(client_order_id: &str) -> Option<ClientOrderIdMetadata> {
    let mut parts = client_order_id.split('_');
    let exchange_prefix = parts.next()?.to_string();
    let market_prefix = parts.next()?.to_string();
    let strategy_prefix = parts.next()?.to_string();
    let timestamp = parts.next()?.parse::<i64>().ok();
    Some(ClientOrderIdMetadata {
        exchange_prefix,
        market_prefix,
        strategy_prefix,
        timestamp,
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientOrderIdMetadata {
    pub exchange_prefix: String,
    pub market_prefix: String,
    pub strategy_prefix: String,
    pub timestamp: Option<i64>,
}

fn compact_id(
    policy: &ClientOrderIdPolicy,
    strategy: &str,
    timestamp: i64,
    counter: u64,
    suffix: &str,
) -> String {
    let mut value = format!(
        "{}{}{}{}{}",
        policy.exchange_prefix,
        policy.market_prefix.chars().next().unwrap_or('X'),
        truncate_safely(strategy, 3),
        timestamp,
        suffix
    );
    if value.len() < policy.max_len {
        value.push_str(&(counter % 10_000).to_string());
    }
    truncate_safely(&value, policy.max_len)
}

fn random_suffix(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .filter(|ch| ch.is_ascii_alphanumeric())
        .take(len)
        .map(char::from)
        .map(|ch| ch.to_ascii_uppercase())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::thread;

    use super::*;

    #[test]
    fn generate_valid_ids_for_supported_exchange_policies() {
        let generator = ClientOrderIdGenerator::new();
        for (exchange, market_type) in [
            ("mexc", MarketType::Spot),
            ("coinex", MarketType::Spot),
            ("binance", MarketType::Spot),
            ("binance", MarketType::Perpetual),
            ("okx", MarketType::Spot),
            ("okx", MarketType::Perpetual),
        ] {
            let id = generator.generate(exchange, market_type, "arb");
            validate_client_order_id(exchange, market_type, id.as_str()).unwrap();
            assert!(id.as_str().len() <= policy_for(exchange, market_type).max_len);
        }
    }

    #[test]
    fn validation_rejects_invalid_characters_and_length() {
        assert!(matches!(
            validate_client_order_id("mexc", MarketType::Spot, "bad/id"),
            Err(ClientOrderIdValidationError::NotUppercase)
                | Err(ClientOrderIdValidationError::InvalidCharacter('/'))
        ));
        assert!(matches!(
            validate_client_order_id("mexc", MarketType::Spot, &"A".repeat(64)),
            Err(ClientOrderIdValidationError::TooLong { .. })
        ));
    }

    #[test]
    fn strategy_prefix_is_sanitized_deterministically() {
        assert_eq!(sanitize_prefix("arb-v1 spot"), "ARBV1SPO");
        assert_eq!(sanitize_prefix("___"), "GEN");
    }

    #[test]
    fn explicit_caller_id_is_validated() {
        validate_client_order_id("okx", MarketType::Perpetual, "OKX_PERP_ARB_1").unwrap();
        assert!(validate_client_order_id("okx", MarketType::Perpetual, "okx_perp_arb_1").is_err());
    }

    #[test]
    fn paper_policy_accepts_existing_crossarb_idempotency_ids() {
        validate_client_order_id("paper", MarketType::Perpetual, "crossarb-ls-mk-1-deadbeef")
            .unwrap();
        validate_client_order_id("paper", MarketType::Spot, "crossarb-sim-tk-2-a1b2c3d4").unwrap();
        assert!(validate_client_order_id("paper", MarketType::Spot, "bad/id").is_err());
    }

    #[test]
    fn generated_ids_are_unique_under_concurrency() {
        let generator = Arc::new(ClientOrderIdGenerator::new());
        let mut handles = Vec::new();
        for _ in 0..8 {
            let generator = generator.clone();
            handles.push(thread::spawn(move || {
                (0..256)
                    .map(|_| {
                        generator
                            .generate("mexc", MarketType::Spot, "arb")
                            .into_string()
                    })
                    .collect::<Vec<_>>()
            }));
        }
        let ids = handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        let unique = ids.iter().collect::<HashSet<_>>();
        assert_eq!(ids.len(), unique.len());
    }

    #[test]
    fn metadata_can_be_extracted_from_verbose_id() {
        let metadata = extract_metadata("BN_SPT_ARB_1780577005_A1B2").unwrap();
        assert_eq!(metadata.exchange_prefix, "BN");
        assert_eq!(metadata.market_prefix, "SPT");
        assert_eq!(metadata.strategy_prefix, "ARB");
        assert_eq!(metadata.timestamp, Some(1_780_577_005));
    }
}
