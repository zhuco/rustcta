pub use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeBalance as Balance, ExchangeError, ExchangeId,
    ExchangePosition as Position, ExchangeSymbol, Fill, MarketType, OrderBookSnapshot, OrderSide,
    OrderStatus, OrderType, PositionSide, RunId, TenantId, TimeInForce,
};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitPlan {
    Unsupported {
        reason: String,
    },
    FixedWindow {
        buckets: Vec<RateLimitBucket>,
    },
    SlidingWindow {
        buckets: Vec<RateLimitBucket>,
    },
    ExchangeHeader {
        buckets: Vec<RateLimitBucket>,
        header_names: Vec<String>,
    },
}

impl RateLimitPlan {
    pub fn unsupported(reason: impl Into<String>) -> Self {
        Self::Unsupported {
            reason: reason.into(),
        }
    }

    pub fn fixed_window(buckets: impl IntoIterator<Item = RateLimitBucket>) -> Self {
        Self::FixedWindow {
            buckets: buckets.into_iter().collect(),
        }
    }

    pub fn sliding_window(buckets: impl IntoIterator<Item = RateLimitBucket>) -> Self {
        Self::SlidingWindow {
            buckets: buckets.into_iter().collect(),
        }
    }

    pub fn exchange_header(
        buckets: impl IntoIterator<Item = RateLimitBucket>,
        header_names: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::ExchangeHeader {
            buckets: buckets.into_iter().collect(),
            header_names: header_names.into_iter().map(Into::into).collect(),
        }
    }

    pub fn buckets(&self) -> &[RateLimitBucket] {
        match self {
            Self::Unsupported { .. } => &[],
            Self::FixedWindow { buckets }
            | Self::SlidingWindow { buckets }
            | Self::ExchangeHeader { buckets, .. } => buckets,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Unsupported { reason } if reason.trim().is_empty() => {
                Err("unsupported rate-limit plan requires a reason".to_string())
            }
            Self::Unsupported { .. } => Ok(()),
            Self::FixedWindow { buckets } | Self::SlidingWindow { buckets } => {
                validate_rate_limit_buckets(buckets)
            }
            Self::ExchangeHeader {
                buckets,
                header_names,
            } => {
                validate_rate_limit_buckets(buckets)?;
                if header_names.is_empty() {
                    return Err(
                        "exchange-header rate-limit plan requires at least one header".to_string(),
                    );
                }
                if header_names.iter().any(|name| name.trim().is_empty()) {
                    return Err("exchange-header rate-limit plan has an empty header".to_string());
                }
                Ok(())
            }
        }
    }
}

fn validate_rate_limit_buckets(buckets: &[RateLimitBucket]) -> Result<(), String> {
    if buckets.is_empty() {
        return Err("rate-limit plan requires at least one bucket".to_string());
    }
    for bucket in buckets {
        bucket.validate()?;
    }
    Ok(())
}

fn default_rate_limit_cost() -> u32 {
    1
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RateLimitBucket {
    pub name: String,
    pub scope: RateLimitScope,
    pub limit: u32,
    pub window_ms: u64,
    #[serde(default = "default_rate_limit_cost")]
    pub cost: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub burst: Option<u32>,
}

impl RateLimitBucket {
    pub fn new(name: impl Into<String>, scope: RateLimitScope, limit: u32, window_ms: u64) -> Self {
        Self {
            name: name.into(),
            scope,
            limit,
            window_ms,
            cost: 1,
            burst: None,
        }
    }

    pub fn with_cost(mut self, cost: u32) -> Self {
        self.cost = cost;
        self
    }

    pub fn with_burst(mut self, burst: u32) -> Self {
        self.burst = Some(burst);
        self
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("rate-limit bucket name must not be empty".to_string());
        }
        if self.limit == 0 {
            return Err(format!(
                "rate-limit bucket {} limit must be positive",
                self.name
            ));
        }
        if self.window_ms == 0 {
            return Err(format!(
                "rate-limit bucket {} window_ms must be positive",
                self.name
            ));
        }
        if self.cost == 0 {
            return Err(format!(
                "rate-limit bucket {} cost must be positive",
                self.name
            ));
        }
        if let Some(burst) = self.burst {
            if burst == 0 {
                return Err(format!(
                    "rate-limit bucket {} burst must be positive",
                    self.name
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitScope {
    Rest,
    WebSocket,
    Orders,
    Cancels,
    UserStream,
    Endpoint,
    Ip,
    Account,
    Global,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PageRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<PageCursor>,
}

impl PageRequest {
    pub fn new(limit: Option<u32>, cursor: Option<PageCursor>) -> Self {
        Self { limit, cursor }
    }

    pub fn first_page(limit: u32) -> Self {
        Self {
            limit: Some(limit),
            cursor: None,
        }
    }

    pub fn next_page(limit: Option<u32>, cursor: PageCursor) -> Self {
        Self {
            limit,
            cursor: Some(cursor),
        }
    }

    pub fn validate(&self, max_limit: Option<u32>) -> Result<(), String> {
        if let Some(limit) = self.limit {
            if limit == 0 {
                return Err("page request limit must be positive".to_string());
            }
            if let Some(max_limit) = max_limit {
                if limit > max_limit {
                    return Err(format!(
                        "page request limit {limit} exceeds max_limit {max_limit}"
                    ));
                }
            }
        }
        if self.cursor.as_ref().is_some_and(PageCursor::is_empty) {
            return Err("page request cursor must not be empty".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PageCursor {
    Offset {
        offset: u64,
    },
    Id {
        id: String,
    },
    Timestamp {
        millis: i64,
    },
    Token {
        token: String,
    },
    TimeRange {
        start_ms: i64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        end_ms: Option<i64>,
    },
}

impl PageCursor {
    pub fn token(token: impl Into<String>) -> Self {
        Self::Token {
            token: token.into(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Offset { .. } | Self::Timestamp { .. } | Self::TimeRange { .. } => false,
            Self::Id { id } => id.trim().is_empty(),
            Self::Token { token } => token.trim().is_empty(),
        }
    }
}
