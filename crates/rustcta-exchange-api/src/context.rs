use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{AccountId, ExchangeId, RunId, TenantId, EXCHANGE_API_SCHEMA_VERSION};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestContext {
    pub schema_version: u16,
    pub tenant_id: Option<TenantId>,
    pub account_id: Option<AccountId>,
    pub run_id: Option<RunId>,
    pub request_id: Option<String>,
    pub requested_at: DateTime<Utc>,
}

impl RequestContext {
    pub fn new(requested_at: DateTime<Utc>) -> Self {
        Self {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            tenant_id: None,
            account_id: None,
            run_id: None,
            request_id: None,
            requested_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponseMetadata {
    pub schema_version: u16,
    pub exchange: ExchangeId,
    pub request_id: Option<String>,
    pub received_at: DateTime<Utc>,
    pub exchange_timestamp: Option<DateTime<Utc>>,
}

impl ResponseMetadata {
    pub fn new(exchange: ExchangeId, received_at: DateTime<Utc>) -> Self {
        Self {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            exchange,
            request_id: None,
            received_at,
            exchange_timestamp: None,
        }
    }
}
