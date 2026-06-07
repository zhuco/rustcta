use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::GATEWAY_API_VERSION;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GatewayMode {
    Local,
    RemoteAgent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CredentialBoundary {
    GatewayOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayIdentity {
    pub gateway_id: String,
    pub mode: GatewayMode,
    pub credential_boundary: CredentialBoundary,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GatewayExchangeStatus {
    pub exchange: String,
    pub enabled: bool,
    pub public_stream_connected: bool,
    pub private_stream_connected: bool,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub rate_limit_used: Option<f64>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GatewayStatus {
    pub api_version: String,
    pub identity: GatewayIdentity,
    pub exchanges: Vec<GatewayExchangeStatus>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GatewayHealth {
    pub status: String,
    pub component: String,
    pub api_version: String,
    pub checked_at: DateTime<Utc>,
}

impl GatewayHealth {
    pub fn ok(checked_at: DateTime<Utc>) -> Self {
        Self {
            status: "ok".to_string(),
            component: "rustcta-exchange-gateway".to_string(),
            api_version: GATEWAY_API_VERSION.to_string(),
            checked_at,
        }
    }
}
