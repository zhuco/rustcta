use std::str::FromStr;

use chrono::{DateTime, Utc};
use rustcta_types::{AccountId, TenantId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{GatewayOperation, GatewayProtocolRequest, GatewayRequestPayload, GetStatusRequest};
use crate::{GatewayError, GATEWAY_PROTOCOL_SCHEMA_VERSION};

/// Compatibility request for the early gateway scaffold.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GatewayRequest {
    pub request_id: String,
    pub tenant_id: String,
    pub account_id: Option<String>,
    pub operation: String,
    #[serde(default)]
    pub payload: Value,
    pub requested_at: DateTime<Utc>,
}

/// Compatibility response for the early gateway scaffold.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GatewayResponse {
    pub request_id: String,
    pub accepted: bool,
    #[serde(default)]
    pub payload: Value,
    pub error: Option<String>,
    pub responded_at: DateTime<Utc>,
}

pub(crate) fn legacy_request_to_typed(
    request: GatewayRequest,
) -> Result<GatewayProtocolRequest, GatewayError> {
    let operation = GatewayOperation::from_str(&request.operation)?;
    let tenant_id = TenantId::new(request.tenant_id).map_err(|error| {
        GatewayError::Rejected(format!("invalid tenant_id in gateway request: {error}"))
    })?;
    let account_id = request
        .account_id
        .map(AccountId::new)
        .transpose()
        .map_err(|error| GatewayError::Rejected(format!("invalid account_id: {error}")))?;
    let payload = typed_payload_from_legacy_operation(operation, request.payload)?;
    Ok(GatewayProtocolRequest {
        schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
        request_id: request.request_id,
        tenant_id,
        account_id,
        operation,
        payload,
        requested_at: request.requested_at,
    })
}

fn typed_payload_from_legacy_operation(
    operation: GatewayOperation,
    payload: Value,
) -> Result<GatewayRequestPayload, GatewayError> {
    let payload = match operation {
        GatewayOperation::GetStatus => {
            if payload.is_null() || payload.as_object().is_some_and(|object| object.is_empty()) {
                GatewayRequestPayload::GetStatus(GetStatusRequest::default())
            } else {
                GatewayRequestPayload::GetStatus(decode_payload(payload)?)
            }
        }
        GatewayOperation::GetCapabilities => {
            GatewayRequestPayload::GetCapabilities(decode_payload(payload)?)
        }
        GatewayOperation::GetBalances => {
            GatewayRequestPayload::GetBalances(decode_payload(payload)?)
        }
        GatewayOperation::GetPositions => {
            GatewayRequestPayload::GetPositions(decode_payload(payload)?)
        }
        GatewayOperation::GetSymbolRules => {
            GatewayRequestPayload::GetSymbolRules(decode_payload(payload)?)
        }
        GatewayOperation::GetOrderBook => {
            GatewayRequestPayload::GetOrderBook(decode_payload(payload)?)
        }
        GatewayOperation::GetFees => GatewayRequestPayload::GetFees(decode_payload(payload)?),
        GatewayOperation::PlaceOrder => GatewayRequestPayload::PlaceOrder(decode_payload(payload)?),
        GatewayOperation::PlaceQuoteMarketOrder => {
            GatewayRequestPayload::PlaceQuoteMarketOrder(decode_payload(payload)?)
        }
        GatewayOperation::CancelOrder => {
            GatewayRequestPayload::CancelOrder(decode_payload(payload)?)
        }
        GatewayOperation::AmendOrder => GatewayRequestPayload::AmendOrder(decode_payload(payload)?),
        GatewayOperation::PlaceOrderList => {
            GatewayRequestPayload::PlaceOrderList(decode_payload(payload)?)
        }
        GatewayOperation::BatchPlaceOrders => {
            GatewayRequestPayload::BatchPlaceOrders(decode_payload(payload)?)
        }
        GatewayOperation::BatchCancelOrders => {
            GatewayRequestPayload::BatchCancelOrders(decode_payload(payload)?)
        }
        GatewayOperation::CancelAllOrders => {
            GatewayRequestPayload::CancelAllOrders(decode_payload(payload)?)
        }
        GatewayOperation::QueryOrder => GatewayRequestPayload::QueryOrder(decode_payload(payload)?),
        GatewayOperation::GetOpenOrders => {
            GatewayRequestPayload::GetOpenOrders(decode_payload(payload)?)
        }
        GatewayOperation::GetRecentFills => {
            GatewayRequestPayload::GetRecentFills(decode_payload(payload)?)
        }
        GatewayOperation::SubscribeBooks => {
            GatewayRequestPayload::SubscribeBooks(decode_payload(payload)?)
        }
        GatewayOperation::SubscribePrivate => {
            GatewayRequestPayload::SubscribePrivate(decode_payload(payload)?)
        }
    };
    Ok(payload)
}

fn decode_payload<T>(payload: Value) -> Result<T, GatewayError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(payload).map_err(|error| GatewayError::InvalidPayload {
        message: error.to_string(),
    })
}
