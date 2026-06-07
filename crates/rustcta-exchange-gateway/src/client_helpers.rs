use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderResponse, BalancesResponse, BatchCancelOrdersResponse, BatchPlaceOrdersResponse,
    CancelAllOrdersResponse, CancelOrderResponse, FeesResponse, OpenOrdersResponse,
    OrderBookResponse, OrderListResponse, PlaceOrderResponse, PositionsResponse,
    QueryOrderResponse, RecentFillsResponse, SymbolRulesResponse,
};
use rustcta_types::{AccountId, TenantId};

use crate::{
    GatewayError, GatewayOperation, GatewayProtocolRequest, GatewayProtocolResponse,
    GatewayRequestPayload, GatewayResponsePayload, GetCapabilitiesResponse, GetStatusResponse,
    SubscribeBooksResponse, SubscribePrivateResponse, GATEWAY_PROTOCOL_SCHEMA_VERSION,
};

impl GatewayProtocolResponse {
    pub fn into_status(self) -> Result<GetStatusResponse, GatewayError> {
        self.require_accepted()?;
        match self.payload {
            GatewayResponsePayload::Status(response) => Ok(response),
            other => Err(unexpected_payload(self.operation, other)),
        }
    }

    pub fn into_capabilities(self) -> Result<GetCapabilitiesResponse, GatewayError> {
        self.require_accepted()?;
        match self.payload {
            GatewayResponsePayload::Capabilities(response) => Ok(response),
            other => Err(unexpected_payload(self.operation, other)),
        }
    }

    pub fn into_balances(self) -> Result<BalancesResponse, GatewayError> {
        self.require_accepted()?;
        match self.payload {
            GatewayResponsePayload::Balances(response) => Ok(response),
            other => Err(unexpected_payload(self.operation, other)),
        }
    }

    pub fn into_positions(self) -> Result<PositionsResponse, GatewayError> {
        self.require_accepted()?;
        match self.payload {
            GatewayResponsePayload::Positions(response) => Ok(response),
            other => Err(unexpected_payload(self.operation, other)),
        }
    }

    pub fn into_symbol_rules(self) -> Result<SymbolRulesResponse, GatewayError> {
        self.require_accepted()?;
        match self.payload {
            GatewayResponsePayload::SymbolRules(response) => Ok(response),
            other => Err(unexpected_payload(self.operation, other)),
        }
    }

    pub fn into_order_book(self) -> Result<OrderBookResponse, GatewayError> {
        self.require_accepted()?;
        match self.payload {
            GatewayResponsePayload::OrderBook(response) => Ok(response),
            other => Err(unexpected_payload(self.operation, other)),
        }
    }

    pub fn into_fees(self) -> Result<FeesResponse, GatewayError> {
        self.require_accepted()?;
        match self.payload {
            GatewayResponsePayload::Fees(response) => Ok(response),
            other => Err(unexpected_payload(self.operation, other)),
        }
    }

    pub fn into_place_order(self) -> Result<PlaceOrderResponse, GatewayError> {
        self.require_accepted()?;
        match self.payload {
            GatewayResponsePayload::PlaceOrder(response) => Ok(response),
            other => Err(unexpected_payload(self.operation, other)),
        }
    }

    pub fn into_cancel_order(self) -> Result<CancelOrderResponse, GatewayError> {
        self.require_accepted()?;
        match self.payload {
            GatewayResponsePayload::CancelOrder(response) => Ok(response),
            other => Err(unexpected_payload(self.operation, other)),
        }
    }

    pub fn into_amend_order(self) -> Result<AmendOrderResponse, GatewayError> {
        self.require_accepted()?;
        match self.payload {
            GatewayResponsePayload::AmendOrder(response) => Ok(response),
            other => Err(unexpected_payload(self.operation, other)),
        }
    }

    pub fn into_order_list(self) -> Result<OrderListResponse, GatewayError> {
        self.require_accepted()?;
        match self.payload {
            GatewayResponsePayload::OrderList(response) => Ok(response),
            other => Err(unexpected_payload(self.operation, other)),
        }
    }

    pub fn into_batch_place_orders(self) -> Result<BatchPlaceOrdersResponse, GatewayError> {
        self.require_accepted()?;
        match self.payload {
            GatewayResponsePayload::BatchPlaceOrders(response) => Ok(response),
            other => Err(unexpected_payload(self.operation, other)),
        }
    }

    pub fn into_batch_cancel_orders(self) -> Result<BatchCancelOrdersResponse, GatewayError> {
        self.require_accepted()?;
        match self.payload {
            GatewayResponsePayload::BatchCancelOrders(response) => Ok(response),
            other => Err(unexpected_payload(self.operation, other)),
        }
    }

    pub fn into_cancel_all_orders(self) -> Result<CancelAllOrdersResponse, GatewayError> {
        self.require_accepted()?;
        match self.payload {
            GatewayResponsePayload::CancelAllOrders(response) => Ok(response),
            other => Err(unexpected_payload(self.operation, other)),
        }
    }

    pub fn into_query_order(self) -> Result<QueryOrderResponse, GatewayError> {
        self.require_accepted()?;
        match self.payload {
            GatewayResponsePayload::QueryOrder(response) => Ok(response),
            other => Err(unexpected_payload(self.operation, other)),
        }
    }

    pub fn into_open_orders(self) -> Result<OpenOrdersResponse, GatewayError> {
        self.require_accepted()?;
        match self.payload {
            GatewayResponsePayload::OpenOrders(response) => Ok(response),
            other => Err(unexpected_payload(self.operation, other)),
        }
    }

    pub fn into_recent_fills(self) -> Result<RecentFillsResponse, GatewayError> {
        self.require_accepted()?;
        match self.payload {
            GatewayResponsePayload::RecentFills(response) => Ok(response),
            other => Err(unexpected_payload(self.operation, other)),
        }
    }

    pub fn into_books_subscribed(self) -> Result<SubscribeBooksResponse, GatewayError> {
        self.require_accepted()?;
        match self.payload {
            GatewayResponsePayload::BooksSubscribed(response) => Ok(response),
            other => Err(unexpected_payload(self.operation, other)),
        }
    }

    pub fn into_private_subscribed(self) -> Result<SubscribePrivateResponse, GatewayError> {
        self.require_accepted()?;
        match self.payload {
            GatewayResponsePayload::PrivateSubscribed(response) => Ok(response),
            other => Err(unexpected_payload(self.operation, other)),
        }
    }

    fn require_accepted(&self) -> Result<(), GatewayError> {
        if self.accepted {
            return Ok(());
        }
        Err(GatewayError::Rejected(
            self.error
                .as_ref()
                .map(|error| error.message.clone())
                .unwrap_or_else(|| "gateway request was rejected".to_string()),
        ))
    }
}

pub(crate) fn gateway_protocol_request(
    request_id: String,
    tenant_id: TenantId,
    account_id: Option<AccountId>,
    operation: GatewayOperation,
    payload: GatewayRequestPayload,
) -> GatewayProtocolRequest {
    GatewayProtocolRequest {
        schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
        request_id,
        tenant_id,
        account_id,
        operation,
        payload,
        requested_at: Utc::now(),
    }
}

fn unexpected_payload(
    operation: GatewayOperation,
    payload: GatewayResponsePayload,
) -> GatewayError {
    GatewayError::InvalidPayload {
        message: format!(
            "gateway operation {} returned unexpected payload {payload:?}",
            operation.as_str()
        ),
    }
}
