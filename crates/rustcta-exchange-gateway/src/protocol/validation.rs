use rustcta_exchange_api::{PublicStreamKind, RequestContext, EXCHANGE_API_SCHEMA_VERSION};

use super::{GatewayProtocolRequest, GatewayRequestPayload};
use crate::{GatewayError, GATEWAY_PROTOCOL_SCHEMA_VERSION};

impl GatewayProtocolRequest {
    pub fn validate(&self) -> Result<(), GatewayError> {
        if self.schema_version != GATEWAY_PROTOCOL_SCHEMA_VERSION {
            return Err(GatewayError::Rejected(format!(
                "unsupported gateway schema_version {}, expected {}",
                self.schema_version, GATEWAY_PROTOCOL_SCHEMA_VERSION
            )));
        }

        let payload_operation = self.payload.operation();
        if self.operation != payload_operation {
            return Err(GatewayError::Rejected(format!(
                "gateway operation {} does not match payload {}",
                self.operation.as_str(),
                payload_operation.as_str()
            )));
        }

        match &self.payload {
            GatewayRequestPayload::GetStatus(request) => {
                if request.schema_version != GATEWAY_PROTOCOL_SCHEMA_VERSION {
                    return Err(schema_rejected(request.schema_version));
                }
            }
            GatewayRequestPayload::GetCapabilities(request) => {
                validate_exchange_schema(request.schema_version)?;
                self.validate_context(&request.context)?;
            }
            GatewayRequestPayload::GetBalances(request) => {
                validate_exchange_schema(request.schema_version)?;
                self.validate_context(&request.context)?;
            }
            GatewayRequestPayload::GetPositions(request) => {
                validate_exchange_schema(request.schema_version)?;
                self.validate_context(&request.context)?;
            }
            GatewayRequestPayload::GetSymbolRules(request) => {
                validate_exchange_schema(request.schema_version)?;
                self.validate_context(&request.context)?;
            }
            GatewayRequestPayload::GetOrderBook(request) => {
                validate_exchange_schema(request.schema_version)?;
                self.validate_context(&request.context)?;
            }
            GatewayRequestPayload::GetFees(request) => {
                validate_exchange_schema(request.schema_version)?;
                self.validate_context(&request.context)?;
            }
            GatewayRequestPayload::PlaceOrder(request) => {
                validate_exchange_schema(request.schema_version)?;
                self.validate_context(&request.context)?;
            }
            GatewayRequestPayload::PlaceQuoteMarketOrder(request) => {
                validate_exchange_schema(request.schema_version)?;
                self.validate_context(&request.context)?;
            }
            GatewayRequestPayload::CancelOrder(request) => {
                validate_exchange_schema(request.schema_version)?;
                self.validate_context(&request.context)?;
            }
            GatewayRequestPayload::AmendOrder(request) => {
                validate_exchange_schema(request.schema_version)?;
                self.validate_context(&request.context)?;
                if request.client_order_id.is_none() && request.exchange_order_id.is_none() {
                    return Err(GatewayError::Rejected(
                        "amend_order requires client_order_id or exchange_order_id".to_string(),
                    ));
                }
            }
            GatewayRequestPayload::PlaceOrderList(request) => {
                let symbol = request.symbol();
                let context = match request {
                    rustcta_exchange_api::OrderListRequest::Oco {
                        schema_version,
                        context,
                        ..
                    }
                    | rustcta_exchange_api::OrderListRequest::Oto {
                        schema_version,
                        context,
                        ..
                    } => {
                        validate_exchange_schema(*schema_version)?;
                        context
                    }
                };
                self.validate_context(context)?;
                if symbol.exchange.as_str().trim().is_empty() {
                    return Err(GatewayError::Rejected(
                        "place_order_list requires a symbol exchange".to_string(),
                    ));
                }
            }
            GatewayRequestPayload::BatchPlaceOrders(request) => {
                validate_exchange_schema(request.schema_version)?;
                self.validate_context(&request.context)?;
                if request.orders.is_empty() {
                    return Err(GatewayError::Rejected(
                        "batch_place_orders requires at least one order".to_string(),
                    ));
                }
                for order in &request.orders {
                    validate_exchange_schema(order.schema_version)?;
                    self.validate_context(&order.context)?;
                    if order.symbol.exchange != request.exchange {
                        return Err(GatewayError::Rejected(
                            "batch_place_orders order exchange does not match request exchange"
                                .to_string(),
                        ));
                    }
                }
            }
            GatewayRequestPayload::BatchCancelOrders(request) => {
                validate_exchange_schema(request.schema_version)?;
                self.validate_context(&request.context)?;
                if request.cancels.is_empty() {
                    return Err(GatewayError::Rejected(
                        "batch_cancel_orders requires at least one cancel".to_string(),
                    ));
                }
                for cancel in &request.cancels {
                    validate_exchange_schema(cancel.schema_version)?;
                    self.validate_context(&cancel.context)?;
                    if cancel.symbol.exchange != request.exchange {
                        return Err(GatewayError::Rejected(
                            "batch_cancel_orders cancel exchange does not match request exchange"
                                .to_string(),
                        ));
                    }
                    if cancel.client_order_id.is_none() && cancel.exchange_order_id.is_none() {
                        return Err(GatewayError::Rejected(
                            "batch_cancel_orders requires client_order_id or exchange_order_id"
                                .to_string(),
                        ));
                    }
                }
            }
            GatewayRequestPayload::CancelAllOrders(request) => {
                validate_exchange_schema(request.schema_version)?;
                self.validate_context(&request.context)?;
                if let Some(symbol) = &request.symbol {
                    if symbol.exchange != request.exchange {
                        return Err(GatewayError::Rejected(
                            "cancel_all_orders symbol exchange does not match request exchange"
                                .to_string(),
                        ));
                    }
                }
            }
            GatewayRequestPayload::QueryOrder(request) => {
                validate_exchange_schema(request.schema_version)?;
                self.validate_context(&request.context)?;
            }
            GatewayRequestPayload::GetOpenOrders(request) => {
                validate_exchange_schema(request.schema_version)?;
                self.validate_context(&request.context)?;
            }
            GatewayRequestPayload::GetRecentFills(request) => {
                validate_exchange_schema(request.schema_version)?;
                self.validate_context(&request.context)?;
            }
            GatewayRequestPayload::SubscribeBooks(request) => {
                if request.schema_version != GATEWAY_PROTOCOL_SCHEMA_VERSION {
                    return Err(schema_rejected(request.schema_version));
                }
                self.validate_context(&request.context)?;
                for subscription in &request.subscriptions {
                    validate_exchange_schema(subscription.schema_version)?;
                    self.validate_context(&subscription.context)?;
                    if !matches!(
                        subscription.kind,
                        PublicStreamKind::OrderBookDelta | PublicStreamKind::OrderBookSnapshot
                    ) {
                        return Err(GatewayError::Rejected(
                            "subscribe_books only accepts order-book stream kinds".to_string(),
                        ));
                    }
                }
            }
            GatewayRequestPayload::SubscribePrivate(request) => {
                if request.schema_version != GATEWAY_PROTOCOL_SCHEMA_VERSION {
                    return Err(schema_rejected(request.schema_version));
                }
                self.validate_context(&request.context)?;
                if request.subscriptions.is_empty() {
                    return Err(GatewayError::Rejected(
                        "subscribe_private requires at least one subscription".to_string(),
                    ));
                }
                for subscription in &request.subscriptions {
                    validate_exchange_schema(subscription.schema_version)?;
                    self.validate_context(&subscription.context)?;
                }
            }
        }

        Ok(())
    }

    fn validate_context(&self, context: &RequestContext) -> Result<(), GatewayError> {
        if let Some(context_request_id) = &context.request_id {
            if context_request_id != &self.request_id {
                return Err(GatewayError::Rejected(
                    "payload request_id does not match gateway envelope".to_string(),
                ));
            }
        }
        if let Some(context_tenant_id) = &context.tenant_id {
            if context_tenant_id != &self.tenant_id {
                return Err(GatewayError::Rejected(
                    "payload tenant_id does not match gateway envelope".to_string(),
                ));
            }
        }
        if let (Some(context_account_id), Some(account_id)) =
            (&context.account_id, &self.account_id)
        {
            if context_account_id != account_id {
                return Err(GatewayError::Rejected(
                    "payload account_id does not match gateway envelope".to_string(),
                ));
            }
        }
        Ok(())
    }
}

fn schema_rejected(schema_version: u16) -> GatewayError {
    GatewayError::Rejected(format!(
        "unsupported gateway schema_version {}, expected {}",
        schema_version, GATEWAY_PROTOCOL_SCHEMA_VERSION
    ))
}

fn validate_exchange_schema(schema_version: u16) -> Result<(), GatewayError> {
    if schema_version != EXCHANGE_API_SCHEMA_VERSION {
        return Err(GatewayError::Rejected(format!(
            "unsupported exchange schema_version {}, expected {}",
            schema_version, EXCHANGE_API_SCHEMA_VERSION
        )));
    }
    Ok(())
}
