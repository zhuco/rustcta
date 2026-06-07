use chrono::{DateTime, Utc};
use rustcta_event_ledger::{
    EventIdentity, EventKind, LedgerEvent, LedgerPayload, LedgerWriter, OrderLifecycleRecord,
};
use rustcta_exchange_api::{
    CancelAllOrdersRequest, CancelOrderRequest, PlaceOrderRequest, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_exchange_gateway::{
    ExchangeGateway, GatewayClient, GatewayError, GatewayOperation, GatewayProtocolRequest,
    GatewayRequest, GatewayRequestPayload, GatewayResponse, GatewayResponsePayload,
    GATEWAY_PROTOCOL_SCHEMA_VERSION,
};
use rustcta_execution_api::{
    CancelAck, CancelAllAck, CancelAllCommand, CancelCommand, ExecutionApiError, OrderAck,
    OrderCommand, OrderState, EXECUTION_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, OrderSide, OrderStatus, PositionSide, RunId, TenantId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RouterMode {
    DryRun,
    LiveDryRun,
    Live,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionIdentity {
    pub tenant_id: String,
    pub account_id: String,
    pub strategy_id: String,
    pub run_id: String,
    pub command_id: String,
    pub idempotency_key: String,
    pub risk_profile_id: String,
}

impl ExecutionIdentity {
    pub fn validate(&self) -> Result<(), ExecutionRouterError> {
        for (name, value) in [
            ("tenant_id", &self.tenant_id),
            ("account_id", &self.account_id),
            ("strategy_id", &self.strategy_id),
            ("run_id", &self.run_id),
            ("command_id", &self.command_id),
            ("idempotency_key", &self.idempotency_key),
            ("risk_profile_id", &self.risk_profile_id),
        ] {
            if value.trim().is_empty() {
                return Err(ExecutionRouterError::MissingIdentityField {
                    field: name.to_string(),
                });
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoutedExecutionCommand {
    pub identity: ExecutionIdentity,
    pub operation: String,
    #[serde(default)]
    pub payload: Value,
    pub requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoutedExecutionAck {
    pub identity: ExecutionIdentity,
    pub accepted: bool,
    pub dry_run: bool,
    #[serde(default)]
    pub payload: Value,
    pub message: Option<String>,
    pub acknowledged_at: DateTime<Utc>,
}

#[derive(Debug, Error)]
pub enum ExecutionRouterError {
    #[error("missing execution identity field {field}")]
    MissingIdentityField { field: String },
    #[error("gateway error: {0}")]
    Gateway(#[from] GatewayError),
    #[error("execution API error: {0}")]
    ExecutionApi(#[from] ExecutionApiError),
    #[error("gateway payload mismatch: {0}")]
    GatewayPayload(String),
    #[error("ledger error: {0}")]
    Ledger(#[from] rustcta_event_ledger::LedgerError),
}

#[derive(Debug, Clone)]
pub struct ExecutionRouterConfig {
    pub mode: RouterMode,
}

impl ExecutionRouterConfig {
    pub fn dry_run() -> Self {
        Self {
            mode: RouterMode::DryRun,
        }
    }
}

pub struct ExecutionRouter<G> {
    config: ExecutionRouterConfig,
    gateway: G,
    ledger: Option<Arc<dyn LedgerWriter>>,
}

impl<G> ExecutionRouter<G> {
    pub fn new(config: ExecutionRouterConfig, gateway: G) -> Self {
        Self {
            config,
            gateway,
            ledger: None,
        }
    }

    pub fn with_ledger(
        config: ExecutionRouterConfig,
        gateway: G,
        ledger: Arc<dyn LedgerWriter>,
    ) -> Self {
        Self {
            config,
            gateway,
            ledger: Some(ledger),
        }
    }
}

impl<G> ExecutionRouter<G>
where
    G: ExchangeGateway,
{
    pub async fn route(
        &self,
        command: RoutedExecutionCommand,
    ) -> Result<RoutedExecutionAck, ExecutionRouterError> {
        command.identity.validate()?;
        if matches!(
            self.config.mode,
            RouterMode::DryRun | RouterMode::LiveDryRun
        ) {
            return Ok(RoutedExecutionAck {
                identity: command.identity,
                accepted: false,
                dry_run: true,
                payload: command.payload,
                message: Some("execution router dry-run: gateway mutation not called".to_string()),
                acknowledged_at: Utc::now(),
            });
        }

        let response: GatewayResponse = self
            .gateway
            .handle(GatewayRequest {
                request_id: command.identity.command_id.clone(),
                tenant_id: command.identity.tenant_id.clone(),
                account_id: Some(command.identity.account_id.clone()),
                operation: command.operation,
                payload: command.payload,
                requested_at: command.requested_at,
            })
            .await?;

        Ok(RoutedExecutionAck {
            identity: command.identity,
            accepted: response.accepted,
            dry_run: false,
            payload: response.payload,
            message: response.error,
            acknowledged_at: response.responded_at,
        })
    }
}

impl<G> ExecutionRouter<G>
where
    G: GatewayClient,
{
    pub async fn place_order(
        &self,
        command: OrderCommand,
    ) -> Result<OrderAck, ExecutionRouterError> {
        command.validate()?;
        if matches!(
            self.config.mode,
            RouterMode::DryRun | RouterMode::LiveDryRun
        ) {
            self.append_order_command_event(&command).await?;
            let ack = OrderAck::rejected(
                &command,
                "execution router dry-run: gateway mutation not called",
                Utc::now(),
            );
            self.append_order_ack_event(&command, &ack).await?;
            return Ok(ack);
        }

        let gateway_request = gateway_request_for_place_order(&command);
        self.append_order_command_event(&command).await?;
        let response = self
            .gateway
            .send_request(
                gateway_request.request_id,
                gateway_request.tenant_id,
                gateway_request.account_id,
                gateway_request.operation,
                gateway_request.payload,
            )
            .await?;
        let place_order = match response.payload {
            GatewayResponsePayload::PlaceOrder(response) => response,
            other => {
                return Err(ExecutionRouterError::GatewayPayload(format!(
                    "expected place_order response, got {other:?}"
                )));
            }
        };

        let ack = OrderAck {
            schema_version: EXECUTION_API_SCHEMA_VERSION,
            tenant_id: command.identity.tenant_id.clone(),
            account_id: command.identity.account_id.clone(),
            strategy_id: command.identity.strategy_id.clone(),
            run_id: command.identity.run_id.clone(),
            command_id: command.command_id.clone(),
            exchange_id: command.exchange_id.clone(),
            client_order_id: command.client_order_id.clone(),
            exchange_order_id: place_order.order.exchange_order_id,
            idempotency_key: command.identity.idempotency_key.clone(),
            accepted: response.accepted && place_order.order.status != OrderStatus::Rejected,
            state: execution_order_state(place_order.order.status),
            message: response.error.map(|error| error.message),
            acknowledged_at: response.responded_at,
        };
        self.append_order_ack_event(&command, &ack).await?;
        Ok(ack)
    }

    pub async fn cancel_order(
        &self,
        command: CancelCommand,
    ) -> Result<CancelAck, ExecutionRouterError> {
        command.validate()?;
        if matches!(
            self.config.mode,
            RouterMode::DryRun | RouterMode::LiveDryRun
        ) {
            self.append_cancel_command_event(&command).await?;
            let ack = cancel_ack(
                &command,
                false,
                OrderState::Rejected,
                Some("execution router dry-run: gateway mutation not called".to_string()),
                Utc::now(),
            );
            self.append_cancel_ack_event(&command, &ack).await?;
            return Ok(ack);
        }

        let gateway_request = gateway_request_for_cancel_order(&command);
        self.append_cancel_command_event(&command).await?;
        let response = self
            .gateway
            .send_request(
                gateway_request.request_id,
                gateway_request.tenant_id,
                gateway_request.account_id,
                gateway_request.operation,
                gateway_request.payload,
            )
            .await?;
        let cancel_order = match response.payload {
            GatewayResponsePayload::CancelOrder(response) => response,
            other => {
                return Err(ExecutionRouterError::GatewayPayload(format!(
                    "expected cancel_order response, got {other:?}"
                )));
            }
        };

        let ack = cancel_ack(
            &command,
            response.accepted && cancel_order.cancelled,
            execution_order_state(cancel_order.order.status),
            response.error.map(|error| error.message),
            response.responded_at,
        );
        self.append_cancel_ack_event(&command, &ack).await?;
        Ok(ack)
    }

    pub async fn cancel_all_orders(
        &self,
        command: CancelAllCommand,
    ) -> Result<CancelAllAck, ExecutionRouterError> {
        command.validate()?;
        if matches!(
            self.config.mode,
            RouterMode::DryRun | RouterMode::LiveDryRun
        ) {
            self.append_cancel_all_command_event(&command).await?;
            let ack = cancel_all_ack(
                &command,
                false,
                0,
                Some("execution router dry-run: gateway mutation not called".to_string()),
                Utc::now(),
            );
            self.append_cancel_all_ack_event(&command, &ack).await?;
            return Ok(ack);
        }

        let gateway_request = gateway_request_for_cancel_all_orders(&command);
        self.append_cancel_all_command_event(&command).await?;
        let response = self
            .gateway
            .send_request(
                gateway_request.request_id,
                gateway_request.tenant_id,
                gateway_request.account_id,
                gateway_request.operation,
                gateway_request.payload,
            )
            .await?;
        let cancel_all = match response.payload {
            GatewayResponsePayload::CancelAllOrders(response) => response,
            other => {
                return Err(ExecutionRouterError::GatewayPayload(format!(
                    "expected cancel_all_orders response, got {other:?}"
                )));
            }
        };

        let ack = cancel_all_ack(
            &command,
            response.accepted,
            cancel_all.cancelled_count,
            response.error.map(|error| error.message),
            response.responded_at,
        );
        self.append_cancel_all_ack_event(&command, &ack).await?;
        Ok(ack)
    }

    async fn append_order_command_event(
        &self,
        command: &OrderCommand,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let mut record = order_lifecycle_record(
            EventKind::OrderCommandEvent,
            event_identity_for_order(command),
            command.exchange_id.clone(),
            command.market_type,
            command.canonical_symbol.clone(),
            command.client_order_id.clone(),
            command.side,
            command.position_side,
            OrderStatus::New,
            command.quantity,
        );
        record.requested_price = command.price;
        ledger.append(LedgerEvent::order(record)).await?;
        Ok(())
    }

    async fn append_order_ack_event(
        &self,
        command: &OrderCommand,
        ack: &OrderAck,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let mut record = order_lifecycle_record(
            EventKind::OrderAckEvent,
            event_identity_for_order(command),
            ack.exchange_id.clone(),
            command.market_type,
            command.canonical_symbol.clone(),
            ack.client_order_id.clone(),
            command.side,
            command.position_side,
            gateway_order_status_from_execution_state(ack.state),
            command.quantity,
        );
        record.requested_price = command.price;
        record.exchange_order_id = ack.exchange_order_id.clone();
        record.message = ack.message.clone();
        record.metadata = serde_json::json!({
            "accepted": ack.accepted
        });
        ledger.append(LedgerEvent::order(record)).await?;
        Ok(())
    }

    async fn append_cancel_command_event(
        &self,
        command: &CancelCommand,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let event = LedgerEvent::new(
            EventKind::CancelCommandEvent,
            event_identity_for_cancel(command),
            LedgerPayload::Raw(serde_json::json!({
                "schema_version": EXECUTION_API_SCHEMA_VERSION,
                "command_id": command.command_id,
                "exchange_id": command.exchange_id,
                "market_type": command.market_type,
                "canonical_symbol": command.canonical_symbol,
                "exchange_symbol": command.exchange_symbol,
                "client_order_id": command.cancellation_ids.client_order_id,
                "exchange_order_id": command.cancellation_ids.exchange_order_id,
                "reason": command.reason,
                "requested_at": command.identity.requested_at
            })),
        );
        ledger.append(event).await?;
        Ok(())
    }

    async fn append_cancel_ack_event(
        &self,
        command: &CancelCommand,
        ack: &CancelAck,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let event = LedgerEvent::new(
            EventKind::CancelAckEvent,
            event_identity_for_cancel(command),
            LedgerPayload::Raw(serde_json::json!({
                "schema_version": EXECUTION_API_SCHEMA_VERSION,
                "command_id": ack.command_id,
                "exchange_id": ack.exchange_id,
                "client_order_id": ack.client_order_id,
                "exchange_order_id": ack.exchange_order_id,
                "accepted": ack.accepted,
                "state": ack.state,
                "message": ack.message,
                "acknowledged_at": ack.acknowledged_at
            })),
        );
        ledger.append(event).await?;
        Ok(())
    }

    async fn append_cancel_all_command_event(
        &self,
        command: &CancelAllCommand,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let event = LedgerEvent::new(
            EventKind::CancelCommandEvent,
            event_identity_for_cancel_all(command),
            LedgerPayload::Raw(serde_json::json!({
                "schema_version": EXECUTION_API_SCHEMA_VERSION,
                "command_id": command.command_id,
                "exchange_id": command.exchange_id,
                "market_type": command.market_type,
                "canonical_symbol": command.canonical_symbol,
                "exchange_symbol": command.exchange_symbol,
                "cancel_all_id": command.cancel_all_id,
                "reason": command.reason,
                "requested_at": command.identity.requested_at
            })),
        );
        ledger.append(event).await?;
        Ok(())
    }

    async fn append_cancel_all_ack_event(
        &self,
        command: &CancelAllCommand,
        ack: &CancelAllAck,
    ) -> Result<(), ExecutionRouterError> {
        let Some(ledger) = &self.ledger else {
            return Ok(());
        };
        let event = LedgerEvent::new(
            EventKind::CancelAckEvent,
            event_identity_for_cancel_all(command),
            LedgerPayload::Raw(serde_json::json!({
                "schema_version": EXECUTION_API_SCHEMA_VERSION,
                "command_id": ack.command_id,
                "exchange_id": ack.exchange_id,
                "cancel_all_id": ack.cancel_all_id,
                "accepted": ack.accepted,
                "cancelled_orders": ack.cancelled_orders,
                "message": ack.message,
                "acknowledged_at": ack.acknowledged_at
            })),
        );
        ledger.append(event).await?;
        Ok(())
    }
}

fn gateway_request_for_place_order(command: &OrderCommand) -> GatewayProtocolRequest {
    GatewayProtocolRequest {
        schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
        request_id: command.command_id.clone(),
        tenant_id: command.identity.tenant_id.clone(),
        account_id: Some(command.identity.account_id.clone()),
        operation: GatewayOperation::PlaceOrder,
        payload: GatewayRequestPayload::PlaceOrder(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: request_context(command),
            symbol: SymbolScope {
                exchange: command.exchange_id.clone(),
                market_type: command.market_type,
                canonical_symbol: Some(command.canonical_symbol.clone()),
                exchange_symbol: command.exchange_symbol.clone(),
            },
            client_order_id: Some(command.client_order_id.clone()),
            side: command.side,
            position_side: Some(command.position_side),
            order_type: command.order_type,
            time_in_force: Some(command.time_in_force),
            quantity: command.quantity.to_string(),
            price: command.price.map(|price| price.to_string()),
            quote_quantity: None,
            reduce_only: command.reduce_only,
            post_only: command.post_only,
        }),
        requested_at: command.identity.requested_at,
    }
}

fn gateway_request_for_cancel_order(command: &CancelCommand) -> GatewayProtocolRequest {
    GatewayProtocolRequest {
        schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
        request_id: command.command_id.clone(),
        tenant_id: command.identity.tenant_id.clone(),
        account_id: Some(command.identity.account_id.clone()),
        operation: GatewayOperation::CancelOrder,
        payload: GatewayRequestPayload::CancelOrder(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: request_context(command),
            symbol: SymbolScope {
                exchange: command.exchange_id.clone(),
                market_type: command.market_type,
                canonical_symbol: Some(command.canonical_symbol.clone()),
                exchange_symbol: command.exchange_symbol.clone(),
            },
            client_order_id: command.cancellation_ids.client_order_id.clone(),
            exchange_order_id: command.cancellation_ids.exchange_order_id.clone(),
        }),
        requested_at: command.identity.requested_at,
    }
}

fn gateway_request_for_cancel_all_orders(command: &CancelAllCommand) -> GatewayProtocolRequest {
    GatewayProtocolRequest {
        schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
        request_id: command.command_id.clone(),
        tenant_id: command.identity.tenant_id.clone(),
        account_id: Some(command.identity.account_id.clone()),
        operation: GatewayOperation::CancelAllOrders,
        payload: GatewayRequestPayload::CancelAllOrders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: request_context(command),
            exchange: command.exchange_id.clone(),
            market_type: Some(command.market_type),
            symbol: command
                .exchange_symbol
                .clone()
                .map(|exchange_symbol| SymbolScope {
                    exchange: command.exchange_id.clone(),
                    market_type: command.market_type,
                    canonical_symbol: command.canonical_symbol.clone(),
                    exchange_symbol,
                }),
        }),
        requested_at: command.identity.requested_at,
    }
}

fn request_context<C>(command: &C) -> RequestContext
where
    C: CommandContext,
{
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(command.tenant_id()),
        account_id: Some(command.account_id()),
        run_id: Some(command.run_id()),
        request_id: Some(command.command_id()),
        requested_at: command.requested_at(),
    }
}

trait CommandContext {
    fn tenant_id(&self) -> TenantId;
    fn account_id(&self) -> AccountId;
    fn run_id(&self) -> RunId;
    fn command_id(&self) -> String;
    fn requested_at(&self) -> DateTime<Utc>;
}

impl CommandContext for OrderCommand {
    fn tenant_id(&self) -> TenantId {
        self.identity.tenant_id.clone()
    }

    fn account_id(&self) -> AccountId {
        self.identity.account_id.clone()
    }

    fn run_id(&self) -> RunId {
        self.identity.run_id.clone()
    }

    fn command_id(&self) -> String {
        self.command_id.clone()
    }

    fn requested_at(&self) -> DateTime<Utc> {
        self.identity.requested_at
    }
}

impl CommandContext for CancelCommand {
    fn tenant_id(&self) -> TenantId {
        self.identity.tenant_id.clone()
    }

    fn account_id(&self) -> AccountId {
        self.identity.account_id.clone()
    }

    fn run_id(&self) -> RunId {
        self.identity.run_id.clone()
    }

    fn command_id(&self) -> String {
        self.command_id.clone()
    }

    fn requested_at(&self) -> DateTime<Utc> {
        self.identity.requested_at
    }
}

impl CommandContext for CancelAllCommand {
    fn tenant_id(&self) -> TenantId {
        self.identity.tenant_id.clone()
    }

    fn account_id(&self) -> AccountId {
        self.identity.account_id.clone()
    }

    fn run_id(&self) -> RunId {
        self.identity.run_id.clone()
    }

    fn command_id(&self) -> String {
        self.command_id.clone()
    }

    fn requested_at(&self) -> DateTime<Utc> {
        self.identity.requested_at
    }
}

fn cancel_ack(
    command: &CancelCommand,
    accepted: bool,
    state: OrderState,
    message: Option<String>,
    acknowledged_at: DateTime<Utc>,
) -> CancelAck {
    CancelAck {
        schema_version: EXECUTION_API_SCHEMA_VERSION,
        tenant_id: command.identity.tenant_id.clone(),
        account_id: command.identity.account_id.clone(),
        strategy_id: command.identity.strategy_id.clone(),
        run_id: command.identity.run_id.clone(),
        command_id: command.command_id.clone(),
        exchange_id: command.exchange_id.clone(),
        client_order_id: command.cancellation_ids.client_order_id.clone(),
        exchange_order_id: command.cancellation_ids.exchange_order_id.clone(),
        idempotency_key: command.identity.idempotency_key.clone(),
        accepted,
        state,
        message,
        acknowledged_at,
    }
}

fn cancel_all_ack(
    command: &CancelAllCommand,
    accepted: bool,
    cancelled_orders: u32,
    message: Option<String>,
    acknowledged_at: DateTime<Utc>,
) -> CancelAllAck {
    CancelAllAck {
        schema_version: EXECUTION_API_SCHEMA_VERSION,
        tenant_id: command.identity.tenant_id.clone(),
        account_id: command.identity.account_id.clone(),
        strategy_id: command.identity.strategy_id.clone(),
        run_id: command.identity.run_id.clone(),
        command_id: command.command_id.clone(),
        exchange_id: command.exchange_id.clone(),
        cancel_all_id: command.cancel_all_id.clone(),
        idempotency_key: command.identity.idempotency_key.clone(),
        accepted,
        cancelled_orders,
        message,
        acknowledged_at,
    }
}

#[allow(clippy::too_many_arguments)]
fn order_lifecycle_record(
    event_kind: EventKind,
    identity: EventIdentity,
    exchange_id: rustcta_types::ExchangeId,
    market_type: rustcta_types::MarketType,
    canonical_symbol: rustcta_types::CanonicalSymbol,
    client_order_id: impl Into<String>,
    side: OrderSide,
    position_side: PositionSide,
    status: OrderStatus,
    requested_quantity: f64,
) -> OrderLifecycleRecord {
    OrderLifecycleRecord::new(
        identity,
        event_kind,
        exchange_id,
        market_type,
        canonical_symbol,
        client_order_id,
        side,
        position_side,
        status,
        requested_quantity,
    )
}

fn event_identity_for_order(command: &OrderCommand) -> EventIdentity {
    EventIdentity::new(
        command.identity.tenant_id.clone(),
        "rustcta-execution-router",
        command.identity.requested_at,
    )
    .with_account(command.identity.account_id.clone())
    .with_strategy_run(
        command.identity.strategy_id.clone(),
        command.identity.run_id.clone(),
    )
    .with_command(command.command_id.clone())
    .with_idempotency_key(command.identity.idempotency_key.clone())
}

fn event_identity_for_cancel(command: &CancelCommand) -> EventIdentity {
    EventIdentity::new(
        command.identity.tenant_id.clone(),
        "rustcta-execution-router",
        command.identity.requested_at,
    )
    .with_account(command.identity.account_id.clone())
    .with_strategy_run(
        command.identity.strategy_id.clone(),
        command.identity.run_id.clone(),
    )
    .with_command(command.command_id.clone())
    .with_idempotency_key(command.identity.idempotency_key.clone())
}

fn event_identity_for_cancel_all(command: &CancelAllCommand) -> EventIdentity {
    EventIdentity::new(
        command.identity.tenant_id.clone(),
        "rustcta-execution-router",
        command.identity.requested_at,
    )
    .with_account(command.identity.account_id.clone())
    .with_strategy_run(
        command.identity.strategy_id.clone(),
        command.identity.run_id.clone(),
    )
    .with_command(command.command_id.clone())
    .with_idempotency_key(command.identity.idempotency_key.clone())
}

fn gateway_order_status_from_execution_state(state: OrderState) -> OrderStatus {
    match state {
        OrderState::Planned => OrderStatus::New,
        OrderState::Submitted => OrderStatus::New,
        OrderState::Accepted => OrderStatus::Open,
        OrderState::PartiallyFilled => OrderStatus::PartiallyFilled,
        OrderState::Filled => OrderStatus::Filled,
        OrderState::CancelRequested => OrderStatus::PendingCancel,
        OrderState::Cancelled => OrderStatus::Cancelled,
        OrderState::Rejected => OrderStatus::Rejected,
        OrderState::Expired => OrderStatus::Expired,
        OrderState::Failed | OrderState::Unknown => OrderStatus::Unknown,
    }
}

fn execution_order_state(status: OrderStatus) -> OrderState {
    match status {
        OrderStatus::New => OrderState::Submitted,
        OrderStatus::Open => OrderState::Accepted,
        OrderStatus::PartiallyFilled => OrderState::PartiallyFilled,
        OrderStatus::Filled => OrderState::Filled,
        OrderStatus::PendingCancel => OrderState::CancelRequested,
        OrderStatus::Cancelled => OrderState::Cancelled,
        OrderStatus::Rejected => OrderState::Rejected,
        OrderStatus::Expired => OrderState::Expired,
        OrderStatus::Unknown => OrderState::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use rustcta_event_ledger::{InMemoryLedger, LedgerReader};
    use rustcta_exchange_gateway::{
        CredentialBoundary, GatewayIdentity, GatewayMode, GatewayStatus, InProcessGatewayClient,
        MockExchangeGateway,
    };
    use rustcta_execution_api::{CancellationIds, MutationIdentity};
    use rustcta_types::{
        AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
        PositionSide, RunId, StrategyId, TenantId, TimeInForce,
    };
    use std::sync::Arc;

    struct MockGateway;

    #[async_trait]
    impl ExchangeGateway for MockGateway {
        async fn status(&self) -> Result<GatewayStatus, GatewayError> {
            Ok(GatewayStatus {
                api_version: "test".to_string(),
                identity: GatewayIdentity {
                    gateway_id: "mock".to_string(),
                    mode: GatewayMode::Local,
                    credential_boundary: CredentialBoundary::GatewayOnly,
                    started_at: Utc::now(),
                },
                exchanges: Vec::new(),
            })
        }

        async fn handle(&self, request: GatewayRequest) -> Result<GatewayResponse, GatewayError> {
            Ok(GatewayResponse {
                request_id: request.request_id,
                accepted: true,
                payload: Value::Null,
                error: None,
                responded_at: Utc::now(),
            })
        }
    }

    #[tokio::test]
    async fn dry_run_router_should_not_call_gateway_mutation() {
        let router = ExecutionRouter::new(ExecutionRouterConfig::dry_run(), MockGateway);
        let ack = router
            .route(RoutedExecutionCommand {
                identity: ExecutionIdentity {
                    tenant_id: "tenant".to_string(),
                    account_id: "account".to_string(),
                    strategy_id: "strategy".to_string(),
                    run_id: "run".to_string(),
                    command_id: "cmd".to_string(),
                    idempotency_key: "idem".to_string(),
                    risk_profile_id: "risk".to_string(),
                },
                operation: "place_order".to_string(),
                payload: Value::Null,
                requested_at: Utc::now(),
            })
            .await
            .expect("dry-run ack");

        assert!(!ack.accepted);
        assert!(ack.dry_run);
    }

    #[tokio::test]
    async fn live_router_should_send_typed_order_and_cancel_through_gateway_client() {
        let gateway = Arc::new(MockExchangeGateway::with_exchanges(
            "mock-gateway",
            [exchange_id()],
        ));
        let client = InProcessGatewayClient::new(gateway);
        let router = ExecutionRouter::new(
            ExecutionRouterConfig {
                mode: RouterMode::Live,
            },
            client,
        );

        let order = order_command();
        let ack = router.place_order(order.clone()).await.expect("order ack");
        assert!(ack.accepted);
        assert_eq!(ack.state, OrderState::Accepted);
        assert_eq!(ack.client_order_id, "cli-typed-1");
        assert!(ack.exchange_order_id.is_some());

        let cancel = CancelCommand::new(
            order.identity,
            "cmd-cancel",
            exchange_id(),
            MarketType::Spot,
            canonical_symbol(),
            exchange_symbol(),
            CancellationIds::by_client_order_id("cli-typed-1"),
        );
        let cancel_ack = router.cancel_order(cancel).await.expect("cancel ack");
        assert!(cancel_ack.accepted);
        assert_eq!(cancel_ack.state, OrderState::Cancelled);

        let second_order = OrderCommand::new(
            identity(),
            "cmd-order-2",
            exchange_id(),
            MarketType::Spot,
            canonical_symbol(),
            exchange_symbol(),
            "cli-typed-2",
            OrderSide::Buy,
            PositionSide::None,
            OrderType::Limit,
            TimeInForce::GTC,
            0.01,
            Some(100.0),
        );
        let second_ack = router
            .place_order(second_order)
            .await
            .expect("second order ack");
        assert!(second_ack.accepted);

        let cancel_all = CancelAllCommand::new(
            identity(),
            "cmd-cancel-all",
            exchange_id(),
            MarketType::Spot,
            "cancel-all-1",
        )
        .for_symbol(canonical_symbol(), exchange_symbol());
        let cancel_all_ack = router
            .cancel_all_orders(cancel_all)
            .await
            .expect("cancel-all ack");
        assert!(cancel_all_ack.accepted);
        assert_eq!(cancel_all_ack.cancelled_orders, 1);
    }

    #[tokio::test]
    async fn live_router_should_append_order_and_cancel_events_to_ledger() {
        let gateway = Arc::new(MockExchangeGateway::with_exchanges(
            "mock-gateway",
            [exchange_id()],
        ));
        let client = InProcessGatewayClient::new(gateway);
        let ledger = Arc::new(InMemoryLedger::new());
        let router = ExecutionRouter::with_ledger(
            ExecutionRouterConfig {
                mode: RouterMode::Live,
            },
            client,
            ledger.clone(),
        );

        let order = order_command();
        router
            .place_order(order.clone())
            .await
            .expect("order should route");
        let cancel = CancelCommand::new(
            order.identity,
            "cmd-cancel",
            exchange_id(),
            MarketType::Spot,
            canonical_symbol(),
            exchange_symbol(),
            CancellationIds::by_client_order_id("cli-typed-1"),
        );
        router
            .cancel_order(cancel)
            .await
            .expect("cancel should route");
        let cancel_all = CancelAllCommand::new(
            identity(),
            "cmd-cancel-all",
            exchange_id(),
            MarketType::Spot,
            "cancel-all-1",
        );
        router
            .cancel_all_orders(cancel_all)
            .await
            .expect("cancel-all should route");

        let events = ledger.replay(None).await.expect("replay events");
        assert_eq!(
            events.iter().map(|event| event.kind).collect::<Vec<_>>(),
            vec![
                EventKind::OrderCommandEvent,
                EventKind::OrderAckEvent,
                EventKind::CancelCommandEvent,
                EventKind::CancelAckEvent,
                EventKind::CancelCommandEvent,
                EventKind::CancelAckEvent,
            ]
        );
        assert_eq!(
            events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5, 6]
        );
        assert!(events
            .iter()
            .all(|event| event.identity.command_id.is_some()));
    }

    #[tokio::test]
    async fn dry_run_typed_router_should_append_rejected_order_and_cancel_events_to_ledger() {
        let gateway = Arc::new(MockExchangeGateway::with_exchanges(
            "mock-gateway",
            [exchange_id()],
        ));
        let client = InProcessGatewayClient::new(gateway);
        let ledger = Arc::new(InMemoryLedger::new());
        let router =
            ExecutionRouter::with_ledger(ExecutionRouterConfig::dry_run(), client, ledger.clone());

        let order = order_command();
        let order_ack = router
            .place_order(order.clone())
            .await
            .expect("dry-run order should route");
        assert!(!order_ack.accepted);
        assert_eq!(order_ack.state, OrderState::Rejected);

        let cancel = CancelCommand::new(
            order.identity,
            "cmd-cancel",
            exchange_id(),
            MarketType::Spot,
            canonical_symbol(),
            exchange_symbol(),
            CancellationIds::by_client_order_id("cli-typed-1"),
        );
        let cancel_ack = router
            .cancel_order(cancel)
            .await
            .expect("dry-run cancel should route");
        assert!(!cancel_ack.accepted);
        assert_eq!(cancel_ack.state, OrderState::Rejected);

        let cancel_all = CancelAllCommand::new(
            identity(),
            "cmd-cancel-all",
            exchange_id(),
            MarketType::Spot,
            "cancel-all-1",
        );
        let cancel_all_ack = router
            .cancel_all_orders(cancel_all)
            .await
            .expect("dry-run cancel-all should route");
        assert!(!cancel_all_ack.accepted);
        assert_eq!(cancel_all_ack.cancelled_orders, 0);

        let events = ledger.replay(None).await.expect("replay events");
        assert_eq!(
            events.iter().map(|event| event.kind).collect::<Vec<_>>(),
            vec![
                EventKind::OrderCommandEvent,
                EventKind::OrderAckEvent,
                EventKind::CancelCommandEvent,
                EventKind::CancelAckEvent,
                EventKind::CancelCommandEvent,
                EventKind::CancelAckEvent,
            ]
        );
        assert!(events
            .iter()
            .all(|event| event.identity.idempotency_key.is_some()));
    }

    fn order_command() -> OrderCommand {
        OrderCommand::new(
            identity(),
            "cmd-order",
            exchange_id(),
            MarketType::Spot,
            canonical_symbol(),
            exchange_symbol(),
            "cli-typed-1",
            OrderSide::Buy,
            PositionSide::None,
            OrderType::Limit,
            TimeInForce::GTC,
            0.01,
            Some(100.0),
        )
    }

    fn identity() -> MutationIdentity {
        MutationIdentity {
            tenant_id: TenantId::new("tenant").expect("tenant id"),
            account_id: AccountId::new("account").expect("account id"),
            strategy_id: StrategyId::new("strategy").expect("strategy id"),
            run_id: RunId::new("run").expect("run id"),
            idempotency_key: "idem-typed".to_string(),
            risk_profile_id: "risk".to_string(),
            requested_at: Utc::now(),
        }
    }

    fn exchange_id() -> ExchangeId {
        ExchangeId::new("paper").expect("exchange id")
    }

    fn canonical_symbol() -> CanonicalSymbol {
        CanonicalSymbol::new("BTC", "USDT").expect("canonical symbol")
    }

    fn exchange_symbol() -> ExchangeSymbol {
        ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTCUSDT").expect("exchange symbol")
    }
}
