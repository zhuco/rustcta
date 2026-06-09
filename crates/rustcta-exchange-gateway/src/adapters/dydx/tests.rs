use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, BatchAtomicity, BatchCancelOrdersRequest, BatchExecutionMode,
    BatchPlaceOrdersRequest, CancelOrderRequest, CapabilitySupport, CredentialScope, EndpointAuth,
    ExchangeApiError, ExchangeClient, PlaceOrderRequest, RequestContext, SymbolScope,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderSide, OrderType,
    TenantId,
};
use serde_json::Value;

use super::parser::{parse_markets, parse_orderbook};
use super::private_parser::{parse_balances, parse_fills, parse_orders, parse_positions};
use super::signing::{
    unsupported_node_signing_boundary, AMEND_PROJECT_UNIMPLEMENTED,
    BATCH_CANCEL_PROJECT_UNIMPLEMENTED, BATCH_PLACE_PROJECT_UNIMPLEMENTED,
};
use super::streams;
use super::{DydxGatewayAdapter, DydxGatewayConfig};

fn exchange_id() -> ExchangeId {
    ExchangeId::new("dydx").unwrap()
}

fn btc_scope() -> SymbolScope {
    let exchange = exchange_id();
    SymbolScope {
        exchange: exchange.clone(),
        market_type: MarketType::Perpetual,
        canonical_symbol: Some(CanonicalSymbol::new("BTC", "USD").unwrap()),
        exchange_symbol: ExchangeSymbol::new(exchange, MarketType::Perpetual, "BTC-USD").unwrap(),
    }
}

fn fixture(name: &str) -> Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/dydx/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("fixture");
    serde_json::from_str(&text).expect("json")
}

fn request_context(label: &str) -> RequestContext {
    let mut context = RequestContext::new(Utc::now());
    context.request_id = Some(format!("dydx-{label}"));
    context
}

fn limit_order(label: &str) -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: request_context(label),
        symbol: btc_scope(),
        client_order_id: Some(format!("{label}-client-order")),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Limit,
        time_in_force: None,
        quantity: "0.001".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}

fn cancel_order(label: &str) -> CancelOrderRequest {
    CancelOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: request_context(label),
        symbol: btc_scope(),
        client_order_id: Some(format!("{label}-client-order")),
        exchange_order_id: Some(format!("{label}-exchange-order")),
    }
}

fn assert_unsupported_operation(error: ExchangeApiError, expected: &'static str) {
    assert!(matches!(
        error,
        ExchangeApiError::Unsupported { operation } if operation == expected
    ));
}

#[test]
fn dydx_parser_should_parse_public_fixtures() {
    let exchange = exchange_id();
    let rules = parse_markets(
        &exchange,
        &[btc_scope()],
        &fixture("perpetual_markets.json"),
    )
    .unwrap();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].base_asset, "BTC");
    assert_eq!(rules[0].price_increment.as_deref(), Some("0.1"));

    let book = parse_orderbook(&exchange, btc_scope(), &fixture("orderbook.json")).unwrap();
    assert_eq!(book.best_bid().unwrap().price, 65000.0);
    assert_eq!(book.best_ask().unwrap().price, 65001.0);
}

#[test]
fn dydx_parser_should_parse_private_indexer_fixtures() {
    let exchange = exchange_id();
    let tenant = TenantId::new("tenant").unwrap();
    let account = AccountId::new("account").unwrap();
    let balances = parse_balances(
        &exchange,
        tenant.clone(),
        account.clone(),
        &fixture("subaccount.json"),
    )
    .unwrap();
    assert_eq!(balances[0].balances[0].asset, "USDC");

    let positions = parse_positions(
        &exchange,
        tenant.clone(),
        account.clone(),
        &[],
        &fixture("positions.json"),
    )
    .unwrap();
    assert_eq!(positions[0].quantity, 0.02);

    let orders = parse_orders(&exchange, None, &fixture("orders.json")).unwrap();
    assert_eq!(orders[0].exchange_order_id.as_deref(), Some("order-1"));

    let fills = parse_fills(&exchange, tenant, account, None, &fixture("fills.json")).unwrap();
    assert_eq!(fills[0].fill_id.as_deref(), Some("fill-1"));
}

#[test]
fn dydx_node_signing_fixture_should_remain_unsupported() {
    let boundary = fixture("signing_vectors/chain_signing_boundary.json");
    assert_eq!(boundary["status"], "unsupported");
    assert_eq!(boundary["secret_material"], "redacted_none");

    let fixture = fixture("signing_vectors/node_write_unsupported.json");
    assert_eq!(fixture["supported"], false);
    let error = unsupported_node_signing_boundary().expect_err("unsupported");
    assert!(error
        .to_string()
        .contains(fixture["expected_error"].as_str().unwrap()));
}

#[test]
fn dydx_advanced_order_source_fixture_should_separate_boundaries() {
    let source = fixture("request_specs/advanced_orders_chain_tx_source_boundary.json");
    assert_eq!(source["runtime_status"], "offline_source_boundary");
    assert_eq!(source["trade_enabled"], false);
    assert_eq!(source["parser_boundaries"].as_array().unwrap().len(), 3);
    assert_eq!(
        source["unsupported_separation"]["project_unimplemented"][0],
        "amend_order"
    );
    assert_eq!(
        source["unsupported_separation"]["unsupported"][0],
        "place_order_list"
    );
}

#[test]
fn dydx_advanced_order_parser_boundary_fixtures_should_remain_disabled() {
    for (name, expected_operation, expected_error) in [
        (
            "parser/amend_order_tx_response_boundary.json",
            "amend_order",
            AMEND_PROJECT_UNIMPLEMENTED,
        ),
        (
            "parser/batch_place_orders_tx_response_boundary.json",
            "batch_place_orders",
            BATCH_PLACE_PROJECT_UNIMPLEMENTED,
        ),
        (
            "parser/batch_cancel_orders_tx_response_boundary.json",
            "batch_cancel_orders",
            BATCH_CANCEL_PROJECT_UNIMPLEMENTED,
        ),
    ] {
        let parser = fixture(name);
        assert_eq!(parser["operation"], expected_operation);
        assert_eq!(parser["parser_enabled"], false);
        assert_eq!(parser["expected_error"], expected_error);
        assert_eq!(parser["trade_enabled"], false);
    }
}

#[test]
fn dydx_capabilities_v2_should_expose_runtime_disabled_chain_tx_boundaries() {
    let mut config = DydxGatewayConfig::default();
    config.enabled_private_indexer_rest = true;
    config.enabled_private_streams = true;
    config.wallet_address = Some("dydx1testaddress000000000000000000000000000000".to_string());
    let adapter = DydxGatewayAdapter::new(config).expect("adapter");
    let capabilities = adapter.capabilities();

    assert!(capabilities.capabilities_v2.private_rest.is_supported());
    assert!(capabilities.capabilities_v2.private_streams.is_supported());
    assert!(!capabilities.supports_batch_place_order);
    assert!(!capabilities.supports_batch_cancel_order);
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.atomicity,
        BatchAtomicity::Partial
    );
    assert!(
        capabilities
            .capabilities_v2
            .batch_place_orders
            .supports_partial_failure
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_cancel_orders.mode,
        BatchExecutionMode::Native
    );
    assert!(matches!(
        capabilities.capabilities_v2.batch_cancel_orders.support,
        CapabilitySupport::Unsupported { ref reason }
            if reason == BATCH_CANCEL_PROJECT_UNIMPLEMENTED
    ));

    let amend = capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .find(|endpoint| endpoint.operation == "amend_order")
        .expect("amend endpoint");
    assert!(matches!(
        amend.support,
        CapabilitySupport::Unsupported { ref reason }
            if reason == AMEND_PROJECT_UNIMPLEMENTED
    ));
    assert_eq!(amend.method.as_deref(), Some("CHAIN_TX"));
    assert_eq!(amend.auth, EndpointAuth::Hmac);
    assert!(amend.credential_scopes.contains(&CredentialScope::Trade));

    let order_list = capabilities
        .capabilities_v2
        .endpoints
        .iter()
        .find(|endpoint| endpoint.operation == "place_order_list")
        .expect("order-list endpoint");
    assert!(matches!(
        order_list.support,
        CapabilitySupport::Unsupported { .. }
    ));
}

#[tokio::test]
async fn dydx_advanced_order_runtime_should_remain_project_unimplemented() {
    let mut config = DydxGatewayConfig::default();
    config.enabled_node_private_write = true;
    config.wallet_address = Some("dydx1testaddress000000000000000000000000000000".to_string());
    let adapter = DydxGatewayAdapter::new(config).expect("adapter");

    let amend_spec = fixture("request_specs/amend_order_node_tx_boundary.json");
    let amend_error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: request_context("amend"),
            symbol: btc_scope(),
            client_order_id: Some("amend-client-order".to_string()),
            exchange_order_id: Some("amend-exchange-order".to_string()),
            new_client_order_id: None,
            new_quantity: "0.002".to_string(),
        })
        .await
        .expect_err("project-unimplemented amend");
    assert_eq!(amend_spec["trade_enabled"], false);
    assert_eq!(amend_spec["expected_error"], AMEND_PROJECT_UNIMPLEMENTED);
    assert_unsupported_operation(amend_error, AMEND_PROJECT_UNIMPLEMENTED);

    let batch_place_spec = fixture("request_specs/batch_place_orders_node_tx_boundary.json");
    let batch_place_error = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: request_context("batch-place"),
            exchange: exchange_id(),
            orders: vec![limit_order("batch-place")],
        })
        .await
        .expect_err("project-unimplemented batch place");
    assert_eq!(batch_place_spec["trade_enabled"], false);
    assert_eq!(
        batch_place_spec["expected_error"],
        BATCH_PLACE_PROJECT_UNIMPLEMENTED
    );
    assert_unsupported_operation(batch_place_error, BATCH_PLACE_PROJECT_UNIMPLEMENTED);

    let batch_cancel_spec = fixture("request_specs/batch_cancel_orders_node_tx_boundary.json");
    let batch_cancel_error = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: request_context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![cancel_order("batch-cancel")],
        })
        .await
        .expect_err("project-unimplemented batch cancel");
    assert_eq!(batch_cancel_spec["trade_enabled"], false);
    assert_eq!(
        batch_cancel_spec["expected_error"],
        BATCH_CANCEL_PROJECT_UNIMPLEMENTED
    );
    assert_unsupported_operation(batch_cancel_error, BATCH_CANCEL_PROJECT_UNIMPLEMENTED);
}

#[test]
fn dydx_request_specs_should_cover_private_indexer_readback() {
    let open_orders = fixture("request_specs/open_orders.json");
    assert_eq!(open_orders["method"], "GET");
    assert_eq!(open_orders["path"], "/v4/orders");
    assert_eq!(open_orders["auth"], "wallet_address_subaccount");
    assert_eq!(open_orders["secret_free"], true);
    assert_eq!(open_orders["query"]["status"], "OPEN");
}

#[test]
fn dydx_ws_fixture_should_match_payload_helper() {
    let expected = fixture("ws/orderbook_subscribe.json");
    let payload = streams::public_subscribe(&rustcta_exchange_api::PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: RequestContext::new(Utc::now()),
        symbol: btc_scope(),
        kind: rustcta_exchange_api::PublicStreamKind::OrderBookSnapshot,
    });
    assert_eq!(payload, expected);
}
