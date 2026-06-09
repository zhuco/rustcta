use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelOrderRequest, ExchangeApiError, ExchangeClient, OpenOrdersRequest, PlaceOrderRequest,
    PositionsRequest, QueryOrderRequest, RecentFillsRequest, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeErrorClass, MarketType, OrderSide, OrderStatus, OrderType};
use serde_json::Value;

use super::test_support::{
    context, exchange_id, perp_symbol_scope, private_adapter, spawn_rest_server,
};
use super::{DeltaGatewayAdapter, DeltaGatewayConfig};

#[tokio::test]
async fn delta_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = DeltaGatewayAdapter::new(DeltaGatewayConfig {
        api_key: None,
        api_secret: None,
        enabled_private_rest: true,
        ..DeltaGatewayConfig::default()
    })
    .expect("adapter");
    assert!(!adapter.capabilities().supports_private_rest);
    assert!(!adapter.capabilities().supports_amend_order);
    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            assets: Vec::new(),
        })
        .await
        .expect_err("private operation should be unsupported");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));

    let error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend-guard"),
            symbol: perp_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("2001".to_string()),
            new_client_order_id: None,
            new_quantity: "3".to_string(),
        })
        .await
        .expect_err("amend should be guarded without credentials");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

#[tokio::test]
async fn delta_adapter_should_classify_error_fixture() {
    let (base_url, _seen) = spawn_rest_server(vec![fixture("error")]).await;
    let adapter = private_adapter(base_url);

    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances-error"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            assets: Vec::new(),
        })
        .await
        .expect_err("delta error fixture");

    match error {
        ExchangeApiError::Exchange(error) => {
            assert_eq!(error.class, ExchangeErrorClass::Authentication);
            assert_eq!(error.code.as_deref(), Some("invalid_signature"));
        }
        other => panic!("expected exchange error, got {other:?}"),
    }
}

#[tokio::test]
async fn delta_adapter_should_route_private_readbacks_with_signed_headers() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture("balances"),
        fixture("order"),
        fixture("open_orders"),
        fixture("fills"),
        fixture("positions"),
    ])
    .await;
    let adapter = private_adapter(base_url);

    let balances = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect("balances");
    assert_eq!(balances.balances[0].balances[0].available, 1000.0);

    let order = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: perp_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("query")
        .order
        .expect("order");
    assert_eq!(order.status, OrderStatus::Filled);

    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(perp_symbol_scope()),
            page: None,
        })
        .await
        .expect("open");
    assert_eq!(open.orders.len(), 1);

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(perp_symbol_scope()),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
            from_trade_id: None,
            start_time: None,
            end_time: None,
            limit: Some(25),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("9001"));

    let positions = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![perp_symbol_scope().exchange_symbol],
        })
        .await
        .expect("positions");
    assert_eq!(positions.positions[0].quantity, 2.0);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/v2/wallet/balances");
    assert_eq!(requests[0].header("api-key"), Some("key"));
    assert_eq!(requests[1].path, "/v2/orders/1001");
    assert_eq!(requests[2].path, "/v2/orders");
    assert_eq!(requests[3].path, "/v2/fills");
    assert_eq!(requests[4].path, "/v2/positions/margined");
}

#[tokio::test]
async fn delta_adapter_should_route_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture("place_ack"),
        fixture("amend_ack"),
        fixture("cancel_ack"),
    ])
    .await;
    let adapter = private_adapter(base_url);
    assert!(adapter.capabilities().supports_amend_order);
    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("cid-1".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "2".to_string(),
            price: Some("65000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("2001"));

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("cid-1".to_string()),
            exchange_order_id: Some("2001".to_string()),
            new_client_order_id: None,
            new_quantity: "3".to_string(),
        })
        .await
        .expect("amend");
    assert_eq!(amended.order.exchange_order_id.as_deref(), Some("2001"));
    assert_eq!(amended.order.quantity, "3");
    assert_eq!(amended.order.status, OrderStatus::New);

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("cid-1".to_string()),
            exchange_order_id: Some("2001".to_string()),
        })
        .await
        .expect("cancel");
    assert!(cancelled.cancelled);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].method, "POST");
    assert_eq!(requests[0].path, "/v2/orders");
    assert_eq!(
        requests[0].body.as_ref().unwrap()["product_symbol"],
        "BTCUSDT"
    );
    assert_eq!(requests[1].method, "PUT");
    assert_eq!(requests[1].path, "/v2/orders");
    assert_eq!(requests[1].body.as_ref().unwrap()["id"], "2001");
    assert_eq!(requests[1].body.as_ref().unwrap()["size"], "3");
    assert_eq!(
        requests[1].body.as_ref().unwrap()["client_order_id"],
        "cid-1"
    );
    assert_eq!(requests[1].header("api-key"), Some("key"));
    assert_eq!(requests[2].method, "DELETE");
}

#[test]
fn delta_amend_request_spec_should_be_runtime_enabled_and_parser_backed() {
    let spec: Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/delta/request_specs/amend_order.json"
    ))
    .expect("request spec");
    assert_eq!(spec["exchange"], "delta");
    assert_eq!(spec["name"], "amend_order");
    assert_eq!(spec["method"], "PUT");
    assert_eq!(spec["path"], "/v2/orders");
    assert_eq!(spec["json_body"]["product_symbol"], "BTCUSDT");
    assert_eq!(spec["json_body"]["size"], "3");
    assert_eq!(spec["runtime_status"], "runtime_enabled");
    assert_eq!(
        spec["response_parser_fixture"],
        "tests/fixtures/exchanges/delta/amend_ack.json"
    );

    let ack = fixture("amend_ack");
    assert_eq!(ack["result"]["id"], 2001);
    assert_eq!(ack["result"]["state"], "open");
}

#[tokio::test]
async fn delta_amend_should_reject_unverified_new_client_order_id() {
    let (base_url, seen) = spawn_rest_server(Vec::new()).await;
    let adapter = private_adapter(base_url);
    let error = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend-new-client-id"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("cid-1".to_string()),
            exchange_order_id: Some("2001".to_string()),
            new_client_order_id: Some("cid-2".to_string()),
            new_quantity: "3".to_string(),
        })
        .await
        .expect_err("new client id amend is not mapped");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
    assert!(seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn delta_adapter_should_compose_batch_order_calls() {
    let (base_url, seen) =
        spawn_rest_server(vec![fixture("place_ack"), fixture("cancel_ack")]).await;
    let adapter = private_adapter(base_url);
    let symbol = perp_symbol_scope();
    let order = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: symbol.clone(),
        client_order_id: Some("cid-batch".to_string()),
        side: OrderSide::Buy,
        position_side: None,
        order_type: OrderType::Market,
        time_in_force: None,
        quantity: "1".to_string(),
        price: None,
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![order],
        })
        .await
        .expect("batch place");
    assert_eq!(placed.orders.len(), 1);

    let cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("cancel"),
                symbol,
                client_order_id: None,
                exchange_order_id: Some("2001".to_string()),
            }],
        })
        .await
        .expect("batch cancel");
    assert_eq!(cancelled.cancelled_count, 1);
    assert_eq!(seen.lock().unwrap().len(), 2);
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "balances" => include_str!("../../../../../tests/fixtures/exchanges/delta/balances.json"),
        "order" => include_str!("../../../../../tests/fixtures/exchanges/delta/order.json"),
        "open_orders" => {
            include_str!("../../../../../tests/fixtures/exchanges/delta/open_orders.json")
        }
        "fills" => include_str!("../../../../../tests/fixtures/exchanges/delta/fills.json"),
        "positions" => include_str!("../../../../../tests/fixtures/exchanges/delta/positions.json"),
        "place_ack" => include_str!("../../../../../tests/fixtures/exchanges/delta/place_ack.json"),
        "amend_ack" => include_str!("../../../../../tests/fixtures/exchanges/delta/amend_ack.json"),
        "cancel_ack" => {
            include_str!("../../../../../tests/fixtures/exchanges/delta/cancel_ack.json")
        }
        "error" => include_str!("../../../../../tests/fixtures/exchanges/delta/error.json"),
        _ => unreachable!("unknown fixture"),
    };
    serde_json::from_str(text).expect("fixture json")
}
