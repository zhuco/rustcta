use rustcta_exchange_api::{
    BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelOrderRequest,
    ExchangeApiError, ExchangeClient, PlaceOrderRequest, PositionsRequest, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeSymbol, MarketType, OrderSide, OrderType, PositionSide};
use serde_json::json;

use super::private_parser::parse_positions;
use super::signing::sign_raw_query;
use super::test_support::{
    assert_signed_request_method, context, exchange_id, private_config, spawn_rest_server,
    symbol_scope,
};
use super::BinanceCoinMGatewayAdapter;
use crate::request_spec::RequestSpec;
use crate::signing_spec::SigningVector;

#[test]
fn binancecoinm_signing_should_match_hmac_vector() {
    let query = "newClientOrderId=cli-place&price=25000.0&quantity=2&recvWindow=5000&side=BUY&symbol=BTCUSD_PERP&timeInForce=GTC&timestamp=1710000000000&type=LIMIT";
    let signature = sign_raw_query("test-secret", query).expect("signature");
    assert_eq!(
        signature,
        "501b588912a8a35dd54f6bffb92e0056c9a4b4245b99d1d224fd3895dcbb26ac"
    );
}

#[test]
fn binancecoinm_signing_vector_fixture_should_verify() {
    let vector = load_signing_vector("binancecoinm/signing_vectors/place_order_limit.json");
    vector.verify().expect("fixture signature");
    let vector = load_signing_vector("binancecoinm/signing_vectors/batch_place_orders.json");
    vector.verify().expect("batch place fixture signature");
    let vector = load_signing_vector("binancecoinm/signing_vectors/batch_cancel_orders.json");
    vector.verify().expect("batch cancel fixture signature");
}

#[tokio::test]
async fn binancecoinm_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = BinanceCoinMGatewayAdapter::default_public().expect("adapter");
    assert!(adapter.capabilities().supports_public_rest);
    assert!(!adapter.capabilities().supports_private_rest);
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
}

#[tokio::test]
async fn binancecoinm_adapter_should_load_balances_from_coin_m_balance_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!([
        {"asset": "BTC", "balance": "0.12000000", "availableBalance": "0.10000000", "crossWalletBalance": "0.02000000"},
        {"asset": "ETH", "balance": "0.00000000", "availableBalance": "0.00000000"}
    ])])
    .await;
    let adapter = BinanceCoinMGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let response = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            assets: vec!["BTC".to_string()],
        })
        .await
        .expect("balances");

    assert_eq!(response.balances[0].market_type, MarketType::Perpetual);
    assert_eq!(response.balances[0].balances[0].asset, "BTC");
    assert_eq!(response.balances[0].balances[0].total, 0.12);
    let request = seen.lock().unwrap()[0].clone();
    load_request_spec("binancecoinm/request_specs/get_balances.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
    assert_signed_request_method(&request, "GET");
}

#[test]
fn binancecoinm_parser_should_parse_position_risk() {
    let positions = parse_positions(
        &exchange_id(),
        rustcta_types::TenantId::new("tenant").unwrap(),
        rustcta_types::AccountId::new("account").unwrap(),
        &[],
        &json!([{
            "symbol": "BTCUSD_PERP",
            "positionAmt": "-2",
            "entryPrice": "26000.0",
            "markPrice": "25500.0",
            "unRealizedProfit": "0.01",
            "liquidationPrice": "30000.0",
            "leverage": "5",
            "positionSide": "BOTH"
        }]),
    )
    .expect("positions");

    assert_eq!(positions.len(), 1);
    assert_eq!(positions[0].market_type, MarketType::Perpetual);
    assert_eq!(positions[0].canonical_symbol.to_string(), "BTC/USD");
    assert_eq!(positions[0].side, PositionSide::Short);
    assert_eq!(positions[0].quantity, 2.0);
    assert_eq!(positions[0].leverage, Some(5.0));
}

#[tokio::test]
async fn binancecoinm_adapter_should_load_positions_from_position_risk_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!([{
        "symbol": "BTCUSD_PERP",
        "positionAmt": "2",
        "entryPrice": "25000.0",
        "markPrice": "25100.0",
        "unRealizedProfit": "0.02",
        "liquidationPrice": "20000.0",
        "leverage": "3",
        "positionSide": "LONG"
    }])])
    .await;
    let adapter = BinanceCoinMGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let response = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![
                ExchangeSymbol::new(exchange_id(), MarketType::Perpetual, "BTCUSD_PERP").unwrap(),
            ],
        })
        .await
        .expect("positions");

    assert_eq!(response.positions[0].side, PositionSide::Long);
    let request = seen.lock().unwrap()[0].clone();
    load_request_spec("binancecoinm/request_specs/get_positions.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
    assert_eq!(
        request.query.get("pair").map(String::as_str),
        Some("BTCUSD")
    );
}

#[tokio::test]
async fn binancecoinm_adapter_should_place_inverse_limit_order() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "symbol": "BTCUSD_PERP",
        "orderId": 30,
        "clientOrderId": "cli-place",
        "price": "25000.0",
        "origQty": "2",
        "executedQty": "0",
        "status": "NEW",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "BUY",
        "positionSide": "LONG",
        "reduceOnly": false,
        "updateTime": 1710000000000_i64
    })])
    .await;
    let adapter = BinanceCoinMGatewayAdapter::new(private_config(base_url)).expect("adapter");

    let response = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: symbol_scope("BTCUSD_PERP"),
            client_order_id: Some("cli-place".to_string()),
            side: OrderSide::Buy,
            position_side: Some(PositionSide::Long),
            order_type: OrderType::Limit,
            time_in_force: None,
            quantity: "2".to_string(),
            price: Some("25000.0".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place order");

    assert_eq!(response.order.exchange_symbol.symbol, "BTCUSD_PERP");
    assert_eq!(response.order.position_side, Some(PositionSide::Long));
    let request = seen.lock().unwrap()[0].clone();
    load_request_spec("binancecoinm/request_specs/place_order.json")
        .assert_matches(&request.actual_http_request())
        .expect("request spec");
    assert_signed_request_method(&request, "POST");
}

#[tokio::test]
async fn binancecoinm_adapter_should_route_batch_place_and_cancel_orders() {
    let batch_place_ack = json!([{
        "symbol": "BTCUSD_PERP",
        "orderId": 30,
        "clientOrderId": "cli-batch-1",
        "price": "25000.0",
        "origQty": "2",
        "executedQty": "0",
        "status": "NEW",
        "timeInForce": "GTC",
        "type": "LIMIT",
        "side": "BUY",
        "positionSide": "LONG",
        "reduceOnly": false,
        "updateTime": 1710000000001_i64
    }]);
    let batch_cancel_ack = json!([
        {
            "symbol": "BTCUSD_PERP",
            "orderId": 30,
            "clientOrderId": "cli-batch-1",
            "price": "25000.0",
            "origQty": "2",
            "executedQty": "0",
            "status": "CANCELED",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "BUY",
            "positionSide": "LONG",
            "reduceOnly": false,
            "updateTime": 1710000000002_i64
        },
        {
            "symbol": "BTCUSD_PERP",
            "orderId": 31,
            "clientOrderId": "cli-batch-2",
            "price": "25010.0",
            "origQty": "1",
            "executedQty": "0",
            "status": "CANCELED",
            "timeInForce": "GTC",
            "type": "LIMIT",
            "side": "SELL",
            "positionSide": "SHORT",
            "reduceOnly": false,
            "updateTime": 1710000000003_i64
        }
    ]);
    let (base_url, seen) = spawn_rest_server(vec![batch_place_ack, batch_cancel_ack]).await;
    let adapter = BinanceCoinMGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let symbol = symbol_scope("BTCUSD_PERP");

    let placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![PlaceOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("batch-place-a"),
                symbol: symbol.clone(),
                client_order_id: Some("cli-batch-1".to_string()),
                side: OrderSide::Buy,
                position_side: Some(PositionSide::Long),
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: "2".to_string(),
                price: Some("25000.0".to_string()),
                quote_quantity: None,
                reduce_only: false,
                post_only: false,
            }],
        })
        .await
        .expect("batch place orders");
    assert_eq!(placed.orders.len(), 1);
    assert_eq!(placed.orders[0].exchange_order_id.as_deref(), Some("30"));
    assert_eq!(
        placed
            .report
            .as_ref()
            .expect("place report")
            .succeeded_count(),
        1
    );

    let cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-a"),
                    symbol: symbol.clone(),
                    client_order_id: None,
                    exchange_order_id: Some("30".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-b"),
                    symbol,
                    client_order_id: None,
                    exchange_order_id: Some("31".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel orders");
    assert_eq!(cancelled.cancelled_count, 2);
    assert_eq!(
        cancelled
            .report
            .as_ref()
            .expect("cancel report")
            .succeeded_count(),
        2
    );

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen[0].path, "/dapi/v1/batchOrders");
    load_request_spec("binancecoinm/request_specs/batch_place_orders.json")
        .assert_matches(&seen[0].actual_http_request())
        .expect("batch place request spec");
    assert_signed_request_method(&seen[0], "POST");
    assert_eq!(seen[1].path, "/dapi/v1/batchOrders");
    load_request_spec("binancecoinm/request_specs/batch_cancel_orders.json")
        .assert_matches(&seen[1].actual_http_request())
        .expect("batch cancel request spec");
    assert_signed_request_method(&seen[1], "DELETE");
}

#[tokio::test]
async fn binancecoinm_batch_orders_should_report_partial_failures_and_guard_boundaries() {
    let (base_url, _) = spawn_rest_server(vec![json!([
        {"code": -2022, "msg": "ReduceOnly Order is rejected."}
    ])])
    .await;
    let adapter = BinanceCoinMGatewayAdapter::new(private_config(base_url)).expect("adapter");
    let symbol = symbol_scope("BTCUSD_PERP");

    let placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place-fail"),
            exchange: exchange_id(),
            orders: vec![PlaceOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("batch-place-fail-a"),
                symbol: symbol.clone(),
                client_order_id: Some("cli-batch-fail".to_string()),
                side: OrderSide::Sell,
                position_side: Some(PositionSide::Short),
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: "1".to_string(),
                price: Some("25010.0".to_string()),
                quote_quantity: None,
                reduce_only: true,
                post_only: false,
            }],
        })
        .await
        .expect("batch place partial failure report");
    let report = placed.report.as_ref().expect("place report");
    assert_eq!(report.succeeded_count(), 0);
    assert_eq!(report.failed_count(), 1);
    assert_eq!(
        report.results[0]
            .error
            .as_ref()
            .and_then(|error| error.code.as_deref()),
        Some("-2022")
    );

    let public_adapter = BinanceCoinMGatewayAdapter::default_public().expect("adapter");
    let error = public_adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel-public"),
            exchange: exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("batch-cancel-public-a"),
                symbol,
                client_order_id: None,
                exchange_order_id: Some("30".to_string()),
            }],
        })
        .await
        .expect_err("private REST guard");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

fn load_request_spec(path: &str) -> RequestSpec {
    let full_path = format!(
        "{}/../../tests/fixtures/exchanges/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(full_path).expect("request spec fixture");
    serde_json::from_str(&text).expect("request spec")
}

fn load_signing_vector(path: &str) -> SigningVector {
    let full_path = format!(
        "{}/../../tests/fixtures/exchanges/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(full_path).expect("signing vector fixture");
    serde_json::from_str(&text).expect("signing vector")
}
