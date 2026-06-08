use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelAllOrdersRequest, CancelOrderRequest, ExchangeApiError, ExchangeClient,
    OpenOrdersRequest, PlaceOrderRequest, PositionsRequest, QueryOrderRequest, RecentFillsRequest,
    TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::private::{bydfi_cancel_body, bydfi_place_order_body};
use super::signing::sign_rest_request;
use super::test_support::{
    assert_signed_bydfi_request, context, exchange_id, perp_symbol_scope, spawn_rest_server,
};
use super::{BydfiGatewayAdapter, BydfiGatewayConfig};
use crate::{AdapterBackedGateway, LocalGateway};

#[tokio::test]
async fn bydfi_gateway_should_register_named_adapter() {
    let gateway = AdapterBackedGateway::with_named_adapters("test", ["bydfi"]).expect("gateway");
    let status = gateway.status().await.expect("status");
    assert_eq!(status.exchanges[0].exchange, "bydfi");
}

#[tokio::test]
async fn bydfi_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = BydfiGatewayAdapter::default_public().expect("adapter");
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
async fn bydfi_adapter_should_sign_private_readbacks_and_parse_responses() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!([{"wallet": "W001", "asset": "USDT", "balance": "1000", "frozen": "10", "availableBalance": "990"}]),
        json!([{"wallet": "W001", "symbol": "BTC-USDT", "side": "BUY", "quantity": "0.5", "avgPrice": "65000", "markPrice": "65100", "liqPrice": "50000", "unPnl": "10", "leverage": 5}]),
        json!([{"wallet": "W001", "symbol": "BTC-USDT", "orderId": "1001", "clientOrderId": "CLIENT1", "price": "65000", "quantity": "0.01", "dealQuantity": "0.004", "orderType": "LIMIT", "side": "BUY", "status": "PARTIALLY_FILLED"}]),
        json!([{"wallet": "W001", "symbol": "BTC-USDT", "orderId": "1002", "clientOrderId": "CLIENT2", "price": "66000", "quantity": "0.02", "dealQuantity": "0", "orderType": "LIMIT", "side": "SELL", "status": "NEW"}]),
        json!([{"orderId": "1001", "clientOrderId": "CLIENT1", "symbol": "BTC-USDT", "time": 1700000000000i64, "dealPrice": "65010", "dealQuantity": "0.004", "fee": "0.26", "side": "BUY", "type": "LIMIT"}]),
    ])
    .await;
    let adapter = private_adapter(base_url);

    let balances = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            assets: Vec::new(),
        })
        .await
        .expect("balances");
    assert_eq!(balances.balances[0].balances[0].asset, "USDT");

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
    assert_eq!(positions.positions[0].side, PositionSide::Long);

    let order = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("CLIENT1".to_string()),
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("query")
        .order
        .expect("order");
    assert_eq!(order.status, OrderStatus::PartiallyFilled);

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
        .expect("open orders");
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
            limit: Some(50),
            page: None,
        })
        .await
        .expect("fills");
    assert_eq!(fills.fills[0].order_id.as_deref(), Some("1001"));

    let requests = seen.lock().unwrap().clone();
    assert_signed_bydfi_request(&requests[0], "GET", "/v1/fapi/account/balance");
    assert_signed_bydfi_request(&requests[1], "GET", "/v2/fapi/trade/positions");
    assert_signed_bydfi_request(&requests[2], "GET", "/v2/fapi/trade/open_order");
    assert_signed_bydfi_request(&requests[3], "GET", "/v2/fapi/trade/open_order");
    assert_signed_bydfi_request(&requests[4], "GET", "/v2/fapi/trade/history_trade");
}

#[tokio::test]
async fn bydfi_adapter_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"orderId": "2001", "clientOrderId": "LIMIT1", "symbol": "BTC-USDT", "quantity": "0.02", "side": "BUY", "status": "NEW"}),
        json!({"orderId": "2001", "clientOrderId": "LIMIT1", "symbol": "BTC-USDT", "quantity": "0.02", "side": "BUY", "status": "CANCELED"}),
        json!({"orderId": "2001", "clientOrderId": "LIMIT1", "symbol": "BTC-USDT", "quantity": "0.03", "side": "BUY", "status": "NEW"}),
        json!([{"orderId": "3001", "clientOrderId": "BATCH1", "symbol": "BTC-USDT", "quantity": "0.01", "side": "BUY", "status": "NEW"}]),
        json!([{"orderId": "3001", "clientOrderId": "BATCH1", "symbol": "BTC-USDT", "quantity": "0.01", "side": "BUY", "status": "CANCELED"}]),
        json!([{"orderId": "3001", "clientOrderId": "BATCH1", "symbol": "BTC-USDT", "quantity": "0.01", "side": "BUY", "status": "CANCELED"}]),
    ])
    .await;
    let adapter = private_adapter(base_url);

    let place = place_request("LIMIT1");
    let placed = adapter.place_order(place.clone()).await.expect("place");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("2001"));

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("LIMIT1".to_string()),
            exchange_order_id: Some("2001".to_string()),
        })
        .await
        .expect("cancel");
    assert!(cancelled.cancelled);

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("LIMIT1".to_string()),
            exchange_order_id: Some("2001".to_string()),
            new_client_order_id: None,
            new_quantity: "0.03".to_string(),
        })
        .await
        .expect("amend");
    assert_eq!(amended.order.quantity, "0.03");

    adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![place_request("BATCH1")],
        })
        .await
        .expect("batch place");
    adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("batch-cancel-item"),
                symbol: perp_symbol_scope(),
                client_order_id: Some("BATCH1".to_string()),
                exchange_order_id: Some("3001".to_string()),
            }],
        })
        .await
        .expect("batch cancel");
    adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(perp_symbol_scope()),
        })
        .await
        .expect("cancel all");

    let requests = seen.lock().unwrap().clone();
    assert_signed_bydfi_request(&requests[0], "POST", "/v2/fapi/trade/place_order");
    assert_signed_bydfi_request(&requests[1], "POST", "/v2/fapi/trade/cancel_order");
    assert_signed_bydfi_request(&requests[2], "POST", "/v2/fapi/trade/edit_order");
    assert_signed_bydfi_request(&requests[3], "POST", "/v2/fapi/trade/batch_place_order");
    assert_signed_bydfi_request(&requests[4], "POST", "/v2/fapi/trade/batch_cancel_order");
    assert_signed_bydfi_request(&requests[5], "POST", "/v2/fapi/trade/cancel_all_order");
}

#[test]
fn bydfi_signing_vectors_and_request_specs_should_load() {
    for vector in [
        include_str!(
            "../../../../../tests/fixtures/exchanges/bydfi/signing_vectors/rest_query_order.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/bydfi/signing_vectors/rest_body_place_order.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/bydfi/signing_vectors/rest_body_cancel_order.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/bydfi/signing_vectors/rest_body_amend_order.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/bydfi/signing_vectors/rest_body_batch_place_order.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/bydfi/signing_vectors/rest_body_batch_cancel_order.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/bydfi/signing_vectors/rest_body_cancel_all_orders.json"
        ),
    ] {
        let vector: Value = serde_json::from_str(vector).expect("vector");
        let signature = sign_rest_request(
            vector["api_key"].as_str().expect("api_key"),
            vector["api_secret"].as_str().expect("api_secret"),
            vector["timestamp"].as_str().expect("timestamp"),
            vector["query_string"].as_str().unwrap_or_default(),
            vector["body"].as_str().unwrap_or_default(),
        )
        .expect("signature");
        assert_eq!(signature, vector["expected_signature"]);
    }

    for spec in [
        include_str!("../../../../../tests/fixtures/exchanges/bydfi/request_specs/balances.json"),
        include_str!("../../../../../tests/fixtures/exchanges/bydfi/request_specs/positions.json"),
        include_str!(
            "../../../../../tests/fixtures/exchanges/bydfi/request_specs/query_order.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/bydfi/request_specs/place_order.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/bydfi/request_specs/cancel_order.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/bydfi/request_specs/amend_order.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/bydfi/request_specs/batch_place_orders.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/bydfi/request_specs/batch_cancel_orders.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/bydfi/request_specs/cancel_all_orders.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/bydfi/request_specs/open_orders.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/bydfi/request_specs/recent_fills.json"
        ),
    ] {
        let spec: Value = serde_json::from_str(spec).expect("request spec");
        assert_eq!(spec["exchange"], "bydfi");
    }
}

#[test]
fn bydfi_request_body_builders_should_match_official_fields() {
    let place = bydfi_place_order_body(&place_request("CID1"), "W001").expect("place body");
    assert_eq!(place["wallet"], "W001");
    assert_eq!(place["symbol"], "BTC-USDT");
    assert_eq!(place["type"], "LIMIT");
    assert_eq!(place["positionSide"], "LONG");

    let cancel = bydfi_cancel_body(
        &CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-body"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("CID1".to_string()),
            exchange_order_id: None,
        },
        "W001",
    )
    .expect("cancel body");
    assert_eq!(cancel["clientOrderId"], "CID1");
}

fn private_adapter(rest_base_url: String) -> BydfiGatewayAdapter {
    BydfiGatewayAdapter::new(BydfiGatewayConfig {
        rest_base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        wallet: "W001".to_string(),
        ..BydfiGatewayConfig::default()
    })
    .expect("adapter")
}

fn place_request(client_order_id: &str) -> PlaceOrderRequest {
    PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("place"),
        symbol: perp_symbol_scope(),
        client_order_id: Some(client_order_id.to_string()),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::Long),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.02".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    }
}
