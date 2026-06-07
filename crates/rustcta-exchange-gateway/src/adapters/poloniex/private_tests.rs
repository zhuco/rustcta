use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchAtomicity, BatchCancelOrdersRequest,
    BatchExecutionMode, BatchPlaceOrdersRequest, CancelOrderRequest, CapabilitySupport,
    ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest, PlaceOrderRequest,
    PositionsRequest, QueryOrderRequest, RecentFillsRequest, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::json;

use crate::request_spec::RequestSpec;
use crate::signing_spec::SigningVector;

use super::test_support::{
    assert_signed_poloniex_request, context, exchange_id, perp_symbol_scope, spawn_rest_server,
    spot_symbol_scope,
};
use super::{
    PoloniexGatewayAdapter, PoloniexGatewayConfig, PoloniexMarginMode, PoloniexPositionMode,
};

#[tokio::test]
async fn poloniex_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = PoloniexGatewayAdapter::default_public().expect("adapter");
    assert!(adapter.capabilities().supports_public_rest);
    assert!(!adapter.capabilities().supports_private_rest);

    let error = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect_err("private operation should be unsupported");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
}

#[tokio::test]
async fn poloniex_adapter_should_sign_private_readbacks_and_parse_responses() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "accountId": "123",
            "accountType": "SPOT",
            "balances": [
                {"currency": "BTC", "available": "0.1", "hold": "0.02"},
                {"currency": "USDT", "available": "10", "hold": "0"}
            ]
        }),
        json!({
            "data": [{
                "symbol": "BTC_USDT_PERP",
                "posSide": "LONG",
                "qty": "0.5",
                "openAvgPx": "65000",
                "markPx": "65100",
                "lever": "5"
            }]
        }),
        json!({
            "makerRate": "0.0002",
            "takerRate": "0.0005"
        }),
        json!({
            "symbol": "BTC_USDT",
            "id": "1001",
            "clientOrderId": "CLIENT1",
            "price": "65000",
            "quantity": "0.01",
            "filledQuantity": "0.004",
            "state": "PARTIALLY_FILLED",
            "type": "LIMIT",
            "side": "BUY",
            "createTime": 1700000000000i64,
            "updateTime": 1700000001000i64
        }),
        json!([{
                "symbol": "BTC_USDT",
                "id": "1002",
                "clientOrderId": "CLIENT2",
                "price": "70000",
                "quantity": "0.02",
                "filledQuantity": "0",
                "state": "NEW",
                "type": "LIMIT",
                "side": "SELL"
        }]),
        json!([{
                "symbol": "BTC_USDT",
                "id": "2001",
                "orderId": "1001",
                "clientOrderId": "CLIENT1",
                "price": "64950",
                "quantity": "0.004",
                "fee": "0.2598",
                "feeCurrency": "USDT",
                "side": "BUY",
                "createTime": 1700000001000i64
        }]),
    ])
    .await;
    let adapter = private_adapter(base_url);

    let balances = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: Vec::new(),
        })
        .await
        .expect("balances");
    assert_eq!(balances.balances[0].balances[0].asset, "BTC");

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

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![spot_symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees[0].maker_rate, "0.0002");

    let order = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: spot_symbol_scope(),
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
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
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
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
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
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("2001"));

    let requests = seen.lock().unwrap().clone();
    assert_signed_poloniex_request(&requests[0], "GET", "/accounts/balances");
    assert_signed_poloniex_request(&requests[1], "GET", "/v3/trade/position/opens");
    assert_signed_poloniex_request(&requests[2], "GET", "/feeinfo");
    assert_signed_poloniex_request(&requests[3], "GET", "/orders/1001");
    assert_signed_poloniex_request(&requests[4], "GET", "/orders");
    assert_signed_poloniex_request(&requests[5], "GET", "/trades");
}

#[tokio::test]
async fn poloniex_adapter_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 200, "data": {"ordId": "2001", "clOrdId": "LIMIT1"}}),
        json!({"code": 200, "data": {"ordId": "2001", "clOrdId": "LIMIT1"}}),
        json!({"code": 200, "data": [
            {"ordId": "2002", "clOrdId": "OPEN1", "code": 200, "msg": "Success"}
        ]}),
        json!({"id": "3001", "clientOrderId": "AMEND1"}),
        json!([
            {"id": "4001", "clientOrderId": "BATCH1"},
            {"id": "4002", "clientOrderId": "BATCH2"}
        ]),
        json!([
            {"orderId": "4001", "clientOrderId": "BATCH1", "state": "PENDING_CANCEL", "code": 200, "message": ""},
            {"orderId": "4002", "clientOrderId": "BATCH2", "state": "PENDING_CANCEL", "code": 200, "message": ""}
        ]),
    ])
    .await;
    let adapter = private_adapter(base_url);
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_batch_place_order);
    assert!(capabilities.supports_batch_cancel_order);
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        BatchExecutionMode::Native
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.atomicity,
        BatchAtomicity::Partial
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.max_items,
        Some(10)
    );
    assert!(
        capabilities
            .capabilities_v2
            .batch_place_orders
            .supports_partial_failure
    );
    assert!(matches!(
        capabilities.capabilities_v2.stream_runtime.private,
        CapabilitySupport::Native
    ));
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .reconnect
            .requires_resubscribe
    );
    assert!(capabilities.supports_amend_order);

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("LIMIT1".to_string()),
            side: OrderSide::Buy,
            position_side: Some(PositionSide::Long),
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "0.02".to_string(),
            price: Some("65000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place order");
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

    let cancel_all = adapter
        .cancel_all_orders(rustcta_exchange_api::CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbol: Some(perp_symbol_scope()),
        })
        .await
        .expect("cancel all");
    assert_eq!(cancel_all.cancelled_count, 1);

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: spot_symbol_scope(),
            client_order_id: Some("OLD1".to_string()),
            exchange_order_id: Some("3000".to_string()),
            new_client_order_id: Some("AMEND1".to_string()),
            new_quantity: "0.01".to_string(),
        })
        .await
        .expect("amend");
    assert_eq!(amended.order.exchange_order_id.as_deref(), Some("3001"));

    let batch_placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-place-1"),
                    symbol: spot_symbol_scope(),
                    client_order_id: Some("BATCH1".to_string()),
                    side: OrderSide::Buy,
                    position_side: None,
                    order_type: OrderType::Market,
                    time_in_force: None,
                    quantity: "0".to_string(),
                    price: None,
                    quote_quantity: Some("10".to_string()),
                    reduce_only: false,
                    post_only: false,
                },
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-place-2"),
                    symbol: spot_symbol_scope(),
                    client_order_id: Some("BATCH2".to_string()),
                    side: OrderSide::Sell,
                    position_side: None,
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.01".to_string(),
                    price: Some("70000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
            ],
        })
        .await
        .expect("batch place");
    assert_eq!(batch_placed.orders.len(), 2);

    let batch_cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-1"),
                    symbol: spot_symbol_scope(),
                    client_order_id: Some("BATCH1".to_string()),
                    exchange_order_id: Some("4001".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-cancel-2"),
                    symbol: spot_symbol_scope(),
                    client_order_id: Some("BATCH2".to_string()),
                    exchange_order_id: Some("4002".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(batch_cancelled.cancelled_count, 2);

    let requests = seen.lock().unwrap().clone();
    assert_signed_poloniex_request(&requests[0], "POST", "/v3/trade/order");
    assert!(requests[0].body.contains("\"posSide\":\"LONG\""));
    assert_signed_poloniex_request(&requests[1], "DELETE", "/v3/trade/order");
    assert_signed_poloniex_request(&requests[2], "DELETE", "/v3/trade/allOrders");
    assert_signed_poloniex_request(&requests[3], "PUT", "/orders/3000");
    assert!(requests[3].body.contains("\"quantity\":\"0.01\""));
    assert_signed_poloniex_request(&requests[4], "POST", "/orders/batch");
    assert!(requests[4].body.contains("\"clientOrderId\":\"BATCH1\""));
    assert_signed_poloniex_request(&requests[5], "DELETE", "/orders/cancelByIds");
    assert!(requests[5]
        .body
        .contains("\"orderIds\":[\"4001\",\"4002\"]"));
}

#[tokio::test]
async fn poloniex_adapter_should_route_perp_position_setting_extensions() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 200, "data": {"positionMode": "ONE_WAY"}}),
        json!({"code": 200, "data": {"positionMode": "HEDGE"}}),
        json!({"code": 200, "data": [{"symbol": "BTC_USDT_PERP", "lever": "5"}]}),
        json!({"code": 200, "data": {"symbol": "BTC_USDT_PERP", "lever": "8"}}),
        json!({"code": 200, "data": {"symbol": "BTC_USDT_PERP", "amt": "25"}}),
    ])
    .await;
    let adapter = private_adapter(base_url);
    assert!(adapter.capabilities().supports_private_rest);

    let mode = adapter
        .get_position_mode()
        .await
        .expect("get position mode");
    assert_eq!(mode.data.expect("data")["positionMode"], "ONE_WAY");
    adapter
        .switch_position_mode(PoloniexPositionMode::Hedge)
        .await
        .expect("switch position mode");
    adapter
        .get_leverages(perp_symbol_scope())
        .await
        .expect("get leverages");
    adapter
        .set_leverage(
            perp_symbol_scope(),
            "8",
            PoloniexMarginMode::Isolated,
            Some(PositionSide::Long),
        )
        .await
        .expect("set leverage");
    adapter
        .adjust_isolated_margin(perp_symbol_scope(), "25", PositionSide::Long, true)
        .await
        .expect("adjust isolated margin");

    let requests = seen.lock().unwrap().clone();
    assert_signed_poloniex_request(&requests[0], "GET", "/v3/position/mode");
    assert_signed_poloniex_request(&requests[1], "POST", "/v3/position/mode");
    assert!(requests[1].body.contains("\"positionMode\":\"HEDGE\""));
    assert_signed_poloniex_request(&requests[2], "GET", "/v3/position/leverages");
    assert_eq!(
        requests[2].query.get("symbol").map(String::as_str),
        Some("BTC_USDT_PERP")
    );
    assert_signed_poloniex_request(&requests[3], "POST", "/v3/position/leverage");
    assert!(requests[3].body.contains("\"lever\":\"8\""));
    assert!(requests[3].body.contains("\"mgnMode\":\"ISOLATED\""));
    assert!(requests[3].body.contains("\"posSide\":\"LONG\""));
    assert_signed_poloniex_request(&requests[4], "POST", "/v3/position/margin");
    assert!(requests[4].body.contains("\"amt\":\"25\""));
    assert!(requests[4].body.contains("\"type\":\"ADD\""));
}

#[test]
fn poloniex_signing_should_match_known_hmac() {
    let params = std::collections::HashMap::new();
    let payload =
        super::signing::signature_payload("GET", "/orders", &params, "1631018760000", None);
    let signature = super::signing::sign_payload("secret", &payload).expect("signature");
    assert_eq!(signature, "f8hx9HnL/j46wehFGyhnBuJxjx+z+9zOtYPsohoXjNk=");
}

#[test]
fn poloniex_signing_vector_fixture_should_verify_hmac_base64() {
    signing_vector("rest_get_orders.json")
        .verify()
        .expect("signing vector");
}

#[tokio::test]
async fn poloniex_request_spec_fixture_should_cover_private_place_order() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 200,
        "data": {"ordId": "2001", "clOrdId": "LIMIT1"}
    })])
    .await;
    let adapter = private_adapter(base_url);

    adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("request-spec"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("LIMIT1".to_string()),
            side: OrderSide::Buy,
            position_side: Some(PositionSide::Long),
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "0.02".to_string(),
            price: Some("65000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place order");

    request_spec("place_order_perp_limit.json")
        .assert_matches(&seen.lock().unwrap()[0].actual_http_request())
        .expect("request spec");
}

fn private_adapter(base_url: String) -> PoloniexGatewayAdapter {
    PoloniexGatewayAdapter::new(PoloniexGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        recv_window_ms: 5_000,
        ..PoloniexGatewayConfig::default()
    })
    .expect("adapter")
}

fn request_spec(name: &str) -> RequestSpec {
    let raw = match name {
        "place_order_perp_limit.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/poloniex/request_specs/place_order_perp_limit.json"
        ),
        _ => unreachable!("unknown poloniex request spec"),
    };
    serde_json::from_str(raw).expect("request spec")
}

fn signing_vector(name: &str) -> SigningVector {
    let raw = match name {
        "rest_get_orders.json" => include_str!(
            "../../../../../tests/fixtures/exchanges/poloniex/signing_vectors/rest_get_orders.json"
        ),
        _ => unreachable!("unknown poloniex signing vector"),
    };
    serde_json::from_str(raw).expect("signing vector")
}
