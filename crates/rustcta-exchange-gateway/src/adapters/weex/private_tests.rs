use rustcta_exchange_api::{
    BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelOrderRequest,
    ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest, PlaceOrderRequest,
    PositionsRequest, QueryOrderRequest, RecentFillsRequest, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::{json, Value};

use super::private_parser::{parse_balances, parse_fills, parse_order};
use super::test_support::{
    actual_request, assert_signed_weex_request, context, exchange_id, perp_symbol_scope,
    request_spec, signing_vector, spawn_rest_server, spot_symbol_scope,
};
use super::{WeexGatewayAdapter, WeexGatewayConfig};

#[tokio::test]
async fn weex_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = WeexGatewayAdapter::default_public().expect("adapter");
    assert!(adapter.capabilities().supports_public_rest);
    assert!(!adapter.capabilities().supports_private_rest);
    assert!(!adapter.capabilities().supports_batch_place_order);
    assert!(!adapter.capabilities().supports_batch_cancel_order);

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
async fn weex_adapter_should_sign_private_readbacks_and_parse_responses() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": "00000",
            "msg": "success",
            "data": [
                {"coinName": "BTC", "available": "0.1", "frozen": "0.02", "equity": "0.12"},
                {"coinName": "USDT", "available": "10", "frozen": "0", "equity": "10"}
            ]
        }),
        json!([{
            "symbol": "BTCUSDT",
            "side": "LONG",
            "size": "0.5",
            "openValue": "32500",
            "liquidatePrice": "50000",
            "unrealizePnl": "10",
            "leverage": "5"
        }]),
        json!({
            "symbol": "BTCUSDT",
            "makerCommissionRate": "0.0002",
            "takerCommissionRate": "0.0005"
        }),
        json!({
            "symbol": "BTCUSDT",
                "orderId": 1001,
                "clientOrderId": "CLIENT1",
                "price": "65000",
                "origQty": "0.01",
                "executedQty": "0.004",
                "status": "PARTIALLY_FILLED",
                "type": "LIMIT",
                "side": "BUY",
                "time": 1700000000000i64,
                "updateTime": 1700000001000i64
        }),
        json!([{
            "symbol": "BTCUSDT",
            "orderId": 1002,
            "clientOrderId": "CLIENT2",
            "price": "70000",
            "origQty": "0.02",
            "executedQty": "0",
            "status": "NEW",
            "type": "LIMIT",
            "side": "SELL"
        }]),
        json!([{
            "symbol": "BTCUSDT",
            "id": 2001,
            "orderId": 1001,
            "clientOrderId": "CLIENT1",
            "price": "64950",
            "qty": "0.004",
            "commission": "0.2598",
            "commissionAsset": "USDT",
            "side": "BUY",
            "time": 1700000001000i64
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
            symbols: vec![perp_symbol_scope()],
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
    assert_signed_weex_request(&requests[0], "GET", "/api/v2/account/assets");
    assert_signed_weex_request(&requests[1], "GET", "/capi/v3/account/position/allPosition");
    assert_signed_weex_request(&requests[2], "GET", "/capi/v3/account/commissionRate");
    assert_signed_weex_request(&requests[3], "GET", "/api/v3/order");
    assert_signed_weex_request(&requests[4], "GET", "/api/v3/openOrders");
    assert_signed_weex_request(&requests[5], "GET", "/api/v3/myTrades");
    for (request, spec) in requests.into_iter().zip([
        "get_balances_spot",
        "get_positions_perp",
        "get_fees_perp",
        "query_order_spot",
        "get_open_orders_spot",
        "get_recent_fills_spot",
    ]) {
        request_spec(spec)
            .assert_matches(&actual_request(request))
            .expect("WEEX private read request spec");
    }
}

#[tokio::test]
async fn weex_adapter_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"orderId": 2001, "clientOrderId": "LIMIT1", "success": true}),
        json!({"orderId": 2001, "origClientOrderId": "LIMIT1", "success": true}),
        json!([
            {"orderId": 2002, "success": true}
        ]),
        json!({"data": [{"orderId": 3001, "clientOrderId": "BATCH1", "success": true}]}),
        json!({"data": [{"orderId": 3001, "origClientOrderId": "BATCH1", "success": true}]}),
    ])
    .await;
    let adapter = private_adapter(base_url);
    assert!(adapter.capabilities().supports_batch_place_order);
    assert!(adapter.capabilities().supports_batch_cancel_order);

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

    let batch_placed = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![PlaceOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("batch-place-order"),
                symbol: perp_symbol_scope(),
                client_order_id: Some("BATCH1".to_string()),
                side: OrderSide::Sell,
                position_side: Some(PositionSide::Short),
                order_type: OrderType::Limit,
                time_in_force: Some(TimeInForce::GTC),
                quantity: "0.03".to_string(),
                price: Some("66000".to_string()),
                quote_quantity: None,
                reduce_only: false,
                post_only: false,
            }],
        })
        .await
        .expect("batch place");
    assert_eq!(
        batch_placed.orders[0].exchange_order_id.as_deref(),
        Some("3001")
    );

    let batch_cancelled = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![CancelOrderRequest {
                schema_version: EXCHANGE_API_SCHEMA_VERSION,
                context: context("batch-cancel-order"),
                symbol: perp_symbol_scope(),
                client_order_id: Some("BATCH1".to_string()),
                exchange_order_id: Some("3001".to_string()),
            }],
        })
        .await
        .expect("batch cancel");
    assert_eq!(batch_cancelled.cancelled_count, 1);

    let requests = seen.lock().unwrap().clone();
    assert_signed_weex_request(&requests[0], "POST", "/capi/v3/order");
    assert!(requests[0].body.contains("\"positionSide\":\"LONG\""));
    assert_eq!(
        requests[0].query.get("positionSide").map(String::as_str),
        None
    );
    assert_signed_weex_request(&requests[1], "DELETE", "/capi/v3/order");
    assert_signed_weex_request(&requests[2], "DELETE", "/capi/v3/allOpenOrders");
    assert_signed_weex_request(&requests[3], "POST", "/capi/v3/batchOrders");
    assert!(requests[3].body.contains("\"batchOrders\""));
    assert!(requests[3].body.contains("\"positionSide\":\"SHORT\""));
    assert_signed_weex_request(&requests[4], "DELETE", "/capi/v3/batchOrders");
    assert!(requests[4].body.contains("\"orderIdList\""));
    for (request, spec) in requests.into_iter().zip([
        "place_order_perp",
        "cancel_order_perp",
        "cancel_all_orders_perp",
        "batch_place_orders_perp",
        "batch_cancel_orders_perp",
    ]) {
        request_spec(spec)
            .assert_matches(&actual_request(request))
            .expect("WEEX private write request spec");
    }
}

#[test]
fn weex_signing_should_match_known_hmac() {
    let prehash = super::signing::prehash(
        "1591089508404",
        "GET",
        "/api/v3/market/depth",
        "symbol=BTCUSDT&limit=20",
        "",
    );
    let signature = super::signing::sign_prehash("secret", &prehash).expect("signature");
    assert_eq!(signature, "74UazbPZpLIfsdaGjiRcR5N/ZumxpVcWCRkJEKhuPJs=");
}

#[test]
fn weex_signing_vector_should_verify_access_hmac_base64() {
    signing_vector("access_place_perpetual_limit")
        .verify()
        .expect("WEEX ACCESS signing vector");
}

#[tokio::test]
async fn weex_request_spec_should_cover_private_order_write() {
    let (base_url, seen) = spawn_rest_server(vec![fixture("order_ack")]).await;
    let adapter = private_adapter(base_url);

    adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("request-spec-place"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("CLIENT1".to_string()),
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

    let request = seen.lock().unwrap()[0].clone();
    request_spec("place_order_perp")
        .assert_matches(&actual_request(request))
        .expect("WEEX request spec");
}

#[test]
fn weex_parser_fixtures_should_cover_success_empty_error_and_missing_field() {
    let balances = parse_balances(
        &exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        rustcta_types::AccountId::new("account").expect("account"),
        MarketType::Spot,
        &[],
        &fixture("balance"),
    )
    .expect("balance fixture");
    assert_eq!(balances[0].balances[0].asset, "BTC");

    let order = parse_order(
        &exchange_id(),
        Some(&spot_symbol_scope()),
        MarketType::Spot,
        &fixture("order_ack"),
    )
    .expect("order fixture")
    .expect("order");
    assert_eq!(order.exchange_order_id.as_deref(), Some("1001"));

    let empty_fills = parse_fills(
        &exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        rustcta_types::AccountId::new("account").expect("account"),
        Some(&spot_symbol_scope()),
        MarketType::Spot,
        &fixture("fills_empty"),
    )
    .expect("empty fills fixture");
    assert!(empty_fills.is_empty());

    let missing = parse_order(
        &exchange_id(),
        Some(&spot_symbol_scope()),
        MarketType::Spot,
        &fixture("order_missing_side"),
    )
    .expect_err("missing side should be a decode error");
    assert!(format!("{missing}").contains("order missing side"));

    assert_eq!(fixture("error")["code"], "-1121");
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "balance" => include_str!("../../../../../tests/fixtures/exchanges/weex/balance.json"),
        "order_ack" => {
            include_str!("../../../../../tests/fixtures/exchanges/weex/order_ack.json")
        }
        "fills_empty" => {
            include_str!("../../../../../tests/fixtures/exchanges/weex/fills_empty.json")
        }
        "error" => include_str!("../../../../../tests/fixtures/exchanges/weex/error.json"),
        "order_missing_side" => {
            include_str!("../../../../../tests/fixtures/exchanges/weex/order_missing_side.json")
        }
        other => panic!("unknown WEEX fixture {other}"),
    };
    serde_json::from_str(text).expect("WEEX fixture JSON")
}

fn private_adapter(base_url: String) -> WeexGatewayAdapter {
    WeexGatewayAdapter::new(WeexGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        contract_rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        recv_window_ms: 5_000,
        ..WeexGatewayConfig::default()
    })
    .expect("adapter")
}
