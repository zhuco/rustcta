use rustcta_exchange_api::{
    BalancesRequest, BatchCancelOrdersRequest, BatchExecutionMode, BatchPlaceOrdersRequest,
    CancelAllOrdersRequest, CancelOrderRequest, CapabilitySupport, ExchangeApiError,
    ExchangeClient, FeesRequest, OpenOrdersRequest, PlaceOrderRequest, PositionsRequest,
    QueryOrderRequest, RecentFillsRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    ExchangeErrorClass, MarketType, OrderSide, OrderStatus, OrderType, PositionSide,
};
use serde_json::{json, Value};

use crate::request_spec::{ActualHttpRequest, RequestSpec};
use crate::signing_spec::SigningVector;

use super::capabilities::BITRUE_COMPOSED_BATCH_MAX_ITEMS;
use super::test_support::{
    context, exchange_id, perp_symbol_scope, spawn_rest_server, spot_symbol_scope,
};
use super::{BitrueGatewayAdapter, BitrueGatewayConfig};

#[tokio::test]
async fn bitrue_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = BitrueGatewayAdapter::default_public().expect("adapter");
    assert!(adapter.capabilities().supports_public_rest);
    assert!(!adapter.capabilities().supports_private_rest);
    assert!(matches!(
        adapter.capabilities().capabilities_v2.private_rest,
        CapabilitySupport::Unsupported { .. }
    ));

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
async fn bitrue_adapter_should_sign_private_readbacks_and_parse_responses() {
    let (base_url, seen) = spawn_rest_server(vec![
        fixture("balance"),
        fixture("balance"),
        fixture("position"),
        fixture("fees"),
        fixture("order_ack"),
        fixture("open_orders"),
        fixture("fill"),
    ])
    .await;
    let adapter = private_adapter(base_url);
    let capabilities = adapter.capabilities();
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        BatchExecutionMode::ComposedSequential
    );
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.max_items,
        Some(BITRUE_COMPOSED_BATCH_MAX_ITEMS)
    );

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

    let spot_fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-fees"),
            symbols: vec![spot_symbol_scope()],
        })
        .await
        .expect("spot fees");
    assert_eq!(spot_fees.fees[0].maker_rate, "0.0015");
    assert_eq!(spot_fees.fees[0].taker_rate, "0.002");

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
    assert_eq!(fills.fills[0].side, OrderSide::Buy);

    let requests = seen.lock().unwrap().clone();
    assert_request_matches_spec(&requests[0], "get_balances_spot");
    assert_request_matches_spec(&requests[1], "get_balances_spot");
    assert!(!requests[1].query.contains_key("symbol"));
    assert_request_matches_spec(&requests[2], "get_positions_perp");
    assert_request_matches_spec(&requests[3], "get_fees_perp");
    assert_request_matches_spec(&requests[4], "query_order_spot");
    assert_request_matches_spec(&requests[5], "get_open_orders_spot");
    assert_request_matches_spec(&requests[6], "get_recent_fills_spot");
}

#[tokio::test]
async fn bitrue_adapter_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "data": {"orderId": 2001, "clientOrderId": "LIMIT1"}}),
        json!({"code": 0, "data": {"orderId": 2001, "clientOrderId": "LIMIT1"}}),
        json!({"code": 0, "data": [{
            "contractName": "E-BTC-USDT",
            "orderId": 2002,
            "clientOrderId": "OPEN1",
            "price": "66000",
            "origQty": "0.03",
            "executedQty": "0",
            "status": "NEW",
            "type": "LIMIT",
            "side": "SELL"
        }]}),
        json!({"code": 0, "data": {"orderId": 2002, "clientOrderId": "OPEN1"}}),
        json!({"code": 0, "data": {"orderId": 3001, "clientOrderId": "BATCH1"}}),
        json!({"code": 0, "data": {"orderId": 3001, "clientOrderId": "BATCH1"}}),
    ])
    .await;
    let adapter = private_adapter(base_url);

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
        .cancel_all_orders(CancelAllOrdersRequest {
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
    assert_request_matches_spec(&requests[0], "place_order_perp");
    assert_request_matches_spec(&requests[1], "cancel_order_perp");
    assert_request_matches_spec(&requests[2], "cancel_all_orders_perp");
    assert_request_matches_spec(&requests[3], "cancel_order_perp");
    assert_request_matches_spec(&requests[4], "batch_place_perp");
    assert_request_matches_spec(&requests[5], "batch_cancel_perp");
}

#[tokio::test]
async fn bitrue_adapter_should_reject_unverified_order_variants_before_rest() {
    let (base_url, seen) = spawn_rest_server(Vec::new()).await;
    let adapter = private_adapter(base_url);
    let request = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("unsupported-place"),
        symbol: spot_symbol_scope(),
        client_order_id: Some("UNSUPPORTED1".to_string()),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::None),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "1".to_string(),
        price: Some("100".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };

    let quote_error = adapter
        .place_order(PlaceOrderRequest {
            quote_quantity: Some("100".to_string()),
            ..request.clone()
        })
        .await
        .expect_err("quote-sized order should be unsupported");
    assert!(matches!(quote_error, ExchangeApiError::Unsupported { .. }));

    let spot_post_only_error = adapter
        .place_order(PlaceOrderRequest {
            order_type: OrderType::PostOnly,
            time_in_force: Some(TimeInForce::GTX),
            post_only: true,
            ..request.clone()
        })
        .await
        .expect_err("spot post-only should be unsupported");
    assert!(matches!(
        spot_post_only_error,
        ExchangeApiError::Unsupported { .. }
    ));

    let stop_error = adapter
        .place_order(PlaceOrderRequest {
            symbol: perp_symbol_scope(),
            position_side: Some(PositionSide::Long),
            order_type: OrderType::StopMarket,
            time_in_force: None,
            price: None,
            ..request
        })
        .await
        .expect_err("stop order should be unsupported");
    assert!(matches!(stop_error, ExchangeApiError::Unsupported { .. }));

    assert!(seen.lock().unwrap().is_empty());
}

#[test]
fn bitrue_signing_should_match_known_hmac() {
    let query =
        "price=65000&quantity=0.01&side=BUY&symbol=BTCUSDT&timestamp=1649404670162&type=LIMIT";
    let signature = super::signing::sign_raw_query("secret", query).expect("signature");
    assert_eq!(
        signature,
        "c4b6ab7dae7526d1d779a336cdf2ea93ceef7d02cec1173f08f302147940b0cd"
    );
    let vector: SigningVector = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitrue/signing_vectors/spot_query_hmac.json"
    ))
    .expect("spot signing vector");
    vector.verify().expect("spot signing vector verifies");
}

#[test]
fn bitrue_futures_signing_should_match_known_hmac() {
    let signature = super::signing::sign_futures_payload(
        "secret",
        1649404670162,
        "POST",
        "/fapi/v2/order",
        r#"{"contractName":"E-BTC-USDT","side":"BUY","type":"LIMIT","volume":"0.01","price":"65000","recvWindow":5000}"#,
    )
    .expect("signature");
    assert_eq!(
        signature,
        "fb2fd2a036366564942f76ec1960d912d41f7f888e1065eb85e29eb86c0677cc"
    );
    let vector: SigningVector = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitrue/signing_vectors/futures_payload_hmac.json"
    ))
    .expect("futures signing vector");
    vector.verify().expect("futures signing vector verifies");
}

#[tokio::test]
async fn bitrue_private_parser_fixtures_should_cover_empty_and_error_responses() {
    let (base_url, _seen) = spawn_rest_server(vec![fixture("open_orders_empty")]).await;
    let adapter = private_adapter(base_url);
    let open = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("empty-open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
            page: None,
        })
        .await
        .expect("empty open orders");
    assert!(open.orders.is_empty());

    let (base_url, _seen) = spawn_rest_server(vec![fixture("error")]).await;
    let adapter = private_adapter(base_url);
    let error = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("error"),
            symbol: spot_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("missing".to_string()),
        })
        .await
        .expect_err("error response");
    assert!(matches!(
        error,
        ExchangeApiError::Exchange(ref exchange_error)
            if exchange_error.class == ExchangeErrorClass::OrderNotFound
    ));
}

#[tokio::test]
async fn bitrue_batch_should_enforce_declared_max_items() {
    let (base_url, seen) = spawn_rest_server(Vec::new()).await;
    let adapter = private_adapter(base_url);
    let template = PlaceOrderRequest {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("batch-too-large-order"),
        symbol: perp_symbol_scope(),
        client_order_id: Some("BATCH-TOO-LARGE".to_string()),
        side: OrderSide::Buy,
        position_side: Some(PositionSide::Long),
        order_type: OrderType::Limit,
        time_in_force: Some(TimeInForce::GTC),
        quantity: "0.01".to_string(),
        price: Some("65000".to_string()),
        quote_quantity: None,
        reduce_only: false,
        post_only: false,
    };
    let orders = (0..=BITRUE_COMPOSED_BATCH_MAX_ITEMS)
        .map(|index| PlaceOrderRequest {
            client_order_id: Some(format!("BATCH-{index}")),
            ..template.clone()
        })
        .collect();

    let error = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-too-large"),
            exchange: exchange_id(),
            orders,
        })
        .await
        .expect_err("too large batch");
    assert!(matches!(error, ExchangeApiError::Unsupported { .. }));
    assert!(seen.lock().unwrap().is_empty());
}

#[test]
fn bitrue_request_specs_should_cover_declared_private_rest_operations() {
    let operations = load_request_specs()
        .into_iter()
        .map(|spec| spec.operation)
        .collect::<std::collections::BTreeSet<_>>();
    for operation in [
        "get_balances_spot",
        "get_balances_perp",
        "get_positions_perp",
        "get_fees_perp",
        "place_order_spot",
        "place_order_perp",
        "cancel_order_spot",
        "cancel_order_perp",
        "batch_place_spot",
        "batch_place_perp",
        "batch_cancel_spot",
        "batch_cancel_perp",
        "get_open_orders_spot",
        "get_open_orders_perp",
        "get_recent_fills_spot",
        "get_recent_fills_perp",
        "listen_key_spot",
        "listen_key_perp",
    ] {
        assert!(operations.contains(operation), "missing {operation}");
    }
}

fn fixture(name: &str) -> Value {
    let text = match name {
        "balance" => include_str!("../../../../../tests/fixtures/exchanges/bitrue/balance.json"),
        "position" => include_str!("../../../../../tests/fixtures/exchanges/bitrue/position.json"),
        "fees" => include_str!("../../../../../tests/fixtures/exchanges/bitrue/fees.json"),
        "order_ack" => {
            include_str!("../../../../../tests/fixtures/exchanges/bitrue/order_ack.json")
        }
        "open_orders" => {
            include_str!("../../../../../tests/fixtures/exchanges/bitrue/open_orders.json")
        }
        "open_orders_empty" => {
            include_str!("../../../../../tests/fixtures/exchanges/bitrue/open_orders_empty.json")
        }
        "fill" => include_str!("../../../../../tests/fixtures/exchanges/bitrue/fill.json"),
        "error" => include_str!("../../../../../tests/fixtures/exchanges/bitrue/error.json"),
        _ => unreachable!("unknown Bitrue fixture"),
    };
    serde_json::from_str(text).expect("fixture json")
}

fn private_adapter(base_url: String) -> BitrueGatewayAdapter {
    BitrueGatewayAdapter::new(BitrueGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        futures_rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        recv_window_ms: 5_000,
        ..BitrueGatewayConfig::default()
    })
    .expect("adapter")
}

fn load_request_specs() -> Vec<RequestSpec> {
    serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitrue/request_specs/private_rest.json"
    ))
    .expect("request specs")
}

fn assert_request_matches_spec(request: &super::test_support::SeenRequest, operation: &str) {
    let specs = load_request_specs();
    let spec = specs
        .iter()
        .find(|spec| spec.operation == operation)
        .unwrap_or_else(|| panic!("missing Bitrue request spec {operation}"));
    spec.assert_matches(&actual_request(request))
        .unwrap_or_else(|error| panic!("{error}"));
}

fn actual_request(request: &super::test_support::SeenRequest) -> ActualHttpRequest {
    ActualHttpRequest::new(request.method.clone(), request.path.clone())
        .with_query(
            request
                .query
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        )
        .with_headers(
            request
                .headers
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        )
}
