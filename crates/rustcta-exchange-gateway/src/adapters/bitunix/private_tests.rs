use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelAllOrdersRequest, CancelOrderRequest, ExchangeApiError, ExchangeClient,
    OpenOrdersRequest, PlaceOrderRequest, PositionsRequest, QueryOrderRequest,
    QuoteMarketOrderRequest, RecentFillsRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::json;

use super::private::{
    BitunixMarginMode, BitunixPositionMode, BitunixStopType, BitunixTpslOrderRequest,
    BitunixTriggerOrderType,
};
use super::test_support::{
    assert_signed_bitunix_request, context, exchange_id, perp_symbol_scope, spawn_rest_server,
    spot_symbol_scope,
};
use super::{BitunixGatewayAdapter, BitunixGatewayConfig};

#[test]
fn bitunix_capabilities_v2_should_declare_native_batch_and_stream_resync() {
    let adapter = BitunixGatewayAdapter::new(BitunixGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        enabled_private_streams: true,
        ..BitunixGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();

    assert!(capabilities.capabilities_v2.private_rest.is_supported());
    assert!(capabilities.capabilities_v2.private_streams.is_supported());
    assert!(matches!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        rustcta_exchange_api::BatchExecutionMode::Native
    ));
    assert!(
        capabilities
            .capabilities_v2
            .batch_place_orders
            .same_market_type_required
    );
    assert!(
        capabilities
            .capabilities_v2
            .stream_runtime
            .resync
            .order_book
    );
    assert!(capabilities.capabilities_v2.stream_runtime.resync.positions);
}

#[tokio::test]
async fn bitunix_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = BitunixGatewayAdapter::default_public().expect("adapter");
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
async fn bitunix_adapter_should_sign_private_readbacks_and_parse_responses() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": 0,
            "data": [{"marginCoin": "USDT", "available": "1000", "frozen": "10", "margin": "5"}]
        }),
        json!({
            "code": 0,
            "data": [{
                "positionId": "P1",
                "symbol": "BTCUSDT",
                "qty": "0.5",
                "side": "LONG",
                "leverage": 10,
                "avgOpenPrice": "65000",
                "liqPrice": "50000",
                "unrealizedPNL": "12.5"
            }]
        }),
        json!({
            "code": 0,
            "data": {
                "symbol": "BTCUSDT",
                "orderId": "1001",
                "clientId": "CLIENT1",
                "price": "65000",
                "qty": "0.01",
                "tradeQty": "0.004",
                "status": "PART_FILLED",
                "type": "LIMIT",
                "effect": "GTC",
                "side": "BUY",
                "ctime": 1700000000000i64,
                "mtime": 1700000001000i64
            }
        }),
        json!({
            "code": 0,
            "data": {"orderList": [{
                "symbol": "BTCUSDT",
                "orderId": "1002",
                "clientId": "CLIENT2",
                "price": "70000",
                "qty": "0.02",
                "tradeQty": "0",
                "status": "NEW",
                "type": "LIMIT",
                "side": "SELL"
            }]}
        }),
        json!({
            "code": 0,
            "data": {"tradeList": [{
                "tradeId": "2001",
                "orderId": "1001",
                "clientId": "CLIENT1",
                "symbol": "BTCUSDT",
                "price": "64950",
                "qty": "0.004",
                "fee": "0.2598",
                "feeCoin": "USDT",
                "side": "BUY",
                "roleType": "TAKER",
                "ctime": 1700000001000i64
            }]}
        }),
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
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("2001"));

    let requests = seen.lock().unwrap().clone();
    assert_signed_bitunix_request(&requests[0], "GET", "/api/v1/futures/account");
    assert_signed_bitunix_request(
        &requests[1],
        "GET",
        "/api/v1/futures/position/get_pending_positions",
    );
    assert_signed_bitunix_request(
        &requests[2],
        "GET",
        "/api/v1/futures/trade/get_order_detail",
    );
    assert_signed_bitunix_request(
        &requests[3],
        "GET",
        "/api/v1/futures/trade/get_pending_orders",
    );
    assert_signed_bitunix_request(
        &requests[4],
        "GET",
        "/api/v1/futures/trade/get_history_trades",
    );
}

#[tokio::test]
async fn bitunix_adapter_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "data": {"orderId": "2001", "clientId": "LIMIT1"}}),
        json!({"code": 0, "data": {"orderId": "2001", "clientId": "LIMIT1", "symbol": "BTCUSDT", "qty": "0.01", "side": "BUY", "type": "LIMIT"}}),
        json!({"code": 0, "data": {"successList": [
            {"orderId": "3001", "clientId": "BATCH1", "symbol": "BTCUSDT", "qty": "0.01", "side": "BUY", "type": "LIMIT"},
            {"orderId": "3002", "clientId": "BATCH2", "symbol": "BTCUSDT", "qty": "0.02", "side": "SELL", "type": "LIMIT"}
        ]}}),
        json!({"code": 0, "data": {"successList": [{"orderId": "2001", "clientId": "LIMIT1"}]}}),
        json!({"code": 0, "data": {"successList": [{"orderId": "3001", "clientId": "BATCH1"}, {"orderId": "3002", "clientId": "BATCH2"}]}}),
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

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("LIMIT1".to_string()),
            exchange_order_id: Some("2001".to_string()),
            new_client_order_id: None,
            new_quantity: "0.01".to_string(),
        })
        .await
        .expect("amend");
    assert_eq!(amended.order.quantity, "0.01");

    let batch = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: exchange_id(),
            orders: vec![
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-1"),
                    symbol: perp_symbol_scope(),
                    client_order_id: Some("BATCH1".to_string()),
                    side: OrderSide::Buy,
                    position_side: Some(PositionSide::Long),
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.01".to_string(),
                    price: Some("65000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-2"),
                    symbol: perp_symbol_scope(),
                    client_order_id: Some("BATCH2".to_string()),
                    side: OrderSide::Sell,
                    position_side: Some(PositionSide::Short),
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.02".to_string(),
                    price: Some("70000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
            ],
        })
        .await
        .expect("batch place");
    assert_eq!(batch.orders.len(), 2);

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

    let batch_cancel = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: exchange_id(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-1"),
                    symbol: perp_symbol_scope(),
                    client_order_id: Some("BATCH1".to_string()),
                    exchange_order_id: Some("3001".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-2"),
                    symbol: perp_symbol_scope(),
                    client_order_id: Some("BATCH2".to_string()),
                    exchange_order_id: Some("3002".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");
    assert_eq!(batch_cancel.cancelled_count, 2);

    let requests = seen.lock().unwrap().clone();
    assert_signed_bitunix_request(&requests[0], "POST", "/api/v1/futures/trade/place_order");
    assert!(requests[0].body.contains("\"tradeSide\":\"OPEN\""));
    assert_signed_bitunix_request(&requests[1], "POST", "/api/v1/futures/trade/modify_order");
    assert!(requests[1].body.contains("\"qty\":\"0.01\""));
    assert_signed_bitunix_request(&requests[2], "POST", "/api/v1/futures/trade/batch_order");
    assert!(requests[2].body.contains("\"orderList\""));
    assert_signed_bitunix_request(&requests[3], "POST", "/api/v1/futures/trade/cancel_orders");
    assert_signed_bitunix_request(&requests[4], "POST", "/api/v1/futures/trade/cancel_orders");
}

#[tokio::test]
async fn bitunix_adapter_should_route_advanced_futures_controls() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "data": {"symbol": "BTCUSDT", "marginCoin": "USDT", "leverage": 10, "marginMode": "ISOLATION"}}),
        json!({"code": 0, "data": [{"symbol": "BTCUSDT", "marginCoin": "USDT", "leverage": 12}]}),
        json!({"code": 0, "data": [{"symbol": "BTCUSDT", "marginCoin": "USDT", "marginMode": "CROSS"}]}),
        json!({"code": 0, "data": [{"positionMode": "HEDGE"}]}),
        json!({"code": 0, "data": ""}),
        json!({"code": 0, "data": {"orderId": "TPSL1"}}),
        json!({"code": 0, "data": {"orderId": "TPSL1"}}),
    ])
    .await;
    let adapter = private_adapter(base_url);

    let leverage_mode = adapter
        .get_leverage_margin_mode(perp_symbol_scope(), "USDT")
        .await
        .expect("get leverage mode");
    assert_eq!(leverage_mode.data["leverage"], json!(10));
    adapter
        .change_leverage(perp_symbol_scope(), "USDT", 12)
        .await
        .expect("change leverage");
    adapter
        .change_margin_mode(perp_symbol_scope(), "USDT", BitunixMarginMode::Cross)
        .await
        .expect("change margin mode");
    adapter
        .change_position_mode(BitunixPositionMode::Hedge)
        .await
        .expect("change position mode");
    adapter
        .adjust_position_margin(
            perp_symbol_scope(),
            "USDT",
            "-100",
            Some(PositionSide::Long),
            None,
        )
        .await
        .expect("adjust margin");
    adapter
        .place_tpsl_order(BitunixTpslOrderRequest {
            symbol: perp_symbol_scope(),
            position_id: "POS1".to_string(),
            take_profit_price: Some("70000".to_string()),
            take_profit_stop_type: Some(BitunixStopType::LastPrice),
            take_profit_order_type: Some(BitunixTriggerOrderType::Limit),
            take_profit_order_price: Some("69950".to_string()),
            take_profit_quantity: Some("0.01".to_string()),
            stop_loss_price: Some("60000".to_string()),
            stop_loss_stop_type: Some(BitunixStopType::MarkPrice),
            stop_loss_order_type: Some(BitunixTriggerOrderType::Market),
            stop_loss_order_price: None,
            stop_loss_quantity: Some("0.01".to_string()),
        })
        .await
        .expect("place tpsl");
    adapter
        .cancel_tpsl_order(perp_symbol_scope(), "TPSL1")
        .await
        .expect("cancel tpsl");

    let requests = seen.lock().unwrap().clone();
    assert_signed_bitunix_request(
        &requests[0],
        "GET",
        "/api/v1/futures/account/get_leverage_margin_mode",
    );
    assert_eq!(
        requests[0].query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_signed_bitunix_request(
        &requests[1],
        "POST",
        "/api/v1/futures/account/change_leverage",
    );
    assert!(requests[1].body.contains("\"leverage\":12"));
    assert_signed_bitunix_request(
        &requests[2],
        "POST",
        "/api/v1/futures/account/change_margin_mode",
    );
    assert!(requests[2].body.contains("\"marginMode\":\"CROSS\""));
    assert_signed_bitunix_request(
        &requests[3],
        "POST",
        "/api/v1/futures/account/change_position_mode",
    );
    assert!(requests[3].body.contains("\"positionMode\":\"HEDGE\""));
    assert_signed_bitunix_request(
        &requests[4],
        "POST",
        "/api/v1/futures/account/adjust_position_margin",
    );
    assert!(requests[4].body.contains("\"side\":\"LONG\""));
    assert_signed_bitunix_request(&requests[5], "POST", "/api/v1/futures/tpsl/place_order");
    assert!(requests[5].body.contains("\"tpPrice\":\"70000\""));
    assert!(requests[5].body.contains("\"slOrderType\":\"MARKET\""));
    assert_signed_bitunix_request(&requests[6], "POST", "/api/v1/futures/tpsl/cancel_order");
    assert!(requests[6].body.contains("\"orderId\":\"TPSL1\""));
}

#[tokio::test]
async fn bitunix_adapter_should_use_spot_order_request_shape() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": "0", "data": {"orderId": "5001", "symbol": "BTCUSDT", "side": 2, "type": 1, "status": 1, "volume": "0.01", "price": "65000"}}),
        json!({"code": "0", "data": [
            {"orderId": "5002", "symbol": "BTCUSDT", "side": 2, "type": 1, "status": 1, "volume": "0.01", "price": "65000"},
            {"orderId": "5003", "symbol": "BTCUSDT", "side": 1, "type": 1, "status": 1, "volume": "0.02", "price": "70000"}
        ]}),
    ])
    .await;
    let adapter = private_adapter(base_url);

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-place"),
            symbol: spot_symbol_scope(),
            client_order_id: None,
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: None,
            quantity: "0.01".to_string(),
            price: Some("65000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("spot place order");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("5001"));
    assert_eq!(placed.order.side, OrderSide::Buy);

    let batch = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-batch-place"),
            exchange: exchange_id(),
            orders: vec![
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("spot-batch-1"),
                    symbol: spot_symbol_scope(),
                    client_order_id: None,
                    side: OrderSide::Buy,
                    position_side: None,
                    order_type: OrderType::Limit,
                    time_in_force: None,
                    quantity: "0.01".to_string(),
                    price: Some("65000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("spot-batch-2"),
                    symbol: spot_symbol_scope(),
                    client_order_id: None,
                    side: OrderSide::Sell,
                    position_side: None,
                    order_type: OrderType::Limit,
                    time_in_force: None,
                    quantity: "0.02".to_string(),
                    price: Some("70000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
            ],
        })
        .await
        .expect("spot batch place");
    assert_eq!(batch.orders.len(), 2);
    assert_eq!(batch.orders[1].side, OrderSide::Sell);

    let requests = seen.lock().unwrap().clone();
    assert_signed_bitunix_request(&requests[0], "POST", "/api/spot/v1/order/place_order");
    assert!(requests[0].body.contains("\"side\":2"));
    assert!(requests[0].body.contains("\"type\":1"));
    assert!(requests[0].body.contains("\"volume\":\"0.01\""));
    assert!(!requests[0].body.contains("\"orderType\""));
    assert!(!requests[0].body.contains("\"qty\""));
    assert_signed_bitunix_request(&requests[1], "POST", "/api/spot/v1/order/place_order/batch");
    assert!(requests[1].body.contains("\"orderList\""));
    assert!(requests[1].body.contains("\"side\":1"));
}

#[tokio::test]
async fn bitunix_adapter_should_support_spot_buy_quote_market_order_only() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "0",
        "data": {"orderId": "5101", "symbol": "BTCUSDT", "side": 2, "type": 2, "status": 1, "volume": "0", "amount": "25"}
    })])
    .await;
    let adapter = private_adapter(base_url);

    let placed = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-quote-market"),
            symbol: spot_symbol_scope(),
            client_order_id: None,
            side: OrderSide::Buy,
            quote_quantity: "25".to_string(),
        })
        .await
        .expect("spot quote market");
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("5101"));

    let requests = seen.lock().unwrap().clone();
    assert_signed_bitunix_request(&requests[0], "POST", "/api/spot/v1/order/place_order");
    assert!(requests[0].body.contains("\"type\":2"));
    assert!(requests[0].body.contains("\"amount\":\"25\""));
    assert!(requests[0].body.contains("\"volume\":\"0\""));

    let unsupported = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-quote-market"),
            symbol: perp_symbol_scope(),
            client_order_id: None,
            side: OrderSide::Buy,
            quote_quantity: "25".to_string(),
        })
        .await
        .expect_err("perp quote market unsupported");
    assert!(matches!(unsupported, ExchangeApiError::Unsupported { .. }));
}

#[tokio::test]
async fn bitunix_adapter_should_compose_spot_cancel_all_from_open_orders() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": "0",
            "data": {"orderList": [
                {"symbol": "BTCUSDT", "orderId": "4001", "clientId": "SPOT1", "side": "BUY", "type": "LIMIT", "status": "NEW", "volume": "0.01", "price": "65000"},
                {"symbol": "BTCUSDT", "orderId": "4002", "clientId": "SPOT2", "side": "SELL", "type": "LIMIT", "status": "NEW", "volume": "0.02", "price": "70000"}
            ]}
        }),
        json!({"code": "0", "data": {"orderIdList": ["4001", "4002"]}}),
    ])
    .await;
    let adapter = private_adapter(base_url);

    let cancelled = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("spot-cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
        })
        .await
        .expect("spot cancel all");

    assert_eq!(cancelled.cancelled_count, 2);
    assert!(cancelled
        .orders
        .iter()
        .all(|order| order.status == OrderStatus::Cancelled));
    let requests = seen.lock().unwrap().clone();
    assert_signed_bitunix_request(&requests[0], "POST", "/api/spot/v1/order/pending/list");
    assert_signed_bitunix_request(&requests[1], "POST", "/api/spot/v1/order/cancel");
    assert!(requests[1].body.contains("\"orderIdList\""));
}

#[test]
fn bitunix_signing_should_match_double_sha256_vector() {
    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/bitunix/signing_vectors/rest_double_sha256.json"
    ))
    .expect("signing vector");
    let signature = super::signing::sign_request(
        vector["api_key"].as_str().expect("api_key"),
        vector["secret"].as_str().expect("secret"),
        vector["nonce"].as_str().expect("nonce"),
        vector["timestamp"].as_str().expect("timestamp"),
        vector["query_params"].as_str().expect("query_params"),
        vector["body"].as_str().expect("body"),
    )
    .expect("signature");
    assert_eq!(
        signature,
        vector["expected_signature"]
            .as_str()
            .expect("expected signature")
    );
}

fn private_adapter(base_url: String) -> BitunixGatewayAdapter {
    BitunixGatewayAdapter::new(BitunixGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        futures_rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..BitunixGatewayConfig::default()
    })
    .expect("adapter")
}
