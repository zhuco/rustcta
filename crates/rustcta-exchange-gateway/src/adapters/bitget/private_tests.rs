use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchExecutionMode, CancelAllOrdersRequest,
    CancelOrderRequest, CapabilitySupport, CredentialScope, ExchangeApiError, ExchangeClient,
    FeesRequest, OpenOrdersRequest, PlaceOrderRequest, PositionsRequest, QueryOrderRequest,
    QuoteMarketOrderRequest, RecentFillsRequest, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderStatus, OrderType, PositionSide};
use serde_json::json;

use super::test_support::{
    assert_signed_bitget_request, assert_signed_bitget_request_method, context, exchange_id,
    perpetual_symbol_scope, spawn_rest_server, symbol_scope,
};
use super::{BitgetGatewayAdapter, BitgetGatewayConfig};
use crate::request_spec::RequestSpec;

#[tokio::test]
async fn bitget_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = BitgetGatewayAdapter::new(BitgetGatewayConfig {
        api_key: None,
        api_secret: None,
        passphrase: None,
        enabled_private_rest: true,
        ..BitgetGatewayConfig::default()
    })
    .expect("adapter");
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
async fn bitget_adapter_should_query_unified_balances_when_market_type_is_omitted() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "00000",
        "msg": "success",
        "data": {
            "accountEquity": "11.13919278",
            "usdtEquity": "11.13921165",
            "assets": [
                {
                    "coin": "USDT",
                    "equity": "6.19300826",
                    "balance": "6.19300826",
                    "available": "6.19300826",
                    "locked": "0"
                },
                {
                    "coin": "BGB",
                    "equity": "1.15582129",
                    "balance": "1.15582129",
                    "available": "1.15582129",
                    "locked": "0"
                }
            ]
        }
    })])
    .await;
    let adapter = BitgetGatewayAdapter::new(BitgetGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..BitgetGatewayConfig::default()
    })
    .expect("adapter");

    let balances = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("unified-balances"),
            exchange: exchange_id(),
            market_type: None,
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect("balances");
    assert_eq!(balances.balances.len(), 1);
    assert_eq!(balances.balances[0].market_type, MarketType::Perpetual);
    assert_eq!(balances.balances[0].balances.len(), 1);
    assert_eq!(balances.balances[0].balances[0].asset, "USDT");
    assert_eq!(balances.balances[0].balances[0].total, 11.13921165);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_signed_bitget_request(&requests[0], "/api/v3/account/assets");
    assert!(!requests[0].query.contains_key("productType"));
}

#[test]
fn bitget_adapter_should_declare_capabilities_v2_for_toolchain_audit() {
    let adapter = BitgetGatewayAdapter::new(BitgetGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        enabled_public_streams: true,
        ..BitgetGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert_eq!(
        capabilities.market_types,
        vec![MarketType::Spot, MarketType::Perpetual]
    );
    assert!(capabilities.supports_positions);
    assert!(capabilities.supports_reduce_only);
    assert!(capabilities.supports_post_only);
    assert!(capabilities
        .supports_time_in_force
        .contains(&TimeInForce::IOC));
    assert!(capabilities
        .supports_time_in_force
        .contains(&TimeInForce::FOK));
    assert!(matches!(
        &capabilities.capabilities_v2.public_rest,
        CapabilitySupport::Native
    ));
    assert!(matches!(
        &capabilities.capabilities_v2.private_rest,
        CapabilitySupport::Native
    ));
    assert!(matches!(
        &capabilities.capabilities_v2.public_streams,
        CapabilitySupport::Native
    ));
    assert!(matches!(
        &capabilities.capabilities_v2.private_streams,
        CapabilitySupport::RestFallback { .. }
    ));
    assert_eq!(
        capabilities.capabilities_v2.batch_place_orders.mode,
        BatchExecutionMode::ComposedSequential
    );
    assert!(
        capabilities
            .capabilities_v2
            .batch_place_orders
            .supports_partial_failure
    );
    assert_eq!(
        capabilities.capabilities_v2.fills_history.max_limit,
        Some(100)
    );
    assert!(capabilities.capabilities_v2.fills_history.supports_since);
    assert_eq!(
        capabilities.capabilities_v2.credential_scopes,
        vec![CredentialScope::ReadOnly, CredentialScope::Trade]
    );
    assert!(capabilities.capabilities_v2.stream_runtime.resync.orders);
}

#[tokio::test]
async fn bitget_adapter_should_route_perpetual_place_cancel_and_positions() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": "00000", "data": {"orderId": "P1001", "clientOid": "PERP1"}}),
        json!({"code": "00000", "data": {"orderId": "P1001", "clientOid": "PERP1"}}),
        json!({
            "code": "00000",
            "data": [{
                "symbol": "BTCUSDT",
                "holdSide": "long",
                "total": "0.02",
                "openPriceAvg": "65000",
                "markPrice": "65100",
                "unrealizedPL": "2.0",
                "leverage": "5"
            }]
        }),
    ])
    .await;
    let adapter = BitgetGatewayAdapter::new(BitgetGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..BitgetGatewayConfig::default()
    })
    .expect("adapter");
    let symbol = perpetual_symbol_scope();

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-place"),
            symbol: symbol.clone(),
            client_order_id: Some("PERP1".to_string()),
            side: OrderSide::Buy,
            position_side: Some(PositionSide::Net),
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::IOC),
            quantity: "0.02".to_string(),
            price: Some("65000".to_string()),
            quote_quantity: None,
            reduce_only: true,
            post_only: false,
        })
        .await
        .expect("perpetual place");
    assert_eq!(placed.order.market_type, MarketType::Perpetual);
    assert_eq!(placed.order.exchange_order_id.as_deref(), Some("P1001"));
    assert!(placed.order.reduce_only);

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-cancel"),
            symbol: symbol.clone(),
            client_order_id: Some("PERP1".to_string()),
            exchange_order_id: Some("P1001".to_string()),
        })
        .await
        .expect("perpetual cancel");
    assert!(cancelled.cancelled);
    assert_eq!(cancelled.order.market_type, MarketType::Perpetual);

    let positions = adapter
        .get_positions(PositionsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-positions"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Perpetual),
            symbols: vec![symbol.exchange_symbol.clone()],
        })
        .await
        .expect("perpetual positions");
    assert_eq!(positions.positions.len(), 1);
    assert_eq!(positions.positions[0].market_type, MarketType::Perpetual);
    assert_eq!(positions.positions[0].side, PositionSide::Long);
    assert_eq!(positions.positions[0].quantity, 0.02);
    assert_eq!(positions.positions[0].entry_price, Some(65000.0));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 3);
    assert_signed_bitget_request_method(&requests[0], "POST", "/api/v2/mix/order/place-order");
    let body = requests[0].body.as_ref().expect("place body");
    assert_eq!(body["productType"], "USDT-FUTURES");
    assert_eq!(body["marginMode"], "crossed");
    assert_eq!(body["marginCoin"], "USDT");
    assert_eq!(body["symbol"], "BTCUSDT");
    assert_eq!(body["orderType"], "limit");
    assert_eq!(body["force"], "ioc");
    assert_eq!(body["reduceOnly"], "yes");

    assert_signed_bitget_request_method(&requests[1], "POST", "/api/v2/mix/order/cancel-order");
    let body = requests[1].body.as_ref().expect("cancel body");
    assert_eq!(body["productType"], "USDT-FUTURES");
    assert_eq!(body["marginCoin"], "USDT");
    assert_eq!(body["orderId"], "P1001");
    assert_eq!(body["clientOid"], "PERP1");

    assert_signed_bitget_request(&requests[2], "/api/v2/mix/position/all-position");
    assert_eq!(
        requests[2].query.get("productType").map(String::as_str),
        Some("USDT-FUTURES")
    );
    assert_eq!(
        requests[2].query.get("marginCoin").map(String::as_str),
        Some("USDT")
    );
    assert_eq!(
        requests[2].query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
}

#[test]
fn bitget_signing_should_match_known_hmac_vector() {
    let vector = signing_vector("place_order_limit.json");
    let signature = super::signing::sign_request(
        vector["secret"].as_str().expect("secret"),
        vector["timestamp"].as_str().expect("timestamp"),
        vector["method"].as_str().expect("method"),
        vector["request_path"].as_str().expect("request path"),
        vector["body"].as_str().expect("body"),
    );
    assert_eq!(
        signature,
        vector["expected_signature"]
            .as_str()
            .expect("expected signature")
    );
}

fn load_request_spec(path: &str) -> RequestSpec {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/bitget/request_specs/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("request spec fixture");
    serde_json::from_str(&text).expect("request spec fixture")
}

fn signing_vector(path: &str) -> serde_json::Value {
    let path = format!(
        "{}/../../tests/fixtures/exchanges/bitget/signing_vectors/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(path).expect("signing vector fixture");
    serde_json::from_str(&text).expect("signing vector fixture")
}

#[tokio::test]
async fn bitget_adapter_should_route_private_rest_readbacks_with_signed_headers() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": "00000",
            "data": [
                {"coin": "BTC", "available": "0.5", "frozen": "0.1", "totalAmount": "0.6"},
                {"coin": "ETH", "available": "0", "frozen": "0", "totalAmount": "0"}
            ]
        }),
        json!({
            "code": "00000",
            "data": {
                "symbol": "BTCUSDT",
                "orderId": "1001",
                "clientOid": "CID1001",
                "side": "buy",
                "orderType": "limit",
                "force": "gtc",
                "status": "filled",
                "price": "70000",
                "size": "0.01",
                "filledSize": "0.01",
                "priceAvg": "70001",
                "cTime": "1743054548123",
                "uTime": "1743054549123"
            }
        }),
        json!({
            "code": "00000",
            "data": [{
                "symbol": "BTCUSDT",
                "orderId": "1002",
                "clientOid": "CID1002",
                "side": "sell",
                "orderType": "limit",
                "force": "gtc",
                "status": "live",
                "price": "71000",
                "size": "0.02",
                "filledSize": "0",
                "cTime": "1743054548123",
                "uTime": "1743054549123"
            }]
        }),
        json!({
            "code": "00000",
            "data": {
                "makerFeeRate": "0.0008",
                "takerFeeRate": "0.001"
            }
        }),
        json!({
            "code": "00000",
            "data": {
                "fillList": [{
                    "symbol": "BTCUSDT",
                    "tradeId": "T1001",
                    "orderId": "1001",
                    "clientOid": "CID1001",
                    "side": "buy",
                    "price": "70000",
                    "size": "0.01",
                    "feeCcy": "USDT",
                    "fee": "-0.7",
                    "tradeScope": "taker",
                    "cTime": "1743054548123"
                }]
            }
        }),
    ])
    .await;
    let adapter = BitgetGatewayAdapter::new(BitgetGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..BitgetGatewayConfig::default()
    })
    .expect("adapter");

    assert!(adapter.capabilities().supports_private_rest);
    assert!(adapter.capabilities().supports_balances);
    assert!(adapter.capabilities().supports_query_order);
    assert!(adapter.capabilities().supports_open_orders);
    assert!(adapter.capabilities().supports_fees);
    assert!(adapter.capabilities().supports_recent_fills);

    let balances = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            assets: vec!["BTC".to_string()],
        })
        .await
        .expect("balances");
    assert_eq!(balances.balances.len(), 1);
    assert_eq!(balances.balances[0].balances.len(), 1);
    assert_eq!(balances.balances[0].balances[0].asset, "BTC");
    assert_eq!(balances.balances[0].balances[0].available, 0.5);
    assert_eq!(balances.balances[0].balances[0].locked, 0.1);

    let queried = adapter
        .query_order(QueryOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("query"),
            symbol: symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("1001".to_string()),
        })
        .await
        .expect("query order")
        .order
        .expect("order");
    assert_eq!(queried.exchange_order_id.as_deref(), Some("1001"));
    assert_eq!(queried.client_order_id.as_deref(), Some("CID1001"));
    assert_eq!(queried.side, OrderSide::Buy);
    assert_eq!(queried.status, OrderStatus::Filled);
    assert_eq!(queried.quantity, "0.01");
    assert_eq!(queried.average_fill_price.as_deref(), Some("70001"));

    let open_orders = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
            page: None,
        })
        .await
        .expect("open orders");
    assert_eq!(open_orders.orders.len(), 1);
    assert_eq!(
        open_orders.orders[0].exchange_order_id.as_deref(),
        Some("1002")
    );
    assert_eq!(open_orders.orders[0].side, OrderSide::Sell);
    assert_eq!(open_orders.orders[0].status, OrderStatus::New);

    let empty_open_orders = super::private_parser::parse_orders(
        &exchange_id(),
        Some(&symbol_scope()),
        MarketType::Spot,
        &json!({
            "code": "00000",
            "msg": "success",
            "requestTime": 1780949470141i64,
            "data": {
                "endId": null,
                "entrustedList": null
            }
        }),
    )
    .expect("empty open orders response");
    assert!(empty_open_orders.is_empty());

    let fees = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("fees");
    assert_eq!(fees.fees.len(), 1);
    assert_eq!(fees.fees[0].maker_rate, "0.0008");
    assert_eq!(fees.fees[0].taker_rate, "0.001");

    let fills = adapter
        .get_recent_fills(RecentFillsRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fills"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
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
    assert_eq!(fills.fills.len(), 1);
    assert_eq!(fills.fills[0].fill_id.as_deref(), Some("T1001"));
    assert_eq!(fills.fills[0].order_id.as_deref(), Some("1001"));
    assert_eq!(fills.fills[0].side, OrderSide::Buy);
    assert_eq!(fills.fills[0].price, 70000.0);
    assert_eq!(fills.fills[0].quantity, 0.01);
    assert_eq!(fills.fills[0].fee_asset.as_deref(), Some("USDT"));
    assert_eq!(fills.fills[0].fee_amount, Some(0.7));

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 5);
    load_request_spec("get_balances.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("get balances request spec");
    assert_signed_bitget_request(&requests[0], "/api/v2/spot/account/assets");
    load_request_spec("query_order.json")
        .assert_matches(&requests[1].actual_http_request())
        .expect("query order request spec");
    assert_signed_bitget_request(&requests[1], "/api/v2/spot/trade/orderInfo");
    assert_eq!(
        requests[1].query.get("orderId").map(String::as_str),
        Some("1001")
    );
    assert_signed_bitget_request(&requests[2], "/api/v2/spot/trade/unfilled-orders");
    assert_eq!(
        requests[2].query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    load_request_spec("get_open_orders.json")
        .assert_matches(&requests[2].actual_http_request())
        .expect("get open orders request spec");
    assert_signed_bitget_request(&requests[3], "/api/v3/account/fee-rate");
    load_request_spec("get_fees.json")
        .assert_matches(&requests[3].actual_http_request())
        .expect("get fees request spec");
    assert_eq!(
        requests[3].query.get("category").map(String::as_str),
        Some("SPOT")
    );
    load_request_spec("get_recent_fills.json")
        .assert_matches(&requests[4].actual_http_request())
        .expect("get recent fills request spec");
    assert_signed_bitget_request(&requests[4], "/api/v2/spot/trade/fills");
    assert_eq!(
        requests[4].query.get("limit").map(String::as_str),
        Some("25")
    );
    assert_eq!(
        requests[4].query.get("orderId").map(String::as_str),
        Some("1001")
    );
    assert!(requests
        .iter()
        .all(|request| !request.query.contains_key("secret")));
}

#[tokio::test]
async fn bitget_adapter_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": "00000", "data": {"orderId": "2001", "clientOid": "LIMIT1"}}),
        json!({"code": "00000", "data": {"orderId": "2002", "clientOid": "QUOTE1"}}),
        json!({"code": "00000", "data": {"orderId": "2001", "clientOid": "LIMIT1"}}),
        json!({"code": "00000", "data": {"orderId": "2003", "clientOid": "CANCELALL1"}}),
        json!({"code": "00000", "data": {"orderId": "2004", "clientOid": "AMEND1"}}),
    ])
    .await;
    let adapter = BitgetGatewayAdapter::new(BitgetGatewayConfig {
        rest_base_url: base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        passphrase: Some("passphrase".to_string()),
        enabled_private_rest: true,
        ..BitgetGatewayConfig::default()
    })
    .expect("adapter");
    let capabilities = adapter.capabilities();
    assert!(capabilities.supports_place_order);
    assert!(capabilities.supports_cancel_order);
    assert!(capabilities.supports_cancel_all_orders);
    assert!(capabilities.supports_quote_market_order);
    assert!(capabilities.supports_amend_order);
    assert!(!capabilities.supports_order_list);

    let placed = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place-order"),
            symbol: symbol_scope(),
            client_order_id: Some("LIMIT1".to_string()),
            side: OrderSide::Buy,
            position_side: None,
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
    assert_eq!(placed.order.client_order_id.as_deref(), Some("LIMIT1"));

    let quote = adapter
        .place_quote_market_order(QuoteMarketOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("quote-order"),
            symbol: symbol_scope(),
            client_order_id: Some("QUOTE1".to_string()),
            side: OrderSide::Buy,
            quote_quantity: "25.5".to_string(),
        })
        .await
        .expect("quote order");
    assert_eq!(quote.order.exchange_order_id.as_deref(), Some("2002"));
    assert_eq!(quote.order.order_type, OrderType::Market);

    let cancelled = adapter
        .cancel_order(CancelOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-order"),
            symbol: symbol_scope(),
            client_order_id: Some("LIMIT1".to_string()),
            exchange_order_id: Some("2001".to_string()),
        })
        .await
        .expect("cancel order");
    assert!(cancelled.cancelled);
    assert_eq!(cancelled.order.status, OrderStatus::Cancelled);

    let cancel_all = adapter
        .cancel_all_orders(CancelAllOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("cancel-all"),
            exchange: exchange_id(),
            market_type: Some(MarketType::Spot),
            symbol: Some(symbol_scope()),
        })
        .await
        .expect("cancel all");
    assert_eq!(cancel_all.cancelled_count, 1);

    let amended = adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend-order"),
            symbol: symbol_scope(),
            client_order_id: Some("AMEND1".to_string()),
            exchange_order_id: Some("2004".to_string()),
            new_client_order_id: None,
            new_quantity: "0.015".to_string(),
        })
        .await
        .expect("amend order");
    assert_eq!(amended.order.exchange_order_id.as_deref(), Some("2004"));
    assert_eq!(amended.order.client_order_id.as_deref(), Some("AMEND1"));
    assert_eq!(amended.order.quantity, "0.015");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests.len(), 5);

    load_request_spec("place_order.json")
        .assert_matches(&requests[0].actual_http_request())
        .expect("place order request spec");
    assert_signed_bitget_request_method(&requests[0], "POST", "/api/v2/spot/trade/place-order");
    let body = requests[0].body.as_ref().expect("place body");
    assert_eq!(body["symbol"], "BTCUSDT");
    assert_eq!(body["side"], "buy");
    assert_eq!(body["orderType"], "limit");
    assert_eq!(body["size"], "0.02");
    assert_eq!(body["price"], "65000");
    assert_eq!(body["force"], "gtc");
    assert_eq!(body["clientOid"], "LIMIT1");

    assert_signed_bitget_request_method(&requests[1], "POST", "/api/v2/spot/trade/place-order");
    load_request_spec("place_quote_market_order.json")
        .assert_matches(&requests[1].actual_http_request())
        .expect("quote market order request spec");
    let body = requests[1].body.as_ref().expect("quote body");
    assert_eq!(body["orderType"], "market");
    assert_eq!(body["size"], "25.5");
    assert_eq!(body["clientOid"], "QUOTE1");

    load_request_spec("cancel_order.json")
        .assert_matches(&requests[2].actual_http_request())
        .expect("cancel order request spec");
    assert_signed_bitget_request_method(&requests[2], "POST", "/api/v2/spot/trade/cancel-order");
    let body = requests[2].body.as_ref().expect("cancel body");
    assert_eq!(body["symbol"], "BTCUSDT");
    assert_eq!(body["orderId"], "2001");
    assert_eq!(body["clientOid"], "LIMIT1");

    assert_signed_bitget_request_method(
        &requests[3],
        "POST",
        "/api/v2/spot/trade/cancel-symbol-order",
    );
    load_request_spec("cancel_all_orders.json")
        .assert_matches(&requests[3].actual_http_request())
        .expect("cancel all request spec");
    let body = requests[3].body.as_ref().expect("cancel all body");
    assert_eq!(body["symbol"], "BTCUSDT");

    assert_signed_bitget_request_method(&requests[4], "POST", "/api/v3/trade/modify-order");
    load_request_spec("amend_order.json")
        .assert_matches(&requests[4].actual_http_request())
        .expect("amend order request spec");
    let body = requests[4].body.as_ref().expect("amend body");
    assert_eq!(body["category"], "SPOT");
    assert_eq!(body["symbol"], "BTCUSDT");
    assert_eq!(body["qty"], "0.015");
    assert_eq!(body["autoCancel"], "no");
    assert_eq!(body["orderId"], "2004");
    assert_eq!(body["clientOid"], "AMEND1");
}
