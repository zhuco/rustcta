use std::collections::HashMap;

use rustcta_exchange_api::{
    AmendOrderRequest, BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest,
    CancelOrderRequest, ExchangeApiError, ExchangeClient, FeesRequest, OpenOrdersRequest,
    PlaceOrderRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide, OrderType, PositionSide, TimeInForce};
use serde_json::json;

use super::private::PhemexPositionMode;
use super::private_parser::parse_order_state;
use super::signing::{sign_phemex_request, sign_phemex_ws_auth};
use super::test_support::{
    assert_signed_request, context, perp_symbol_scope, spawn_rest_server, spot_symbol_scope,
};
use super::{PhemexGatewayAdapter, PhemexGatewayConfig};
use crate::request_spec::RequestSpec;
use crate::signing_spec::SigningVector;
use crate::AdapterBackedGateway;

#[tokio::test]
async fn phemex_adapter_should_keep_private_operations_unsupported_without_credentials() {
    let adapter = PhemexGatewayAdapter::default_public().expect("adapter");
    let err = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: adapter.exchange(),
            market_type: Some(MarketType::Spot),
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect_err("missing credentials");

    assert!(matches!(
        err,
        ExchangeApiError::Unsupported {
            operation: "phemex.get_balances"
        }
    ));
}

#[tokio::test]
async fn phemex_adapter_should_sign_private_readback_requests_and_parse_responses() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "msg": "OK",
        "data": {
            "rows": [{
                "currency": "USDT",
                "availableBalanceRv": "12.5",
                "accountBalanceRv": "20.0"
            }]
        }
    })])
    .await;
    let adapter = private_adapter(base_url);

    let response = adapter
        .get_balances(BalancesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("balances"),
            exchange: adapter.exchange(),
            market_type: Some(MarketType::Perpetual),
            assets: vec!["USDT".to_string()],
        })
        .await
        .expect("balances");

    assert_eq!(response.balances[0].balances[0].asset, "USDT");
    assert_eq!(response.balances[0].balances[0].available, 12.5);
    let request = seen.lock().unwrap()[0].clone();
    assert_signed_request(&request, "GET", "/g-accounts/accountPositions");
    assert_eq!(
        request.query.get("currency").map(String::as_str),
        Some("USDT")
    );
}

#[tokio::test]
async fn phemex_adapter_should_load_perp_fee_rate() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "msg": "OK",
        "data": {
            "symbolFeeRates": [{
                "symbol": "BTCUSDT",
                "makerFeeRateEr": "10000",
                "takerFeeRateEr": "60000"
            }]
        }
    })])
    .await;
    let adapter = private_adapter(base_url);

    let response = adapter
        .get_fees(FeesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("fees"),
            symbols: vec![perp_symbol_scope()],
        })
        .await
        .expect("fees");

    assert_eq!(response.fees[0].symbol.market_type, MarketType::Perpetual);
    assert_eq!(response.fees[0].maker_rate, "0.0001");
    assert_eq!(
        response.fees[0].source.as_deref(),
        Some("phemex.futures_fee_rate")
    );
    let request = seen.lock().unwrap()[0].clone();
    assert_signed_request(&request, "GET", "/api-data/futures/fee-rate");
    assert_eq!(
        request.query.get("settleCurrency").map(String::as_str),
        Some("USDT")
    );
}

#[tokio::test]
async fn phemex_adapter_should_route_private_order_mutations() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "msg": "OK",
        "data": {
            "symbol": "BTCUSDT",
            "orderID": "exchange-1",
            "clOrdID": "client-1",
            "side": "Buy",
            "ordType": "Limit",
            "timeInForce": "GoodTillCancel",
            "ordStatus": "New",
            "orderQtyRq": "0.001",
            "priceRp": "61000"
        }
    })])
    .await;
    let adapter = private_adapter(base_url);

    let response = adapter
        .place_order(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("place"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("client-1".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "0.001".to_string(),
            price: Some("61000".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: false,
        })
        .await
        .expect("place");

    assert_eq!(
        response.order.exchange_order_id.as_deref(),
        Some("exchange-1")
    );
    let request = seen.lock().unwrap()[0].clone();
    assert_signed_request(&request, "PUT", "/g-orders/create");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(
        request.query.get("timeInForce").map(String::as_str),
        Some("GoodTillCancel")
    );
}

#[tokio::test]
async fn phemex_adapter_should_batch_place_spot_and_perp_orders_with_composed_requests() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": 0,
            "msg": "OK",
            "data": {
                "symbol": "sBTCUSDT",
                "orderID": "spot-exchange-1",
                "clOrdID": "spot-client-1",
                "side": "Buy",
                "ordType": "Limit",
                "timeInForce": "GoodTillCancel",
                "ordStatus": "New",
                "baseQtyRq": "0.01",
                "priceRp": "61000"
            }
        }),
        json!({
            "code": 0,
            "msg": "OK",
            "data": {
                "symbol": "BTCUSDT",
                "orderID": "perp-exchange-1",
                "clOrdID": "perp-client-1",
                "side": "Sell",
                "ordType": "Limit",
                "timeInForce": "GoodTillCancel",
                "ordStatus": "New",
                "orderQtyRq": "0.002",
                "priceRp": "62000"
            }
        }),
    ])
    .await;
    let adapter = private_adapter(base_url);

    let response = adapter
        .batch_place_orders(BatchPlaceOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-place"),
            exchange: adapter.exchange(),
            orders: vec![
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-place-spot"),
                    symbol: spot_symbol_scope(),
                    client_order_id: Some("spot-client-1".to_string()),
                    side: OrderSide::Buy,
                    position_side: None,
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.01".to_string(),
                    price: Some("61000".to_string()),
                    quote_quantity: None,
                    reduce_only: false,
                    post_only: false,
                },
                PlaceOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("batch-place-perp"),
                    symbol: perp_symbol_scope(),
                    client_order_id: Some("perp-client-1".to_string()),
                    side: OrderSide::Sell,
                    position_side: None,
                    order_type: OrderType::Limit,
                    time_in_force: Some(TimeInForce::GTC),
                    quantity: "0.002".to_string(),
                    price: Some("62000".to_string()),
                    quote_quantity: None,
                    reduce_only: true,
                    post_only: false,
                },
            ],
        })
        .await
        .expect("batch place");

    assert_eq!(response.orders.len(), 2);
    assert_eq!(
        response.orders[0].exchange_order_id.as_deref(),
        Some("spot-exchange-1")
    );
    assert_eq!(
        response.orders[1].exchange_order_id.as_deref(),
        Some("perp-exchange-1")
    );
    let requests = seen.lock().unwrap().clone();
    assert_signed_request(&requests[0], "PUT", "/spot/orders/create");
    assert_eq!(
        requests[0].query.get("symbol").map(String::as_str),
        Some("sBTCUSDT")
    );
    assert_signed_request(&requests[1], "PUT", "/g-orders/create");
    assert_eq!(
        requests[1].query.get("reduceOnly").map(String::as_str),
        Some("true")
    );
}

#[tokio::test]
async fn phemex_adapter_should_amend_spot_and_perp_orders() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": 0,
            "msg": "OK",
            "data": {
                "symbol": "sBTCUSDT",
                "orderID": "spot-order",
                "clOrdID": "spot-client",
                "side": "Buy",
                "ordType": "Limit",
                "ordStatus": "New",
                "baseQtyRq": "0.25"
            }
        }),
        json!({
            "code": 0,
            "msg": "OK",
            "data": {
                "symbol": "BTCUSDT",
                "orderID": "perp-order",
                "clOrdID": "perp-client",
                "side": "Sell",
                "ordType": "Limit",
                "ordStatus": "New",
                "orderQtyRq": "0.01"
            }
        }),
    ])
    .await;
    let adapter = private_adapter(base_url);

    adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend-spot"),
            symbol: spot_symbol_scope(),
            client_order_id: None,
            exchange_order_id: Some("spot-order".to_string()),
            new_client_order_id: None,
            new_quantity: "0.25".to_string(),
        })
        .await
        .expect("spot amend");
    adapter
        .amend_order(AmendOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("amend-perp"),
            symbol: perp_symbol_scope(),
            client_order_id: Some("perp-client".to_string()),
            exchange_order_id: None,
            new_client_order_id: None,
            new_quantity: "0.01".to_string(),
        })
        .await
        .expect("perp amend");

    let requests = seen.lock().unwrap().clone();
    assert_signed_request(&requests[0], "PUT", "/spot/orders");
    assert_eq!(
        requests[0].query.get("baseQtyEv").map(String::as_str),
        Some("25000000")
    );
    assert_eq!(
        requests[0].query.get("orderID").map(String::as_str),
        Some("spot-order")
    );
    assert_signed_request(&requests[1], "PUT", "/g-orders/replace");
    assert_eq!(
        requests[1].query.get("origClOrdID").map(String::as_str),
        Some("perp-client")
    );
    assert_eq!(
        requests[1].query.get("orderQtyRq").map(String::as_str),
        Some("0.01")
    );
}

#[tokio::test]
async fn phemex_adapter_should_batch_cancel_perp_orders() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "msg": "OK",
        "data": {}
    })])
    .await;
    let adapter = private_adapter(base_url);

    let response = adapter
        .batch_cancel_orders(BatchCancelOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("batch-cancel"),
            exchange: adapter.exchange(),
            cancels: vec![
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-1"),
                    symbol: perp_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("order-1".to_string()),
                },
                CancelOrderRequest {
                    schema_version: EXCHANGE_API_SCHEMA_VERSION,
                    context: context("cancel-2"),
                    symbol: perp_symbol_scope(),
                    client_order_id: None,
                    exchange_order_id: Some("order-2".to_string()),
                },
            ],
        })
        .await
        .expect("batch cancel");

    assert_eq!(response.cancelled_count, 2);
    let request = seen.lock().unwrap()[0].clone();
    assert_signed_request(&request, "DELETE", "/g-orders");
    assert_eq!(
        request.query.get("orderID").map(String::as_str),
        Some("order-1,order-2")
    );
    assert_eq!(
        request.query.get("posSide").map(String::as_str),
        Some("Merged")
    );
}

#[tokio::test]
async fn phemex_adapter_should_manage_perp_position_settings() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "msg": "OK", "data": "success"}),
        json!({"code": 0, "msg": "OK", "data": "success"}),
        json!({"code": 0, "msg": "OK", "data": "success"}),
        json!({"code": 0, "msg": "OK", "data": "success"}),
    ])
    .await;
    let adapter = private_adapter(base_url);

    adapter
        .switch_position_mode(perp_symbol_scope(), PhemexPositionMode::Hedged)
        .await
        .expect("switch position mode");
    adapter
        .set_one_way_leverage(perp_symbol_scope(), "5")
        .await
        .expect("one-way leverage");
    adapter
        .set_hedged_leverage(perp_symbol_scope(), "3", "4")
        .await
        .expect("hedged leverage");
    adapter
        .assign_position_balance(perp_symbol_scope(), PositionSide::Long, "25")
        .await
        .expect("assign balance");

    let requests = seen.lock().unwrap().clone();
    assert_signed_request(&requests[0], "PUT", "/g-positions/switch-pos-mode-sync");
    assert_eq!(
        requests[0].query.get("targetPosMode").map(String::as_str),
        Some("Hedged")
    );
    assert_signed_request(&requests[1], "PUT", "/g-positions/leverage");
    assert_eq!(
        requests[1].query.get("leverageRr").map(String::as_str),
        Some("5")
    );
    assert_signed_request(&requests[2], "PUT", "/g-positions/leverage");
    assert_eq!(
        requests[2].query.get("longLeverageRr").map(String::as_str),
        Some("3")
    );
    assert_eq!(
        requests[2].query.get("shortLeverageRr").map(String::as_str),
        Some("4")
    );
    assert_signed_request(&requests[3], "POST", "/g-positions/assign");
    assert_eq!(
        requests[3].query.get("posSide").map(String::as_str),
        Some("Long")
    );
    assert_eq!(
        requests[3].query.get("posBalanceRv").map(String::as_str),
        Some("25")
    );
}

#[tokio::test]
async fn phemex_adapter_should_query_perp_risk_and_history_extensions() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "msg": "OK", "data": {"riskUnits": []}}),
        json!({"code": 0, "msg": "OK", "data": {"rows": []}}),
        json!({"code": 0, "msg": "OK", "data": {"rows": []}}),
        json!({"code": 0, "msg": "OK", "data": {"rows": []}}),
    ])
    .await;
    let adapter = private_adapter(base_url);

    adapter.get_perp_risk_units().await.expect("risk units");
    adapter
        .get_perp_funding_fees(perp_symbol_scope(), Some(250), Some(1))
        .await
        .expect("funding fees");
    adapter
        .get_perp_closed_positions(Some(perp_symbol_scope()), None, Some(20), Some(2))
        .await
        .expect("closed positions");
    adapter
        .get_spot_funds_history(Some("USDT"), Some(20), Some(3))
        .await
        .expect("spot funds");

    let requests = seen.lock().unwrap().clone();
    assert_signed_request(&requests[0], "GET", "/g-accounts/riskUnit");
    assert_signed_request(&requests[1], "GET", "/api-data/g-futures/funding-fees");
    assert_eq!(
        requests[1].query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(
        requests[1].query.get("limit").map(String::as_str),
        Some("200")
    );
    assert_signed_request(&requests[2], "GET", "/api-data/g-futures/closedPosition");
    assert_eq!(
        requests[2].query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_signed_request(&requests[3], "GET", "/api-data/spots/funds");
    assert_eq!(
        requests[3].query.get("currency").map(String::as_str),
        Some("USDT")
    );
}

#[tokio::test]
async fn phemex_adapter_should_query_wallet_and_spot_account_history_extensions() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "msg": "OK", "data": {"rows": []}}),
        json!({"address": "0xabc", "tag": null}),
        json!({"code": 0, "msg": "OK", "data": [{"currency": "USDT"}]}),
        json!({"code": 0, "msg": "OK", "data": [{"currency": "USDT"}]}),
    ])
    .await;
    let adapter = private_adapter(base_url);

    adapter
        .get_spot_pnls(Some(1_700_000_000_000), Some(1_700_086_400_000))
        .await
        .expect("spot pnls");
    adapter
        .get_deposit_address("USDT", "ETH")
        .await
        .expect("deposit address");
    adapter
        .get_recent_deposits("USDT", Some(250), Some(1))
        .await
        .expect("deposits");
    adapter
        .get_recent_withdrawals("USDT", Some(50), Some(2))
        .await
        .expect("withdrawals");

    let requests = seen.lock().unwrap().clone();
    assert_signed_request(&requests[0], "GET", "/api-data/spots/pnls");
    assert_eq!(
        requests[0].query.get("start").map(String::as_str),
        Some("1700000000000")
    );
    assert_signed_request(&requests[1], "GET", "/exchange/wallets/v2/depositAddress");
    assert_eq!(
        requests[1].query.get("chainName").map(String::as_str),
        Some("ETH")
    );
    assert_signed_request(&requests[2], "GET", "/exchange/wallets/depositList");
    assert_eq!(
        requests[2].query.get("limit").map(String::as_str),
        Some("200")
    );
    assert_signed_request(&requests[3], "GET", "/exchange/wallets/withdrawList");
    assert_eq!(
        requests[3].query.get("offset").map(String::as_str),
        Some("2")
    );
}

#[tokio::test]
async fn phemex_adapter_should_route_transfer_and_convert_extensions() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"amountEv": 100000000_i64, "currency": "USDT"}),
        json!([{"currency": "USDT"}]),
        json!({"requestKey": "spot-key"}),
        json!([{"requestKey": "spot-key"}]),
        json!({"requestKey": "futures-key"}),
        json!([{"requestKey": "futures-key"}]),
        json!({"requestKey": "universal-key"}),
        json!({"code": "quote-code"}),
        json!({"linkKey": "convert-key"}),
        json!([{"linkKey": "convert-key"}]),
    ])
    .await;
    let adapter = private_adapter(base_url);

    adapter
        .transfer_between_spot_and_futures("USDT", 100_000_000, 2)
        .await
        .expect("spot futures transfer");
    adapter
        .get_transfer_history(
            "USDT",
            Some(10),
            Some(20),
            Some(1),
            Some(250),
            Some(2),
            Some(10),
        )
        .await
        .expect("transfer history");
    adapter
        .create_spot_sub_account_transfer("USDT", 50_000_000, Some("spot-key"))
        .await
        .expect("spot sub transfer");
    adapter
        .get_spot_sub_account_transfer_history("USDT", Some(10), Some(20), Some(2), Some(250))
        .await
        .expect("spot sub history");
    adapter
        .create_futures_sub_account_transfer("USDT", 60_000_000, Some("futures-key"))
        .await
        .expect("futures sub transfer");
    adapter
        .get_futures_sub_account_transfer_history("USDT", Some(10), Some(20), Some(3), Some(250))
        .await
        .expect("futures sub history");
    adapter
        .create_universal_transfer(
            "USDT",
            70_000_000,
            "SPOT",
            Some(1001),
            Some(1002),
            Some("uni-key"),
        )
        .await
        .expect("universal transfer");
    adapter
        .get_convert_quote("USDT", "BTC", 100_000_000)
        .await
        .expect("convert quote");
    adapter
        .create_convert("USDT", "BTC", "quote-code", Some(100_000_000))
        .await
        .expect("convert");
    adapter
        .get_convert_history(
            Some("USDT"),
            Some("BTC"),
            Some(10),
            Some(20),
            Some(4),
            Some(250),
        )
        .await
        .expect("convert history");

    let requests = seen.lock().unwrap().clone();
    assert_signed_request(&requests[0], "POST", "/assets/transfer");
    assert_eq!(
        requests[0].body,
        r#"{"amountEv":100000000,"currency":"USDT","moveOp":2}"#
    );
    assert_signed_request(&requests[1], "GET", "/assets/transfer");
    assert_eq!(
        requests[1].query.get("limit").map(String::as_str),
        Some("200")
    );
    assert_eq!(
        requests[1].query.get("bizType").map(String::as_str),
        Some("10")
    );
    assert_signed_request(&requests[2], "POST", "/assets/spots/sub-accounts/transfer");
    assert!(requests[2].body.contains(r#""requestKey":"spot-key""#));
    assert_signed_request(&requests[3], "GET", "/assets/spots/sub-accounts/transfer");
    assert_signed_request(
        &requests[4],
        "POST",
        "/assets/futures/sub-accounts/transfer",
    );
    assert!(requests[4].body.contains(r#""requestKey":"futures-key""#));
    assert_signed_request(&requests[5], "GET", "/assets/futures/sub-accounts/transfer");
    assert_signed_request(&requests[6], "POST", "/assets/universal-transfer");
    assert!(requests[6].body.contains(r#""bizType":"SPOT""#));
    assert!(requests[6].body.contains(r#""fromUserId":1001"#));
    assert_signed_request(&requests[7], "GET", "/assets/quote");
    assert_eq!(
        requests[7].query.get("fromAmountEv").map(String::as_str),
        Some("100000000")
    );
    assert_signed_request(&requests[8], "POST", "/assets/convert");
    assert!(requests[8].body.contains(r#""code":"quote-code""#));
    assert_signed_request(&requests[9], "GET", "/assets/convert");
    assert_eq!(
        requests[9].query.get("startTime").map(String::as_str),
        Some("10")
    );
    assert_eq!(
        requests[9].query.get("limit").map(String::as_str),
        Some("200")
    );
}

#[tokio::test]
async fn phemex_adapter_should_route_deposit_and_withdraw_v2_extensions() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "msg": "OK", "data": [{"currency": "USDT"}]}),
        json!({"code": 0, "msg": "OK", "data": {"address": "0xabc"}}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": {"id": 100}}),
        json!({"code": 0, "msg": "OK", "data": "OK"}),
        json!({"code": 0, "msg": "OK", "data": {"currency": "USDT"}}),
    ])
    .await;
    let adapter = private_adapter(base_url);

    adapter
        .get_deposit_chain_config("USDT")
        .await
        .expect("deposit chain config");
    adapter
        .get_deposit_address_v2("USDT", "ETH")
        .await
        .expect("deposit address v2");
    adapter
        .get_deposit_history_v2(
            &["USDT".to_string(), "BTC".to_string()],
            Some(1),
            Some(250),
            Some(true),
        )
        .await
        .expect("deposit history v2");
    adapter
        .get_withdraw_history_v2(
            &["USDT".to_string()],
            &["ETH".to_string(), "TRX".to_string()],
            Some(2),
            Some(250),
            Some(true),
        )
        .await
        .expect("withdraw history v2");
    adapter
        .create_withdraw("USDT", "0xabc", "12.5", "ETH", Some("memo"))
        .await
        .expect("create withdraw");
    adapter.cancel_withdraw(100).await.expect("cancel withdraw");
    adapter
        .get_withdraw_asset_info(Some("USDT"), Some("12.5"))
        .await
        .expect("withdraw asset info");

    let requests = seen.lock().unwrap().clone();
    assert_signed_request(&requests[0], "GET", "/phemex-deposit/wallets/api/chainCfg");
    assert_signed_request(
        &requests[1],
        "GET",
        "/phemex-deposit/wallets/api/depositAddress",
    );
    assert_eq!(
        requests[1].query.get("chainName").map(String::as_str),
        Some("ETH")
    );
    assert_signed_request(
        &requests[2],
        "GET",
        "/phemex-deposit/wallets/api/depositHist",
    );
    assert_eq!(
        requests[2].query.get("currency").map(String::as_str),
        Some("USDT,BTC")
    );
    assert_eq!(
        requests[2].query.get("limit").map(String::as_str),
        Some("200")
    );
    assert_eq!(
        requests[2].query.get("withCount").map(String::as_str),
        Some("true")
    );
    assert_signed_request(
        &requests[3],
        "GET",
        "/phemex-withdraw/wallets/api/withdrawHist",
    );
    assert_eq!(
        requests[3].query.get("chainName").map(String::as_str),
        Some("ETH,TRX")
    );
    assert_signed_request(
        &requests[4],
        "POST",
        "/phemex-withdraw/wallets/api/createWithdraw",
    );
    assert_eq!(
        requests[4].query.get("addressTag").map(String::as_str),
        Some("memo")
    );
    assert_signed_request(
        &requests[5],
        "POST",
        "/phemex-withdraw/wallets/api/cancelWithdraw",
    );
    assert_eq!(requests[5].query.get("id").map(String::as_str), Some("100"));
    assert_signed_request(
        &requests[6],
        "GET",
        "/phemex-withdraw/wallets/api/asset/info",
    );
    assert_eq!(
        requests[6].query.get("amount").map(String::as_str),
        Some("12.5")
    );
}

#[tokio::test]
async fn phemex_adapter_should_route_legacy_account_and_history_extensions() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "msg": "OK", "data": {}}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": []}),
    ])
    .await;
    let adapter = private_adapter(base_url);

    adapter
        .get_legacy_contract_account_positions("BTC")
        .await
        .expect("legacy account positions");
    adapter
        .get_legacy_contract_positions("BTC")
        .await
        .expect("legacy positions");
    adapter
        .get_perp_positions_by_currency("USDT")
        .await
        .expect("perp positions");
    adapter
        .get_perp_risk_units_v2()
        .await
        .expect("risk units v2");
    adapter
        .get_legacy_futures_fee_rate("BTC")
        .await
        .expect("legacy futures fee");
    adapter
        .get_legacy_futures_funding_fees(perp_symbol_scope())
        .await
        .expect("legacy funding fees");
    adapter
        .get_futures_trade_account_details(
            "BTC",
            Some("transfer"),
            Some(10),
            Some(20),
            Some(1),
            Some(250),
            Some(true),
        )
        .await
        .expect("trade account details");
    adapter
        .get_exchange_order_list(
            perp_symbol_scope(),
            Some(10),
            Some(20),
            Some(2),
            Some(250),
            Some("Filled"),
            Some(true),
        )
        .await
        .expect("exchange order list");
    adapter
        .get_exchange_trade_list(
            perp_symbol_scope(),
            Some(10),
            Some(20),
            Some(3),
            Some(250),
            Some(true),
        )
        .await
        .expect("exchange trade list");
    adapter
        .get_exchange_orders_by_ids(
            perp_symbol_scope(),
            &["order-1".to_string(), "order-2".to_string()],
            &[],
        )
        .await
        .expect("exchange orders by ids");
    adapter
        .get_exchange_order_v2_list(
            Some(perp_symbol_scope()),
            Some("USDT"),
            Some("Filled"),
            Some("Limit"),
            Some(10),
            Some(20),
            Some(4),
            Some(250),
            Some(true),
        )
        .await
        .expect("exchange order v2 list");
    adapter
        .get_exchange_trade_v2_list(
            Some(perp_symbol_scope()),
            Some("USDT"),
            Some("Trade"),
            Some(5),
            Some(250),
            Some(true),
        )
        .await
        .expect("exchange trade v2 list");

    let requests = seen.lock().unwrap().clone();
    assert_signed_request(&requests[0], "GET", "/accounts/accountPositions");
    assert_eq!(
        requests[0].query.get("currency").map(String::as_str),
        Some("BTC")
    );
    assert_signed_request(&requests[1], "GET", "/accounts/positions");
    assert_signed_request(&requests[2], "GET", "/g-accounts/positions");
    assert_signed_request(&requests[3], "GET", "/g-accounts/risk-unit");
    assert_signed_request(&requests[4], "GET", "/api-data/futures/fee-rate");
    assert_eq!(
        requests[4].query.get("settleCurrency").map(String::as_str),
        Some("BTC")
    );
    assert_signed_request(&requests[5], "GET", "/api-data/futures/funding-fees");
    assert_signed_request(
        &requests[6],
        "GET",
        "/api-data/futures/v2/tradeAccountDetail",
    );
    assert_eq!(
        requests[6].query.get("withCount").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        requests[6].query.get("limit").map(String::as_str),
        Some("200")
    );
    assert_signed_request(&requests[7], "GET", "/exchange/order/list");
    assert_eq!(
        requests[7].query.get("ordStatus").map(String::as_str),
        Some("Filled")
    );
    assert_signed_request(&requests[8], "GET", "/exchange/order/trade");
    assert_signed_request(&requests[9], "GET", "/exchange/order");
    assert_eq!(
        requests[9].query.get("orderID").map(String::as_str),
        Some("order-1,order-2")
    );
    assert_signed_request(&requests[10], "GET", "/exchange/order/v2/orderList");
    assert_eq!(
        requests[10].query.get("ordType").map(String::as_str),
        Some("Limit")
    );
    assert_signed_request(&requests[11], "GET", "/exchange/order/v2/tradingList");
    assert_eq!(
        requests[11].query.get("execType").map(String::as_str),
        Some("Trade")
    );
}

#[tokio::test]
async fn phemex_adapter_should_route_margin_uta_and_account_transfer_extensions() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "msg": "OK", "data": {}}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": {}}),
        json!({"code": 0, "msg": "OK", "data": {}}),
        json!({"code": 0, "msg": "OK", "data": {}}),
        json!({"code": 0, "msg": "OK", "data": {}}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": {}}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": {}}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": {}}),
        json!({"code": 0, "msg": "OK", "data": {}}),
        json!({"code": 0, "msg": "OK", "data": {}}),
        json!({"code": 0, "msg": "OK", "data": {}}),
        json!({"code": 0, "msg": "OK", "data": {}}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": {}}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": {}}),
        json!({"code": 0, "msg": "OK", "data": {}}),
    ])
    .await;
    let adapter = private_adapter(base_url);

    adapter
        .get_margin_wallets(Some("USDT"))
        .await
        .expect("margin wallets");
    adapter
        .get_margin_orders(spot_symbol_scope())
        .await
        .expect("margin orders");
    adapter
        .get_margin_active_order(spot_symbol_scope(), Some("order-1"), None)
        .await
        .expect("margin active order");
    let mut margin_order = HashMap::new();
    margin_order.insert("symbol".to_string(), "sBTCUSDT".to_string());
    margin_order.insert("side".to_string(), "Buy".to_string());
    margin_order.insert("ordType".to_string(), "Limit".to_string());
    adapter
        .create_margin_order(&margin_order)
        .await
        .expect("create margin order");
    adapter
        .cancel_margin_order(spot_symbol_scope(), None, Some("client-1"))
        .await
        .expect("cancel margin order");
    adapter
        .cancel_all_margin_orders(spot_symbol_scope(), Some(false))
        .await
        .expect("cancel all margin orders");
    adapter
        .get_margin_borrow_history(
            &["USDT".to_string()],
            Some(10),
            Some(20),
            Some(1),
            Some(250),
        )
        .await
        .expect("margin borrow history");
    adapter
        .create_margin_borrow("USDT", "10")
        .await
        .expect("margin borrow");
    adapter
        .get_margin_borrow_interest_history(
            &["USDT".to_string()],
            Some(10),
            Some(20),
            Some(2),
            Some(250),
        )
        .await
        .expect("margin interest");
    adapter
        .get_margin_payback_history(
            &["USDT".to_string()],
            Some(10),
            Some(20),
            Some(3),
            Some(250),
        )
        .await
        .expect("margin payback history");
    adapter
        .create_margin_payback("USDT", "5")
        .await
        .expect("margin payback");
    adapter
        .get_margin_order_history(
            spot_symbol_scope(),
            Some("Filled"),
            Some("Limit"),
            Some(10),
            Some(20),
            Some(4),
            Some(250),
        )
        .await
        .expect("margin order history");
    adapter
        .get_margin_trade_history(
            spot_symbol_scope(),
            Some("Trade"),
            Some(10),
            Some(20),
            Some(5),
            Some(250),
        )
        .await
        .expect("margin trade history");
    adapter.get_uta_risk_mode().await.expect("uta risk mode");
    adapter
        .switch_uta_risk_mode("PORTFOLIO")
        .await
        .expect("switch uta risk");
    adapter
        .get_uta_risk_units("USDT", "CONTRACT")
        .await
        .expect("uta risk units");
    adapter
        .get_uta_assets(Some("USDT"))
        .await
        .expect("uta assets");
    adapter
        .get_uta_convert_assets("USDT", "BTC")
        .await
        .expect("uta convert assets");
    adapter
        .get_uta_contract_borrow_history(
            &["USDT".to_string()],
            Some(10),
            Some(20),
            Some(6),
            Some(250),
        )
        .await
        .expect("uta borrow history");
    adapter
        .get_uta_contract_borrow_interest_history(
            &["USDT".to_string()],
            Some(10),
            Some(20),
            Some(7),
            Some(250),
        )
        .await
        .expect("uta interest history");
    adapter
        .get_uta_contract_payback_history(
            &["USDT".to_string()],
            Some(10),
            Some(20),
            Some(8),
            Some(250),
        )
        .await
        .expect("uta payback history");
    adapter
        .create_uta_contract_payback(&json!({"currency": "USDT", "amountRv": "5"}))
        .await
        .expect("uta payback");
    adapter
        .get_account_transfer_history()
        .await
        .expect("account transfer history");
    adapter
        .create_account_transfer(&json!({"currency": "USDT", "amountRv": "1"}))
        .await
        .expect("account transfer");
    adapter
        .create_main_sub_account_transfer(&json!({"currency": "USDT", "amountRv": "1"}))
        .await
        .expect("main sub transfer");

    let requests = seen.lock().unwrap().clone();
    assert_signed_request(&requests[0], "GET", "/margin-trade/wallets");
    assert_signed_request(&requests[3], "PUT", "/margin-trade/orders/create");
    assert_eq!(
        requests[3].query.get("symbol").map(String::as_str),
        Some("sBTCUSDT")
    );
    assert_signed_request(&requests[5], "DELETE", "/margin-trade/orders/all");
    assert_eq!(
        requests[5].query.get("untriggered").map(String::as_str),
        Some("false")
    );
    assert_signed_request(&requests[6], "GET", "/margin/borrow");
    assert_eq!(
        requests[6].query.get("pageSize").map(String::as_str),
        Some("200")
    );
    assert_signed_request(&requests[8], "GET", "/margin/borrow/interests");
    assert_signed_request(&requests[9], "GET", "/margin/payback");
    assert_signed_request(&requests[11], "GET", "/margin/orders");
    assert_eq!(
        requests[11].query.get("ordStatus").map(String::as_str),
        Some("Filled")
    );
    assert_signed_request(&requests[12], "GET", "/margin/orders/trades");
    assert_signed_request(&requests[13], "GET", "/uta-api/risk/risk-mode");
    assert_signed_request(&requests[14], "POST", "/uta-account/switch-mode");
    assert_eq!(
        requests[14].query.get("riskMode").map(String::as_str),
        Some("PORTFOLIO")
    );
    assert_signed_request(&requests[15], "GET", "/uta-api/risk/risk-units");
    assert_signed_request(&requests[16], "GET", "/uta-biz/assets");
    assert_signed_request(&requests[17], "GET", "/uta-exchanger/assets/convert");
    assert_signed_request(&requests[18], "GET", "/uta-funds/contract/borrow");
    assert_signed_request(&requests[19], "GET", "/uta-funds/contract/borrow/interests");
    assert_signed_request(&requests[20], "GET", "/uta-funds/contract/payback");
    assert_signed_request(&requests[21], "POST", "/uta-funds/contract/payback");
    assert!(requests[21].body.contains(r#""amountRv":"5""#));
    assert_signed_request(&requests[22], "GET", "/wallets/account/transfer");
    assert_signed_request(&requests[23], "POST", "/wallets/account/transfer");
    assert_signed_request(&requests[24], "POST", "/wallets/account/main-sub-transfer");
}

#[tokio::test]
async fn phemex_adapter_should_route_raw_signed_extension_endpoints() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "msg": "OK", "data": {"balance": "1"}}),
        json!({"code": 0, "msg": "OK", "data": {"borrow": "ok"}}),
        json!({"code": 0, "msg": "OK", "data": {"riskLimit": "ok"}}),
        json!({"code": 0, "msg": "OK", "data": {"deleted": true}}),
        json!({"code": 0, "msg": "OK", "data": {"order": "ok"}}),
    ])
    .await;
    let adapter = private_adapter(base_url);

    let mut get_params = HashMap::new();
    get_params.insert("currency".to_string(), "USDT".to_string());
    adapter
        .get_signed_raw("/margin-trade/wallets", &get_params)
        .await
        .expect("raw signed get");

    let mut post_params = HashMap::new();
    post_params.insert("currency".to_string(), "USDT".to_string());
    post_params.insert("amountRv".to_string(), "10".to_string());
    adapter
        .post_signed_raw("/margin/borrow", &post_params)
        .await
        .expect("raw signed post");

    let mut put_params = HashMap::new();
    put_params.insert("symbol".to_string(), "BTCUSD".to_string());
    put_params.insert("riskLimitEv".to_string(), "100000000".to_string());
    adapter
        .put_signed_raw("/positions/riskLimit", &put_params)
        .await
        .expect("raw signed put");

    let mut delete_params = HashMap::new();
    delete_params.insert("symbol".to_string(), "sBTCUSDT".to_string());
    delete_params.insert("untriggered".to_string(), "false".to_string());
    adapter
        .delete_signed_raw("/margin-trade/orders/all", &delete_params)
        .await
        .expect("raw signed delete");

    adapter
        .post_signed_json_raw(
            "/orders",
            &json!({
                "symbol": "BTCUSD",
                "side": "Buy",
                "ordType": "Limit"
            }),
        )
        .await
        .expect("raw signed post json");

    let requests = seen.lock().unwrap().clone();
    assert_signed_request(&requests[0], "GET", "/margin-trade/wallets");
    assert_eq!(
        requests[0].query.get("currency").map(String::as_str),
        Some("USDT")
    );
    assert_signed_request(&requests[1], "POST", "/margin/borrow");
    assert_eq!(
        requests[1].query.get("amountRv").map(String::as_str),
        Some("10")
    );
    assert_signed_request(&requests[2], "PUT", "/positions/riskLimit");
    assert_eq!(
        requests[2].query.get("riskLimitEv").map(String::as_str),
        Some("100000000")
    );
    assert_signed_request(&requests[3], "DELETE", "/margin-trade/orders/all");
    assert_eq!(
        requests[3].query.get("untriggered").map(String::as_str),
        Some("false")
    );
    assert_signed_request(&requests[4], "POST", "/orders");
    assert!(requests[4].body.contains(r#""symbol":"BTCUSD""#));

    let err = adapter
        .get_signed_raw("/orders?symbol=BTCUSD", &HashMap::new())
        .await
        .expect_err("query text rejected");
    assert!(matches!(err, ExchangeApiError::InvalidRequest { .. }));
}

#[tokio::test]
async fn phemex_adapter_should_mark_missing_or_deprecated_native_controls_unsupported() {
    let adapter = private_adapter("http://127.0.0.1:1".to_string());

    let dead_man = adapter
        .set_dead_man_switch(30)
        .await
        .expect_err("dead man unsupported");
    assert!(matches!(
        dead_man,
        ExchangeApiError::Unsupported {
            operation: "phemex.dead_man_switch.no_official_endpoint"
        }
    ));

    let risk_limit = adapter
        .set_manual_risk_limit(perp_symbol_scope(), "100")
        .await
        .expect_err("manual risk limit unsupported");
    assert!(matches!(
        risk_limit,
        ExchangeApiError::Unsupported {
            operation: "phemex.manual_risk_limit.deprecated"
        }
    ));
}

#[tokio::test]
async fn phemex_adapter_should_parse_open_orders() {
    let (base_url, _seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "msg": "OK",
        "data": {
            "rows": [{
                "symbol": "sBTCUSDT",
                "orderID": "exchange-1",
                "clOrdID": "client-1",
                "side": "Sell",
                "ordType": "Limit",
                "ordStatus": "PartiallyFilled",
                "baseQtyRq": "0.01",
                "priceRp": "62000",
                "cumBaseQtyRq": "0.005"
            }]
        }
    })])
    .await;
    let adapter = private_adapter(base_url);

    let response = adapter
        .get_open_orders(OpenOrdersRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("open"),
            exchange: adapter.exchange(),
            market_type: Some(MarketType::Spot),
            symbol: Some(spot_symbol_scope()),
            page: None,
        })
        .await
        .expect("open orders");

    assert_eq!(response.orders.len(), 1);
    assert_eq!(
        response.orders[0].client_order_id.as_deref(),
        Some("client-1")
    );
}

#[test]
fn phemex_gateway_should_register_named_adapter() {
    let gateway = AdapterBackedGateway::with_named_adapters("test", ["phemex"]).expect("gateway");
    assert_eq!(gateway.adapter_count().expect("count"), 1);
}

#[test]
fn phemex_task14_signing_vectors_should_cover_rest_query_body_and_ws_auth() {
    assert_eq!(
        sign_phemex_request(
            "sample-secret",
            "/g-orders/create",
            "clOrdID=client-1&orderQtyRq=0.001&side=Buy&symbol=BTCUSDT",
            "1780837600",
            "",
        ),
        "6f8651dd20012512cc70343d2e29835d2e7f6b2ed653aa17f462fc83072bbd23"
    );
    assert_eq!(
        sign_phemex_request(
            "sample-secret",
            "/assets/transfer",
            "",
            "1780837600",
            r#"{"amount":"10","currency":"USDT","from":"spot","to":"futures"}"#,
        ),
        "66a1abc498b03fd5018e963576804f34c325a75617f039734fdc85deeb564b60"
    );
    assert_eq!(
        sign_phemex_ws_auth("sample-secret", "sample-key", 1_780_837_600),
        "ba7a39d2688e8f2001d79dadd63031c302dfa8067cfda7dcd373ec1ae52ab317"
    );
}

#[test]
fn phemex_task14_private_parser_fixture_should_cover_order_ack() {
    let exchange_id = rustcta_types::ExchangeId::new("phemex").expect("exchange");
    let order_ack: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/phemex/order_ack.json"
    ))
    .expect("order ack fixture");
    let order =
        parse_order_state(&exchange_id, Some(&perp_symbol_scope()), &order_ack).expect("order");

    assert_eq!(order.exchange_order_id.as_deref(), Some("exchange-1"));
    assert_eq!(order.client_order_id.as_deref(), Some("client-1"));
    assert_eq!(order.quantity, "0.001");
    assert_eq!(order.price.as_deref(), Some("61000"));
}

#[test]
fn phemex_task14_request_spec_and_signing_vector_fixtures_should_load() {
    let request_specs = [
        (
            "spot_balances",
            include_str!("../../../../../tests/fixtures/exchanges/phemex/request_specs/spot_balances.json"),
        ),
        (
            "perp_account_positions",
            include_str!("../../../../../tests/fixtures/exchanges/phemex/request_specs/perp_account_positions.json"),
        ),
        (
            "spot_fees",
            include_str!("../../../../../tests/fixtures/exchanges/phemex/request_specs/spot_fees.json"),
        ),
        (
            "perp_fees",
            include_str!("../../../../../tests/fixtures/exchanges/phemex/request_specs/perp_fees.json"),
        ),
        (
            "spot_place_order",
            include_str!("../../../../../tests/fixtures/exchanges/phemex/request_specs/spot_place_order.json"),
        ),
        (
            "perp_place_order",
            include_str!("../../../../../tests/fixtures/exchanges/phemex/request_specs/perp_place_order.json"),
        ),
        (
            "batch_place_orders",
            include_str!("../../../../../tests/fixtures/exchanges/phemex/request_specs/batch_place_orders.json"),
        ),
        (
            "batch_cancel_orders",
            include_str!("../../../../../tests/fixtures/exchanges/phemex/request_specs/batch_cancel_orders.json"),
        ),
        (
            "spot_cancel_order",
            include_str!("../../../../../tests/fixtures/exchanges/phemex/request_specs/spot_cancel_order.json"),
        ),
        (
            "perp_cancel_order",
            include_str!("../../../../../tests/fixtures/exchanges/phemex/request_specs/perp_cancel_order.json"),
        ),
        (
            "cancel_all_orders",
            include_str!("../../../../../tests/fixtures/exchanges/phemex/request_specs/cancel_all_orders.json"),
        ),
        (
            "open_orders",
            include_str!("../../../../../tests/fixtures/exchanges/phemex/request_specs/open_orders.json"),
        ),
        (
            "recent_fills",
            include_str!("../../../../../tests/fixtures/exchanges/phemex/request_specs/recent_fills.json"),
        ),
    ];

    for (operation, raw) in request_specs {
        let spec: RequestSpec = serde_json::from_str(raw).expect("request spec fixture");
        assert_eq!(spec.exchange, "phemex");
        assert_eq!(spec.operation, operation);
        assert_eq!(spec.transport.as_deref(), Some("rest"));
    }

    for raw in [
        include_str!(
            "../../../../../tests/fixtures/exchanges/phemex/signing_vectors/rest_query_order.json"
        ),
        include_str!(
            "../../../../../tests/fixtures/exchanges/phemex/signing_vectors/rest_body_transfer.json"
        ),
        include_str!("../../../../../tests/fixtures/exchanges/phemex/signing_vectors/ws_auth.json"),
    ] {
        let vector: SigningVector = serde_json::from_str(raw).expect("signing vector fixture");
        assert_eq!(vector.exchange, "phemex");
        vector.verify().expect("signing vector verifies");
    }
}

fn private_adapter(rest_base_url: String) -> PhemexGatewayAdapter {
    PhemexGatewayAdapter::new(PhemexGatewayConfig {
        rest_base_url,
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..PhemexGatewayConfig::default()
    })
    .expect("adapter")
}
