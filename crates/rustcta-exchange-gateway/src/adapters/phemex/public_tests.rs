use std::collections::HashMap;

use rustcta_exchange_api::{
    AuthRenewalKind, BatchAtomicity, BatchExecutionMode, ExchangeClient, OrderBookRequest,
    SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{MarketType, OrderSide};
use serde_json::json;

use super::parser::{parse_funding_history, parse_orderbook_snapshot, parse_symbol_rules};
use super::test_support::{context, perp_symbol_scope, spawn_rest_server, spot_symbol_scope};
use super::{PhemexGatewayAdapter, PhemexGatewayConfig};

#[tokio::test]
async fn phemex_adapter_should_load_spot_and_perp_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": 0,
        "msg": "",
        "data": {
            "products": [{
                "symbol": "sBTCUSDT",
                "type": "Spot",
                "quoteCurrency": "USDT",
                "pricePrecision": 2,
                "baseCurrency": "BTC",
                "baseTickSize": "0.000001 BTC",
                "quoteTickSize": "0.01 USDT",
                "baseQtyPrecision": 6,
                "minOrderValue": "1 USDT",
                "status": "Listed"
            }],
            "perpProductsV2": [{
                "symbol": "BTCUSDT",
                "type": "PerpetualV2",
                "settleCurrency": "USDT",
                "quoteCurrency": "USDT",
                "tickSize": "0.1",
                "pricePrecision": 1,
                "baseCurrency": "BTC",
                "status": "Listed",
                "minOrderValueRv": "1",
                "qtyPrecision": 3,
                "qtyStepSize": "0.001"
            }]
        }
    })])
    .await;
    let adapter = PhemexGatewayAdapter::new(PhemexGatewayConfig {
        rest_base_url: base_url,
        ..PhemexGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![spot_symbol_scope(), perp_symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 2);
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(
        response.rules[1].quantity_increment.as_deref(),
        Some("0.001")
    );
    assert_eq!(seen.lock().unwrap()[0].path, "/public/products");
}

#[tokio::test]
async fn phemex_adapter_should_load_spot_scaled_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "error": null,
        "id": 0,
        "result": {
            "book": {
                "bids": [[6177835000000_i64, 24500800]],
                "asks": [[6177880000000_i64, 4572600]]
            },
            "sequence": 40090391919_u64,
            "symbol": "sBTCUSDT",
            "timestamp": 1780836164295635979_u64,
            "type": "snapshot"
        }
    })])
    .await;
    let adapter = PhemexGatewayAdapter::new(PhemexGatewayConfig {
        rest_base_url: base_url,
        ..PhemexGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: spot_symbol_scope(),
            depth: Some(5),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.sequence, Some(40090391919));
    assert!((response.order_book.bids[0].price - 61778.35).abs() < 0.000001);
    assert!((response.order_book.asks[0].quantity - 0.045726).abs() < 0.000001);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/md/orderbook");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("sBTCUSDT")
    );
}

#[tokio::test]
async fn phemex_adapter_should_load_perp_real_value_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "error": null,
        "id": 0,
        "result": {
            "orderbook_p": {
                "bids": [["61750.7", "0.001"]],
                "asks": [["61750.8", "0.984"]]
            },
            "sequence": 62439824887_u64,
            "symbol": "BTCUSDT",
            "timestamp": 1780836164743398111_u64,
            "type": "snapshot"
        }
    })])
    .await;
    let adapter = PhemexGatewayAdapter::new(PhemexGatewayConfig {
        rest_base_url: base_url,
        ..PhemexGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-book"),
            symbol: perp_symbol_scope(),
            depth: Some(5),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.sequence, Some(62439824887));
    assert_eq!(response.order_book.bids[0].price, 61750.7);
    assert_eq!(response.order_book.asks[0].quantity, 0.984);
    assert_eq!(seen.lock().unwrap()[0].path, "/md/v2/orderbook");
}

#[tokio::test]
async fn phemex_adapter_should_load_public_ticker_and_trade_extensions() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "msg": "", "data": {"serverTime": 1780837555336_i64}}),
        json!({
            "error": null,
            "id": 0,
            "result": {
                "askEp": 6178668000000_i64,
                "bidEp": 6178655000000_i64,
                "highEp": 6291646000000_i64,
                "lastEp": 6178667000000_i64,
                "lowEp": 6039494000000_i64,
                "openEp": 6105993000000_i64,
                "symbol": "sBTCUSDT",
                "timestamp": 1780837530831747545_u64,
                "turnoverEv": 27192474702709348_i64,
                "volumeEv": 442530080900_i64
            }
        }),
        json!({
            "error": null,
            "id": 0,
            "result": {
                "sequence": 40090442502_u64,
                "symbol": "sBTCUSDT",
                "trades": [[1780837532577305359_u64, "Sell", 6178304000000_i64, 4113600_i64]]
            }
        }),
    ])
    .await;
    let adapter = PhemexGatewayAdapter::new(PhemexGatewayConfig {
        rest_base_url: base_url,
        ..PhemexGatewayConfig::default()
    })
    .expect("adapter");

    let time = adapter.get_server_time().await.expect("time");
    assert_eq!(time.server_time_ms, 1780837555336);

    let ticker = adapter
        .get_ticker_24h(spot_symbol_scope())
        .await
        .expect("ticker");
    assert_eq!(ticker.last_price, "61786.67");
    assert_eq!(ticker.bid_price.as_deref(), Some("61786.55"));

    let trades = adapter
        .get_recent_public_trades(spot_symbol_scope())
        .await
        .expect("trades");
    assert_eq!(trades[0].side, OrderSide::Sell);
    assert_eq!(trades[0].price, "61783.04");
    assert_eq!(trades[0].quantity, "0.041136");

    let requests = seen.lock().unwrap();
    assert_eq!(requests[0].path, "/public/time");
    assert_eq!(requests[1].path, "/md/spot/ticker/24hr");
    assert_eq!(requests[2].path, "/md/trade");
}

#[tokio::test]
async fn phemex_adapter_should_load_public_products_chain_and_funding_extensions() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": 0,
            "msg": "",
            "data": {
                "products": [{
                    "symbol": "sBTCUSDT",
                    "type": "Spot"
                }]
            }
        }),
        json!({
            "error": null,
            "id": 0,
            "result": [{
                "askEp": 6178668000000_i64,
                "bidEp": 6178655000000_i64,
                "highEp": 6291646000000_i64,
                "lastEp": 6178667000000_i64,
                "lowEp": 6039494000000_i64,
                "openEp": 6105993000000_i64,
                "symbol": "sBTCUSDT",
                "timestamp": 1780837530831747545_u64,
                "turnoverEv": 27192474702709348_i64,
                "volumeEv": 442530080900_i64
            }]
        }),
        json!({
            "code": 0,
            "msg": "OK",
            "data": [{
                "currency": "USDT",
                "chainName": "ETH"
            }]
        }),
        json!({
            "code": 0,
            "msg": "OK",
            "data": [{
                "symbol": "BTCUSDT",
                "fundingRateRr": "0.0001"
            }]
        }),
        json!({
            "code": 0,
            "msg": "OK",
            "data": {
                "rows": [{
                    "strategyId": "strategy-1"
                }]
            }
        }),
    ])
    .await;
    let adapter = PhemexGatewayAdapter::new(PhemexGatewayConfig {
        rest_base_url: base_url,
        ..PhemexGatewayConfig::default()
    })
    .expect("adapter");

    let products = adapter.get_products_plus().await.expect("products plus");
    assert!(products["data"]["products"].is_array());

    let tickers = adapter
        .get_spot_tickers_24h_all()
        .await
        .expect("all tickers");
    assert_eq!(tickers[0].symbol.exchange_symbol.symbol, "sBTCUSDT");
    assert_eq!(tickers[0].last_price, "61786.67");

    let chains = adapter.get_chain_settings("USDT").await.expect("chains");
    assert_eq!(chains["data"][0]["chainName"], "ETH");

    let funding = adapter
        .get_real_funding_rates(Some(perp_symbol_scope()))
        .await
        .expect("real funding");
    assert_eq!(funding["data"][0]["symbol"], "BTCUSDT");

    let performance = adapter
        .get_trader_performance_info(&["strategy-1".to_string()], Some(1), Some(250))
        .await
        .expect("trader performance");
    assert_eq!(performance["data"]["rows"][0]["strategyId"], "strategy-1");

    let requests = seen.lock().unwrap();
    assert_eq!(requests[0].path, "/public/products-plus");
    assert_eq!(requests[1].path, "/md/spot/ticker/24hr/all");
    assert_eq!(requests[2].path, "/exchange/public/cfg/chain-settings");
    assert_eq!(
        requests[2].query.get("currency").map(String::as_str),
        Some("USDT")
    );
    assert_eq!(requests[3].path, "/contract-biz/public/real-funding-rates");
    assert_eq!(
        requests[3].query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(
        requests[4].path,
        "/phemex-lb/public/api/trader/performance-info"
    );
    assert_eq!(
        requests[4].query.get("strategyIds").map(String::as_str),
        Some("strategy-1")
    );
    assert_eq!(
        requests[4].query.get("pageSize").map(String::as_str),
        Some("200")
    );
}

#[tokio::test]
async fn phemex_adapter_should_route_public_market_data_extension_endpoints() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"error": null, "id": 0, "result": {"book": {"bids": [], "asks": []}}}),
        json!({"error": null, "id": 0, "result": {"symbol": "BTCUSDT"}}),
        json!({"error": null, "id": 0, "result": []}),
        json!({"error": null, "id": 0, "result": []}),
        json!({"error": null, "id": 0, "result": {"symbol": "BTCUSDT"}}),
        json!({"error": null, "id": 0, "result": []}),
        json!({"error": null, "id": 0, "result": {"rows": []}}),
        json!({"error": null, "id": 0, "result": {"rows": []}}),
        json!({"error": null, "id": 0, "result": {"rows": []}}),
        json!({"code": 0, "msg": "OK", "data": []}),
        json!({"code": 0, "msg": "OK", "data": []}),
    ])
    .await;
    let adapter = PhemexGatewayAdapter::new(PhemexGatewayConfig {
        rest_base_url: base_url,
        ..PhemexGatewayConfig::default()
    })
    .expect("adapter");

    adapter.get_index_sources().await.expect("index sources");
    adapter
        .get_full_order_book(perp_symbol_scope())
        .await
        .expect("full book");
    adapter
        .get_legacy_ticker_24h_v1(perp_symbol_scope())
        .await
        .expect("v1 ticker");
    adapter
        .get_legacy_tickers_24h_v1_all()
        .await
        .expect("v1 tickers all");
    adapter
        .get_perp_tickers_24h_v2_all()
        .await
        .expect("v2 tickers all");
    adapter
        .get_perp_ticker_24h_v3(perp_symbol_scope())
        .await
        .expect("v3 ticker");
    adapter
        .get_perp_tickers_24h_v3_all()
        .await
        .expect("v3 tickers all");
    adapter
        .get_legacy_contract_candles(perp_symbol_scope(), "1m", 100, 200)
        .await
        .expect("legacy candles");
    adapter
        .get_perp_candles_v2(perp_symbol_scope(), "5m", Some(2500))
        .await
        .expect("v2 candles");
    adapter
        .get_perp_candles_v2_list(perp_symbol_scope(), "15m", 100, 200)
        .await
        .expect("v2 candle list");
    adapter
        .get_nomics_trades("BTCUSDT", Some(100))
        .await
        .expect("nomics trades");
    let mut raw_params = HashMap::new();
    raw_params.insert("symbol".to_string(), "BTCUSDT".to_string());
    adapter
        .get_public_raw("/md/v2/ticker/24hr/all", &raw_params)
        .await
        .expect("raw public");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/public/index-sources");
    assert_eq!(requests[1].path, "/md/fullbook");
    assert_eq!(
        requests[1].query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(requests[2].path, "/md/v1/ticker/24hr");
    assert_eq!(requests[3].path, "/md/v1/ticker/24hr/all");
    assert_eq!(requests[4].path, "/md/v2/ticker/24hr/all");
    assert_eq!(requests[5].path, "/md/v3/ticker/24hr");
    assert_eq!(requests[6].path, "/md/v3/ticker/24hr/all");
    assert_eq!(requests[7].path, "/exchange/public/md/kline");
    assert_eq!(
        requests[7].query.get("resolution").map(String::as_str),
        Some("60")
    );
    assert_eq!(requests[8].path, "/exchange/public/md/v2/kline");
    assert_eq!(
        requests[8].query.get("limit").map(String::as_str),
        Some("1000")
    );
    assert_eq!(requests[9].path, "/exchange/public/md/v2/kline/list");
    assert_eq!(
        requests[9].query.get("resolution").map(String::as_str),
        Some("900")
    );
    assert_eq!(requests[10].path, "/exchange/public/nomics/trades");
    assert_eq!(
        requests[10].query.get("since").map(String::as_str),
        Some("100")
    );
    assert_eq!(requests[11].path, "/md/v2/ticker/24hr/all");
}

#[tokio::test]
async fn phemex_adapter_should_load_perp_ticker_funding_trades_and_candles() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "error": null,
            "id": 0,
            "result": {
                "closeRp": "61766.3",
                "fundingRateRr": "0.00007348",
                "highRp": "62931.1",
                "indexPriceRp": "61788.04182534",
                "lowRp": "60367.9",
                "markPriceRp": "61766.3",
                "openInterestRv": "2552.1189959",
                "openRp": "61029.5",
                "predFundingRateRr": "0.00007348",
                "symbol": "BTCUSDT",
                "timestamp": 1780837531243525456_u64,
                "turnoverRv": "380074663.9595",
                "volumeRq": "6194.597"
            }
        }),
        json!({
            "error": null,
            "id": 0,
            "result": {
                "symbol": "BTCUSDT",
                "trades_p": [[1780837532240681339_u64, "Sell", "61766.1", "0.001"]]
            }
        }),
        json!({
            "code": 0,
            "msg": "OK",
            "data": {
                "rows": [{
                    "symbol": "BTCUSDT",
                    "fundingRateRr": "0.0001",
                    "fundingTime": 1780837531243_i64
                }]
            }
        }),
        json!({
            "code": 0,
            "msg": "OK",
            "data": {
                "rows": [{
                    "timestamp": 1780837200000_i64,
                    "openRp": "61700.1",
                    "highRp": "61800.2",
                    "lowRp": "61600.3",
                    "closeRp": "61750.4",
                    "volumeRq": "12.5",
                    "turnoverRv": "771880"
                }]
            }
        }),
    ])
    .await;
    let adapter = PhemexGatewayAdapter::new(PhemexGatewayConfig {
        rest_base_url: base_url,
        ..PhemexGatewayConfig::default()
    })
    .expect("adapter");

    let ticker = adapter
        .get_ticker_24h(perp_symbol_scope())
        .await
        .expect("ticker");
    assert_eq!(ticker.last_price, "61766.3");
    assert_eq!(ticker.mark_price.as_deref(), Some("61766.3"));
    assert_eq!(ticker.open_interest.as_deref(), Some("2552.1189959"));

    let trades = adapter
        .get_recent_public_trades(perp_symbol_scope())
        .await
        .expect("trades");
    assert_eq!(trades[0].price, "61766.1");

    let funding = adapter
        .get_funding_history(perp_symbol_scope(), Some(2))
        .await
        .expect("funding");
    assert_eq!(funding[0].funding_rate, "0.0001");

    let candles = adapter
        .get_perp_candles(perp_symbol_scope(), "1m", Some(1))
        .await
        .expect("candles");
    assert_eq!(candles[0].close, "61750.4");

    let requests = seen.lock().unwrap();
    assert_eq!(requests[0].path, "/md/v2/ticker/24hr");
    assert_eq!(requests[1].path, "/md/v2/trade");
    assert_eq!(
        requests[2].path,
        "/api-data/public/data/funding-rate-history"
    );
    assert_eq!(requests[3].path, "/exchange/public/md/v2/kline/last");
    assert_eq!(
        requests[3].query.get("resolution").map(String::as_str),
        Some("60")
    );
}

#[test]
fn phemex_task14_endpoint_mapping_should_declare_required_boundaries() {
    let mapping = include_str!("endpoint_mapping.yaml");

    for required in [
        "exchange: phemex",
        "Dated futures and options are not opened",
        "Order books are snapshot-only",
        "operation: funding_open_interest_ticker",
        "openInterestRv",
        "phemex_public_market",
        "phemex_private_trade",
        "kill-switch",
        "disabled-symbol",
        "max-notional",
    ] {
        assert!(
            mapping.contains(required),
            "mapping missing required fragment: {required}"
        );
    }

    assert!(
        mapping.contains("auth_renewal:"),
        "mapping should declare private WS relogin renewal"
    );
    assert!(mapping.contains("path: /public/products"));
    assert!(mapping.contains("path: /g-orders/create"));
    assert!(mapping.contains("path: /api-data/public/data/funding-rate-history"));
    assert!(mapping.contains("atomicity: non_atomic"));
    assert!(mapping.contains("native_batch: true"));
}

#[test]
fn phemex_task14_capabilities_v2_should_declare_runtime_and_history_contracts() {
    let adapter = PhemexGatewayAdapter::new(PhemexGatewayConfig {
        api_key: Some("key".to_string()),
        api_secret: Some("secret".to_string()),
        enabled_private_rest: true,
        ..PhemexGatewayConfig::default()
    })
    .expect("adapter");

    let capabilities = adapter.capabilities();
    let v2 = capabilities.capabilities_v2;
    assert!(v2.public_rest.is_supported());
    assert!(v2.private_rest.is_supported());
    assert!(v2.public_streams.is_supported());
    assert!(v2.private_streams.is_supported());
    assert!(v2.stream_runtime.public.is_supported());
    assert!(v2.stream_runtime.private.is_supported());
    assert_eq!(v2.stream_runtime.heartbeat_policy.ping_interval_ms, 15_000);
    assert_eq!(v2.stream_runtime.heartbeat_policy.pong_timeout_ms, 30_000);
    assert_eq!(
        v2.stream_runtime.auth_renewal_policy.kind,
        AuthRenewalKind::ReLogin
    );
    assert!(v2.stream_runtime.resync.order_book);
    assert!(v2.stream_runtime.reconnect_requires_login);
    assert!(v2.stream_runtime.reconnect_requires_resubscribe);
    assert!(
        v2.stream_runtime
            .orderbook_requires_snapshot_after_reconnect
    );
    assert_eq!(
        v2.batch_place_orders.mode,
        BatchExecutionMode::ComposedSequential
    );
    assert_eq!(v2.batch_place_orders.atomicity, BatchAtomicity::NonAtomic);
    assert!(v2.batch_place_orders.supports_partial_failure);
    assert_eq!(v2.batch_cancel_orders.mode, BatchExecutionMode::Native);
    assert!(v2.batch_cancel_orders.same_symbol_required);
    assert_eq!(v2.batch_cancel_orders.max_items, Some(20));
    assert!(v2.order_history.supports_cursor);
    assert_eq!(v2.fills_history.max_limit, Some(200));
    assert!(v2.endpoints.iter().any(|endpoint| {
        endpoint.operation == "funding_open_interest_ticker"
            && endpoint.market_types == vec![MarketType::Perpetual]
    }));
}

#[test]
fn phemex_task14_parser_fixtures_should_cover_success_empty_error_and_missing_field() {
    let exchange_id = rustcta_types::ExchangeId::new("phemex").expect("exchange");
    let instruments: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/phemex/instruments.json"
    ))
    .expect("instruments fixture");
    let rules = parse_symbol_rules(&exchange_id, &instruments).expect("symbol rules");
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(rules[1].quantity_increment.as_deref(), Some("0.001"));

    let empty: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/phemex/instruments_empty.json"
    ))
    .expect("empty fixture");
    assert!(parse_symbol_rules(&exchange_id, &empty)
        .expect("empty rules")
        .is_empty());

    let error: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/phemex/error.json"
    ))
    .expect("error fixture");
    assert!(parse_symbol_rules(&exchange_id, &error)
        .expect("error-shaped rules fixture should parse as no products")
        .is_empty());

    let book: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/phemex/orderbook.json"
    ))
    .expect("book fixture");
    let parsed_book =
        parse_orderbook_snapshot(&exchange_id, perp_symbol_scope(), &book).expect("book");
    assert_eq!(parsed_book.sequence, Some(62439824887));
    assert_eq!(parsed_book.bids[0].price, 61750.7);

    let missing: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/phemex/orderbook_missing_bids.json"
    ))
    .expect("missing book fixture");
    assert!(parse_orderbook_snapshot(&exchange_id, perp_symbol_scope(), &missing).is_err());

    let funding: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../../tests/fixtures/exchanges/phemex/funding.json"
    ))
    .expect("funding fixture");
    let funding_rows =
        parse_funding_history(&exchange_id, perp_symbol_scope(), &funding).expect("funding");
    assert_eq!(funding_rows[0].funding_rate, "0.0001");
}
