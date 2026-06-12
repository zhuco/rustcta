use rustcta_exchange_api::{
    ExchangeClient, FundingRatesRequest, OrderBookRequest, SymbolRulesRequest,
    EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use super::test_support::{context, spawn_rest_server, symbol_scope};
use super::{KuCoinFuturesGatewayAdapter, KuCoinFuturesGatewayConfig};

#[tokio::test]
async fn kucoinfutures_adapter_should_load_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "200000",
        "data": [{
            "symbol": "XBTUSDTM",
            "baseCurrency": "XBT",
            "quoteCurrency": "USDT",
            "tickSize": "0.1",
            "lotSize": "1",
            "minOrderQty": 1,
            "maxOrderQty": 1000000,
            "multiplier": "0.001",
            "enableTrading": true
        }]
    })])
    .await;
    let adapter = KuCoinFuturesGatewayAdapter::new(KuCoinFuturesGatewayConfig {
        rest_base_url: base_url,
        ..KuCoinFuturesGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "XBT");
    assert_eq!(response.rules[0].quote_asset, "USDT");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.1"));
    assert_eq!(response.rules[0].quantity_increment.as_deref(), Some("1"));
    assert_eq!(seen.lock().unwrap()[0].path, "/api/v1/contracts/active");
}

#[tokio::test]
async fn kucoinfutures_adapter_should_load_level2_20_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "200000",
        "data": {
            "sequence": "100",
            "time": 1710000000000_i64,
            "bids": [["99.5", "1.25"], ["99.0", "2"]],
            "asks": [["100.5", "1.5"], ["101.0", "2"]]
        }
    })])
    .await;
    let adapter = KuCoinFuturesGatewayAdapter::new(KuCoinFuturesGatewayConfig {
        rest_base_url: base_url,
        ..KuCoinFuturesGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: symbol_scope(),
            depth: Some(7),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.sequence, Some(100));
    assert_eq!(response.order_book.bids[0].price, 99.5);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v1/level2/snapshot");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("XBTUSDTM")
    );
}

#[tokio::test]
async fn kucoinfutures_adapter_should_load_latest_funding_rate_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "code": "200000",
            "data": [{
                "symbol": "XBTUSDTM",
                "fundingFeeRate": "-0.0002",
                "predictedFundingFeeRate": "0.0003",
                "markPrice": "65000.5",
                "indexPrice": "64999.9",
                "openInterest": "12345",
                "turnoverOf24h": "9876543.21",
                "volumeOf24h": "151.25",
                "nextFundingRateDateTime": 1743084000000_i64
            }]
        }),
        json!({
            "code": "200000",
            "data": {
                "items": [{
                    "symbol": "XBTUSDTM",
                    "timePoint": 1743055200000_i64,
                    "fundingRate": "0.0001",
                    "funding": "-0.0123",
                    "currency": "USDT"
                }]
            }
        }),
    ])
    .await;
    let adapter = KuCoinFuturesGatewayAdapter::new(KuCoinFuturesGatewayConfig {
        rest_base_url: base_url,
        ..KuCoinFuturesGatewayConfig::default()
    })
    .expect("adapter");

    let response = ExchangeClient::get_funding_rates(
        &adapter,
        FundingRatesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("funding"),
            symbols: vec![symbol_scope()],
        },
    )
    .await
    .expect("funding");

    assert_eq!(response.rates.len(), 1);
    assert_eq!(response.rates[0].funding_rate, "-0.0002");
    assert_eq!(
        response.rates[0].predicted_funding_rate.as_deref(),
        Some("0.0003")
    );
    assert!(response.rates[0].funding_time.is_some());
    assert_eq!(
        response.rates[0]
            .next_funding_time
            .expect("next funding")
            .timestamp_millis(),
        1743084000000
    );
    assert_eq!(response.rates[0].mark_price.as_deref(), Some("65000.5"));
    assert_eq!(response.rates[0].index_price.as_deref(), Some("64999.9"));
    assert_eq!(response.rates[0].open_interest.as_deref(), Some("12345"));
    assert_eq!(
        response.rates[0].turnover_24h.as_deref(),
        Some("9876543.21")
    );
    assert_eq!(response.rates[0].volume_24h.as_deref(), Some("151.25"));
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v1/contracts/active");
    let request = seen.lock().unwrap()[1].clone();
    assert_eq!(request.path, "/api/v1/funding-history");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("XBTUSDTM")
    );
    assert_eq!(request.query.get("pageSize").map(String::as_str), Some("1"));
}

#[tokio::test]
async fn kucoinfutures_adapter_should_load_level2_100_order_book_for_deep_request() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "code": "200000",
        "data": {
            "sequence": 101,
            "bids": [["99.5", "1.25"]],
            "asks": [["100.5", "1.5"]]
        }
    })])
    .await;
    let adapter = KuCoinFuturesGatewayAdapter::new(KuCoinFuturesGatewayConfig {
        rest_base_url: base_url,
        ..KuCoinFuturesGatewayConfig::default()
    })
    .expect("adapter");

    adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book-100"),
            symbol: symbol_scope(),
            depth: Some(80),
        })
        .await
        .expect("book");

    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/api/v1/level2/snapshot");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("XBTUSDTM")
    );
}
