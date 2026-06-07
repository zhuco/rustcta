use chrono::TimeZone;
use rustcta_exchange_api::{
    ExchangeClient, ExchangeStreamEvent, OrderBookRequest, PublicStreamKind,
    PublicStreamSubscription, SymbolRulesRequest, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::json;

use crate::streams::StreamReconnectPolicy;

use super::streams::{lbank_ping_payload, LBankWsControlMessage, LBankWsSessionEvent};
use super::test_support::{context, perp_symbol_scope, spawn_rest_server, spot_symbol_scope};
use super::{LBankGatewayAdapter, LBankGatewayConfig};

#[test]
fn lbank_public_parser_fixtures_should_cover_success_and_missing_fields() {
    let exchange = super::test_support::exchange_id();
    let spot_rules = super::parser::parse_spot_symbol_rules(
        &exchange,
        &serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/lbank/spot_accuracy.json"
        ))
        .expect("spot accuracy fixture"),
    )
    .expect("spot rules");
    assert_eq!(spot_rules[0].base_asset, "BTC");
    assert_eq!(spot_rules[0].min_notional.as_deref(), Some("5"));

    let spot_book = super::parser::parse_spot_orderbook_snapshot(
        &exchange,
        spot_symbol_scope(),
        &serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/lbank/spot_depth.json"
        ))
        .expect("spot depth fixture"),
    )
    .expect("spot book");
    assert_eq!(spot_book.bids[0].price, 65000.0);
    assert_eq!(spot_book.asks[0].quantity, 0.15);

    let contract_rules = super::parser::parse_contract_symbol_rules(
        &exchange,
        &serde_json::from_str(include_str!(
            "../../../../../tests/fixtures/exchanges/lbank/contract_instrument.json"
        ))
        .expect("contract instrument fixture"),
    )
    .expect("contract rules");
    assert_eq!(
        contract_rules[0].symbol.market_type,
        rustcta_types::MarketType::Perpetual
    );
    assert_eq!(
        contract_rules[0].quantity_increment.as_deref(),
        Some("0.001")
    );

    let missing_price = serde_json::json!({
        "result": "true",
        "data": {"bids": [["65000", "0.1"]], "asks": [["65001"]]}
    });
    assert!(super::parser::parse_spot_orderbook_snapshot(
        &exchange,
        spot_symbol_scope(),
        &missing_price
    )
    .is_err());

    let exchange_error = serde_json::json!({
        "result": false,
        "error_code": 10008,
        "msg": "symbol not exist"
    });
    assert!(super::parser::parse_spot_symbol_rules(&exchange, &exchange_error).is_err());
}

#[tokio::test]
async fn lbank_adapter_should_load_spot_symbol_rules_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "result": "true",
        "error_code": 0,
        "data": [{
            "symbol": "btc_usdt",
            "priceAccuracy": "2",
            "quantityAccuracy": "6",
            "minTranQua": "0.00001",
            "minOrderAmount": "5"
        }]
    })])
    .await;
    let adapter = LBankGatewayAdapter::new(LBankGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        contract_rest_base_url: base_url,
        ..LBankGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("rules"),
            symbols: vec![spot_symbol_scope()],
        })
        .await
        .expect("rules");

    assert_eq!(response.rules.len(), 1);
    assert_eq!(response.rules[0].base_asset, "BTC");
    assert_eq!(response.rules[0].price_increment.as_deref(), Some("0.01"));
    assert_eq!(
        response.rules[0].quantity_increment.as_deref(),
        Some("0.000001")
    );
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/v2/accuracy.do");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("btc_usdt")
    );
}

#[tokio::test]
async fn lbank_adapter_should_build_spot_public_stream_subscriptions() {
    let adapter = LBankGatewayAdapter::default_public().expect("adapter");

    let subscription_id = adapter
        .subscribe_public_stream(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("stream"),
            symbol: spot_symbol_scope(),
            kind: PublicStreamKind::Candles {
                interval: "5m".to_string(),
            },
        })
        .await
        .expect("stream");

    assert!(subscription_id.contains("wss://www.lbkex.net/ws/V2/"));
    assert!(subscription_id.contains(":kbar:btc_usdt"));
}

#[test]
fn lbank_public_ws_session_should_subscribe_and_handle_heartbeat() {
    let adapter = LBankGatewayAdapter::default_public().expect("adapter");
    let mut session = adapter
        .public_ws_session(PublicStreamSubscription {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("stream-session"),
            symbol: spot_symbol_scope(),
            kind: PublicStreamKind::OrderBookDelta,
        })
        .expect("session");

    assert_eq!(session.url, "wss://www.lbkex.net/ws/V2/");
    assert_eq!(session.initial_requests()[0]["subscribe"], "depth");
    session.on_connected(chrono::Utc::now());
    let ping_at = chrono::Utc
        .timestamp_millis_opt(1_700_000_000_000)
        .single()
        .expect("timestamp");
    assert_eq!(
        session.heartbeat_request(ping_at),
        lbank_ping_payload("rustcta-1700000000000")
    );
    assert!(session.state().last_ping_at.is_some());
    assert!(matches!(
        session.supervisor_action(chrono::Utc::now(), &StreamReconnectPolicy::default()),
        crate::streams::StreamSupervisorAction::None
            | crate::streams::StreamSupervisorAction::SendPing
    ));

    let events = session
        .handle_text_message(r#"{"action":"ping","ping":"server-nonce"}"#)
        .expect("heartbeat");
    assert!(matches!(
        events.first(),
        Some(LBankWsSessionEvent::Public(
            LBankWsControlMessage::HeartbeatPing { nonce }
        )) if nonce == "server-nonce"
    ));
    assert!(matches!(
        events.iter().find(|event| matches!(event, LBankWsSessionEvent::Outbound(_))),
        Some(LBankWsSessionEvent::Outbound(payload))
            if payload["action"] == "pong" && payload["pong"] == "server-nonce"
    ));
    assert!(matches!(
        events.iter().find(|event| matches!(event, LBankWsSessionEvent::Stream(_))),
        Some(LBankWsSessionEvent::Stream(items))
            if matches!(items.first(), Some(ExchangeStreamEvent::Heartbeat { .. }))
    ));
    assert!(session.state().last_pong_at.is_some());

    let events = session
        .handle_text_message(
            r#"{"type":"depth","pair":"btc_usdt","data":{"asks":[["100.5","1.5"]],"bids":[["99.5","2.0"]],"timestamp":1700000000000}}"#,
        )
        .expect("book");
    assert!(matches!(
        events.iter().find(|event| {
            matches!(
                event,
                LBankWsSessionEvent::Stream(items)
                    if matches!(items.first(), Some(ExchangeStreamEvent::OrderBookSnapshot(_)))
            )
        }),
        Some(LBankWsSessionEvent::Stream(_))
    ));
}

#[tokio::test]
async fn lbank_adapter_should_load_spot_order_book_from_public_rest() {
    let (base_url, seen) = spawn_rest_server(vec![json!({
        "result": "true",
        "error_code": 0,
        "data": {
            "asks": [["100.5", "1.5"], ["101.0", "2"]],
            "bids": [["99.5", "1.25"], ["99.0", "2"]],
            "timestamp": 1700000000000i64
        }
    })])
    .await;
    let adapter = LBankGatewayAdapter::new(LBankGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        contract_rest_base_url: base_url,
        ..LBankGatewayConfig::default()
    })
    .expect("adapter");

    let response = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("book"),
            symbol: spot_symbol_scope(),
            depth: Some(7),
        })
        .await
        .expect("book");

    assert_eq!(response.order_book.bids[0].price, 99.5);
    assert_eq!(response.order_book.asks[0].quantity, 1.5);
    let request = seen.lock().unwrap()[0].clone();
    assert_eq!(request.path, "/v2/depth.do");
    assert_eq!(
        request.query.get("symbol").map(String::as_str),
        Some("btc_usdt")
    );
    assert_eq!(request.query.get("size").map(String::as_str), Some("7"));
}

#[tokio::test]
async fn lbank_adapter_should_load_spot_public_market_data_helpers() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({"result": "true", "error_code": 0, "data": 1700000000123i64}),
        json!({"result": "true", "error_code": 0, "data": ["btc_usdt", "eth_usdt"]}),
        json!({
            "result": "true",
            "error_code": 0,
            "timestamp": "1700000000123",
            "ticker": {
                "change": "1.5",
                "high": "66000",
                "latest": "65000",
                "low": "64000",
                "turnover": "1000000",
                "vol": "123.45"
            }
        }),
        json!({
            "result": "true",
            "error_code": 0,
            "data": {
                "symbol": "btc_usdt",
                "bidPrice": "64999",
                "bidQty": "0.5",
                "askPrice": "65001",
                "askQty": "0.4"
            }
        }),
        json!({
            "result": "true",
            "error_code": 0,
            "data": [{
                "date_ms": 1700000000000i64,
                "amount": "0.01",
                "price": "65000",
                "type": "buy",
                "tid": "trade-1"
            }]
        }),
        json!({
            "result": "true",
            "error_code": 0,
            "data": [[1700000000i64, "64000", "65100", "63900", "65000", "10.5"]]
        }),
    ])
    .await;
    let adapter = LBankGatewayAdapter::new(LBankGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        contract_rest_base_url: base_url,
        ..LBankGatewayConfig::default()
    })
    .expect("adapter");

    assert_eq!(
        adapter.get_spot_server_time().await.expect("time"),
        1700000000123
    );
    assert_eq!(
        adapter.get_spot_currency_pairs().await.expect("pairs"),
        vec!["btc_usdt".to_string(), "eth_usdt".to_string()]
    );
    let ticker = adapter.get_spot_ticker("BTC/USDT").await.expect("ticker");
    assert_eq!(ticker.latest, "65000");
    assert_eq!(ticker.volume.as_deref(), Some("123.45"));

    let book_ticker = adapter
        .get_spot_book_ticker("btc_usdt")
        .await
        .expect("book ticker");
    assert_eq!(book_ticker.bid_price, "64999");
    assert_eq!(book_ticker.ask_quantity.as_deref(), Some("0.4"));

    let trades = adapter
        .get_spot_public_trades("btc_usdt", Some(700), Some(1699999999000))
        .await
        .expect("trades");
    assert_eq!(trades[0].trade_id.as_deref(), Some("trade-1"));
    assert_eq!(trades[0].quantity, "0.01");

    let klines = adapter
        .get_spot_klines("btc_usdt", "1m", 5000, 1700000000)
        .await
        .expect("klines");
    assert_eq!(klines[0].close, "65000");
    assert_eq!(klines[0].volume, "10.5");

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/v2/timestamp.do");
    assert_eq!(requests[1].path, "/v2/currencyPairs.do");
    assert_eq!(requests[2].path, "/v2/ticker.do");
    assert_eq!(
        requests[2].query.get("symbol").map(String::as_str),
        Some("btc_usdt")
    );
    assert_eq!(requests[3].path, "/v2/supplement/ticker/bookTicker.do");
    assert_eq!(requests[4].path, "/v2/trades.do");
    assert_eq!(
        requests[4].query.get("size").map(String::as_str),
        Some("600")
    );
    assert_eq!(
        requests[4].query.get("time").map(String::as_str),
        Some("1699999999000")
    );
    assert_eq!(requests[5].path, "/v2/kline.do");
    assert_eq!(
        requests[5].query.get("type").map(String::as_str),
        Some("minute1")
    );
    assert_eq!(
        requests[5].query.get("size").map(String::as_str),
        Some("2880")
    );
}

#[tokio::test]
async fn lbank_adapter_should_load_perpetual_public_rules_and_book() {
    let (base_url, seen) = spawn_rest_server(vec![
        json!({
            "success": true,
            "error_code": 0,
            "data": [{
                "symbol": "BTCUSDT",
                "baseCurrency": "BTC",
                "priceCurrency": "USDT",
                "clearCurrency": "USDT",
                "priceTick": "0.1",
                "volumeTick": "0.001",
                "minOrderVolume": "0.001",
                "minOrderCost": "5"
            }]
        }),
        json!({
            "success": true,
            "error_code": 0,
            "data": {
                "symbol": "BTCUSDT",
                "asks": [{"price": "100.5", "volume": "1.5", "orders": 1}],
                "bids": [{"price": "99.5", "volume": "2.0", "orders": 1}]
            }
        }),
    ])
    .await;
    let adapter = LBankGatewayAdapter::new(LBankGatewayConfig {
        spot_rest_base_url: base_url.clone(),
        contract_rest_base_url: base_url,
        ..LBankGatewayConfig::default()
    })
    .expect("adapter");

    let rules = adapter
        .get_symbol_rules(SymbolRulesRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-rules"),
            symbols: vec![perp_symbol_scope()],
        })
        .await
        .expect("rules");
    assert_eq!(rules.rules.len(), 1);
    assert_eq!(
        rules.rules[0].symbol.market_type,
        rustcta_types::MarketType::Perpetual
    );
    assert_eq!(rules.rules[0].price_increment.as_deref(), Some("0.1"));

    let book = adapter
        .get_order_book(OrderBookRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context("perp-book"),
            symbol: perp_symbol_scope(),
            depth: Some(8),
        })
        .await
        .expect("book");
    assert_eq!(
        book.order_book.market_type,
        rustcta_types::MarketType::Perpetual
    );
    assert_eq!(book.order_book.bids[0].quantity, 2.0);

    let requests = seen.lock().unwrap().clone();
    assert_eq!(requests[0].path, "/cfd/openApi/v1/pub/instrument");
    assert_eq!(
        requests[0].query.get("productGroup").map(String::as_str),
        Some("SwapU")
    );
    assert_eq!(requests[1].path, "/cfd/openApi/v1/pub/marketOrder");
    assert_eq!(
        requests[1].query.get("symbol").map(String::as_str),
        Some("BTCUSDT")
    );
    assert_eq!(
        requests[1].query.get("depth").map(String::as_str),
        Some("8")
    );
}
