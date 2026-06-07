use rustcta_exchange_api::{
    PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind, PublicStreamSubscription,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{AccountId, MarketType};
use serde_json::json;

use super::streams::{
    bigone_private_subscribe_payload, bigone_public_subscribe_payload,
    parse_bigone_private_stream_message, parse_bigone_public_stream_message,
    BigOnePrivateStreamMessage, BigOnePublicStreamMessage,
};
use super::test_support::{context, exchange_id, perp_symbol_scope, spot_symbol_scope};

#[test]
fn bigone_ws_payload_should_map_public_and_private_channels() {
    let public = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public"),
        symbol: spot_symbol_scope(),
        kind: PublicStreamKind::OrderBookSnapshot,
    };
    let payload = bigone_public_subscribe_payload(&public).expect("public payload");
    assert_eq!(payload["requestId"], "1");
    assert_eq!(
        payload["subscribeMarketDepthRequest"],
        json!({"market": "BTC-USDT", "limit": 50})
    );

    let trades = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-trades"),
        symbol: spot_symbol_scope(),
        kind: PublicStreamKind::Trades,
    };
    let payload = bigone_public_subscribe_payload(&trades).expect("trades payload");
    assert_eq!(
        payload["subscribeMarketTradesRequest"],
        json!({"market": "BTC-USDT", "limit": 20})
    );

    let candles = PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("public-candles"),
        symbol: spot_symbol_scope(),
        kind: PublicStreamKind::Candles {
            interval: "1m".to_string(),
        },
    };
    let payload = bigone_public_subscribe_payload(&candles).expect("candles payload");
    assert_eq!(
        payload["subscribeMarketCandlesRequest"],
        json!({"market": "BTC-USDT", "period": "MIN1"})
    );

    let private = PrivateStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: context("private"),
        exchange: exchange_id(),
        market_type: Some(MarketType::Perpetual),
        account_id: AccountId::new("account").expect("account"),
        kind: PrivateStreamKind::Positions,
    };
    let payload =
        bigone_private_subscribe_payload(&private, MarketType::Perpetual).expect("private payload");
    assert_eq!(payload["channel"], "positions");
    assert_eq!(payload["market_type"], "contract");
}

#[test]
fn bigone_ws_parser_should_parse_book_and_private_order() {
    let public = parse_bigone_public_stream_message(
        &exchange_id(),
        perp_symbol_scope(),
        &fixture("ws_public_depth.json"),
    )
    .expect("public");
    assert!(matches!(public, BigOnePublicStreamMessage::OrderBook(_)));

    let private = parse_bigone_private_stream_message(
        &exchange_id(),
        rustcta_types::TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        MarketType::Spot,
        Some(spot_symbol_scope()),
        &fixture("ws_private_order.json"),
    )
    .expect("private");
    assert!(matches!(private, BigOnePrivateStreamMessage::Events(_)));
}

#[test]
fn bigone_ws_parser_should_parse_public_trade_ticker_and_candle_messages() {
    let trades = parse_bigone_public_stream_message(
        &exchange_id(),
        spot_symbol_scope(),
        &json!({
            "channel": "marketTrades",
            "data": {
                "trades": [
                    {
                        "id": "trade-1",
                        "market": "BTC-USDT",
                        "price": "65000",
                        "amount": "0.01",
                        "takerSide": "BID",
                        "createdAt": "2026-06-08T00:00:00Z"
                    },
                    {
                        "id": "trade-2",
                        "market": "BTC-USDT",
                        "price": "65001",
                        "amount": "0.02",
                        "takerSide": "ASK",
                        "createdAt": "2026-06-08T00:00:01Z"
                    }
                ]
            }
        }),
    )
    .expect("trades");
    match trades {
        BigOnePublicStreamMessage::Trades(trades) => {
            assert_eq!(trades.len(), 2);
            assert_eq!(trades[0].trade_id.as_deref(), Some("trade-1"));
            assert_eq!(trades[1].side, rustcta_types::OrderSide::Sell);
        }
        other => panic!("unexpected {other:?}"),
    }

    let ticker = parse_bigone_public_stream_message(
        &exchange_id(),
        spot_symbol_scope(),
        &json!({
            "channel": "marketsTicker",
            "data": {
                "market": "BTC-USDT",
                "open": "64000",
                "high": "66000",
                "low": "63000",
                "close": "65000",
                "volume": "12.5",
                "bid": ["64999", "0.5"],
                "ask": ["65001", "0.4"],
                "updatedAt": "2026-06-08T00:00:02Z"
            }
        }),
    )
    .expect("ticker");
    match ticker {
        BigOnePublicStreamMessage::Ticker(ticker) => {
            assert_eq!(ticker.last_price.as_deref(), Some("65000"));
            assert_eq!(ticker.bid_price.as_deref(), Some("64999"));
            assert_eq!(ticker.ask_quantity.as_deref(), Some("0.4"));
        }
        other => panic!("unexpected {other:?}"),
    }

    let candle = parse_bigone_public_stream_message(
        &exchange_id(),
        spot_symbol_scope(),
        &json!({
            "channel": "marketCandles",
            "data": {
                "market": "BTC-USDT",
                "time": "2026-06-08T00:00:00Z",
                "open": "65000",
                "high": "65100",
                "low": "64900",
                "close": "65050",
                "volume": "1.25"
            }
        }),
    )
    .expect("candle");
    match candle {
        BigOnePublicStreamMessage::Candle(candle) => {
            assert_eq!(candle.open, "65000");
            assert_eq!(candle.close, "65050");
            assert_eq!(candle.volume, "1.25");
        }
        other => panic!("unexpected {other:?}"),
    }
}

fn fixture(name: &str) -> serde_json::Value {
    serde_json::from_str(match name {
        "ws_public_depth.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bigone/ws_public_depth.json")
        }
        "ws_private_order.json" => {
            include_str!("../../../../../tests/fixtures/exchanges/bigone/ws_private_order.json")
        }
        other => panic!("unknown BigONE fixture {other}"),
    })
    .expect("fixture json")
}
