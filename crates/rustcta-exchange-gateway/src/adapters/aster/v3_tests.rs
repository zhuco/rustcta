use rustcta_exchange_api::{
    AccountId, PublicStreamKind, PublicStreamSubscription, RequestContext, TenantId,
    EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType};
use serde_json::json;

use super::private_parser::parse_positions;
use super::signing::{sign_message, signing_address_from_private_key};
use super::streams::{aster_public_stream_name, aster_public_subscribe_payload};

#[test]
fn aster_v3_signing_should_emit_recoverable_signature() {
    let private_key = "0000000000000000000000000000000000000000000000000000000000000001";
    let message =
        "nonce=1748310859508867&signer=0x7e5f4552091a69125d5dfcb7b8c2659029395bdf&symbol=ASTERUSDT";
    let signature = sign_message(private_key, message).expect("signature");
    assert_eq!(signature.len(), 132);
    assert_eq!(
        signing_address_from_private_key(private_key).expect("address"),
        "0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"
    );
}

#[test]
fn aster_ws_payload_should_use_lowercase_futures_stream_names() {
    let subscription = public_subscription(PublicStreamKind::OrderBookDelta);
    assert_eq!(
        aster_public_stream_name(&subscription).expect("stream"),
        "btcusdt@depth"
    );
    assert_eq!(
        aster_public_subscribe_payload(&subscription).expect("payload")["params"][0],
        "btcusdt@depth"
    );
}

#[test]
fn aster_position_parser_should_split_usdt_symbols_before_usd_suffix() {
    let exchange = ExchangeId::new("aster").expect("exchange");
    let positions = parse_positions(
        &exchange,
        TenantId::new("tenant").expect("tenant"),
        AccountId::new("account").expect("account"),
        &[],
        &json!([{
            "symbol": "BTCUSDT",
            "positionAmt": "1.5",
            "positionSide": "BOTH",
            "entryPrice": "100.0",
            "markPrice": "101.0",
            "unRealizedProfit": "1.0",
            "leverage": "5"
        }]),
    )
    .expect("positions");
    assert_eq!(positions[0].canonical_symbol.as_str(), "BTC/USDT");
}

fn public_subscription(kind: PublicStreamKind) -> PublicStreamSubscription {
    let exchange = ExchangeId::new("aster").expect("exchange");
    PublicStreamSubscription {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        context: RequestContext::new(chrono::Utc::now()),
        symbol: rustcta_exchange_api::SymbolScope {
            exchange: exchange.clone(),
            market_type: MarketType::Perpetual,
            canonical_symbol: Some(CanonicalSymbol::new("BTC", "USDT").expect("canonical")),
            exchange_symbol: ExchangeSymbol::new(exchange, MarketType::Perpetual, "BTCUSDT")
                .expect("symbol"),
        },
        kind,
    }
}
