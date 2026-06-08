# WavesExchange Gateway Adapter

Status date: 2026-06-08

`wavesexchange` is a conservative WX Network matcher adapter for Waves spot asset pairs. It is scan-only: public matcher REST can read pair restrictions and order book snapshots, while wallet-scoped reads, signed order placement, cancellation, and address streams remain `Unsupported`.

## Official Sources

| Area | Source |
| --- | --- |
| Matcher overview | https://docs.waves.exchange/en/waves-matcher |
| Matcher REST | https://docs.waves.exchange/en/waves-matcher/matcher-api |
| Matcher WebSocket | https://docs.waves.exchange/en/waves-matcher/matcher-websocket-api-common-streams |
| Waves order model | https://docs.waves.tech/en/blockchain/order |

## Products And URLs

- Market type: `MarketType::Spot`.
- Mainnet REST: `https://matcher.waves.exchange`.
- Testnet REST: `https://matcher-testnet.waves.exchange`.
- Public WS spec endpoint: `wss://matcher.waves.exchange/ws/v0`.
- Symbols use matcher pair ids in `amountAsset-priceAsset` form, for example `WAVES-DG2xFkPdDwKUoBkzGAhQtLpSGzfXLiCYPEzeKH2Ad24p`.

## Implemented Surface

- `get_symbol_rules`: `GET /matcher/orderbook/{amountAsset}/{priceAsset}/info`.
- `get_order_book`: `GET /matcher/orderbook/{amountAsset}/{priceAsset}?depth={depth}`.
- Parser fixtures cover matcher restriction data and object-style order book levels.
- Request-spec fixtures verify the two public REST builders offline.
- Public WS helpers build `obs` subscribe and `obu` unsubscribe payloads, but runtime streaming is not enabled.

## Official WebSocket Order Book Detail

P9 official verification confirms matcher WebSocket common streams update every
100ms. Order book subscription uses
`{"T":"obs","S":"amountAsset-priceAsset","d":10}`, where `d` is price depth.
Incoming order book messages include update id `U`, asks `a`, bids `b`,
optional last trade `t`, and subscription id `S`; no checksum is documented.
One connection currently allows 10 order book subscriptions.

## Official Core Trading Detail

官方核心交易核验见 [核心交易官方核验 P2 第三批](../核心交易官方核验_P2_第三批.md)。WX Network matcher API 支持 Place Limit Order、Place Market Order、Cancel Order、Cancel All、Get Order Status、Order History 和 Tradable Balance；订单签名是 Waves order/public-key signature，不是 API-key HMAC。

当前 adapter 只做 public matcher REST scan，signed order placement/cancel 仍是 `项目未实现`。后续要补 Waves signed order payload、public-key cancellation signature、order status/history parser 和 matcher fee/rate 对账。

## Unsupported Boundary

- Balances require Waves address/node balance mapping and are not represented as gateway account balances yet.
- Positions are not applicable to spot.
- P6 official product-line verification found WX matcher is a Waves spot asset
  pair matcher without standard futures/perpetual/options semantics; standard
  contracts are `交易所不支持合约`.
- Fee rates are matcher asset rates, not an account-scoped maker/taker fee contract.
- Place/cancel order flows require signed Waves order payloads or public-key cancellation signatures, not API-key HMAC signing.
- Batch place/cancel, amend, order lists, cancel-all, open orders, recent fills, private streams, withdrawals, transfers, and admin matcher endpoints are unsupported.

## Fixtures

- `tests/fixtures/exchanges/wavesexchange/market_info.json`
- `tests/fixtures/exchanges/wavesexchange/orderbook.json`
- `tests/fixtures/exchanges/wavesexchange/request_specs/markets.json`
- `tests/fixtures/exchanges/wavesexchange/request_specs/orderbook.json`
- `tests/fixtures/exchanges/wavesexchange/signing_vectors/waves_transaction_unsupported.json`
- `tests/fixtures/exchanges/wavesexchange/ws/updates_subscribe_orderbook.json`
- `tests/fixtures/exchanges/wavesexchange/ws/updates_unsubscribe_orderbook.json`

## Validation

Allowed validation commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/wavesexchange/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway wavesexchange --lib --message-format short
cargo test -p rustcta-gateway wavesexchange --message-format short
```

Do not run `cargo build` for this task.
