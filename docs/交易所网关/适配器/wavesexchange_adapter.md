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

## Unsupported Boundary

- Balances require Waves address/node balance mapping and are not represented as gateway account balances yet.
- Positions are not applicable to spot.
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
