# Aster Gateway Adapter

Adapter id: `aster`

Status: Perpetual-only adapter for Aster DEX Futures V3. The adapter exposes public market metadata, order book and funding reads, signed private account/order reads and writes, account-control mutations, advanced order REST helpers, public WebSocket subscription/parser helpers, and private listen-key stream session setup.

## Scope

- Public REST: `https://fapi.asterdex.com`
- Public WebSocket: `wss://fstream.asterdex.com/ws`
- Private WebSocket: `wss://fstream.asterdex.com/ws`
- Market types: `Perpetual`
- Spot: 项目未实现 Spot。Aster 官方文档有 Spot API，当前 adapter 只接 Futures V3。
- Symbols: Aster futures symbols are normalized without separators, for example `BTCUSDT`.
- Signing: EIP-712 `AsterSignTransaction`, version `1`, chain id `1666`, zero verifying contract, `Message(string msg)`.

Spot 边界写入 `spot_product status: project_unimplemented`：当前只接 Futures V3 host 和 perpetual REST/WS。补 Spot 前需要 spot REST/WS host、exchangeInfo/depth/ticker/trades、signed account/order/open-order/fill endpoints、listen-key/private stream 和 parser fixtures。

## Endpoint Mapping

Machine-readable mapping:

`crates/rustcta-exchange-gateway/src/adapters/aster/endpoint_mapping.yaml`

| Gateway capability | Aster endpoint | Status |
| --- | --- | --- |
| Contracts and symbol rules | `GET /fapi/v3/exchangeInfo` | Native parser |
| Order book | `GET /fapi/v3/depth` | Native snapshot parser |
| Mark/funding snapshot | `GET /fapi/v3/premiumIndex` | Public REST helper |
| Funding history | `GET /fapi/v3/fundingRate` | Public REST helper |
| Balances | `GET /fapi/v3/balance` | EIP-712 signed REST |
| Positions | `GET /fapi/v3/positionRisk` | EIP-712 signed REST |
| Trading fees | `GET /fapi/v3/commissionRate` | EIP-712 signed REST |
| Place/cancel/query/open orders | `/fapi/v3/order`, `/fapi/v3/openOrders`, `/fapi/v3/allOpenOrders` | EIP-712 signed REST |
| Recent fills | `GET /fapi/v3/userTrades` | EIP-712 signed REST |
| Account control | `POST /fapi/v3/leverage`, `GET/POST /fapi/v3/positionSide/dual` | EIP-712 signed REST |
| Amend order | `PUT /fapi/v3/order` | EIP-712 signed REST |
| Batch place/cancel | `POST/DELETE /fapi/v3/batchOrders` | EIP-712 signed REST with per-item partial failure parsing |
| Public WS | `bookTicker`, partial depth, diff depth, `aggTrade`, `ticker`, `kline` streams | Payload helpers and parser fixtures |
| Private WS | `POST/PUT/DELETE /fapi/v3/listenKey` then listen-key stream | Session lifecycle helpers and parser-only event normalization |

## Official WebSocket Order Book Detail

Aster Futures public WS follows Binance-style streams. Official order book
channels include bookTicker, partial depth, and diff depth. bookTicker is
real-time; partial/diff depth can be 100ms, 250ms, or 500ms; partial depth
supports 5/10/20 levels. Diff depth uses `U/u/pu` and REST snapshot
`lastUpdateId` replay; if `pu` does not equal the previous `u`, the local book
must be rebuilt. The adapter has local parser coverage for bookTicker, partial
depth, and diff-depth sequence gaps. Capability remains snapshot-first for live
strategy admission until a long-running public WS profile and REST replay loop
are wired end to end. Source batch: [WebSocket 官方核验 P5 衍生品/链上盘口细项](../WebSocket官方核验_P5_衍生品链上盘口细项.md).

## Private WebSocket Boundary

The adapter can create, renew, and delete listen keys and has parser-only
normalization for `ORDER_TRADE_UPDATE`, fills, `ACCOUNT_UPDATE`, balances,
positions, and `listenKeyExpired`. Live private stream capability remains REST
fallback until a long-running ready gate, reconnect, resubscribe, and REST
resync loop are implemented and tested together.

## Unsupported Boundaries

- Spot is 项目未实现 Spot for this adapter; COIN-M delivery, options, transfers and wallet funding are outside this adapter.
- Quote-sized market orders and native order lists are `Unsupported`.
- No derivatives beyond Aster USDT perpetuals are exposed unless an official stable API is mapped later.

## Validation

Recommended targeted validation:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/aster/endpoint_mapping.yaml
rustfmt --edition 2021 --check crates/rustcta-exchange-gateway/src/adapters/aster/*.rs
cargo test -p rustcta-exchange-gateway aster_v3_signing --lib --message-format short
cargo test -p rustcta-exchange-gateway aster_ws_payload --lib --message-format short
cargo test -p rustcta-exchange-gateway aster_position_parser --lib --message-format short
```

Do not run live private trading without a separate dry-run preflight and permission audit.

## P2 Product Line Boundary (2026-06-09)

`spot_product` is an official-source project boundary, not an exchange-unsupported row. Aster has a separate Spot API surface, while this adapter is scoped to Aster Futures V3 USDT perpetual REST/WS.

Do not promote Spot runtime from the Futures V3 profile. Promotion requires spot REST/WS host audit, exchangeInfo/depth/ticker/trades public specs, signed account private specs, spot order lifecycle/open-order/fill private specs, listen-key/private stream handling, and product-scope reconciliation guards.
