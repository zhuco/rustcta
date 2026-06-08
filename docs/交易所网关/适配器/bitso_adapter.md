# Bitso Adapter

Task 16 adds a conservative Bitso gateway adapter for Latin America spot markets.

## Scope

- Products: spot only.
- Native symbol format: `major_minor`, for example `btc_mxn` and `btc_brl`.
- Canonical symbols preserve fiat quotes such as `BTC/MXN`, `BTC/BRL`, `ETH/ARS`, and `USDC/COP`.
- Public REST and public WebSocket are represented by request/payload helpers and fixtures.
- Private REST order, balance, open order, and fill surfaces are offline request-spec only until sandbox/live validation promotes them.

## Official Public WS Order Book Details

Bitso WebSocket connects to `wss://ws.bitso.com` and subscribes with JSON
messages such as `{"action":"subscribe","book":"btc_mxn","type":"diff-orders"}`.
Official channels include `orders`, which maintains the top 20 asks and bids,
and `diff-orders`, which carries full order book mutations. `diff-orders`
includes a strictly increasing `sequence`; a gap means a dropped message and the
book must be rebuilt from the REST order book snapshot. Official docs do not
state a fixed millisecond push interval.

## Boundaries

- Fiat ledger is read-audit-only.
- Official Margin Trading [WIP] is `项目未实现 Margin [WIP]` in this adapter.
- Standard futures/perpetual/options are `交易所不支持合约` under the current official API scope.
- Withdrawals, bank payments, SPEI, card operations, transfers, and margin funding are unsupported.
- Private streams are not promoted; private state should reconcile through REST request specs.
- Fixtures use placeholder keys and synthetic IDs only.

## Verification

Allowed checks:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bitso/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bitso --lib --message-format short
```
