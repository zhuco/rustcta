# Bybit Gateway Adapter

Status: `rustcta-exchange-gateway` Bybit V5 Spot + linear perpetual/futures REST adapter with public order-book WebSocket subscription/parser fixtures and private REST fee-rate readback.

## Product Boundary

- Adapter id: `bybit`.
- Products: Spot, USDT/USDC linear perpetual and futures through Bybit V5 categories.
- REST base URL: `https://api.bybit.com`.
- Testnet REST base URL: `https://api-testnet.bybit.com`.
- Public WS URLs: `wss://stream.bybit.com/v5/public/spot`, `wss://stream.bybit.com/v5/public/linear`, `wss://stream.bybit.com/v5/public/inverse`, and `wss://stream.bybit.com/v5/public/option`.
- Private WS URL: `wss://stream.bybit.com/v5/private`.

Official references:

- V5 market instruments: https://bybit-exchange.github.io/docs/v5/market/instrument
- V5 public orderbook WebSocket: https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook
- V5 account fee rate: https://bybit-exchange.github.io/docs/v5/account/fee-rate
- V5 order create: https://bybit-exchange.github.io/docs/v5/order/create-order
- V5 order amend: https://bybit-exchange.github.io/docs/v5/order/amend-order
- V5 batch create/cancel: https://bybit-exchange.github.io/docs/v5/order/batch-place
- V5 WebSocket auth: https://bybit-exchange.github.io/docs/v5/ws/connect

## Implemented Gateway Surface

- Public REST symbol rules through `/v5/market/instruments-info`.
- Public REST order book snapshots through `/v5/market/orderbook`.
- Private REST balances, positions, fee-rate, place order, cancel order, cancel all, query/open orders, and recent fills.
- Funding-history REST is exposed through the shared `get_funding_rates` API with fixture coverage.
- Account control exposes shared `set_leverage` and `set_position_mode` requests for linear derivative accounts.
- V5 REST signing uses `timestamp + api_key + recv_window + query_or_json_body` with `X-BAPI-*` headers.
- Public WebSocket order-book helpers cover `orderbook.1`, `orderbook.50`, `orderbook.200`, and `orderbook.1000`; Bybit V5 documents 10/20/100/200ms cadences by depth and parser fixtures normalize `u/seq/cts`, snapshot/delta shape, and zero-size delta deletion rows into gateway order-book events. REST snapshots remain the resync source.
- WebSocket private subscribe/auth/heartbeat payload helpers are present; private long-running connection is still spec-only and uses REST reconciliation fallback.

## Unsupported Boundary

- Native amend, batch create, and batch cancel are wired through signed Bybit V5 REST runtime. Batch create/cancel use native `/v5/order/create-batch` and `/v5/order/cancel-batch`, enforce one market type per batch, and expose partial failures through gateway batch reports/reconciliation plans. Shared `AmendOrderRequest` currently maps quantity-only amend; Bybit client-order-id replacement is rejected before send because V5 amend does not support changing `orderLinkId`. `place_order_list` remains unsupported because shared OCO/OTO order-list semantics are not mapped. Margin mode, disconnected cancel-all/dead-man, inverse-specific routing, and options metadata remain separate unsupported or follow-up boundaries as recorded in endpoint mapping.
- Private WS runtime is not long-running in this adapter; fixtures document order/execution/position/wallet channels.
- Web page or unofficial endpoints are not used.

## Validation

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/bybit/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway bybit --lib --message-format short
cargo test -p rustcta-gateway bybit --message-format short
cargo test -p rustcta-exchange-gateway bybit_get_funding_rates_should_send_public_funding_history_request -- --nocapture
cargo test -p rustcta-exchange-gateway bybit_set_leverage_should_send_signed_v5_position_request -- --nocapture
cargo test -p rustcta-exchange-gateway bybit_set_position_mode_should_send_signed_v5_switch_mode_request -- --nocapture
```
