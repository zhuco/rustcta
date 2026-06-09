# D8X Gateway Adapter

Status date: 2026-06-08

Task: C-43 `d8x` from `docs/交易所网关/总览/exchange_gateway_remaining_venues_one_ai_one_exchange_zh.md`.

## Scope

`d8x` is a conservative D8X Polygon zkEVM perpetual adapter. It enables public
REST market metadata and order book snapshots from D8X's CoinGecko-compatible
market-data API. Wallet account reads, contract writes, order execution,
liquidations, liquidity provision and private streams remain `Unsupported`.

## Product Lines

| Product | MarketType | Status |
| --- | --- | --- |
| Perpetual | `Perpetual` | Current adapter scope; public market-data REST is mapped. |
| Spot | n/a | 交易所不支持现货。D8X 官方产品线是 perpetual futures engine。 |

## Official Sources

| Topic | Source | Adapter decision |
| --- | --- | --- |
| Trading access | D8X Traders docs: frontend, D8X Futures Node SDK or direct smart contracts | Do not expose trading runtime until wallet signer and contract calls are audited |
| Public market data | D8X Market data API: `GET /contracts` and `GET /orderbook/{ticker_id}` under `https://drip.d8x.xyz/coingecko` | Enable G1 public REST parser fixtures |
| Node SDK | D8X Node SDK `@d8x/perpetuals-sdk` | Read-only SDK concepts are documented; the Rust adapter does not shell out to Node |
| Contracts | D8X Polygon zkEVM contract docs, chain id 1101 | Contract addresses are documented as audit context only |

## Base URLs

| Profile | URL |
| --- | --- |
| Public REST | `https://drip.d8x.xyz` |
| Polygon zkEVM chain id | `1101` |
| Testnet chain id | `2442` (`cardona`, SDK config) |
| Public WS | Unsupported/runtime unverified |

## Official Core Trading Detail

官方核验见 [核心交易官方核验 P3 第四批](../核心交易官方核验_P3_第四批.md)。D8X trading access 走 Futures Node SDK 或 smart contracts，需要 EVM wallet signer、ABI/calldata、gas/nonce/reorg 和链上/索引器对账。

因此这里写 `官方协议可交易，项目未实现链上交易 runtime`，不是 `交易所不支持下单/撤单`。当前 adapter 只接 CoinGecko-compatible public REST market data，不能提交或模拟 EVM 交易。

## Official Position Detail

官方核验见 [仓位接口官方核验 P0 第一批](../仓位接口官方核验_P0_第一批.md)。D8X 是链上 perpetual futures engine，仓位读取需要 wallet/contract/indexer 或 SDK 账户状态解析。

因此仓位读取写 `官方协议支持，离线 source-boundary 已记录，项目未实现链上仓位 runtime`。`endpoint_mapping.yaml` 的 `get_positions` 使用 `indexer://d8x/polygon-zkevm/wallet/positions` spec-only 边界和 `tests/fixtures/exchanges/d8x/request_specs/get_positions_account_source.json`，只记录 wallet/contract/indexer 或 SDK 账户状态来源；本地测试只验证 source-boundary fixture 结构和 runtime guard。补 runtime 前仍必须完成 EVM wallet/indexer account scan、contract/indexer freshness、positions parser、funding/PnL/liquidation fields、reorg handling 和 reconciliation。

账户/余额项目未实现/未启用：D8X 链上账户/钱包 readback 有余额线索，`endpoint_mapping.yaml` 已将 `get_balances` 写成 `indexer://d8x/polygon-zkevm/wallet/balances` spec-only source boundary，并绑定 `tests/fixtures/exchanges/d8x/request_specs/get_balances_account_source.json`。矩阵应为 `get_balances=离线`；共享 runtime 仍需完成 EVM wallet/contract/indexer account scan、margin/equity parser、reorg handling 和 reconciliation。

## Official WebSocket Order Book Detail

官方核验见 [WebSocket 官方核验 P7 补充交易所盘口细项二](../WebSocket官方核验_P7_补充交易所盘口细项二.md)。本批未找到 D8X 稳定官方公共订单簿 WebSocket 文档；当前可核验的是 CoinGecko-compatible REST `GET /coingecko/orderbook/{ticker_id}`。

因此单交易所文档写 `交易所不支持当前公共 WS runtime`。已有 `ws/public_orderbook_subscribe.json` 只能作为 payload fixture 或后续重核线索，不能进入套利实盘 runtime；行情重建以 REST snapshot 为准。

## Capabilities

| Capability | Status | Notes |
| --- | --- | --- |
| `get_symbol_rules` | Native public REST | `GET /coingecko/contracts?chain_id=1101` |
| `get_order_book` | Native public REST | `GET /coingecko/orderbook/{ticker_id}?chain_id=1101` |
| Funding/risk fields | Parser fixture context only | Present in raw contracts fixture, not yet exposed by shared trait |
| Balances | Spec/source fixture only | `get_balances` source boundary records wallet/contract/indexer balance/equity inputs; no live wallet RPC, Node SDK, contract call or indexer request is executed |
| Positions | Spec/source fixture only | `get_positions` source boundary records wallet/contract/indexer inputs; no live wallet RPC, Node SDK, contract call or indexer request is executed |
| Place/cancel/amend/order list | Unsupported | Require EVM signer, ABI/calldata, gas/nonce/reorg and reconciliation design |
| Batch/cancel-all | Unsupported | No gateway-safe atomicity mapping yet |
| Public/private WS runtime | Unsupported | Payload fixtures only; REST snapshot is the fallback |

## Signing Boundary

D8X does not expose a standard HMAC/API-key REST trading model for this adapter.
Trading uses a wallet signer through the Node SDK or direct EVM smart-contract
transactions. The fixture
`tests/fixtures/exchanges/d8x/signing_vectors/evm_contract_boundary.json`
documents this Unsupported boundary without including any real private key,
wallet, order id or transaction hash.

## Endpoint Mapping

Machine-readable mapping:
`crates/rustcta-exchange-gateway/src/adapters/d8x/endpoint_mapping.yaml`.

Fixtures:

- `tests/fixtures/exchanges/d8x/contracts.json`
- `tests/fixtures/exchanges/d8x/orderbook.json`
- `tests/fixtures/exchanges/d8x/request_specs/contracts.json`
- `tests/fixtures/exchanges/d8x/request_specs/orderbook.json`
- `tests/fixtures/exchanges/d8x/request_specs/get_positions_account_source.json`
- `tests/fixtures/exchanges/d8x/request_specs/place_order_unsupported.json`
- `tests/fixtures/exchanges/d8x/ws/public_orderbook_subscribe.json`
- `tests/fixtures/exchanges/d8x/ws/private_auth_payload.json`

## Unsupported Boundary

The adapter must not:

- create, sign, submit or simulate EVM transactions;
- run live private REST or private WS;
- infer positions from unaudited indexer data;
- map D8X conditional orders to shared order-list semantics;
- expose liquidity provider, liquidation, referral or governance operations.

## Validation

Allowed validation commands:

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/d8x/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway d8x --lib --message-format short
cargo test -p rustcta-gateway d8x --message-format short
```

Do not run `cargo build`, release builds or any live order/transaction command.

`get_positions` remains `离线`: `parse_position_source_boundary` validates the sanitized source fixture, while the live runtime still returns the wallet/contract/indexer boundary because there is no audited EVM account scan, freshness guard, decoded position response, reorg handling, or reconciliation path.

## Fee Boundary

交易所不支持当前费率接口 runtime：当前 profile 未建立稳定账户/链上 fee runtime。
## P2 Core Trading Boundary (2026-06-09)

P2 core place/cancel/query/open/fills are offline/spec-only EVM/SDK source boundaries; cancel-all/order-list remain unsupported shared semantics. Runtime promotion is blocked on EVM signer, ABI/calldata, gas/nonce/reorg handling, indexer parser, and reconciliation.
