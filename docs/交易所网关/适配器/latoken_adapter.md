# LATOKEN Gateway Adapter

状态日期：2026-06-08

## 范围

- Adapter id：`latoken`
- 任务：A-25，长尾现货交易所
- 当前层级：G1 public REST scan-only + public STOMP order book；核心交易私有 REST 为 explicit credential-gated runtime
- 产品线：Spot

LATOKEN 官方 REST v2 文档公开了现货市场、订单簿、账户和订单端点；本 adapter 启用
公开 REST 的 symbol rules 和 order book snapshot，并将 core private order lifecycle
接到 explicit credential-gated HMAC REST runtime。默认无 `enabled_private_rest` 与 API
key/secret 时仍 guard；不宣称 live 成功。

账户/余额接口 `/v2/auth/account?zeros=false` 已提升为 guarded runtime：在 `enabled_private_rest`、API key/secret 齐备时使用 LATOKEN HMAC 私有 REST 签名读取，parser 覆盖 `currency/available/blocked` 响应并按请求资产过滤。默认无凭据仍 fail-closed；余额 readback 不能因为需要认证被标成不支持。

## 官方资料

| 项目 | 值 |
| --- | --- |
| REST docs | https://api.latoken.com/doc/v2/ |
| REST swagger | https://api.latoken.com/doc/v2/swagger.json |
| REST base URL | `https://api.latoken.com`，endpoint 使用 `/v2/...` |
| WebSocket docs | https://api.latoken.com/doc/ws/ |
| WebSocket base URL | `wss://api.latoken.com/stomp` |
| Testnet | 未在官方文档中找到稳定 sandbox URL |
| Rate limit | 文档列出 `TOO_MANY_REQUESTS` 和 `X-Rate-Limit-Remaining`，未给固定数字 |

## 已实现

| 能力 | 状态 | 说明 |
| --- | --- | --- |
| `get_symbol_rules` | Native | `GET /v2/pair` + `GET /v2/currency`，用 currency tag 解析 UUID pair |
| `get_order_book` | Native | `GET /v2/book/{currency}/{quote}?limit=...`，最大 depth 按官方 1000 clamp |
| 私有 REST 签名 | Offline fixture | `X-LA-APIKEY`、`X-LA-SIGNATURE`、`X-LA-DIGEST` HMAC-SHA256 向量 |
| 私有交易 | Guarded runtime | `place/cancel/cancel-all/query/open/fills` 与 `placeBulk`/`cancelBulk` 已接入 credential-gated signed REST runtime；request-spec、HMAC signing、mock request、parser/readback fixture 均已覆盖 |
| Public WebSocket | Native public book | STOMP `/v1/book/{base}/{quote}` 订阅 frame、public book parser、`nonce` sequence 和 gap/regression reconnect policy 已落地 |
| Private WebSocket | Unsupported | 官方文档要求 user id channel 和 STOMP auth，当前缺少稳定 runtime 验证 |

## 签名

私有 REST endpoint 均包含 `/auth/`。签名串为：

```text
METHOD + /v2/...path + query_string_without_question_mark + body_params_string
```

body/query 参数按官方 form-like 规则拼接为 `key=value` 并用 `&` 连接。默认使用
HMAC-SHA256；fixture 位于：

- `tests/fixtures/exchanges/latoken/signing_vectors/rest_place_order_hmac_sha256.json`
- `tests/fixtures/exchanges/latoken/signing_vectors/rest_batch_place_hmac_sha256.json`
- `tests/fixtures/exchanges/latoken/signing_vectors/rest_batch_cancel_hmac_sha256.json`
- `tests/fixtures/exchanges/latoken/signing_vectors/ws_auth_hmac_sha256.json`

## 官方核心交易边界

官方核心交易核验见 [核心交易官方核验 P2 第三批](../核心交易官方核验_P2_第三批.md)。LATOKEN v2 官方 API 有 `/v2/auth/order/place`、`/v2/auth/order/cancel`、`/v2/auth/order/cancelAll`、`/v2/auth/order/getOrder/{id}`、`/v2/auth/order/placeBulk` 和 `/v2/auth/order/cancelBulk`，API 权限说明也列出 `PLACE_ORDER` 和 `CANCEL_ORDER`。

当前项目已把 `place_order`、`cancel_order`、`cancel_all_orders`、`query_order`、`get_open_orders`、`get_recent_fills` 接入 guarded HMAC private REST runtime。默认无 `enabled_private_rest` 与 API key/secret 时仍返回 guard error；mock REST tests 覆盖 `X-LA-*` 签名 headers、body/query/path、order/cancel parser、open-order readback 和 recent fills reconciliation。仍不宣称 live 成功。

高级订单边界：`/v2/auth/order/placeBulk` 和 `/v2/auth/order/cancelBulk` 是官方批量下单/撤单线索，当前 mapping 标为
`native`，runtime 只有在 `enabled_private_rest` 且 API key/secret 存在时启用；没有伪造 live 成功。请求形状由
`tests/fixtures/exchanges/latoken/request_specs/batch_place_orders.json`、`tests/fixtures/exchanges/latoken/request_specs/batch_cancel_orders.json` 固定；对应 HMAC payload 边界由 `rest_batch_place_hmac_sha256.json` 和 `rest_batch_cancel_hmac_sha256.json` 固定；批量响应 parser 由 `tests/fixtures/exchanges/latoken/parser/batch_place_orders_ack.json` 和 `tests/fixtures/exchanges/latoken/parser/batch_cancel_orders_ack.json` 覆盖 per-item success/failure，mock REST 测试覆盖签名 headers、请求 body 与 partial report。amend 和
order-list/OCO/OTO 未找到可无损映射的官方端点，继续写 `交易所不支持`。

## Unsupported 边界

- Perpetual、futures、margin、leverage、funding、open interest：当前 runtime
  Unsupported。
- 私有 REST live read/write：core order lifecycle 与 `placeBulk`/`cancelBulk` 已是 guarded runtime；balances/fees 仍为离线 request-spec/signing/parser 边界。
- Withdraw、deposit、transfer、P2P：资金/支付权限不接入交易 gateway。
- Private WS：官方文档要求 user id channel 和 STOMP auth，当前缺少稳定 runtime 验证。
- Amend order、order-list/OCO/OTO、dead-man/cancel-all-after：未找到可无损映射的官方端点。

## 官方产品线边界

P6 官方核验确认 LATOKEN API v1/v2 主要是 Spot，但官方 `/futures` 页面有产品线索，
当前未在 API v1/v2 文档中找到稳定 futures endpoint spec。因此这里不能写成
`交易所不支持合约`；应写 `项目未实现 Futures/需官方 endpoint spec 核验`。

2026-06-09 产品线边界收窄：`contract_product` 和 `futures_product`
绑定 `tests/fixtures/exchanges/latoken/request_specs/product_line_source_boundary.json`。
当前 Spot REST/STOMP adapter 不根据产品页线索启用 futures runtime；补齐前必须先固定
official futures endpoint specs、auth、symbol metadata、positions/risk 和 product guard。
状态建议：继续保留 `contract_product` / `futures_product = 项目未实现`；LATOKEN
futures 产品线索不能写成交易所不支持，但稳定 futures endpoint/auth/spec 未固定前
不能从 Spot REST/STOMP runtime 外推。

## 官方公共 WebSocket 边界

官方 WS 细项核验见 [WebSocket 官方核验 P3 P2 公共 WS 缺口交易所](../WebSocket官方核验_P3_P2公共WS缺口交易所.md)。LATOKEN 官方 STOMP base URL 是
`wss://api.latoken.com/stomp`，公共 book channel 是
`/v1/book/{base}/{quote}`。官方未给固定推流间隔和档位，`depth: unspecified`；示例通过 `nonce`
连续性检查丢包并在 mismatch 后重连。当前项目已补 STOMP public book 订阅、
`nonce` 到标准 order book sequence 的映射、gap/regression reconnect 和 REST
`/v2/book/{base}/{quote}` snapshot rebuild。官方未公布 checksum，因此 checksum
风险仍记录为 unsupported/none documented。

## 验证命令

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/latoken/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway latoken --lib --message-format short
cargo test -p rustcta-gateway latoken --message-format short
```

不运行 `cargo build` 或发布构建。

## Fee Boundary

LATOKEN `/v2/auth/trade/fee/{currency}/{quote}` 和 `/v2/trade/fee/{currency}/{quote}` 可作为费率来源；当前已记录标准 `get_fees` 离线 request spec `tests/fixtures/exchanges/latoken/request_specs/get_fees.json` 和 HMAC signing vector `tests/fixtures/exchanges/latoken/signing_vectors/rest_get_fees_hmac_sha256.json`，矩阵应识别为 `get_fees=离线`。shared `get_fees` runtime 仍属项目未实现/未启用；补齐前需完成 maker/taker parser、currency/quote 映射和 read-only auth smoke。
