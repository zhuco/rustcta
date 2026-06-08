# LATOKEN Gateway Adapter

状态日期：2026-06-08

## 范围

- Adapter id：`latoken`
- 任务：A-25，长尾现货交易所
- 当前层级：G1 public REST scan-only，私有 REST 仅 request-spec/signing fixture
- 产品线：Spot

LATOKEN 官方 REST v2 文档公开了现货市场、订单簿、账户和订单端点；本 adapter 只启用
公开 REST 的 symbol rules 和 order book snapshot。私有交易端点虽然有官方签名说明，
但在没有独立 read-only/live-dry-run 验证前不宣称 runtime 可交易。

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
| 私有交易 | Unsupported | `place/cancel/cancel-all/batch` 仅 endpoint mapping 和 request-spec |
| WebSocket | 项目未实现公共 WS runtime | 官方 STOMP `/v1/book/{base}/{quote}` 支持公共 book；当前只有订阅/退订/auth helper 和 public book parser fixture，runtime 不启用 |

## 签名

私有 REST endpoint 均包含 `/auth/`。签名串为：

```text
METHOD + /v2/...path + query_string_without_question_mark + body_params_string
```

body/query 参数按官方 form-like 规则拼接为 `key=value` 并用 `&` 连接。默认使用
HMAC-SHA256；fixture 位于：

- `tests/fixtures/exchanges/latoken/signing_vectors/rest_place_order_hmac_sha256.json`
- `tests/fixtures/exchanges/latoken/signing_vectors/ws_auth_hmac_sha256.json`

## Unsupported 边界

- Perpetual、futures、margin、leverage、funding、open interest：当前 runtime
  Unsupported。
- 私有 REST live read/write：Unsupported，当前只做离线 request-spec。
- Withdraw、deposit、transfer、P2P：资金/支付权限不接入交易 gateway。
- Private WS：官方文档要求 user id channel 和 STOMP auth，当前缺少稳定 runtime 验证。
- Amend order、dead-man/cancel-all-after：未找到可无损映射的官方端点。

## 官方产品线边界

P6 官方核验确认 LATOKEN API v1/v2 主要是 Spot，但官方 `/futures` 页面有产品线索，
当前未在 API v1/v2 文档中找到稳定 futures endpoint spec。因此这里不能写成
`交易所不支持合约`；应写 `项目未实现 Futures/需官方 endpoint spec 核验`。

## 官方公共 WebSocket 边界

官方 WS 细项核验见 [WebSocket 官方核验 P3 P2 公共 WS 缺口交易所](../WebSocket官方核验_P3_P2公共WS缺口交易所.md)。LATOKEN 官方 STOMP base URL 是
`wss://api.latoken.com/stomp`，公共 book channel 是
`/v1/book/{base}/{quote}`。官方未给固定推流间隔和档位；示例通过 `nonce`
连续性检查丢包并在 mismatch 后重连。当前项目写 `项目未实现公共 WS runtime`。

## 验证命令

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/latoken/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway latoken --lib --message-format short
cargo test -p rustcta-gateway latoken --message-format short
```

不运行 `cargo build` 或发布构建。
