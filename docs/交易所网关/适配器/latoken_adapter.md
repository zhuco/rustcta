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
| WebSocket | Spec-only | STOMP 订阅/退订/auth helper 和 public book parser fixture；runtime 不启用 |

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

- Perpetual、futures、margin、leverage、funding、open interest：Unsupported。
- 私有 REST live read/write：Unsupported，当前只做离线 request-spec。
- Withdraw、deposit、transfer、P2P：资金/支付权限不接入交易 gateway。
- Private WS：官方文档要求 user id channel 和 STOMP auth，当前缺少稳定 runtime 验证。
- Amend order、dead-man/cancel-all-after：未找到可无损映射的官方端点。

## 验证命令

```bash
python3 scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/latoken/endpoint_mapping.yaml
cargo fmt --check --package rustcta-exchange-gateway
cargo check -p rustcta-exchange-gateway --lib --message-format short
cargo test -p rustcta-exchange-gateway latoken --lib --message-format short
cargo test -p rustcta-gateway latoken --message-format short
```

不运行 `cargo build` 或发布构建。
