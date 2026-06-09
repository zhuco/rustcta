# Equation Gateway Adapter

Task C-41 covers `equation`, an Arbitrum perpetual DEX. This implementation is deliberately audit-only: it registers the adapter and maps markets, risk, and positions first, but it does not promote production REST, GraphQL, WebSocket, or EVM transaction signing.

Official sources reviewed:
- Equation docs: https://docs.equation.org/
- Pricing mechanism: https://docs.equation.org/whitepaper-v3/pricing-mechanism
- Funding rate: https://docs.equation.org/whitepaper-v3/funding-rate
- Examples: https://github.com/EquationDAO/equation-examples

The official examples reference `https://api-v3-arbitrum.equation.trade`, `https://graph-arbitrum.equation.trade/subgraphs/name/equation-v3-arbitrum`, and Arbitrum contract calls through an EVM wallet. Those surfaces are not treated as stable exchange-gateway runtime APIs in this task.

Product lines:
- Perpetual: `MarketType::Perpetual` audit-only adapter.
- Spot: 交易所不支持现货；Equation 官方定位为 perpetual protocol / Perpetual DEX。

Runtime boundary:
- Product: `MarketType::Perpetual`
- Chain: Arbitrum One, chain id `42161`
- Public WS: 交易所不支持公共 WS 行情（当前 audit-only adapter 口径）；官方资料未给稳定 exchange-gateway 公共订单簿 WS，YAML records `websocket.public.support: unsupported`
- Public REST: unsupported unverified
- Private reads: unsupported unverified
- Orders/cancels/batch: unsupported unverified
- Account model: EVM wallet, not API key/HMAC

Core trading official detail:
- Source: [核心交易官方核验 P3 第四批](../核心交易官方核验_P3_第四批.md)
- 写法：`交易所不支持当前交易/私有接口 runtime`。
- 原因：官方资料是 perpetual protocol / Arbitrum contract examples，当前未给稳定 exchange-gateway 交易 API；不启用未验证 browser/API paths 或 EVM signing runtime。

Position official detail:
- Source: [仓位接口官方核验 P1 第二批](../仓位接口官方核验_P1_第二批.md)
- 写法：`交易所不支持当前仓位接口 runtime`。
- 原因：当前 adapter 是 audit-only EVM protocol profile，不启用稳定 private account/position runtime。

账户/余额接口写 `项目未实现/离线边界`：EVM wallet/indexer 可读取抵押品、权益或协议账户状态，`endpoint_mapping.yaml` 已将 `get_balances` 写成 `indexer://equation/arbitrum/wallet-collateral` spec-only source boundary，并绑定 `tests/fixtures/exchanges/equation/request_specs/get_balances_account_source.json`。矩阵应为 `get_balances=离线`；当前 audit profile 未接入 shared runtime、parser、reorg handling 或 reconciliation。

Validation:
- `python scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/equation/endpoint_mapping.yaml`
- `cargo test -p rustcta-exchange-gateway equation --lib`
- `cargo test -p rustcta-gateway equation --lib`

Do not run `cargo build` for this task.

## Fee Boundary

交易所不支持当前费率接口 runtime：audit-only EVM protocol profile，无稳定 gateway fee runtime。
