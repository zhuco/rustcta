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
- Public WS: 交易所不支持公共 WS 行情（当前 audit-only adapter 口径）；官方资料未给稳定 exchange-gateway 公共订单簿 WS
- Public REST: unsupported unverified
- Private reads: unsupported unverified
- Orders/cancels/batch: unsupported unverified
- Account model: EVM wallet, not API key/HMAC

Validation:
- `python scripts/validate_exchange_endpoint_mapping.py crates/rustcta-exchange-gateway/src/adapters/equation/endpoint_mapping.yaml`
- `cargo test -p rustcta-exchange-gateway equation --lib`
- `cargo test -p rustcta-gateway equation --lib`

Do not run `cargo build` for this task.
