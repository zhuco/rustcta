# Smart Money Hyperliquid Vault Monitoring Candidates

Companion document for:

- [smart_money_alpha_platform_development.md](smart_money_alpha_platform_development.md)
- [smart_money_hyperliquid_initial_monitoring_batch.md](smart_money_hyperliquid_initial_monitoring_batch.md)
- [smart_money_hyperliquid_active_whale_monitoring_strategy.md](smart_money_hyperliquid_active_whale_monitoring_strategy.md)

Generated: 2026-06-04

## Source And Interpretation

Source:

- Hyperliquid app vault route: `https://app.hyperliquid.xyz/vaults`
- Hyperliquid app data route used for the public vault list: `https://stats-data.hyperliquid.xyz/Mainnet/vaults`
- Hyperliquid public info endpoint: `https://api.hyperliquid.xyz/info`

The requested filters are interpreted as:

- publicly available vault wallet: open public vault in the Hyperliquid vault list;
- balance greater than 1,000,000: vault TVL greater than 1,000,000 USDC;
- transactions within the past 30 days: latest public `userFills` record is within the last 30 days;
- profits of 50 in the past year: APR greater than or equal to 50%, using the vault list `apr` field as the yearly-return proxy because `vaultDetails` exposes APR and day/week/month/all-time histories but not a dedicated one-year return field.

## Feasibility Result

The current public vault list contains 9,458 vault rows. Under the strict filters:

| Check | Count |
|---|---:|
| Open vaults with TVL greater than 1,000,000 USDC | 22 |
| Open vaults with TVL greater than 1,000,000 USDC and latest fill within 30 days | 19 |
| Open vaults with TVL greater than 1,000,000 USDC, latest fill within 30 days, and APR greater than or equal to 50% | 7 |

Therefore, selecting 50 vaults that satisfy all requested filters is not possible from the current public data. The monitor config should add only the 7 strict candidates below. Do not fill the remaining slots with inactive or sub-threshold vaults unless the selection thresholds are intentionally relaxed.

## Strict Monitoring Candidates

These vaults satisfy the full interpreted filter set: public/open, TVL greater than 1,000,000 USDC, latest fills within 30 days, and APR greater than or equal to 50%.

| Rank | Vault | Name | TVL USDC | APR | Month PnL USDC | Month volume USDC | Latest fill UTC |
|---:|---|---|---:|---:|---:|---:|---|
| 1 | `0xac26cf5f3c46b5e102048c65b977d2551b72a9c7` | Long HYPE & BTC \| Short Garbage | 3395194 | 834.2% | 1445831 | 1035066 | 2026-06-02T19:47:52Z |
| 2 | `0xd6e56265890b76413d1d527eb9b75e334c0c5b42` | [ Systemic Strategies ] HyperGrowth | 14391581 | 337.1% | 3197143 | 50603865 | 2026-06-02T03:15:34Z |
| 3 | `0x060d01aa996003b3731a992462d7f0ba68bf3b04` | Edge & Hedge | 1164184 | 273.9% | 234842 | 22195226 | 2026-06-03T09:54:31Z |
| 4 | `0xca230e816bdb34a46960c2f978a30a563d1ae9e0` | Hyperrr | 1564769 | 201.3% | 260956 | 220930633 | 2026-06-04T01:50:04Z |
| 5 | `0x7048b287889c5913d59f812795d7fd5d724be77a` | Equinox - Blackalgo | 1102443 | 123.4% | 105640 | 5684042 | 2026-06-04T00:00:54Z |
| 6 | `0x780825f3f0ad6799e304fb843387934c1fa06e70` | AILAB TEST ULTRA | 1047211 | 94.8% | 78492 | 133904579 | 2026-06-04T01:46:58Z |
| 7 | `0x27d33e77c8e6335089f56e399bf706ae9ad402b9` | Martyrbit | 1717443 | 65.5% | 88868 | 8219131 | 2026-05-30T11:37:53Z |

## Active But Not Strict

These vaults have TVL greater than 1,000,000 USDC and recent fills, but do not meet the APR greater than or equal to 50% filter. They can be observed as liquidity or control candidates, but should not be treated as the requested profitable-vault set.

| Vault | Name | TVL USDC | APR | Month PnL USDC | Latest fill UTC |
|---|---|---:|---:|---:|---|
| `0x31ca8395cf837de08b24da3f660e77761dfb974b` | HLP Strategy B | 3036178 | 4.5% | 307006 | 2026-06-04T01:54:20Z |
| `0x010461c14e146ac35fe42271bdc1134ee31c703a` | HLP Strategy A | 3031094 | -0.2% | -516949 | 2026-06-04T01:54:16Z |
| `0x654016a8c9fcf0c4cb7ed6078aba21f7f399f7b7` | BredoStrategy | 2193191 | -0.6% | -13639 | 2026-06-04T01:49:55Z |
| `0xa6a34f0bf2ccea9a1ddf9e9a973f17c498dc5e40` | FC Genesis - Quantum | 2175648 | -0.8% | -18557 | 2026-05-27T23:40:59Z |
| `0x115849ce84370f25cadcf0d348510d73837e1aa5` | Orbit Value Strategies | 3081337 | -1.3% | -39112 | 2026-06-02T00:36:58Z |
| `0x1e37a337ed460039d1b15bd3bc489de789768d5e` | Growi HF | 8521444 | -3.4% | -307038 | 2026-06-04T01:54:16Z |
| `0x8231fdf9997c003a267374b45fb25c0455aa1dcb` | AIQuantPulse | 1895005 | -8.1% | -224932 | 2026-06-04T01:53:29Z |
| `0xb1505ad1a4c7755e0eb236aa2f4327bfc3474768` | Bitcoin Moving Average Long/Short | 2949878 | -9.6% | -326876 | 2026-06-02T00:32:25Z |
| `0x07fd993f0fa3a185f7207adccd29f7a87404689d` | [ Systemic Strategies ] L/S Grids | 5729649 | -10.4% | -921234 | 2026-06-04T01:53:23Z |
| `0x45e7014f092c5f9c39482caec131346f13ac5e73` | Ultron | 2902657 | -15.0% | -777938 | 2026-05-28T17:02:45Z |
| `0xc179e03922afe8fa9533d3f896338b9fb87ce0c8` | drkmttr | 4695658 | -15.0% | -888597 | 2026-05-31T16:22:00Z |
| `0x49a648936441b22f28f069d3c088928682b277ae` | Black Ops | 1082610 | -16.9% | -219607 | 2026-06-04T01:34:18Z |

## Inactive High-TVL Vaults

These vaults have TVL greater than 1,000,000 USDC but did not pass the recent-fill activity check. Treat them as manual-review whales or infrastructure vaults, not active trading monitors.

| Vault | Name | TVL USDC | APR | Latest fill UTC |
|---|---|---:|---:|---|
| `0xdfc24b077bc1425ad1dea75bcb6f8158e10df303` | Hyperliquidity Provider (HLP) | 348935324 | 1.5% | none returned by `userFills` |
| `0xb0a55f13d22f66e6d495ac98113841b2326e9540` | HLP Liquidator 2 | 30000000 | 0.0% | 2026-02-06T00:20:00Z |
| `0x469f690213c467c39a23efacfd2816896009d7d8` | HLP Strategy X | 1004955 | 1.5% | 2026-01-07T08:38:00Z |

## Expansion Rule If 50 Vaults Are Required

To reach 50 public vault monitors, the TVL threshold must be lowered below 1,000,000 USDC. Based on the vault-list snapshot:

- TVL greater than 100,000 USDC, nonnegative APR, and positive month PnL gives 39 candidates before fill verification;
- TVL greater than 50,000 USDC, nonnegative APR, and positive month PnL gives 64 candidates before fill verification;
- TVL greater than 10,000 USDC, APR greater than or equal to 50%, and positive month PnL gives 91 candidates before fill verification.

Recommended relaxation, if needed: keep the 7 strict vaults enabled, then add reserve candidates with TVL greater than 50,000 USDC, positive month PnL, nonnegative APR, and latest fill within 30 days as observe-only until they cross the 1,000,000 USDC TVL threshold.

