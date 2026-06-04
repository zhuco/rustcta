# Smart Money Hyperliquid Active Whale Monitoring Strategy

Companion document for:

- [smart_money_alpha_platform_development.md](smart_money_alpha_platform_development.md)
- [smart_money_hyperliquid_initial_monitoring_batch.md](smart_money_hyperliquid_initial_monitoring_batch.md)
- [smart_money_hyperliquid_wallet_ratings.md](smart_money_hyperliquid_wallet_ratings.md)

Generated: 2026-06-04

## Purpose

This note defines the first practical expansion rule for Hyperliquid wallet monitoring. The monitor should prioritize accounts that have traded recently, while still keeping a smaller representative set of whale accounts so that the platform can learn from large-capacity traders even when they trade less frequently.

## Current State

The config has been expanded from 30 to 50 wallets for the next monitor start. If a monitor process was started before this change, it still uses the older 30-wallet runtime set until it is restarted.

The original 30-wallet smoke-test set was useful, but not enough for production research because:

- only a small share of the original monitored wallets were from the public Tier 1 whale set;
- several profitable recent fills appeared on unsupported symbols, especially `ZECUSDT`;
- the live monitor uses a 15 minute fill lookback, which is correct for low-latency detection but not enough to decide whether a wallet has been active during the last 30 days;
- copied-order simulation rows should not be used as the final success metric until the observed-fill to simulated-order linkage is verified.

## Target Universe

Use a 50 to 60 wallet live-monitoring set for the next iteration. This is large enough to cover style diversity and active flow, but small enough to keep polling, scoring, and debugging simple.

Recommended split:

| Cohort | Size | Role | Selection rule |
|---|---:|---|---|
| Active 30d traders | 30-40 | Primary copy-signal candidates | Positive 30d PnL, positive 30d ROI, at least 100 trades, and recent fills in the last 30 days |
| Representative whales | 15-20 | Capacity and market-impact reference | Tier 1 whale wallets with large 30d volume, high PnL, or high consistency, even if not top composite score |
| Control / contrast wallets | 5-10 | Noise rejection and threshold calibration | Active but weak, flat, or inconsistent wallets; observe only, do not copy |

Do not expand directly from 30 to hundreds of active wallets. First confirm the scoring and copy-linkage behavior with this 50 to 60 wallet set, then promote or demote wallets daily.

## First Expansion Candidates

Keep the original 30-wallet monitor list and add the following 20 net-new Tier 1 whale candidates. These wallets are selected for active 30d trading, high 30d volume, style coverage, and representative whale behavior. The high-capacity swing whale `0x2d99fe0f36c1aebd28a1a2c0e82e8ca13c2ea351` was already present in the original config, so `0xf09c63a2b47d7b383ecc517a295727312384eaed` is used as the replacement net-new whale candidate.

| Priority | Wallet | Style | 30d PnL | 30d ROI | Win rate | Trades | Why add |
|---:|---|---|---:|---:|---:|---:|---|
| 1 | `0xf09c63a2b47d7b383ecc517a295727312384eaed` | Trend | 197496 | 34.9% | 92% | 159 | Net-new active Tier 1 whale replacing an already monitored capacity wallet |
| 2 | `0x9a0825ca6c4c577a1202a8fae3b8f044c1d5711d` | Swing | 466356 | 26.0% | 51% | 1013 | High-volume swing trader; useful for capacity and risk contrast |
| 3 | `0x3662dd1d493d1d3cf6a65cb8e4ce2bfb01f63b3c` | Trend | 308234 | 23.9% | 100% | 407 | High-volume, high-consistency trend whale |
| 4 | `0x4f9140107e42982d3f86eae7474f56bc373ade31` | Trend | 72464 | 48.6% | 67% | 449 | Active trend follower with strong ROI and volume |
| 5 | `0x149d35ed0e25ec4b760c28055f744fdd02af1764` | Trend | 37549 | 28.7% | 98% | 593 | Frequent, high win-rate trend trader |
| 6 | `0x8e6c25f50d4be1e649e61b331a804d11b07a5c08` | Trend | 78470 | 32.3% | 100% | 322 | High win-rate trend whale with meaningful volume |
| 7 | `0xcdfcfd848d6539aa75263adba82884c58bdc2aaf` | Trend | 47464 | 23.3% | 75% | 176 | Lower turnover but strong trend representative |
| 8 | `0x05df8c46c75a06ee097c0a90f9fbe8a11a4eba39` | Trend | 42952 | 55.7% | 75% | 375 | High-ROI trend account currently absent from live config |
| 9 | `0xebd7c4b3677a4480fb5f69ac2f51c51d66a19990` | Mixed | 639281 | 73.9% | 65% | 268 | Large PnL mixed-style whale; strong representative account |
| 10 | `0xc7f3611b6a7566961f3abc8db6fe4700d6b8a380` | Trend | 45309 | 26.6% | 92% | 361 | Consistent trend follower for confirmation signals |
| 11 | `0xf191539a2c4ba0af951438bb6abfa0625c7df2ef` | Trend | 263523 | 16.4% | 53% | 333 | Large PnL but lower win rate; useful risk contrast |
| 12 | `0xa2ecada0b45ad16625865843c9fc5e7c985a2e42` | Trend | 325362 | 18.3% | 82% | 318 | Large PnL trend whale with good consistency |
| 13 | `0x3f3192097db8fc6043e459db7926a4d8a26c155b` | Swing | 104536 | 21.9% | 59% | 404 | Swing-capacity representative with high activity |
| 14 | `0x9312e0a61f21affbd5be821f436f8fdf6b6f6663` | Mixed | 68224 | 19.9% | 69% | 384 | Mixed-style whale for cross-style confirmation |
| 15 | `0x1223776bf953ee798a53a34109de5e680cf58180` | Mixed | 82831 | 17.0% | 89% | 116 | Lower activity but high consistency mixed trader |
| 16 | `0x36e6dab460411e402aed071915a21f5c2ff9f223` | Trend | 57352 | 50.0% | 92% | 277 | High-ROI, high win-rate trend wallet |
| 17 | `0xccf6a4da71795428e13a0d1330293c735b66ec07` | Swing | 13555 | 15.1% | 91% | 206 | Smaller swing account with strong consistency |
| 18 | `0xee621cac3443ea9511e4b6050d92ee2a8b80f93d` | Mixed | 2556 | 22.8% | 81% | 1052 | Very active mixed trader; use mainly for activity pattern learning |
| 19 | `0x6f713fd42f173b163a9fbbcdbe6b26c7dbd6b2a2` | Trend | 47945 | 17.8% | 90% | 271 | Consistent trend follower for breadth |
| 20 | `0xb36c7e6e58e38ad608748e6a00bda29dbeaec96a` | Trend | 33810 | 19.4% | 67% | 175 | Additional trend account for cohort stability |

## Promotion And Demotion Rules

Promote a wallet into live monitoring when it passes all of these checks:

- at least one fill in the last 30 days;
- 30d trades greater than or equal to 100;
- positive 30d PnL and positive 30d ROI;
- public win rate greater than or equal to 50%;
- no obvious stale or malformed fill data during ingestion;
- style adds useful diversity or confirms an existing style cluster.

Demote or downgrade a wallet when any of these conditions persists after the next daily refresh:

- no observed fills for 7 days;
- repeated losses after platform-side scoring, even if the public 30d leaderboard remains positive;
- most signals are on unsupported symbols;
- fills are too old for copy execution after polling;
- copy-candidate rows fail linkage validation.

## Inactive 30d Handling

A wallet with no fills in the last 30 days should not be treated as a normal active signal wallet. Classify it into one of two states:

- representative whale manual-review: a known whale or vault-related account that may still matter for capacity, deposits, withdrawals, or reactivation monitoring;
- invalid or unverified: an address with no current trading evidence, malformed data, or no clear reason to monitor.

Do not enable inactive wallets as `active_30d` copy candidates. Keep them disabled or observe-only until a fresh fill appears and the wallet passes the promotion rules above.

## Symbol Coverage

The first several hours of monitoring showed profitable wallet activity on `ZECUSDT`, but it was ignored because the symbol was not in the monitored symbol set. Add `ZECUSDT` to the next config pass. Consider `NEARUSDT` after confirming exchange support and orderbook data quality.

Keep the live copy universe narrower than the observation universe:

- observe: `BTCUSDT`, `ETHUSDT`, `SOLUSDT`, `HYPEUSDT`, `ZECUSDT`, and candidate high-activity symbols;
- copy: only symbols with clean orderbook support, low latency, and validated fill-to-order linkage.

## Operating Cadence

Use two different time windows:

- live monitor: keep the existing short fill lookback for low-latency polling;
- selection refresh: run a daily 30d wallet activity refresh and rebuild candidate rankings.

Daily review should produce three actions: keep, demote, or promote. A wallet should not be promoted to copy execution only because it appears on a public leaderboard; it must also perform well in our observed fills and platform score history.
