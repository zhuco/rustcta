# Smart Money Hyperliquid Wallet Ratings

Companion document for:

- [smart_money_alpha_platform_development.md](smart_money_alpha_platform_development.md)
- [smart_money_hyperliquid_initial_monitoring_batch.md](smart_money_hyperliquid_initial_monitoring_batch.md)
- [smart_money_hyperliquid_active_whale_monitoring_strategy.md](smart_money_hyperliquid_active_whale_monitoring_strategy.md)

Generated: 2026-06-04

## Purpose

This document categorizes the first public Hyperliquid wallet seed list into strategy-monitoring buckets and assigns preliminary ratings. The ratings are for ingestion and research prioritization only. They are not final alpha scores and must be replaced by the platform's own profile, filter, and score workers after historical ingestion.

## Source Check

The current seed source is the public FreedomCore ARENA Hyperliquid leaderboard at `https://arena.freedomcore.io/`. The page states that it tracks 412 profiled wallets, refreshes every 6 hours, and exposes the currently active top 100 rows.

The official Hyperliquid frontend route `https://app.hyperliquid.xyz/leaderboard` was also checked. It is a client-side application route, not a simple static wallet table. No additional official structured wallet rows were needed for this categorization because the seed list already contains 71 positive-PnL, positive-ROI wallets with at least 50 trades.

One important source limitation: FreedomCore's own style universe currently has only 7 `Momentum` traders. Therefore the `Momentum and short-cycle candidates` bucket below uses all remaining source-labeled momentum wallets and fills the bucket with high-activity, high-ROI candidates that should be tested as short-cycle/momentum-like signals by our profiler.

## Rating Method

Input metrics:

- source composite score
- 30d ROI
- 30d PnL
- win rate
- trade count
- 30d volume

Eligibility for this first categorization:

- 30d PnL above 0
- 30d ROI above 0
- at least 50 trades

Rating formula:

```text
rating = 60 + 40 * (
    0.32 * source_score_percentile
  + 0.22 * roi_percentile
  + 0.18 * log_pnl_percentile
  + 0.16 * win_rate
  + 0.12 * activity_percentile
)

activity_percentile = 0.60 * log_trade_count_percentile
                    + 0.40 * log_volume_percentile
```

Grade bands:

- `A+`: rating >= 90
- `A`: rating >= 85
- `A-`: rating >= 80
- `B+`: rating >= 75
- `B`: rating >= 70
- `B-`: rating below 70 but still eligible for this candidate pass

## Core Composite Leaders

Use this category for the first active smoke-test list. These wallets rank highest by the public source's composite score and should be ingested first.

| # | Wallet | Source rank/style | Rating | Grade | 30d PnL | 30d ROI | Win rate | Trades |
|---:|---|---|---:|---|---:|---:|---:|---:|
| 1 | `0x632c9d55ebcca152488018b3cf80472216d1f4cc` | #1 / Mixed | 92.2 | A+ | 19298.75 | 591.71% | 92.9% | 339 |
| 2 | `0x963deb9191d6a7dc1fb3891d472aa358b3e4b140` | #2 / Mixed | 83.0 | A- | 41002.67 | 328.10% | 100.0% | 181 |
| 3 | `0xc850d1f1639458b47b5ce9482a73d11d95f4d89b` | #3 / Momentum | 81.0 | A- | 223788.42 | 190.05% | 100.0% | 203 |
| 4 | `0x66466428990e0f42a4c54f64bee0db6bf2336de5` | #4 / Swing | 83.4 | A- | 2719383.58 | 150.27% | 100.0% | 770 |
| 5 | `0xd60e149a795e83ff52639e3ba708521ee708d74d` | #5 / Mixed | 74.2 | B | 40820.32 | 115.67% | 73.1% | 400 |
| 6 | `0x8be0b9c4dbfb90f47fb94e8968dce08d3152326c` | #6 / Swing | 77.5 | B+ | 71574.17 | 111.83% | 80.9% | 877 |
| 7 | `0xaf2c1206b22629915ff7a0fc98d171a1ef398a3f` | #7 / Swing | 77.5 | B+ | 118480.22 | 99.72% | 81.7% | 820 |
| 8 | `0x41829013ce94b21131b68d5a7b0247bd4eefce96` | #8 / Swing | 74.9 | B | 31154.21 | 90.41% | 70.3% | 568 |
| 9 | `0x05c47beed53eb8b3fc14538f881d0941cbbe05a1` | #9 / Mixed | 75.7 | B+ | 53983.73 | 78.68% | 100.0% | 223 |
| 10 | `0x085d785f5a03a6ed9b454830721cfca951c37555` | #10 / Momentum | 69.2 | B- | 508.97 | 81.32% | 58.0% | 249 |

## Trend Followers

Use this category for directional continuation and regime-sensitive alpha testing.

| # | Wallet | Source rank/style | Rating | Grade | 30d PnL | 30d ROI | Win rate | Trades |
|---:|---|---|---:|---|---:|---:|---:|---:|
| 1 | `0xda830d2d83a57cea255bcfd0cf89c3e94abde0fd` | #11 / Trend | 76.1 | B+ | 34804.12 | 75.20% | 100.0% | 450 |
| 2 | `0xe1f6b0765f27b7db17385a269069ece39f41631d` | #12 / Trend | 75.7 | B+ | 42693.10 | 71.09% | 97.7% | 384 |
| 3 | `0x844dc38bfa4882eec8c5b85dceb08fc66a7a4fbe` | #15 / Trend | 74.7 | B | 55170.80 | 63.84% | 100.0% | 185 |
| 4 | `0xaede390f5b5b7cf77428030ccfc73d99a44e1602` | #16 / Trend | 76.7 | B+ | 153105.99 | 57.79% | 100.0% | 424 |
| 5 | `0xf08208e088f89970068dc0b2deaa440f6a7d0250` | #17 / Trend | 75.7 | B+ | 171166.70 | 60.04% | 100.0% | 148 |
| 6 | `0x05df8c46c75a06ee097c0a90f9fbe8a11a4eba39` | #19 / Trend | 73.9 | B | 42952.01 | 55.71% | 75.5% | 375 |
| 7 | `0x36e6dab460411e402aed071915a21f5c2ff9f223` | #22 / Trend | 74.5 | B | 57352.24 | 49.97% | 91.5% | 277 |
| 8 | `0x4f9140107e42982d3f86eae7474f56bc373ade31` | #24 / Trend | 73.7 | B | 72464.04 | 48.62% | 66.7% | 449 |
| 9 | `0x81a745df888c89893c1b2a359b835898d8a6f128` | #28 / Trend | 72.9 | B | 54670.73 | 45.88% | 96.4% | 67 |
| 10 | `0x8e6c25f50d4be1e649e61b331a804d11b07a5c08` | #30 / Trend | 75.0 | B+ | 78469.69 | 32.32% | 100.0% | 322 |

## Swing And Capacity Traders

Use this category for wallets that combine repeated activity with larger PnL and volume. These are useful for liquidity-aware alpha and execution-simulation work.

| # | Wallet | Source rank/style | Rating | Grade | 30d PnL | 30d ROI | Win rate | Trades |
|---:|---|---|---:|---|---:|---:|---:|---:|
| 1 | `0xe183ef47829ae1eb7234e478e418cd9fa0a511dd` | #14 / Swing | 77.6 | B+ | 186102.82 | 61.10% | 98.1% | 766 |
| 2 | `0x0696a951a96bef98d3c593c3ae772b70ee4c26e5` | #20 / Swing | 75.6 | B+ | 78099.93 | 50.98% | 100.0% | 306 |
| 3 | `0xc646851ce5d79117d5086a5aaf5c81922f94efd9` | #23 / Swing | 77.4 | B+ | 271291.16 | 45.11% | 99.5% | 767 |
| 4 | `0xa8616487ec2605c79c3ad3a3e6bcbe9d059bc774` | #26 / Swing | 74.3 | B | 107684.36 | 45.66% | 100.0% | 104 |
| 5 | `0x2d99fe0f36c1aebd28a1a2c0e82e8ca13c2ea351` | #32 / Swing | 78.7 | B+ | 2041707.35 | 29.13% | 99.6% | 865 |
| 6 | `0x9a0825ca6c4c577a1202a8fae3b8f044c1d5711d` | #44 / Swing | 74.3 | B | 466356.44 | 26.04% | 50.5% | 1013 |
| 7 | `0xcbcedcfc78cd984e2733ba6818c831d2544c2cd6` | #50 / Swing | 71.9 | B | 24286.87 | 22.72% | 68.4% | 473 |
| 8 | `0x807911ee05ff443d84667e83a771a313e8c73951` | #51 / Swing | 71.0 | B | 20113.51 | 23.05% | 62.2% | 358 |
| 9 | `0x3f3192097db8fc6043e459db7926a4d8a26c155b` | #53 / Swing | 72.3 | B | 104536.20 | 21.90% | 59.5% | 404 |
| 10 | `0xccf6a4da71795428e13a0d1330293c735b66ec07` | #63 / Swing | 71.6 | B | 13555.41 | 15.13% | 90.6% | 206 |

## Momentum And Short-Cycle Candidates

Use this category for short-cycle reaction tests. The first five rows are remaining source-labeled momentum wallets. The last five are momentum-like fills based on activity, ROI, volume, and PnL because the public source exposes only 7 total momentum-style wallets.

| # | Wallet | Source rank/style | Rating | Grade | 30d PnL | 30d ROI | Win rate | Trades |
|---:|---|---|---:|---|---:|---:|---:|---:|
| 1 | `0x3eb52cbf0cd6546ed1afe361dfbe902710a80145` | #37 / Momentum | 73.2 | B | 30449.93 | 30.05% | 100.0% | 184 |
| 2 | `0xa8cbf4200595efcd94b7526d04deafe0f284af2d` | #43 / Momentum | 76.3 | B+ | 162251.37 | 21.06% | 100.0% | 1152 |
| 3 | `0x8184e8a5cfac0a0783e389b550f84a0a8bd1b567` | #48 / Momentum | 73.2 | B | 114987.24 | 24.13% | 54.6% | 1051 |
| 4 | `0x6cc021e2f5e8f12d65c860df5c63f152c4e763e1` | #49 / Momentum | 73.6 | B | 67803.95 | 21.75% | 79.0% | 548 |
| 5 | `0xb5101614ad71468a041a83e64d8b834aa17a1ed6` | #64 / Momentum | 72.2 | B | 255266.80 | 17.45% | 50.3% | 388 |
| 6 | `0xcb3ee3f1b22cdff8b6ca4839cd0784d34b85dea9` | #21 / Mixed | 74.7 | B | 121315.20 | 51.46% | 66.1% | 774 |
| 7 | `0xebd7c4b3677a4480fb5f69ac2f51c51d66a19990` | #13 / Mixed | 75.7 | B+ | 639280.95 | 73.88% | 64.5% | 268 |
| 8 | `0x149d35ed0e25ec4b760c28055f744fdd02af1764` | #35 / Trend | 74.8 | B | 37549.25 | 28.71% | 97.7% | 593 |
| 9 | `0xdee7297778b8755343e96e0b99eecf549195b856` | #25 / Mixed | 73.7 | B | 83743.83 | 47.57% | 58.1% | 822 |
| 10 | `0x3662dd1d493d1d3cf6a65cb8e4ce2bfb01f63b3c` | #42 / Trend | 76.1 | B+ | 308234.32 | 23.94% | 100.0% | 407 |

## High-Consistency Executors

Use this category to test low-noise signal aggregation and whether high win-rate public wallets survive deeper drawdown/profit-factor checks.

| # | Wallet | Source rank/style | Rating | Grade | 30d PnL | 30d ROI | Win rate | Trades |
|---:|---|---|---:|---|---:|---:|---:|---:|
| 1 | `0x88a0511a229643ae6e4ef263a08297343e11bf63` | #47 / Mixed | 74.2 | B | 78302.84 | 24.50% | 100.0% | 251 |
| 2 | `0xbb088e9852e18b1df8890d8d0f48bdcec0ab0964` | #61 / Mixed | 73.1 | B | 10808.59 | 12.41% | 100.0% | 618 |
| 3 | `0x854d7b77b762b2ca07b1ccc21e2a19eb1ccc34c7` | #58 / Mixed | 73.1 | B | 50409.23 | 16.75% | 100.0% | 182 |
| 4 | `0x82786b1a0a7a5e31f17f2ecff68b00df645ac0a4` | #60 / Mixed | 71.5 | B | 20695.45 | 18.56% | 100.0% | 64 |
| 5 | `0xb87e055a70d4a82e4ba93dd5f86c5082f3d25c72` | #27 / Mixed | 70.9 | B | 2380.06 | 45.71% | 100.0% | 87 |
| 6 | `0x2640cff4c41e1073051048db9338da5c0a7ca826` | #39 / Mixed | 74.5 | B | 37456.04 | 25.78% | 97.7% | 604 |
| 7 | `0xd6098d5b1381ea2bdff4466a346e73c41046c9f7` | #33 / Mixed | 73.2 | B | 15962.28 | 31.86% | 97.4% | 309 |
| 8 | `0xb1231a4a2dd02f2276fa3c5e2a2f3436e6bfed23` | #34 / Mixed | 73.5 | B | 34120.04 | 31.57% | 95.8% | 220 |
| 9 | `0x2ac4880624dae9e823874d65e43bfeba97fc61b5` | #67 / Mixed | 72.4 | B | 45952.06 | 12.63% | 95.7% | 156 |
| 10 | `0x04850cbec02007711892cdb4bd09c800f95d6fc1` | #41 / Mixed | 71.0 | B | 2285.12 | 26.05% | 95.5% | 277 |

## Diversified Mixed Alpha

Use this category for broad signal-discovery ingestion where the source style is mixed or less directly interpretable.

| # | Wallet | Source rank/style | Rating | Grade | 30d PnL | 30d ROI | Win rate | Trades |
|---:|---|---|---:|---|---:|---:|---:|---:|
| 1 | `0x3705121529bf40d77e8e7b625120551b151d9af2` | #18 / Mixed | 73.8 | B | 13889.25 | 53.82% | 90.2% | 474 |
| 2 | `0xd5b8f0abc92ec1ac194d4b380f37c2207cdb0cab` | #29 / Mixed | 68.6 | B- | 24148.20 | 41.89% | 46.7% | 213 |
| 3 | `0xf1e1845f0b01a6184b1d25b83ef1aca8fac5d372` | #38 / Mixed | 72.5 | B | 27058.55 | 30.44% | 92.3% | 153 |
| 4 | `0xee621cac3443ea9511e4b6050d92ee2a8b80f93d` | #45 / Mixed | 71.6 | B | 2555.54 | 22.82% | 81.5% | 1052 |
| 5 | `0xa8a2188916d3744569327c4b1e067729f9443182` | #46 / Mixed | 72.6 | B | 9570.19 | 24.08% | 95.0% | 378 |
| 6 | `0x9312e0a61f21affbd5be821f436f8fdf6b6f6663` | #54 / Mixed | 72.5 | B | 68224.45 | 19.86% | 69.3% | 384 |
| 7 | `0xe9c2df5b14c7f476e8a5895d5b0c6b015e87bf2e` | #55 / Mixed | 73.4 | B | 34010.45 | 17.08% | 89.5% | 493 |
| 8 | `0x1223776bf953ee798a53a34109de5e680cf58180` | #62 / Mixed | 72.5 | B | 82831.34 | 17.01% | 88.6% | 116 |
| 9 | `0x2bdf1a698d39a4358e2c162deacc038fadb8a9d5` | #65 / Mixed | 72.1 | B | 39559.97 | 13.19% | 60.4% | 923 |
| 10 | `0xa30ecf14316316f355b33eb6f5fb4f6e8ed8be51` | #68 / Mixed | 71.8 | B | 99655.40 | 14.65% | 72.7% | 164 |

## Recommended Use

Initial active monitoring should start with:

- all 10 Core Composite Leaders
- top 5 Trend Followers
- top 5 Swing and Capacity Traders
- top 5 Momentum and Short-Cycle Candidates
- top 5 High-Consistency Executors

That gives a 30-wallet active set with enough behavioral diversity. Keep the remaining 30 categorized wallets as warm standby and keep the rest of the 100-wallet seed list as candidates until ingestion verifies equity, drawdown, leverage, profit factor, and stale-state filters.

Do not promote any wallet to trading influence solely from this document. Promotion requires:

- successful `clearinghouseState` or `webData2` read
- successful `userFills` or `userFillsByTime` backfill
- successful profile snapshot
- accepted filter decision
- no single-wallet pressure above the platform cap
