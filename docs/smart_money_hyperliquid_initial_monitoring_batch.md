# Smart Money Hyperliquid Initial Monitoring Batch

Companion document for [smart_money_alpha_platform_development.md](smart_money_alpha_platform_development.md).

Generated: 2026-06-04

## Scope

This is the first bootstrap monitoring universe for Phase 2, "Hyperliquid Wallet Ingestion", in the smart money alpha platform plan. It provides 100 public wallet addresses and 100 public vault addresses for read-only ingestion, checkpointing, and later Phase 3 filtering/scoring.

These addresses are candidates, not accepted smart-money signals. The platform filters in the development plan still apply:

- minimum wallet equity: 5000 USDT
- minimum closed trades: 50
- minimum history: 45 calendar days
- minimum active days: 15
- maximum leverage: 15x unless explicitly capped as high-risk
- maximum drawdown: 40%
- minimum profit factor: 1.10
- stale state rejection at more than 2 ingestion intervals

Wallet categorization and preliminary ratings are maintained in [smart_money_hyperliquid_wallet_ratings.md](smart_money_hyperliquid_wallet_ratings.md). The next active-wallet and whale expansion rule is maintained in [smart_money_hyperliquid_active_whale_monitoring_strategy.md](smart_money_hyperliquid_active_whale_monitoring_strategy.md). Vault monitoring candidates are maintained in [smart_money_hyperliquid_vault_monitoring_candidates.md](smart_money_hyperliquid_vault_monitoring_candidates.md).

## Source Notes

- Wallet candidates come from the public FreedomCore ARENA Hyperliquid leaderboard, which states it ranks 412 profiled wallets and shows the top 100 public addresses. Source: `https://arena.freedomcore.io/`.
- Vault candidates come from `https://stats-data.hyperliquid.xyz/Mainnet/vaults`, sorted by open status and TVL descending. The official `vaultSummaries` info endpoint returned an empty array during collection. The stats-data route is documented by Trading Strategy's Hyperliquid vault helper as the practical vault listing endpoint used by the web UI.
- Hyperliquid's official docs state user data requests must use the actual master/subaccount address, not an agent wallet address, for info requests.
- Use only public read-only endpoints for these addresses. Do not attempt de-anonymization, private credential access, phishing, social targeting, or off-platform identity mapping.

## Wallet Batch

Selection: top 100 public leaderboard rows, preserving source rank order.

| Rank | Wallet address | Tier | 30d PnL USD | 30d ROI | Trades | Style |
|---:|---|---:|---:|---:|---:|---|
| 1 | `0x632c9d55ebcca152488018b3cf80472216d1f4cc` | T3 | 19298.75 | 591.71% | 339 | Mixed |
| 2 | `0x963deb9191d6a7dc1fb3891d472aa358b3e4b140` | T3 | 41002.67 | 328.10% | 181 | Mixed |
| 3 | `0xc850d1f1639458b47b5ce9482a73d11d95f4d89b` | T3 | 223788.42 | 190.05% | 203 | Momentum |
| 4 | `0x66466428990e0f42a4c54f64bee0db6bf2336de5` | T2 | 2719383.58 | 150.27% | 770 | Swing |
| 5 | `0xd60e149a795e83ff52639e3ba708521ee708d74d` | T3 | 40820.32 | 115.67% | 400 | Mixed |
| 6 | `0x8be0b9c4dbfb90f47fb94e8968dce08d3152326c` | T1 | 71574.17 | 111.83% | 877 | Swing |
| 7 | `0xaf2c1206b22629915ff7a0fc98d171a1ef398a3f` | T2 | 118480.22 | 99.72% | 820 | Swing |
| 8 | `0x41829013ce94b21131b68d5a7b0247bd4eefce96` | T2 | 31154.21 | 90.41% | 568 | Swing |
| 9 | `0x05c47beed53eb8b3fc14538f881d0941cbbe05a1` | T3 | 53983.73 | 78.68% | 223 | Mixed |
| 10 | `0x085d785f5a03a6ed9b454830721cfca951c37555` | T1 | 508.97 | 81.32% | 249 | Momentum |
| 11 | `0xda830d2d83a57cea255bcfd0cf89c3e94abde0fd` | T1 | 34804.12 | 75.20% | 450 | Trend |
| 12 | `0xe1f6b0765f27b7db17385a269069ece39f41631d` | T1 | 42693.10 | 71.09% | 384 | Trend |
| 13 | `0xebd7c4b3677a4480fb5f69ac2f51c51d66a19990` | T1 | 639280.95 | 73.88% | 268 | Mixed |
| 14 | `0xe183ef47829ae1eb7234e478e418cd9fa0a511dd` | T2 | 186102.82 | 61.10% | 766 | Swing |
| 15 | `0x844dc38bfa4882eec8c5b85dceb08fc66a7a4fbe` | T1 | 55170.80 | 63.84% | 185 | Trend |
| 16 | `0xaede390f5b5b7cf77428030ccfc73d99a44e1602` | T1 | 153105.99 | 57.79% | 424 | Trend |
| 17 | `0xf08208e088f89970068dc0b2deaa440f6a7d0250` | T1 | 171166.70 | 60.04% | 148 | Trend |
| 18 | `0x3705121529bf40d77e8e7b625120551b151d9af2` | T1 | 13889.25 | 53.82% | 474 | Mixed |
| 19 | `0x05df8c46c75a06ee097c0a90f9fbe8a11a4eba39` | T1 | 42952.01 | 55.71% | 375 | Trend |
| 20 | `0x0696a951a96bef98d3c593c3ae772b70ee4c26e5` | T2 | 78099.93 | 50.98% | 306 | Swing |
| 21 | `0xcb3ee3f1b22cdff8b6ca4839cd0784d34b85dea9` | T2 | 121315.20 | 51.46% | 774 | Mixed |
| 22 | `0x36e6dab460411e402aed071915a21f5c2ff9f223` | T1 | 57352.24 | 49.97% | 277 | Trend |
| 23 | `0xc646851ce5d79117d5086a5aaf5c81922f94efd9` | T2 | 271291.16 | 45.11% | 767 | Swing |
| 24 | `0x4f9140107e42982d3f86eae7474f56bc373ade31` | T1 | 72464.04 | 48.62% | 449 | Trend |
| 25 | `0xdee7297778b8755343e96e0b99eecf549195b856` | T2 | 83743.83 | 47.57% | 822 | Mixed |
| 26 | `0xa8616487ec2605c79c3ad3a3e6bcbe9d059bc774` | T2 | 107684.36 | 45.66% | 104 | Swing |
| 27 | `0xb87e055a70d4a82e4ba93dd5f86c5082f3d25c72` | T3 | 2380.06 | 45.71% | 87 | Mixed |
| 28 | `0x81a745df888c89893c1b2a359b835898d8a6f128` | T1 | 54670.73 | 45.88% | 67 | Trend |
| 29 | `0xd5b8f0abc92ec1ac194d4b380f37c2207cdb0cab` | T3 | 24148.20 | 41.89% | 213 | Mixed |
| 30 | `0x8e6c25f50d4be1e649e61b331a804d11b07a5c08` | T1 | 78469.69 | 32.32% | 322 | Trend |
| 31 | `0xf09c63a2b47d7b383ecc517a295727312384eaed` | T1 | 197495.53 | 34.92% | 159 | Trend |
| 32 | `0x2d99fe0f36c1aebd28a1a2c0e82e8ca13c2ea351` | T1 | 2041707.35 | 29.13% | 865 | Swing |
| 33 | `0xd6098d5b1381ea2bdff4466a346e73c41046c9f7` | T3 | 15962.28 | 31.86% | 309 | Mixed |
| 34 | `0xb1231a4a2dd02f2276fa3c5e2a2f3436e6bfed23` | T3 | 34120.04 | 31.57% | 220 | Mixed |
| 35 | `0x149d35ed0e25ec4b760c28055f744fdd02af1764` | T1 | 37549.25 | 28.71% | 593 | Trend |
| 36 | `0x8dc44ec285114429a7e65eb2e3fdbde945351d5f` | T1 | 19520.74 | 32.77% | 419 | Trend |
| 37 | `0x3eb52cbf0cd6546ed1afe361dfbe902710a80145` | T1 | 30449.93 | 30.05% | 184 | Momentum |
| 38 | `0xf1e1845f0b01a6184b1d25b83ef1aca8fac5d372` | T3 | 27058.55 | 30.44% | 153 | Mixed |
| 39 | `0x2640cff4c41e1073051048db9338da5c0a7ca826` | T3 | 37456.04 | 25.78% | 604 | Mixed |
| 40 | `0xc7f3611b6a7566961f3abc8db6fe4700d6b8a380` | T1 | 45309.21 | 26.64% | 361 | Trend |
| 41 | `0x04850cbec02007711892cdb4bd09c800f95d6fc1` | T3 | 2285.12 | 26.05% | 277 | Mixed |
| 42 | `0x3662dd1d493d1d3cf6a65cb8e4ce2bfb01f63b3c` | T1 | 308234.32 | 23.94% | 407 | Trend |
| 43 | `0xa8cbf4200595efcd94b7526d04deafe0f284af2d` | T3 | 162251.37 | 21.06% | 1152 | Momentum |
| 44 | `0x9a0825ca6c4c577a1202a8fae3b8f044c1d5711d` | T1 | 466356.44 | 26.04% | 1013 | Swing |
| 45 | `0xee621cac3443ea9511e4b6050d92ee2a8b80f93d` | T1 | 2555.54 | 22.82% | 1052 | Mixed |
| 46 | `0xa8a2188916d3744569327c4b1e067729f9443182` | T3 | 9570.19 | 24.08% | 378 | Mixed |
| 47 | `0x88a0511a229643ae6e4ef263a08297343e11bf63` | T3 | 78302.84 | 24.50% | 251 | Mixed |
| 48 | `0x8184e8a5cfac0a0783e389b550f84a0a8bd1b567` | T3 | 114987.24 | 24.13% | 1051 | Momentum |
| 49 | `0x6cc021e2f5e8f12d65c860df5c63f152c4e763e1` | T1 | 67803.95 | 21.75% | 548 | Momentum |
| 50 | `0xcbcedcfc78cd984e2733ba6818c831d2544c2cd6` | T2 | 24286.87 | 22.72% | 473 | Swing |
| 51 | `0x807911ee05ff443d84667e83a771a313e8c73951` | T2 | 20113.51 | 23.05% | 358 | Swing |
| 52 | `0xcdfcfd848d6539aa75263adba82884c58bdc2aaf` | T1 | 47463.87 | 23.26% | 176 | Trend |
| 53 | `0x3f3192097db8fc6043e459db7926a4d8a26c155b` | T1 | 104536.20 | 21.90% | 404 | Swing |
| 54 | `0x9312e0a61f21affbd5be821f436f8fdf6b6f6663` | T1 | 68224.45 | 19.86% | 384 | Mixed |
| 55 | `0xe9c2df5b14c7f476e8a5895d5b0c6b015e87bf2e` | T3 | 34010.45 | 17.08% | 493 | Mixed |
| 56 | `0xa2ecada0b45ad16625865843c9fc5e7c985a2e42` | T1 | 325362.17 | 18.28% | 318 | Trend |
| 57 | `0x6f713fd42f173b163a9fbbcdbe6b26c7dbd6b2a2` | T1 | 47952.41 | 17.79% | 271 | Trend |
| 58 | `0x854d7b77b762b2ca07b1ccc21e2a19eb1ccc34c7` | T3 | 50409.23 | 16.75% | 182 | Mixed |
| 59 | `0xb36c7e6e58e38ad608748e6a00bda29dbeaec96a` | T1 | 33806.24 | 19.40% | 175 | Trend |
| 60 | `0x82786b1a0a7a5e31f17f2ecff68b00df645ac0a4` | T3 | 20695.45 | 18.56% | 64 | Mixed |
| 61 | `0xbb088e9852e18b1df8890d8d0f48bdcec0ab0964` | T3 | 10808.59 | 12.41% | 618 | Mixed |
| 62 | `0x1223776bf953ee798a53a34109de5e680cf58180` | T1 | 82831.34 | 17.01% | 116 | Mixed |
| 63 | `0xccf6a4da71795428e13a0d1330293c735b66ec07` | T1 | 13555.41 | 15.13% | 206 | Swing |
| 64 | `0xb5101614ad71468a041a83e64d8b834aa17a1ed6` | T3 | 255266.80 | 17.45% | 388 | Momentum |
| 65 | `0x2bdf1a698d39a4358e2c162deacc038fadb8a9d5` | T2 | 39559.97 | 13.19% | 923 | Mixed |
| 66 | `0xf191539a2c4ba0af951438bb6abfa0625c7df2ef` | T1 | 263523.27 | 16.37% | 333 | Trend |
| 67 | `0x2ac4880624dae9e823874d65e43bfeba97fc61b5` | T3 | 45952.06 | 12.63% | 156 | Mixed |
| 68 | `0xa30ecf14316316f355b33eb6f5fb4f6e8ed8be51` | T1 | 99655.40 | 14.65% | 164 | Mixed |
| 69 | `0x1b85f29401e3c772866705ad8d6c2758ae9aea44` | T3 | 16406.96 | 14.31% | 97 | Mixed |
| 70 | `0x69420c67545e5ee95d1375f93ec0da06c05c9c1f` | T3 | 24415.16 | 14.33% | 135 | Mixed |
| 71 | `0x1e7a96f42551e5baedd648eeba181627b8248a26` | T1 | 0.00 | 0.00% | 2000 | Mixed |
| 72 | `0x5c1d4ab7876247571ab2015b8371f0a98b4e2a9e` | T3 | 0.00 | 0.00% | 1444 | Mixed |
| 73 | `0xfb2986b5d3e5604a90396e83a1fedebd768c21dd` | T1 | 56637.65 | 7.60% | 338 | Mixed |
| 74 | `0x68191e792269d8d835ed033b55f09bb3762a73ec` | T3 | 0.00 | 0.00% | 1708 | Mixed |
| 75 | `0x1b44a8777370272118e5412b37085813cf93e6e6` | T3 | 0.00 | 0.00% | 2000 | Mixed |
| 76 | `0x24f45ed27bc6b1b474dd5aff0776c70ed63c2212` | T3 | 0.00 | 0.00% | 1013 | Mixed |
| 77 | `0xf3966357035e1c23c33636c9d3c577ed864f3b94` | T1 | 0.00 | 0.00% | 1538 | Mixed |
| 78 | `0xa49041de8ee64de3897d9c480f143e76129a9f40` | T3 | 0.00 | 0.00% | 1180 | Mixed |
| 79 | `0x6d628f15aff947f42a0d2d0fdfb63085379f7e64` | T1 | 0.00 | 0.00% | 1605 | Mixed |
| 80 | `0x844698b056d4e65ef1c1fb4d43305d5d11f4ba1b` | T1 | 0.00 | 0.00% | 785 | Mixed |
| 81 | `0xac142fee46f8dacbfef23e1e41d79f1f7233487e` | T3 | 0.00 | 0.00% | 2000 | Mixed |
| 82 | `0x333e6d083123ab16be168d0e8e88588ae72baa45` | T3 | 0.00 | 0.00% | 794 | Mixed |
| 83 | `0x7300fed8d27ebabd6a778a231c4fa1d1b6586465` | T1 | 0.00 | 0.00% | 617 | Mixed |
| 84 | `0xe9c312d4a2e69e4247a1e2ba12dedd098010a419` | T1 | 0.00 | 0.00% | 530 | Mixed |
| 85 | `0x8658ccc0732959a37f303173e29180273b3faf88` | T2 | 1619.50 | 0.00% | 647 | Mixed |
| 86 | `0x107b36e01ddcca4abc9cc085a8dae2a33f2a7979` | T1 | 0.00 | 0.00% | 589 | Mixed |
| 87 | `0x52a1ac3717f8173bd175a4c1923594ed6110142d` | T3 | 0.00 | 0.00% | 2000 | Mixed |
| 88 | `0x3b560121cbbc6169491651e60c7866ec33ad1946` | T3 | 0.00 | 0.00% | 2000 | Mixed |
| 89 | `0x562ee97d1256d99e0d2dce9b0158ecc69fefa921` | T3 | 0.00 | 0.00% | 545 | Mixed |
| 90 | `0x5beae772444a36572bc7e26040988edc88e7a02f` | T3 | 0.00 | 0.00% | 1053 | Mixed |
| 91 | `0xc2df55cd5f320226253ccd8ce0c054bde0e074d1` | T1 | 0.00 | 0.00% | 469 | Mixed |
| 92 | `0xecc0e46f64458ae70dca30e298d20e3c993d9541` | T3 | 0.00 | 0.00% | 1168 | Mixed |
| 93 | `0x000000209e0a892842bc06c354e7d097392cbbbb` | T3 | 0.00 | 0.00% | 413 | Mixed |
| 94 | `0x149d4a9839f242ceb9c8c455b664ef001cd2789c` | T3 | 0.00 | 0.00% | 2000 | Mixed |
| 95 | `0xc894fc3cda30bcc48bd581c84e8084a91c3d7ab5` | T3 | 0.00 | 0.00% | 1028 | Mixed |
| 96 | `0x368ff9a3ba3f83211017057f5c7fcea0c9659019` | T3 | 0.00 | 0.00% | 414 | Mixed |
| 97 | `0x9fd7be134d8d4ddb8fa8d093e348c899716f6b0b` | T2 | 2944.53 | 0.00% | 922 | Mixed |
| 98 | `0x2d822349c02cb46131ff8bb9b887ddb0dbf1e897` | T3 | 0.00 | 0.00% | 905 | Mixed |
| 99 | `0x5a34a5b204f254fed85ae8728f8c38dd2f80e6f5` | T3 | 0.00 | 0.00% | 364 | Mixed |
| 100 | `0x0907a4097144692abfc08bf530f2e1bcb825bfc2` | T2 | 0.00 | 0.00% | 427 | Mixed |

## Vault Batch

Selection: open vaults from stats-data, sorted by TVL descending. `Leader address` is included because vault-level ingestion should capture both vault and manager relationships.

| Rank | Vault address | Leader address | TVL USD | APR |
|---:|---|---|---:|---:|
| 1 | `0xdfc24b077bc1425ad1dea75bcb6f8158e10df303` | `0x677d831aef5328190852e24f13c46cac05f984e7` | 349431923.36 | 1.41% |
| 2 | `0xb0a55f13d22f66e6d495ac98113841b2326e9540` | `0xdfc24b077bc1425ad1dea75bcb6f8158e10df303` | 30000000 | 0% |
| 3 | `0xd6e56265890b76413d1d527eb9b75e334c0c5b42` | `0x2b804617c6f63c040377e95bb276811747006f4b` | 14484551.83 | 329.15% |
| 4 | `0x1e37a337ed460039d1b15bd3bc489de789768d5e` | `0x7789450871fb1315fa982ccb8039cb34e8f2f60d` | 8648385.21 | -1.79% |
| 5 | `0x07fd993f0fa3a185f7207adccd29f7a87404689d` | `0x2b804617c6f63c040377e95bb276811747006f4b` | 5797571.27 | -8.67% |
| 6 | `0xc179e03922afe8fa9533d3f896338b9fb87ce0c8` | `0xf4f7cebbd2c7b6dee34ab29fa55a116eff25239f` | 4645424.53 | -12.81% |
| 7 | `0xac26cf5f3c46b5e102048c65b977d2551b72a9c7` | `0xa380444a9299df2a15bddf949dd493bb6d30d939` | 3366497.9 | 834.08% |
| 8 | `0x45e7014f092c5f9c39482caec131346f13ac5e73` | `0x8d3f2ecf66c4752fcb6800d892beead654f6c056` | 3140140.13 | -11.5% |
| 9 | `0x115849ce84370f25cadcf0d348510d73837e1aa5` | `0xf292b42e6167e0d591449ebd67d2e989a3479edf` | 3086240.1 | -1.41% |
| 10 | `0x010461c14e146ac35fe42271bdc1134ee31c703a` | `0xdfc24b077bc1425ad1dea75bcb6f8158e10df303` | 3031833.33 | -0.25% |
| 11 | `0x31ca8395cf837de08b24da3f660e77761dfb974b` | `0xdfc24b077bc1425ad1dea75bcb6f8158e10df303` | 3016391.92 | 4.21% |
| 12 | `0xb1505ad1a4c7755e0eb236aa2f4327bfc3474768` | `0x1fa1b4c4cda61b3c1ce805ae82e64a90d8821d08` | 2754985.34 | -12.06% |
| 13 | `0x654016a8c9fcf0c4cb7ed6078aba21f7f399f7b7` | `0xf5f569a7006860e6e17c92ba9d5959090acf0336` | 2375543.2 | 214.21% |
| 14 | `0xa6a34f0bf2ccea9a1ddf9e9a973f17c498dc5e40` | `0x3d32e286bb737ba348e28823863f02308768cfec` | 2175648.45 | -0.76% |
| 15 | `0x8231fdf9997c003a267374b45fb25c0455aa1dcb` | `0xcbdf15e12fc3b8e8fc8bacf577c4d1071cf2b4b3` | 1906834.49 | -9.14% |
| 16 | `0x27d33e77c8e6335089f56e399bf706ae9ad402b9` | `0x97ee7d16c1aebdfbfa7a9f983ac5a0ec56e63b2e` | 1717442.53 | 64.28% |
| 17 | `0xca230e816bdb34a46960c2f978a30a563d1ae9e0` | `0x76184c92c5e17122a496fe1799adc77f4139c96e` | 1618297 | 200.25% |
| 18 | `0x49a648936441b22f28f069d3c088928682b277ae` | `0x970ed103da353023dc6726c04f183b38ce35943c` | 1133373.5 | -12.64% |
| 19 | `0x060d01aa996003b3731a992462d7f0ba68bf3b04` | `0x42623454883485fc818082df6187f60ec980af8c` | 1132801.94 | 299.32% |
| 20 | `0x7048b287889c5913d59f812795d7fd5d724be77a` | `0x3db735d71673b3d57b34f55013b9b25e2d2dd9cf` | 1078269.56 | 102.67% |
| 21 | `0x7178cc2d79160d524858c843e350ed8207de0854` | `0x83046ec02c58df0448e0f4219413c0a16e6d42b9` | 1047760.94 | 279.09% |
| 22 | `0x469f690213c467c39a23efacfd2816896009d7d8` | `0xdfc24b077bc1425ad1dea75bcb6f8158e10df303` | 1004941.27 | 1.55% |
| 23 | `0x5e177e5e39c0f4e421f5865a6d8beed8d921cb70` | `0xdfc24b077bc1425ad1dea75bcb6f8158e10df303` | 1000000 | 0% |
| 24 | `0x2ed5c4484ea3ff8b57d5f2fb152a40d9f2b68308` | `0xdfc24b077bc1425ad1dea75bcb6f8158e10df303` | 1000000 | 0.78% |
| 25 | `0x2e3d94f0562703b25c83308a05046ddaf9a8dd14` | `0xdfc24b077bc1425ad1dea75bcb6f8158e10df303` | 1000000 | 147.65% |
| 26 | `0x1840bdb83caff17de910ec407cafb817678786b5` | `0xea3695dbbacc903f0a3616d49a05dc9b829309f9` | 965442.33 | 56.87% |
| 27 | `0x780825f3f0ad6799e304fb843387934c1fa06e70` | `0x4692441b5a9e26a690eea6d2f36139679add737b` | 931978.13 | -3.68% |
| 28 | `0x73ce82fb75868af2a687e9889fcf058dd1cf8ce9` | `0xe2823659be02e0f48a4660e4da008b5e1abfdf29` | 762310.56 | -17.52% |
| 29 | `0xda51323fe9800c8365646ad5c7ade0dd17fdc167` | `0x92d3acdf0484a6a8baf6fe3676b23af7cdbdbc98` | 705776.46 | 15.37% |
| 30 | `0x3005fade4c0df5e1cd187d7062da359416f0eb8e` | `0x43c63ea7775a4f7ee93fcdd096f4ff92a9fe4054` | 689327.79 | 6.88% |
| 31 | `0x5a733b25a17dc0f26b862ca9e32b439801b1a8c7` | `0x0a0b4d654d967a00407f5329588a258b68a4f615` | 664925.74 | -1.12% |
| 32 | `0x253b88ebaa7c321d4803c0cfaf708684e3de32be` | `0x55086f715ca142b34db9af614611ebd853224582` | 530364.59 | 507.66% |
| 33 | `0x65aee08c9235025355ac6c5ad020fb167ecef4fe` | `0xf230daae38627c46367334a7494dc37861b4d3ef` | 522778.81 | 163.51% |
| 34 | `0x4dec0a851849056e259128464ef28ce78afa27f6` | `0x526178b96c8c5d7a4e4f76f298ec0170aa0d1d2a` | 505139.75 | -30.64% |
| 35 | `0xbbf7d7a9d0eaeab4115f022a6863450296112422` | `0xaf69b1587b87c78409e5a20c3fd5f1ca386fd350` | 490896.49 | -1.4% |
| 36 | `0x5661a070eb13c7c55ac3210b2447d4bea426cbf5` | `0xc6fd7c7bc701c802cf2e75fa19cacd5e4c55af9c` | 454437.36 | 75.5% |
| 37 | `0xe67dbf2d051106b42104c1a6631af5e5a458b682` | `0xdeb7582b362a752970fbd2507d0fb5dd27ed379a` | 440442.72 | 741.96% |
| 38 | `0x61b1cf5c2d7c4bf6d5db14f36651b2242e7cba0a` | `0xdaffbc69a0be655469257b43e1ceffb5eab920c0` | 417002.45 | -13.46% |
| 39 | `0x9e02aca9865e1859bb7865f6f64801e804a173df` | `0x36752718b4f769a6cfd6f552818f5911c8b649da` | 378613.91 | 11.65% |
| 40 | `0x36c88d0fbe990a6e5042b378a860909a05749f22` | `0x71038a3e5fd46ba4ca5edc81339a4d1e9e493104` | 373834.8 | -6.87% |
| 41 | `0x9a28b01435ca5c4b39a79ae2c27fce01292a7f39` | `0xceec9c279d4f79e1b43cb23014e17a8c34e6793a` | 370201.63 | 9.6% |
| 42 | `0x9114a5161f18739ced233190861ea275c8b3a99b` | `0x3b290def8e0b287a2c8ac0ae1ab31642caecc7e7` | 368436.5 | 54.73% |
| 43 | `0xbc5bf88fd012612ba92c5bd96e183955801b7fdc` | `0x10cd85d0257e666fa74de7c03965a6ec9383e171` | 363866.67 | -9.17% |
| 44 | `0x565ab99e1515c6dd3b8852b15354638d467f999a` | `0xec293152bd9b692eb8849619e4362dbfdfd2815e` | 359048.92 | -15.87% |
| 45 | `0xae6b807ec47e0b7900d1fe2a53885b4602748b03` | `0x1d6dcb838d0edc5d7bf09e5c658adbfb19a7234b` | 343856.49 | 159.06% |
| 46 | `0x0399976a0e0c504da1c07fedb91c30edfd7e4395` | `0xbd9c944dcfb31cd24c81ebf1c974d950f44e42b8` | 326808.72 | -27.82% |
| 47 | `0x8fc7c0442e582bca195978c5a4fdec2e7c5bb0f7` | `0xf5d0308ac17a33e666326df43788afaa4100b9eb` | 311463.65 | -7.98% |
| 48 | `0xa844d7ac9fa3424c4fd38a25baa23e460ec3e802` | `0x84803dc3df988d5493d9be2ee75e36f0043ee272` | 267162.8 | 43.59% |
| 49 | `0xbeebbbe817a69d60dd62e0a942032bc5414dae1c` | `0x9660f151ffe595afc65e64d38a55285ed8fd2021` | 246145.51 | -5.32% |
| 50 | `0x5048900eb10b569e77f515efe85f8da5cfd5fb3a` | `0xb83652316ecd7ae14202c18320df68a4575efea7` | 243614.74 | 28.09% |
| 51 | `0x8fafc23611d59310b957162b8479f354c5a0eb4d` | `0x67ecb94f32c61ae197fa6915276c723a89bf3ac3` | 203971.14 | -6.89% |
| 52 | `0xba939edf38c0ae0cc689c98b492e0535f43e4550` | `0xa7fd61557cd0a61073566766839d5f851a8e92ef` | 199755.56 | 45.75% |
| 53 | `0xfb7b73ff7c93f5552541de37454ffa0f8b76462a` | `0x1f1690074a8744e8d38cdca1ab2b75fafe5d6d07` | 175388.16 | 719.77% |
| 54 | `0x65c6b7f0e9a6f72b351ca133e510a2329d047dbe` | `0x8382655426c1fe72709a7ad028b1547130ed8007` | 171294.32 | -41.49% |
| 55 | `0xa1b6d8efbcb2fb750a84dbc05649fa4968034f04` | `0xb5e32ad7740f86346503352c53aabcb154cfa3f9` | 169430.95 | -1.27% |
| 56 | `0x77fee2df7bad4f1db93052fa82bf78eaab771a16` | `0x288ed4efc8fbd1e42a06fe083ea942d20c90b336` | 167510.84 | -6.23% |
| 57 | `0x3b4f22366857da94f9346f88eac84718c8a8d48d` | `0x20a510e57a5d17fd0a07d4d7435db2854ea5ced0` | 164302.75 | 1.88% |
| 58 | `0x8c7bd04cf8d00d68ce8bc7d2f3f02f98d16a5ab0` | `0xe2422bca1570e1b1c352164b0b41dae434035f6c` | 162456.97 | -25.45% |
| 59 | `0x7d97f7cb48ecc966d75a746b811ca93f1fd6e7ef` | `0x11eaed910eefa22056ef5d2aefb0d632cc0f6906` | 157773.62 | 153.75% |
| 60 | `0xd57c9295947b5a616a3933344ef03a1ad67318ea` | `0xffc8abcd5170b6c106695c8a4a795596b3015542` | 157696.58 | 14.99% |
| 61 | `0x797327122c5ed1b1530e452b7f8723ba834b4c6a` | `0xbee6b4106fc23d7a39101d7d684ebe26862e56fe` | 156234.38 | -0.1% |
| 62 | `0xb7e7d0fdeff5473ed6ef8d3a762d096a040dbb18` | `0x3b290def8e0b287a2c8ac0ae1ab31642caecc7e7` | 142582.32 | 94.37% |
| 63 | `0x503b30b2eff8d62f07ab9fe5f1fb0e6a18e86bc1` | `0xc0a2f3b3990de10a22a00377df0f3e7503c3a14d` | 138285.03 | 3091.11% |
| 64 | `0x5108cd0a328ed28c277f958761fe1cda60c21aa8` | `0x6a4590697be32f65757d1fd480716a73961404fd` | 137167.68 | -2.69% |
| 65 | `0x51b62b4bf8df6f2795b3da30cb46aa47f9f230a8` | `0x8c8290ebf5d157b236948b9f66d09d426503f198` | 133733.07 | 54.15% |
| 66 | `0xd914c5164bc253676386269d90dcf56b441cf75b` | `0x8f300feb1180c149b88e9a2473fc38e0d2d75db3` | 126017.14 | -0% |
| 67 | `0x15a141990fc6591838646467273c41c92999772f` | `0x8803d442d3b15908f9838924a2b196e32cd6ae8e` | 123626.4 | 324.61% |
| 68 | `0xa7f152a5f79bb5483c079610203d8fc03fd77c8e` | `0x49c5bc60d1748296be96d5a2e9ad161bf586812e` | 123265.14 | -32.9% |
| 69 | `0x8825e6ed11c7f9c0874579c9f4bba192b9e81c8f` | `0x0483797d914cf3a104b16a4c277c8f43fe484008` | 122682.41 | -1.68% |
| 70 | `0x15be61aef0ea4e4dc93c79b668f26b3f1be75a66` | `0x98bb9ef6b6bf918ce301be1e3fa14407b1d949a2` | 121465.1 | -3.28% |
| 71 | `0xb65dd7c56afbf3b272ab5fc49be44b47dca18003` | `0x92f33bee92dd86db292e3bc8500ffc5619d50430` | 121389.99 | 13.43% |
| 72 | `0x33821578405bd53f0eed2895496922b528d95e21` | `0x4c9132a369ca40cd6dadaf7c6dd3f67590b60af5` | 121229.78 | -0% |
| 73 | `0xeaee3e55f8dde6779922166e1502887cebbb5f57` | `0xf4150f64418eac01d7e7172b3259618194fc7268` | 116867.42 | -9.32% |
| 74 | `0x9b5b1931fa5838a891973f9877c7648c93ee80cc` | `0x7789450871fb1315fa982ccb8039cb34e8f2f60d` | 111568.39 | -10% |
| 75 | `0x82eba5dc675279cb5967952f0c4b5184505eb17c` | `0xe45e4fc1dbd8ab1c554c8a2bd7fa752d1e53bb35` | 109361.63 | -14.15% |
| 76 | `0x36dc8d4188c57008b7ddbc21662ef28a88a9c684` | `0x349bd094209d50280dfaca12779576c748395c15` | 106635.96 | 466.02% |
| 77 | `0x9a88dd4eacc3aa561c4f50ec66bcede831bec813` | `0x228ed7bff391e92ba3ddcbcda8db7a0134ef2587` | 106469.86 | 842.22% |
| 78 | `0x026a2e082a03200a00a97974b7bf7753ce33540f` | `0x3b290def8e0b287a2c8ac0ae1ab31642caecc7e7` | 102951.28 | 83.92% |
| 79 | `0x22a60014185f1486afb045219f76e4007fb71e4e` | `0x12b37ace7d1145cdc367347c9f8470eaeb35bdf7` | 101588.03 | 65.15% |
| 80 | `0xd2f03635901956b950737bbf02463dfad9f2e9e1` | `0x146d21473dbd19fa18bd1b41aa4712ea2cd8a1de` | 98583.99 | 2.02% |
| 81 | `0x8bb033130be354eed1110ce7228d7095f001d9fe` | `0xc6a06131c84c0c9d2d4c293c349be0c6d5112893` | 92595.59 | 10.63% |
| 82 | `0xa0cbceaac4dc736c457b7c340865695e2b3d0fc9` | `0x5dd032fdd7811e98f2d94bd1991f6d730716119e` | 87953.17 | -0.04% |
| 83 | `0x2431edfcb662e6ff6deab113cc91878a0b53fb0f` | `0x90b88a8df1ad60f87b30032482b9907b89637c96` | 87055.62 | 365.13% |
| 84 | `0xb11fe7f2e97bd02b2da909b32f4a5e7fcb0df099` | `0xa8d2e2a2d33d6159c1c50c078b3dbac55851ca9a` | 86987.38 | -45.22% |
| 85 | `0xa45ee34298f8e5596fef0870a42f80182de0cfb9` | `0x036ce31a25f70256ab2135de05c9dd1f8c557876` | 84505.26 | 4678.08% |
| 86 | `0x0e008684ae576f280c5426a89d3f5e1da1fc7398` | `0x190c5972c541afdcbc43616c091abd18cde8c1ef` | 84488.37 | 25.93% |
| 87 | `0x29b98aaf8eeb316385fe2ed1af564bdc4b03ffd6` | `0x3e502cbaf8a3578f06b295b94993caaee5923ffb` | 82112.15 | -34.2% |
| 88 | `0xd56c2517f2c2fb040cd20259490c7511493d4ed5` | `0x39db0698edb5a7c2c34cfc9a36619036d495725e` | 81012.39 | 23.9% |
| 89 | `0xfeab64de8cdf9dcebc0f49812499e396273efc06` | `0x10cd85d0257e666fa74de7c03965a6ec9383e171` | 78543.99 | 102.36% |
| 90 | `0xc497f1f8840dd65affbab1a610b6e558844743d4` | `0x0635aedc940be2f3961a6b98df118d26e1d2095b` | 77515.42 | -0% |
| 91 | `0x14ecb3f4932522b8d508fa6e237e2746a4999feb` | `0xb6a866e483e5566dfbd7324bc552f11dbed87e7a` | 74997.79 | 43.72% |
| 92 | `0xce56eb8261493462e3eb00a72c2bda2cb5fdccee` | `0x43180050197ccf8037af079f7c823aa0aa9ed2ad` | 74514.23 | -13.14% |
| 93 | `0x44ff912d0f88e27419ec0ddc950096609a9b6997` | `0x45323d50a3e1ca8fdf24df0a323fc9e38fd6b01c` | 73628.98 | -15.24% |
| 94 | `0x50e2fe552727a4b8692c192b4f96d1a6b0d44394` | `0x58206237b3fcfcbb4c9b60317cf652a29089f224` | 72124.03 | 155.85% |
| 95 | `0x239f1ba2dc337485f691e326fca45f8f11f7900a` | `0xbe4e91ae63090eeb4165dfcfc35db3a4eb76e75e` | 71230.16 | 489.38% |
| 96 | `0x697bc3dd77539fa84156d0e1c95287ea5524fd6b` | `0xce741d86bff090a94740f81a80bbb6a335e0271e` | 70571.85 | 312.24% |
| 97 | `0x4078582c42fdb547b1397fabb5d5a4beab81be9e` | `0xe966a12bf7b93838096e4519a684519ab22df618` | 69023.88 | 112.55% |
| 98 | `0x7c5885d2974457eafb1a3a4d848c358111f0714d` | `0x6281f807954f90110c626ba37b183bf9f94fae8d` | 68961.43 | 1017.32% |
| 99 | `0x241ba250828c6a1289330aeec301e7bdcef31a43` | `0xb882b418711686abd88c01395fdc95b189324ef9` | 64814.87 | 63.87% |
| 100 | `0x7cd7ea8a61e48fc48f5a18502c0622e53d159347` | `0xeff8aa80a1756a8f20cb8231a6cd479bffdce203` | 64396.53 | 3.8% |

## Ingestion Plan

For each wallet address:

- `clearinghouseState` or `webData2` for equity and current positions.
- `userFills` and paginated `userFillsByTime` for recent and historical fills.
- `userFunding` for funding payments.
- `userNonFundingLedgerUpdates` where transfer and non-funding ledger events are needed.
- `portfolio` for performance windows.
- `userRole` to classify user, vault, subaccount, agent, or missing.

For each vault address:

- `vaultDetails` for leader, followers, portfolio history, APR, relationship, and deposit flags.
- `userFills` or `userFillsByTime` for vault trade facts if exposed for the vault address.
- `clearinghouseState` for current vault positions.
- `portfolio` for PnL windows.

## Acceptance Gate

The registry should mark every row as `candidate` at first import. Promotion to active monitoring requires a successful profile snapshot and a filter decision. Rows that fail public endpoint reads should remain in the registry as `inactive_unresolved` with the failed endpoint and timestamp.

Suggested metadata:

```yaml
source_plan: docs/smart_money_alpha_platform_development.md
source_batch: docs/smart_money_hyperliquid_initial_monitoring_batch.md
venue: hyperliquid
network: mainnet
candidate_status: candidate
privacy_policy: public_addresses_only_no_deanonymization
```
