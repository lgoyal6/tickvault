# Strategy evaluation

> **What this is.** Local replay of recorded archive data on a single host. Execution is simulated: no order reached a venue, no venue was connected, and no money moved. Queue position is approximate everywhere it appears and every such field is named with an approx_ prefix. Fills come from observed level changes and observed crossings of the recorded book, not from a matching engine. No profitability, alpha, or live trading is claimed.

Manifest `sim/manifest.json`, sha256 `115724584fa605004ee1e22b1b68279c0657f52e0418218dd99692616e08eccf`. Reproduce with `./scripts/run-strategy-eval.sh`.

## Verdict

**Strategy quality: Rejected.**

No profitability and no alpha are claimed. The candidate did not clear the promotion gate that was frozen before any of these numbers existed, every losing baseline is kept below, and what stands is the simulator, the sequence enforcement and the risk system rather than a strategy.

| gate clause | result | detail |
|---|---|---|
| `no_invariant_violations` | pass | 0 held-out window(s) in scope stopped on a replay invariant |
| `no_leakage` | pass | 0 decision(s) used a feature that was not yet visible |
| `risk_controls_pass` | pass | every limit breach observed was answered by a kill-switch activation |
| `beats_no_trade` | pass | candidate 0.0168 against no_trade 0.0000 |
| `beats_fixed_spread_mm` | pass | candidate 0.0168 against fixed_spread_mm -0.5301 |
| `beats_depth_imbalance` | pass | candidate 0.0168 against depth_imbalance -60.9619 |
| `bootstrap_lower_bound_above_zero` | fail | 95 percent interval [-0.4768, 0.5223] |
| `direction_agreement` | pass | 3 of 4 chronological window positions agree in sign |

## Held-out totals per strategy

Summed over the held-out windows of the venues in gate scope. `marked` carries the closing inventory at the mid; `liquidated` takes it out at the touch and pays the taker fee, so it is always the smaller of the two.

| strategy | windows | after-fee PnL (marked) | after-fee PnL (liquidated) | max drawdown | turnover | fills | fill rate | max inventory | kill switch | latency median ms | latency p99 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `depth_imbalance` | 20 | -60.9619 | -64.3933 | 8.1984 | 114247.82 | 328 | 1.0000 | 0.00500 | 0 | 2.453 | 16.621 |
| `fixed_spread_mm` | 20 | -0.5301 | -3.1545 | 0.3628 | 7671.99 | 19 | 0.0084 | 0.01500 | 0 | 3.062 | 19.877 |
| `imbalance_skew_mm` | 20 | 0.0168 | -2.0021 | 0.1633 | 6460.08 | 16 | 0.0137 | 0.01000 | 0 | 2.718 | 16.921 |
| `no_trade` | 20 | 0.0000 | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0 | 2.411 | 11.026 |

Block bootstrap of the candidate's total held-out after-fee PnL: point 0.0168, 95 percent interval [-0.4768, 0.5223], block length 2, 2000 resamples over 20 units. The unit is one held-out (venue, window) pair; a single recording date means the unit is a window rather than a trading day

Direction agreement: 3 chronological held-out window position(s) carry the sign of the total, out of the 4 positions the manifest defines. The gate requires at least 3.

## Rejected orders

| strategy | reason | count |
|---|---|---:|
| `imbalance_skew_mm` | `would_cross_book` | 245 |

## Venues

| venue | symbol | scheme | verifiable | in gate scope | messages | measured tick | median spread (bp) |
|---|---|---|---|---|---:|---:|---:|
| binance-us | BTC-USDT | `range` | true | true | 865 | 0.010000000 | 0.3084 |
| bitstamp | BTC-USD | `timestamp` | false | false | 923 | 0.010000000 | 0.0012 |
| bybit | BTC-USDT | `counter` | true | true | 7928 | 0.100000000 | 0.0124 |
| coinbase | BTC-USD | `counter` | true | true | 5733 | 0.010000000 | 0.0012 |
| kraken | BTC-USD | `checksum` | true | true | 11386 | 0.100000000 | 0.0124 |
| okx | BTC-USDT | `chain` | true | true | 2964 | 0.100000000 | 0.0124 |

A venue whose feed publishes neither a sequence number nor a checksum is reported and kept out of every aggregate: loss on it is undetectable by construction, and unverifiable is not clean.

## Per window

| venue | window | strategy | parameters | after-fee PnL | drawdown | turnover | fills | fill rate | mean inventory | max inventory | rejected | kill switch | messages | truncated |
|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| binance-us | 1 | `depth_imbalance` | depth_k=5 threshold=0.35 horizon_ms=10000 | -2.2578 | 2.2580 | 3634.70 | 12 | 1.0000 | 0.00479 | 0.00500 | 0 | 0 | 142 | no |
| binance-us | 2 | `depth_imbalance` | depth_k=5 threshold=0.35 horizon_ms=10000 | -2.0921 | 2.1275 | 3634.51 | 10 | 1.0000 | 0.00472 | 0.00500 | 0 | 0 | 88 | no |
| binance-us | 3 | `depth_imbalance` | depth_k=5 threshold=0.35 horizon_ms=10000 | -2.1308 | 2.1308 | 3634.05 | 11 | 1.0000 | 0.00324 | 0.00500 | 0 | 0 | 122 | no |
| binance-us | 4 | `depth_imbalance` | depth_k=5 threshold=0.35 horizon_ms=10000 | -3.2009 | 3.2009 | 5248.21 | 16 | 1.0000 | 0.00471 | 0.00500 | 0 | 0 | 190 | no |
| binance-us | 1 | `fixed_spread_mm` | half_spread_bps=1 requote_ms=2000 | -0.0125 | 0.0788 | 807.66 | 2 | 0.0556 | 0.00173 | 0.00500 | 0 | 0 | 142 | no |
| binance-us | 2 | `fixed_spread_mm` | half_spread_bps=1 requote_ms=500 | -0.0755 | 0.1463 | 807.66 | 2 | 0.0250 | 0.00659 | 0.01000 | 0 | 0 | 88 | no |
| binance-us | 3 | `fixed_spread_mm` | half_spread_bps=2.5 requote_ms=500 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 0 | 0 | 122 | no |
| binance-us | 4 | `fixed_spread_mm` | half_spread_bps=1 requote_ms=2000 | -0.3628 | 0.3628 | 1211.11 | 3 | 0.0833 | 0.00955 | 0.01500 | 0 | 0 | 190 | no |
| binance-us | 1 | `imbalance_skew_mm` | half_spread_bps=1 skew_bps=0 inv_skew_bps=0 | -0.0108 | 0.0788 | 807.66 | 2 | 0.0333 | 0.00173 | 0.00500 | 0 | 0 | 142 | no |
| binance-us | 2 | `imbalance_skew_mm` | half_spread_bps=2.5 skew_bps=5 inv_skew_bps=0 | -0.0061 | 0.0415 | 403.80 | 1 | 0.0263 | 0.00153 | 0.00500 | 18 | 0 | 88 | no |
| binance-us | 3 | `imbalance_skew_mm` | half_spread_bps=2.5 skew_bps=5 inv_skew_bps=0 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 12 | 0 | 122 | no |
| binance-us | 4 | `imbalance_skew_mm` | half_spread_bps=2.5 skew_bps=5 inv_skew_bps=0 | 0.0245 | 0.0301 | 807.27 | 2 | 0.0426 | 0.00126 | 0.00500 | 19 | 0 | 190 | no |
| binance-us | 1 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 142 | no |
| binance-us | 2 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 88 | no |
| binance-us | 3 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 122 | no |
| binance-us | 4 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 190 | no |
| bitstamp | 1 | `depth_imbalance` | depth_k=5 threshold=0.35 horizon_ms=10000 | -2.8317 | 2.8317 | 5248.26 | 17 | 1.0000 | 0.00426 | 0.00500 | 0 | 0 | 121 | no |
| bitstamp | 2 | `depth_imbalance` | depth_k=5 threshold=0.35 horizon_ms=10000 | -1.5298 | 1.5298 | 2825.80 | 7 | 1.0000 | 0.00421 | 0.00500 | 0 | 0 | 121 | no |
| bitstamp | 3 | `depth_imbalance` | depth_k=5 threshold=0.35 horizon_ms=10000 | -3.0696 | 3.0696 | 4843.91 | 13 | 1.0000 | 0.00463 | 0.00500 | 0 | 0 | 123 | no |
| bitstamp | 4 | `depth_imbalance` | depth_k=5 threshold=0.35 horizon_ms=10000 | -1.6047 | 1.6115 | 2824.55 | 7 | 1.0000 | 0.00307 | 0.00500 | 0 | 0 | 122 | no |
| bitstamp | 1 | `fixed_spread_mm` | half_spread_bps=2.5 requote_ms=500 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 0 | 0 | 121 | no |
| bitstamp | 2 | `fixed_spread_mm` | half_spread_bps=2.5 requote_ms=500 | 0.1235 | 0.0256 | 807.18 | 2 | 0.0167 | 0.00285 | 0.01000 | 0 | 0 | 121 | no |
| bitstamp | 3 | `fixed_spread_mm` | half_spread_bps=2.5 requote_ms=500 | 0.0730 | 0.0319 | 807.34 | 2 | 0.0192 | 0.00028 | 0.00500 | 0 | 0 | 123 | no |
| bitstamp | 4 | `fixed_spread_mm` | half_spread_bps=2.5 requote_ms=500 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 0 | 0 | 122 | no |
| bitstamp | 1 | `imbalance_skew_mm` | half_spread_bps=2.5 skew_bps=0 inv_skew_bps=0 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 0 | 0 | 121 | no |
| bitstamp | 2 | `imbalance_skew_mm` | half_spread_bps=2.5 skew_bps=0 inv_skew_bps=0 | 0.1235 | 0.0256 | 807.18 | 2 | 0.0323 | 0.00285 | 0.01000 | 0 | 0 | 121 | no |
| bitstamp | 3 | `imbalance_skew_mm` | half_spread_bps=2.5 skew_bps=0 inv_skew_bps=0 | 0.0730 | 0.0319 | 807.34 | 2 | 0.0357 | 0.00028 | 0.00500 | 0 | 0 | 123 | no |
| bitstamp | 4 | `imbalance_skew_mm` | half_spread_bps=2.5 skew_bps=0 inv_skew_bps=0 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 0 | 0 | 122 | no |
| bitstamp | 1 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 121 | no |
| bitstamp | 2 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 121 | no |
| bitstamp | 3 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 123 | no |
| bitstamp | 4 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 122 | no |
| bybit | 1 | `depth_imbalance` | depth_k=1 threshold=0.35 horizon_ms=10000 | -1.6453 | 1.6453 | 3230.53 | 8 | 1.0000 | 0.00409 | 0.00500 | 0 | 0 | 749 | no |
| bybit | 2 | `depth_imbalance` | depth_k=1 threshold=0.35 horizon_ms=10000 | -2.3012 | 2.3427 | 4441.72 | 11 | 1.0000 | 0.00493 | 0.00500 | 0 | 0 | 815 | no |
| bybit | 3 | `depth_imbalance` | depth_k=5 threshold=0.35 horizon_ms=10000 | -2.2910 | 2.3660 | 4441.42 | 11 | 1.0000 | 0.00470 | 0.00500 | 0 | 0 | 1024 | no |
| bybit | 4 | `depth_imbalance` | depth_k=5 threshold=0.35 horizon_ms=10000 | -2.5558 | 2.5558 | 5247.35 | 13 | 1.0000 | 0.00451 | 0.00500 | 0 | 0 | 1079 | no |
| bybit | 1 | `fixed_spread_mm` | half_spread_bps=1 requote_ms=500 | 0.1357 | 0.1315 | 1211.41 | 3 | 0.0217 | 0.00672 | 0.01000 | 0 | 0 | 749 | no |
| bybit | 2 | `fixed_spread_mm` | half_spread_bps=2.5 requote_ms=500 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 0 | 0 | 815 | no |
| bybit | 3 | `fixed_spread_mm` | half_spread_bps=2.5 requote_ms=500 | -0.0263 | 0.0263 | 403.78 | 1 | 0.0068 | 0.00011 | 0.00500 | 0 | 0 | 1024 | no |
| bybit | 4 | `fixed_spread_mm` | half_spread_bps=2.5 requote_ms=500 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 0 | 0 | 1079 | no |
| bybit | 1 | `imbalance_skew_mm` | half_spread_bps=1 skew_bps=0 inv_skew_bps=0 | -0.1525 | 0.1525 | 1615.22 | 4 | 0.0556 | 0.00665 | 0.01000 | 0 | 0 | 749 | no |
| bybit | 2 | `imbalance_skew_mm` | half_spread_bps=2.5 skew_bps=0 inv_skew_bps=0 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 0 | 0 | 815 | no |
| bybit | 3 | `imbalance_skew_mm` | half_spread_bps=2.5 skew_bps=0 inv_skew_bps=0 | -0.0263 | 0.0263 | 403.78 | 1 | 0.0132 | 0.00011 | 0.00500 | 0 | 0 | 1024 | no |
| bybit | 4 | `imbalance_skew_mm` | half_spread_bps=2.5 skew_bps=0 inv_skew_bps=0 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 0 | 0 | 1079 | no |
| bybit | 1 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 749 | no |
| bybit | 2 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 815 | no |
| bybit | 3 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 1024 | no |
| bybit | 4 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 1079 | no |
| coinbase | 1 | `depth_imbalance` | depth_k=5 threshold=0.35 horizon_ms=10000 | -4.3483 | 4.3483 | 8073.97 | 23 | 1.0000 | 0.00419 | 0.00500 | 0 | 0 | 768 | no |
| coinbase | 2 | `depth_imbalance` | depth_k=5 threshold=0.35 horizon_ms=10000 | -8.1948 | 8.1984 | 14934.17 | 40 | 1.0000 | 0.00453 | 0.00500 | 0 | 0 | 756 | no |
| coinbase | 3 | `depth_imbalance` | depth_k=5 threshold=0.35 horizon_ms=10000 | -3.9770 | 4.1574 | 7668.87 | 25 | 1.0000 | 0.00391 | 0.00500 | 0 | 0 | 754 | no |
| coinbase | 4 | `depth_imbalance` | depth_k=5 threshold=0.35 horizon_ms=10000 | -2.2897 | 2.2897 | 4438.42 | 13 | 1.0000 | 0.00469 | 0.00500 | 0 | 0 | 766 | no |
| coinbase | 1 | `fixed_spread_mm` | half_spread_bps=5 requote_ms=500 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 0 | 0 | 768 | no |
| coinbase | 2 | `fixed_spread_mm` | half_spread_bps=2.5 requote_ms=2000 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 0 | 0 | 756 | no |
| coinbase | 3 | `fixed_spread_mm` | half_spread_bps=2.5 requote_ms=2000 | -0.0659 | 0.0659 | 403.64 | 1 | 0.0250 | 0.00033 | 0.00500 | 0 | 0 | 754 | no |
| coinbase | 4 | `fixed_spread_mm` | half_spread_bps=5 requote_ms=500 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 0 | 0 | 766 | no |
| coinbase | 1 | `imbalance_skew_mm` | half_spread_bps=2.5 skew_bps=5 inv_skew_bps=0 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 27 | 0 | 768 | no |
| coinbase | 2 | `imbalance_skew_mm` | half_spread_bps=2.5 skew_bps=5 inv_skew_bps=0 | -0.0093 | 0.0684 | 403.64 | 1 | 0.0154 | 0.00323 | 0.00500 | 13 | 0 | 756 | no |
| coinbase | 3 | `imbalance_skew_mm` | half_spread_bps=2.5 skew_bps=5 inv_skew_bps=0 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 16 | 0 | 754 | no |
| coinbase | 4 | `imbalance_skew_mm` | half_spread_bps=2.5 skew_bps=5 inv_skew_bps=0 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 21 | 0 | 766 | no |
| coinbase | 1 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 768 | no |
| coinbase | 2 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 756 | no |
| coinbase | 3 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 754 | no |
| coinbase | 4 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 766 | no |
| kraken | 1 | `depth_imbalance` | depth_k=1 threshold=0.15 horizon_ms=10000 | -3.2631 | 3.2631 | 6055.31 | 20 | 1.0000 | 0.00495 | 0.00500 | 0 | 0 | 1129 | no |
| kraken | 2 | `depth_imbalance` | depth_k=1 threshold=0.35 horizon_ms=10000 | -3.2652 | 3.2652 | 6055.37 | 15 | 1.0000 | 0.00492 | 0.00500 | 0 | 0 | 1249 | no |
| kraken | 3 | `depth_imbalance` | depth_k=5 threshold=0.35 horizon_ms=10000 | -3.7555 | 3.7555 | 6862.34 | 22 | 1.0000 | 0.00460 | 0.00500 | 0 | 0 | 1606 | no |
| kraken | 4 | `depth_imbalance` | depth_k=5 threshold=0.35 horizon_ms=10000 | -2.2864 | 2.2864 | 4438.85 | 12 | 1.0000 | 0.00498 | 0.00500 | 0 | 0 | 1858 | no |
| kraken | 1 | `fixed_spread_mm` | half_spread_bps=2.5 requote_ms=500 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 0 | 0 | 1129 | no |
| kraken | 2 | `fixed_spread_mm` | half_spread_bps=2.5 requote_ms=500 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 0 | 0 | 1249 | no |
| kraken | 3 | `fixed_spread_mm` | half_spread_bps=2.5 requote_ms=500 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 0 | 0 | 1606 | no |
| kraken | 4 | `fixed_spread_mm` | half_spread_bps=2.5 requote_ms=500 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 0 | 0 | 1858 | no |
| kraken | 1 | `imbalance_skew_mm` | half_spread_bps=1 skew_bps=2 inv_skew_bps=0 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 17 | 0 | 1129 | no |
| kraken | 2 | `imbalance_skew_mm` | half_spread_bps=1 skew_bps=2 inv_skew_bps=0 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 23 | 0 | 1249 | no |
| kraken | 3 | `imbalance_skew_mm` | half_spread_bps=1 skew_bps=2 inv_skew_bps=0 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 22 | 0 | 1606 | no |
| kraken | 4 | `imbalance_skew_mm` | half_spread_bps=1 skew_bps=2 inv_skew_bps=0 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 26 | 0 | 1858 | no |
| kraken | 1 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 1129 | no |
| kraken | 2 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 1249 | no |
| kraken | 3 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 1606 | no |
| kraken | 4 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 1858 | no |
| okx | 1 | `depth_imbalance` | depth_k=1 threshold=0.35 horizon_ms=10000 | -3.1518 | 3.1518 | 6057.79 | 20 | 1.0000 | 0.00476 | 0.00500 | 0 | 0 | 391 | no |
| okx | 2 | `depth_imbalance` | depth_k=5 threshold=0.35 horizon_ms=10000 | -2.9019 | 2.9019 | 5653.37 | 14 | 1.0000 | 0.00470 | 0.00500 | 0 | 0 | 384 | no |
| okx | 3 | `depth_imbalance` | depth_k=5 threshold=0.35 horizon_ms=10000 | -2.0473 | 2.2043 | 4441.90 | 11 | 1.0000 | 0.00482 | 0.00500 | 0 | 0 | 393 | no |
| okx | 4 | `depth_imbalance` | depth_k=5 threshold=0.35 horizon_ms=10000 | -3.0060 | 3.0060 | 6054.96 | 21 | 1.0000 | 0.00484 | 0.00500 | 0 | 0 | 399 | no |
| okx | 1 | `fixed_spread_mm` | half_spread_bps=1 requote_ms=500 | -0.1527 | 0.1557 | 2019.20 | 5 | 0.0333 | 0.00982 | 0.01500 | 0 | 0 | 391 | no |
| okx | 2 | `fixed_spread_mm` | half_spread_bps=2.5 requote_ms=500 | 0.0858 | 0.0167 | 403.72 | 1 | 0.0067 | 0.00112 | 0.00500 | 0 | 0 | 384 | no |
| okx | 3 | `fixed_spread_mm` | half_spread_bps=2.5 requote_ms=500 | -0.0558 | 0.0558 | 403.81 | 1 | 0.0068 | 0.00011 | 0.00500 | 0 | 0 | 393 | no |
| okx | 4 | `fixed_spread_mm` | half_spread_bps=2.5 requote_ms=500 | 0.0000 | 0.0000 | 0.00 | 0 | 0.0000 | 0.00000 | 0.00000 | 0 | 0 | 399 | no |
| okx | 1 | `imbalance_skew_mm` | half_spread_bps=1 skew_bps=2 inv_skew_bps=0 | 0.1743 | 0.0275 | 403.74 | 1 | 0.0179 | 0.00497 | 0.00500 | 20 | 0 | 391 | no |
| okx | 2 | `imbalance_skew_mm` | half_spread_bps=1 skew_bps=2 inv_skew_bps=0 | 0.0545 | 0.1633 | 807.56 | 2 | 0.0308 | 0.00608 | 0.01000 | 11 | 0 | 384 | no |
| okx | 3 | `imbalance_skew_mm` | half_spread_bps=2.5 skew_bps=0 inv_skew_bps=0 | -0.0558 | 0.0558 | 403.81 | 1 | 0.0128 | 0.00011 | 0.00500 | 0 | 0 | 393 | no |
| okx | 4 | `imbalance_skew_mm` | half_spread_bps=2.5 skew_bps=0 inv_skew_bps=0 | 0.0242 | 0.0283 | 403.61 | 1 | 0.0132 | 0.00064 | 0.00500 | 0 | 0 | 399 | no |
| okx | 1 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 391 | no |
| okx | 2 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 384 | no |
| okx | 3 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 393 | no |
| okx | 4 | `no_trade` | none | 0.0000 | 0.0000 | 0.00 | 0 | null | 0.00000 | 0.00000 | 0 | 0 | 399 | no |

## Negative controls

Each of these is a deliberate corruption selected only by the gate script, and each is expected to fail. A control that passes proves nothing.

| control | expected | failed as required | evidence |
|---|---|---|---|
| `leak-future` | the leakage detector rejects the run | true | 60500 decision(s) used a feature that was not yet visible |
| `no-costs` | the result is refused as non-comparable to a run that paid | true | comparable = false, and the zero-cost run differs from the frozen-cost run by depth_imbalance +63.1691, fixed_spread_mm +0.0000, imbalance_skew_mm +0.0000, no_trade +0.0000 |
| `shuffle-seq` | the replay invariants stop every window | true | 96 held-out window(s) stopped on a replay invariant, 80 of them on a venue in gate scope |

## What these numbers are not

- Every fill here is simulated against a recording. No order reached a venue, no venue was connected, and no money moved.
- Queue position is approximate everywhere it appears, and every field carrying one is named `approx_`. On an aggregated feed a shrinking level is either a trade or a cancellation and the venue never says which, so a maker fill needs a crossing as positive evidence.
- A marketable order walks the recorded book without removing those levels from it, so a larger order than the ones used here would be flattered.
- One recording date, under six minutes per venue. The walk-forward unit is a window inside one recording, not a trading day, and nothing here is evidence about another day, another instrument, or another regime.
- The maker rebate is zero, meaning a maker fill pays nothing. Real spot maker fees are usually a positive cost, so the quoting numbers are an upper bound. The zero-cost control measures exactly what that is worth: the taker strategy pays a great deal and the quoting strategies pay nothing at all.
- The half-spread grid was frozen in basis points, and the venue table above shows these books quoting a small fraction of one. A quote at the narrowest grid point still sits far outside the touch, which bounds every fill count here. That is a defect of the frozen experiment rather than of the simulator, and correcting it means a new manifest and a new run, not a rerun of this one.
