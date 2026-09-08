# Microstructure result

A six-day TickVault study found a repeatable statistical relationship and an economic null.

- **Chronological folds:** 46 of 48 one-to-five-second folds had positive out-of-sample R-squared.
- **Out-of-sample R-squared:** 0.021 to 0.091 against a random walk.
- **Gross mid-to-mid edge:** 0.044 to 0.582 basis points across 1, 5, and 30 seconds.
- **Positive break-even fee:** 0.036 to 0.285 basis points per side. Binance.US was negative at every horizon.
- **Conclusion:** Economic null. The measured edge did not cover the measured round-trip spread under the stated execution assumptions.

No orders were placed and no profitable strategy is claimed.

## Headline table

| Venue | Horizon | Samples | OOS R2 vs random walk | Gross edge, bp | Break-even fee, bp/side | Positive folds |
|---|---:|---:|---:|---:|---:|---:|
| binance-us | 1s | 17494 | 0.02084 | 0.04384 | -0.22248 | 3/4 |
| binance-us | 5s | 17047 | 0.03140 | 0.15523 | -0.16678 | 3/4 |
| binance-us | 30s | 14751 | 0.00418 | 0.34251 | -0.07314 | 2/4 |
| bybit | 1s | 35690 | 0.03915 | 0.08513 | 0.03616 | 5/5 |
| bybit | 5s | 35670 | 0.04535 | 0.21943 | 0.10330 | 5/5 |
| bybit | 30s | 35545 | 0.01079 | 0.31576 | 0.15147 | 4/5 |
| coinbase | 1s | 35563 | 0.03180 | 0.08875 | 0.04373 | 5/5 |
| coinbase | 5s | 35535 | 0.03334 | 0.21433 | 0.10653 | 5/5 |
| coinbase | 30s | 35360 | 0.01026 | 0.29131 | 0.14501 | 4/5 |
| kraken | 1s | 30668 | 0.05130 | 0.09362 | 0.04040 | 5/5 |
| kraken | 5s | 30376 | 0.09108 | 0.31459 | 0.15089 | 5/5 |
| kraken | 30s | 28634 | 0.04104 | 0.58194 | 0.28456 | 5/5 |
| okx | 1s | 35626 | 0.04565 | 0.10481 | 0.04599 | 5/5 |
| okx | 5s | 35602 | 0.04523 | 0.23745 | 0.11232 | 5/5 |
| okx | 30s | 35452 | 0.01091 | 0.28079 | 0.13399 | 5/5 |

## Controls

- Planted R2 to median recovered R2: 0.000->-0.0004, 0.005->0.0046, 0.020->0.0197, 0.100->0.1000.
- Future-rewrite control: past rows stayed identical and post-cut books changed on Kraken, Coinbase, and OKX.
- Model look-ahead control: 14/14 earlier folds stayed identical after corrupting all data from the boundary day onward.
- Momentum-only control: 7/15 venue-horizon cells were at or below zero, including every 30-second cell.

## Provenance and limits

The source manifest covers 6,204 files and 53,825,256 rows. The compact [public result JSON](public_result.json) records SHA-256 hashes for the manifest, panel, study output, and controls output.

Bitstamp was excluded because the recorder could not vouch for its rows. Binance.US was missing one day. The study covers BTC at one time of day in one market regime, uses linear models, and measures a mid price that cannot be traded. Market impact and execution mechanics were not modeled.

Verify the checked-in result without credentials or source archives:

```bash
python3 research/public_result.py verify
```
