# Historical period comparison

`period_study.py` extends the research harness with one fixed nonlinear
comparator, `HistGradientBoostingRegressor`. It reuses `tvstudy.prepare` and
the training-only `Fit` linear baseline. Install `requirements-periods.txt`
in a private virtual environment. Building panels also requires the TickVault
Python binding; retain the wheel hash and its source revision separately.

Freeze a JSON protocol before evaluating. The protocol binds the input panel,
archive manifest, study sources, instruments, windows, feature sets, model
parameters, horizons, delay, fees, slippage, seeds and limitations. The command
rejects changed source, panel or archive bytes. Do not overwrite a frozen
protocol after observing a result; use a new identity and disclose the change.

```sh
OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 python research/period_study.py \
  --protocol /private/run/protocol.json --panel /private/run/input-panel.parquet \
  --base /private/archives --out /private/run/comparison
python -m unittest discover -s research -p 'test_*.py' -v
```

The input must use a contiguous one-second availability grid inside each
venue/symbol/window, with no repeated timestamps or future messages. Each fold
trains on earlier dates from the same venue only. Purging uses the actual target
end time. All feature ablations use identical eligible rows. No parameters,
normalization or early-stopping decisions use the test date. The volatility
label compares that date's realized one-second volatility with the median of
the prior training dates. It labels results after the fact and never selects
training or test rows.

The cost diagnostic uses nonoverlapping round trips of one notional unit,
with the frozen decision delay, observed entry/exit half spreads, per-side fees
and per-side slippage. It reports net basis points per opportunity, turnover
in notional units and drawdown of cumulative unit-notional basis points.
These are hypothetical diagnostics with no fill, queue, impact, funding,
borrow, capital-compounding or execution-capacity model. The simulator's
inventory, exposure, loss-floor, cancellation and kill-switch tests remain a
separate execution-risk boundary.

All date/venue/model/feature/horizon folds and predictions are retained.
Uncertainty resamples dates jointly across venues and venues jointly across
dates. Seconds are not counted as independent trials. Intervals are descriptive
and unadjusted across comparisons. Small date and venue counts, historical
window selection, fixed BTC coverage and missing periods remain limitations.
Previously analyzed windows are not a new untouched holdout.

Archive inputs are read-only. Outputs go to a new private run directory.
Rerunning with the identical protocol into a new directory is the recovery path;
the CLI refuses an existing output directory. A partially written directory is
not a completed result. Keep the protocol, source hashes,
raw predictions, outputs and verification receipts together. Delete only
disposable intermediates after review. No network service is needed, and this
path has no live order or credential interface. Protocol SHA-256 is the run's
correlation identifier. Runtime, maximum resident memory and concurrency belong
in the execution receipt; they do not establish a latency or throughput claim.
