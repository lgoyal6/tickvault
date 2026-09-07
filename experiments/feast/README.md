# Feast point-in-time comparison

This experiment runs Feast and a versioned DuckDB SQL view against the same
fixture and expected answers produced by TickVault. It keeps event time,
availability time, and arrival order separate.

The retained results show:

- TickVault historical and online reads agree at all 29 as-of points, including
  a backwards wall-clock step.
- The DuckDB v2 view agrees with TickVault on value and freshness at 29 of 29
  points.
- Feast's event-time view agrees on 23 of 29 values. Its availability-time view
  agrees on 26 of 29 values.
- Feast historical and online reads disagree at the stale watermark for both
  views. Feast therefore remains the comparison arm, not TickVault's serving
  implementation.

## Reproduce

Python 3.11 is required by the pinned Feast version.

```sh
cargo run --release --example pit_fixture -- experiments/feast/data
cd experiments/feast
python3.11 -m venv .venv
.venv/bin/pip install -r requirements.txt
PATH="$PWD/.venv/bin:$PATH" feast apply
.venv/bin/python compare.py
PATH="$PWD/.venv/bin:$PATH" .venv/bin/python online.py
```

`compare.py` rewrites `data/comparison.json`; its timing fields vary between
runs, while the agreement counts must remain 23, 26, 29, and 29. `online.py`
rewrites `data/online_parity.json`; its timings vary, while parity at the three
named watermarks must match the retained report. Both scripts fail if these
semantic results change. Feast writes `registry.db` and `online_store.db`
locally, and both are ignored.

The dataset has 20,001 rows and one feature on purpose. It tests the timestamp
and freshness rules in C21 without turning framework overhead on a small fixture
into a claim about Feast at production scale.
