"""Does Feast's online read agree with its own historical read at the same instant?

Materialize each view up to a watermark, read the online store, and compare
against that view's historical answer at the same instant and against
TickVault's. Serving parity is the second half of C21 and it is a property of
the pair, not of either read alone.
"""

import json
import pathlib
import shutil
import subprocess
import sys
import time

import pandas as pd
from feast import FeatureStore

HERE = pathlib.Path(__file__).parent
DATA = (HERE / "data").resolve()
TRUTH = json.loads((DATA / "tickvault_answers.json").read_text())
ENTITY = TRUTH["entity_id"]
FEAST = shutil.which("feast")

# Three watermarks: an ordinary instant, the in-flight instant that exposed the
# leak, and the instant where the newest value is 51s old.
WATERMARKS = [10_000 * 10**9, 9_999 * 10**9, 20_050 * 10**9]
EXPECTED = {
    10_000 * 10**9: ((1009999, 1009999, True), (1009999, 1009999, True)),
    9_999 * 10**9: ((1009999, 1009999, True), (1009998, 1009998, True)),
    20_050 * 10**9: ((1019999, 2000000, False), (1019999, None, False)),
}


def iso(nanos):
    return pd.Timestamp(nanos, unit="ns", tz="UTC").isoformat()


def materialize(end_nanos):
    # The CLI is the documented path and takes the window explicitly.
    if FEAST is None:
        raise RuntimeError("feast executable is not on PATH")
    subprocess.run(
        [FEAST, "materialize", "1970-01-01T00:00:00", iso(end_nanos).replace("+00:00", "")],
        cwd=str(HERE),
        check=True,
        capture_output=True,
    )


def online(store, view):
    rows = store.get_online_features(
        features=[f"{view}:last_bid_price"],
        entity_rows=[{"entity_id": ENTITY}],
    ).to_dict()
    return rows["last_bid_price"][0]


def historical(store, view, as_of):
    df = store.get_historical_features(
        entity_df=pd.DataFrame(
            {
                "entity_id": [ENTITY],
                "event_timestamp": [pd.Timestamp(as_of, unit="ns", tz="UTC")],
            }
        ),
        features=[f"{view}:last_bid_price"],
    ).to_df()
    # An empty frame is itself a result: Feast dropped the entity row rather
    # than returning an old value, which is the staleness behaviour under test.
    if len(df) == 0:
        return None
    v = df["last_bid_price"].iloc[0]
    return None if pd.isna(v) else int(v)


def main():
    store = FeatureStore(repo_path=str(HERE))
    truth = {a["as_of"]: a for a in TRUTH["answers"]}
    out = []
    for wm in WATERMARKS:
        t0 = time.perf_counter()
        materialize(wm)
        materialize_s = round(time.perf_counter() - t0, 4)
        row = {
            "watermark_s": wm / 1e9,
            "materialize_s": materialize_s,
            "tickvault": {
                "value": truth[wm]["value"],
                "freshness": truth[wm]["freshness"],
            },
        }
        for view in ("bid_by_event_time", "bid_by_availability"):
            t0 = time.perf_counter()
            o = online(store, view)
            row[f"{view}_online_s"] = round(time.perf_counter() - t0, 4)
            h = historical(store, view, wm)
            row[view] = {
                "online": o,
                "historical": h,
                "self_parity": o == h,
                "agrees_with_tickvault": o == truth[wm]["value"],
            }
        actual = tuple(
            (
                row[view]["online"],
                row[view]["historical"],
                row[view]["self_parity"],
            )
            for view in ("bid_by_event_time", "bid_by_availability")
        )
        if actual != EXPECTED[wm]:
            raise AssertionError(
                f"online comparison changed at {wm}: {actual} != {EXPECTED[wm]}"
            )
        out.append(row)
        print(json.dumps(row, indent=2))
    (DATA / "online_parity.json").write_text(json.dumps(out, indent=2))
    print(f"\nwrote {DATA / 'online_parity.json'}")


if __name__ == "__main__":
    sys.exit(main())
