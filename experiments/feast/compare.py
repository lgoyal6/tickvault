"""Ask Feast and a versioned DuckDB view the same point-in-time questions
TickVault has already answered, on the same Parquet file.

Everything is compared against data/tickvault_answers.json, which the Rust
`pit_fixture` example wrote from tickvault::features.
"""

import json
import pathlib
import sys
import time

import duckdb
import pandas as pd
from feast import FeatureStore

HERE = pathlib.Path(__file__).parent
DATA = (HERE / "data").resolve()
TRUTH = json.loads((DATA / "tickvault_answers.json").read_text())
PARQUET = str(DATA / "features.parquet")
TTL_NS = TRUTH["ttl_nanos"]
ENTITY = TRUTH["entity_id"]


def as_ts(nanos):
    return pd.Timestamp(nanos, unit="ns", tz="UTC")


def entity_df():
    return pd.DataFrame(
        {
            "entity_id": [ENTITY] * len(TRUTH["answers"]),
            "event_timestamp": [as_ts(a["as_of"]) for a in TRUTH["answers"]],
        }
    )


def feast_historical(store, view):
    df = store.get_historical_features(
        entity_df=entity_df(),
        features=[f"{view}:last_bid_price"],
    ).to_df()
    df = df.sort_values("event_timestamp").reset_index(drop=True)
    return {
        int(row.event_timestamp.value): (
            None if pd.isna(row.last_bid_price) else int(row.last_bid_price)
        )
        for row in df.itertuples()
    }


# The versioned SQL/Parquet feature view: the comparison arm the plan names.
# The version string is part of the view, so a stored answer can be traced to
# the definition that produced it.
VIEW_SQL_V2 = """
-- tickvault.feature_view.last_bid_price v2
-- v1 ordered by available_at DESC and disagreed with the archive at
-- as_of=10003s, where an NTP step had made the wall clock non-monotonic in
-- arrival order. Eligibility and ordering are two different questions:
--   eligible  = it had arrived      -> available_at <= as_of
--   current   = it arrived last     -> max(arrival_seq) among the eligible
-- Event time is carried through for staleness and is never filtered on.
SELECT
    value,
    event_time_ns,
    available_at_ns,
    $as_of                                   AS as_of_ns,
    CASE
        WHEN event_time_ns IS NULL              THEN 'age_unknown'
        WHEN $as_of - event_time_ns > $ttl      THEN 'stale'
        ELSE 'fresh'
    END                                      AS freshness
FROM (
    SELECT
        last_bid_price                                       AS value,
        epoch_ns(event_time)                                 AS event_time_ns,
        epoch_ns(available_at)                               AS available_at_ns,
        ROW_NUMBER() OVER (
            ORDER BY arrival_seq DESC
        )                                                    AS rank
    FROM feat
    WHERE entity_id = $entity
      AND epoch_ns(available_at) <= $as_of
)
WHERE rank = 1
"""


def duckdb_view(con, as_of):
    rows = con.execute(
        VIEW_SQL_V2,
        {"entity": ENTITY, "as_of": as_of, "ttl": TTL_NS},
    ).fetchall()
    if not rows:
        return {"value": None, "freshness": "missing"}
    value, event_ns, avail_ns, _, freshness = rows[0]
    return {
        "value": int(value),
        "event_time": None if event_ns is None else int(event_ns),
        "available_at": int(avail_ns),
        "freshness": freshness,
    }


def main():
    store = FeatureStore(repo_path=str(HERE))
    con = duckdb.connect()
    # arrival_seq is written by the fixture and is the archive's own order.
    con.execute(f"CREATE VIEW feat AS SELECT * FROM read_parquet('{PARQUET}')")

    report = {"cases": [], "timing": {}}

    # ---- Feast, both configurations ------------------------------------
    feast_results = {}
    for view in ("bid_by_event_time", "bid_by_availability"):
        t0 = time.perf_counter()
        feast_results[view] = feast_historical(store, view)
        report["timing"][f"feast_historical_{view}_s"] = round(
            time.perf_counter() - t0, 4
        )

    # ---- DuckDB versioned view -----------------------------------------
    t0 = time.perf_counter()
    duck_results = {a["as_of"]: duckdb_view(con, a["as_of"]) for a in TRUTH["answers"]}
    report["timing"]["duckdb_view_all_points_s"] = round(time.perf_counter() - t0, 4)
    report["timing"]["points"] = len(TRUTH["answers"])

    # ---- compare --------------------------------------------------------
    for answer in TRUTH["answers"]:
        as_of = answer["as_of"]
        case = {
            "as_of": as_of,
            "tickvault": {
                "value": answer["value"],
                "freshness": answer["freshness"],
            },
            "feast_event_time": feast_results["bid_by_event_time"].get(as_of),
            "feast_availability": feast_results["bid_by_availability"].get(as_of),
            "duckdb": duck_results[as_of],
        }
        case["feast_event_time_agrees"] = case["feast_event_time"] == answer["value"]
        case["feast_availability_agrees"] = (
            case["feast_availability"] == answer["value"]
        )
        case["duckdb_agrees"] = duck_results[as_of]["value"] == answer["value"]
        case["duckdb_freshness_agrees"] = (
            duck_results[as_of]["freshness"] == answer["freshness"]
        )
        report["cases"].append(case)

    n = len(report["cases"])
    report["summary"] = {
        "points": n,
        "feast_event_time_agreements": sum(
            c["feast_event_time_agrees"] for c in report["cases"]
        ),
        "feast_availability_agreements": sum(
            c["feast_availability_agrees"] for c in report["cases"]
        ),
        "duckdb_agreements": sum(c["duckdb_agrees"] for c in report["cases"]),
        "duckdb_freshness_agreements": sum(
            c["duckdb_freshness_agrees"] for c in report["cases"]
        ),
    }
    expected = {
        "points": 29,
        "feast_event_time_agreements": 23,
        "feast_availability_agreements": 26,
        "duckdb_agreements": 29,
        "duckdb_freshness_agreements": 29,
    }
    if report["summary"] != expected:
        raise AssertionError(
            f"point-in-time comparison changed: {report['summary']} != {expected}"
        )

    out = DATA / "comparison.json"
    out.write_text(json.dumps(report, indent=2))
    print(json.dumps(report["summary"], indent=2))
    print(json.dumps(report["timing"], indent=2))

    # The leak instant, spelled out.
    leak_as_of = TRUTH["leak_row"]["event_time"] + 2 * 10**9
    leak = next((c for c in report["cases"] if c["as_of"] == leak_as_of), None)
    if leak:
        print("\n--- the leak instant ---")
        print(json.dumps(leak, indent=2))
    print(f"\nwrote {out}")


if __name__ == "__main__":
    sys.exit(main())
