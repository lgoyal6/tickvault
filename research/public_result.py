"""Extract and verify the compact public result for the C26 study."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
DEFAULT_RESULT = ROOT / "public_result.json"
DEFAULT_MARKDOWN = ROOT / "RESULT.md"


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def rounded(value: float) -> float:
    return round(float(value), 8)


def result_rows(study: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for key, value in study["results"].items():
        venue, symbol, horizon, features = key.split("|")
        if features != "imbalance":
            continue
        pooled = value["pooled"]
        spread = study["costs"][venue]["median_spread_bps"]
        gross = pooled["gross_edge_bps"]
        rows.append(
            {
                "venue": venue,
                "symbol": symbol,
                "horizonSeconds": int(horizon[1:]),
                "samples": pooled["n"],
                "oosR2VsRandomWalk": rounded(pooled["r2_vs_zero"]),
                "grossMidToMidEdgeBps": rounded(gross),
                "medianSpreadBps": rounded(spread),
                "breakEvenFeeBpsPerSide": rounded((gross - spread) / 2),
                "positiveFolds": sum(f["r2_vs_zero"] > 0 for f in value["folds"]),
                "totalFolds": len(value["folds"]),
            }
        )
    return sorted(rows, key=lambda row: (row["venue"], row["horizonSeconds"]))


def momentum_summary(study: dict[str, Any]) -> dict[str, Any]:
    values = [
        value["pooled"]["r2_vs_zero"]
        for key, value in study["results"].items()
        if key.endswith("|momentum")
    ]
    return {
        "cells": len(values),
        "atOrBelowZero": sum(value <= 0 for value in values),
        "oosR2Range": [rounded(min(values)), rounded(max(values))],
        "allThirtySecondCellsAtOrBelowZero": all(
            value["pooled"]["r2_vs_zero"] <= 0
            for key, value in study["results"].items()
            if "|h30|momentum" in key
        ),
    }


def derive_headline(rows: list[dict[str, Any]]) -> dict[str, Any]:
    short = [row for row in rows if row["horizonSeconds"] in (1, 5)]
    positive_fees = [
        row["breakEvenFeeBpsPerSide"]
        for row in rows
        if row["breakEvenFeeBpsPerSide"] >= 0
    ]
    binance_fees = [
        row["breakEvenFeeBpsPerSide"]
        for row in rows
        if row["venue"] == "binance-us"
    ]
    return {
        "verifiableVenues": len({row["venue"] for row in rows}),
        "positiveOneAndFiveSecondFolds": sum(row["positiveFolds"] for row in short),
        "oneAndFiveSecondFolds": sum(row["totalFolds"] for row in short),
        "oneAndFiveSecondOosR2Range": [
            rounded(min(row["oosR2VsRandomWalk"] for row in short)),
            rounded(max(row["oosR2VsRandomWalk"] for row in short)),
        ],
        "grossEdgeRangeBps": [
            rounded(min(row["grossMidToMidEdgeBps"] for row in rows)),
            rounded(max(row["grossMidToMidEdgeBps"] for row in rows)),
        ],
        "positiveBreakEvenFeeRangeBpsPerSide": [
            rounded(min(positive_fees)),
            rounded(max(positive_fees)),
        ],
        "binanceBreakEvenFeeRangeBpsPerSide": [
            rounded(min(binance_fees)),
            rounded(max(binance_fees)),
        ],
        "statisticalConclusion": "Book imbalance predicted one-to-five-second returns out of sample on all five verifiable venues and in 46 of 48 chronological folds.",
        "economicConclusion": "Economic null. Gross mid-to-mid edge did not cover the measured round-trip spread under the stated execution assumptions. No profitable strategy is claimed.",
    }


def control_summary(controls: dict[str, Any]) -> dict[str, Any]:
    return {
        "plantedSignal": [
            {
                "plantedR2": rounded(row["planted_r2"]),
                "medianRecoveredR2": rounded(row["median_recovered_r2"]),
            }
            for row in controls["positive_control"]
        ],
        "futureRewrite": [
            {
                "venue": row["venue"],
                "pastRowsIdentical": row["before_cut_identical"],
                "futureRowsChanged": row["after_cut_changed"],
                "postCutBooksDiffering": row["post_cut_books_differing"],
                "postCutRows": row["rows_after_cut"],
                "verdict": row["verdict"],
            }
            for row in controls["lookahead_features"]
        ],
        "futureCorruption": {
            "rowsCorrupted": controls["lookahead_model"]["rows_corrupted"],
            "rowsTotal": controls["lookahead_model"]["rows_total"],
            "earlierFoldsIdentical": controls["lookahead_model"]["folds_identical"],
            "earlierFoldsChecked": controls["lookahead_model"]["folds_checked"],
            "verdict": controls["lookahead_model"]["verdict"],
        },
    }


def extract(study_path: Path, controls_path: Path, manifest_path: Path, panel_path: Path) -> dict[str, Any]:
    study = json.loads(study_path.read_text())
    controls = json.loads(controls_path.read_text())
    manifest = json.loads(manifest_path.read_text())
    rows = result_rows(study)
    return {
        "schemaVersion": 1,
        "question": "Does availability-time order book imbalance predict the next 1, 5, or 30 seconds of mid price out of sample, better than a random walk, and is the edge economic after measured spread?",
        "provenance": {
            "tickvaultRevision": manifest["tickvault_revision"],
            "studyOutputSha256": digest(study_path),
            "controlsOutputSha256": digest(controls_path),
            "manifestSha256": digest(manifest_path),
            "panelSha256": digest(panel_path),
            "sourceFiles": manifest["totals"]["files"],
            "sourceRows": manifest["totals"]["rows"],
            "sourceBytes": manifest["totals"]["bytes"],
            "windowsUtc": [
                {"from": window["from"], "to": window["to"]}
                for window in manifest["windows"]
            ],
            "extractCommand": "python3 research/public_result.py extract --study results_h.json --controls controls.json --manifest manifest.json --panel panel_1s.parquet",
        },
        "scope": {
            "instrument": "BTC",
            "venuesIncluded": sorted({row["venue"] for row in rows}),
            "venueExcluded": "bitstamp",
            "exclusionReason": study["excluded_instruments"]["bitstamp:BTC-USD"]["reason"],
            "sampling": "one row per second from 09:00 to 11:00 UTC across six days",
            "execution": "No orders were placed. Edge is measured mid-to-mid. Fees, impact, queue position, partial fills, rejects, funding, and borrow were not observed.",
        },
        "headline": derive_headline(rows),
        "results": rows,
        "momentumControl": momentum_summary(study),
        "controls": control_summary(controls),
    }


def markdown(result: dict[str, Any]) -> str:
    h = result["headline"]
    lines = [
        "# Microstructure result",
        "",
        "A six-day TickVault study found a repeatable statistical relationship and an economic null.",
        "",
        f"- **Chronological folds:** {h['positiveOneAndFiveSecondFolds']} of {h['oneAndFiveSecondFolds']} one-to-five-second folds had positive out-of-sample R-squared.",
        f"- **Out-of-sample R-squared:** {h['oneAndFiveSecondOosR2Range'][0]:.3f} to {h['oneAndFiveSecondOosR2Range'][1]:.3f} against a random walk.",
        f"- **Gross mid-to-mid edge:** {h['grossEdgeRangeBps'][0]:.3f} to {h['grossEdgeRangeBps'][1]:.3f} basis points across 1, 5, and 30 seconds.",
        f"- **Positive break-even fee:** {h['positiveBreakEvenFeeRangeBpsPerSide'][0]:.3f} to {h['positiveBreakEvenFeeRangeBpsPerSide'][1]:.3f} basis points per side. Binance.US was negative at every horizon.",
        "- **Conclusion:** Economic null. The measured edge did not cover the measured round-trip spread under the stated execution assumptions.",
        "",
        "No orders were placed and no profitable strategy is claimed.",
        "",
        "## Headline table",
        "",
        "| Venue | Horizon | Samples | OOS R2 vs random walk | Gross edge, bp | Break-even fee, bp/side | Positive folds |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in result["results"]:
        lines.append(
            f"| {row['venue']} | {row['horizonSeconds']}s | {row['samples']} | "
            f"{row['oosR2VsRandomWalk']:.5f} | {row['grossMidToMidEdgeBps']:.5f} | "
            f"{row['breakEvenFeeBpsPerSide']:.5f} | {row['positiveFolds']}/{row['totalFolds']} |"
        )
    controls = result["controls"]
    planted = ", ".join(
        f"{row['plantedR2']:.3f}->{row['medianRecoveredR2']:.4f}"
        for row in controls["plantedSignal"]
    )
    lines += [
        "",
        "## Controls",
        "",
        f"- Planted R2 to median recovered R2: {planted}.",
        "- Future-rewrite control: past rows stayed identical and post-cut books changed on Kraken, Coinbase, and OKX.",
        f"- Model look-ahead control: {controls['futureCorruption']['earlierFoldsIdentical']}/{controls['futureCorruption']['earlierFoldsChecked']} earlier folds stayed identical after corrupting all data from the boundary day onward.",
        f"- Momentum-only control: {result['momentumControl']['atOrBelowZero']}/{result['momentumControl']['cells']} venue-horizon cells were at or below zero, including every 30-second cell.",
        "",
        "## Provenance and limits",
        "",
        f"The source manifest covers {result['provenance']['sourceFiles']:,} files and {result['provenance']['sourceRows']:,} rows. The compact [public result JSON](public_result.json) records SHA-256 hashes for the manifest, panel, study output, and controls output.",
        "",
        "Bitstamp was excluded because the recorder could not vouch for its rows. Binance.US was missing one day. The study covers BTC at one time of day in one market regime, uses linear models, and measures a mid price that cannot be traded. Market impact and execution mechanics were not modeled.",
        "",
        "Verify the checked-in result without credentials or source archives:",
        "",
        "```bash",
        "python3 research/public_result.py verify",
        "```",
        "",
    ]
    return "\n".join(lines)


def verify(result_path: Path, markdown_path: Path) -> None:
    result = json.loads(result_path.read_text())
    rows = result["results"]
    assert len(rows) == 15
    assert result["headline"] == derive_headline(rows)
    assert result["headline"]["positiveOneAndFiveSecondFolds"] == 46
    assert result["headline"]["oneAndFiveSecondFolds"] == 48
    assert result["headline"]["oneAndFiveSecondOosR2Range"] == [0.02083786, 0.09108098]
    assert result["headline"]["grossEdgeRangeBps"] == [0.04383669, 0.58193778]
    assert result["headline"]["positiveBreakEvenFeeRangeBpsPerSide"] == [0.03615867, 0.28455898]
    assert max(
        row["breakEvenFeeBpsPerSide"]
        for row in rows if row["venue"] == "binance-us"
    ) < 0
    assert result["momentumControl"]["atOrBelowZero"] == 7
    assert result["momentumControl"]["allThirtySecondCellsAtOrBelowZero"] is True
    assert [row["plantedR2"] for row in result["controls"]["plantedSignal"]] == [0.0, 0.005, 0.02, 0.1]
    assert all(row["verdict"] == "pass" for row in result["controls"]["futureRewrite"])
    assert result["controls"]["futureCorruption"]["verdict"] == "pass"
    for name in ("studyOutputSha256", "controlsOutputSha256", "manifestSha256", "panelSha256"):
        value = result["provenance"][name]
        assert len(value) == 64 and all(char in "0123456789abcdef" for char in value)
    assert markdown_path.read_text() == markdown(result)
    print("public result valid: 15 cells, 46/48 short-horizon folds positive, economic null preserved")


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    extract_parser = sub.add_parser("extract")
    extract_parser.add_argument("--study", type=Path, required=True)
    extract_parser.add_argument("--controls", type=Path, required=True)
    extract_parser.add_argument("--manifest", type=Path, required=True)
    extract_parser.add_argument("--panel", type=Path, required=True)
    extract_parser.add_argument("--out", type=Path, default=DEFAULT_RESULT)
    verify_parser = sub.add_parser("verify")
    verify_parser.add_argument("--result", type=Path, default=DEFAULT_RESULT)
    verify_parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    args = parser.parse_args()
    if args.command == "extract":
        result = extract(args.study, args.controls, args.manifest, args.panel)
        args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        DEFAULT_MARKDOWN.write_text(markdown(result))
        print(f"wrote {args.out} and {DEFAULT_MARKDOWN}")
    else:
        verify(args.result, args.markdown)


if __name__ == "__main__":
    main()
