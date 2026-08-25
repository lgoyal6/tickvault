"""Fixture archives, built from tapes rather than recorded live.

A test that opens a socket to Kraken fails on a plane, fails in CI without
egress, and fails whenever the venue has a bad afternoon. So every archive here
is replayed from a tape committed under ``tests/fixtures``, through the same
ingest path the recorder uses. The frames are real; only the receipt stamps are
synthesised, and they are synthesised from a fixed instant so the Parquet comes
out byte-identical every run.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
FIXTURES = REPO / "tests" / "fixtures"


def _binary() -> Path:
    override = os.environ.get("TICKVAULT_BIN")
    if override:
        return Path(override)
    for profile in ("release", "debug"):
        candidate = REPO / "target" / profile / "tickvault"
        if candidate.exists():
            return candidate
    pytest.skip(
        "no tickvault binary; run `cargo build --release` or set TICKVAULT_BIN"
    )


def _replay(tmp: Path, name: str, *args: str) -> Path:
    out = tmp / name
    if out.exists():
        shutil.rmtree(out)
    subprocess.run(
        [str(_binary()), "replay-tape", "--payloads", "--archive", str(out), *args],
        check=True,
        capture_output=True,
    )
    return out


@pytest.fixture(scope="session")
def kraken_archive(tmp_path_factory) -> Path:
    """Kraken at L2: a ten-deep aggregated feed, so no traded volume."""
    return _replay(
        tmp_path_factory.mktemp("kraken"),
        "archive",
        "--venue",
        "kraken",
        "--symbols",
        "BTC-USD",
        "--tape",
        str(FIXTURES / "kraken_book.jsonl"),
        # Passing precision here keeps the replay off the network entirely;
        # otherwise Kraken is asked for its tick size over REST.
        "--precision",
        "1,8",
    )


@pytest.fixture(scope="session")
def bitstamp_archive(tmp_path_factory) -> Path:
    """Bitstamp at L3: order by order, so executions are real numbers."""
    return _replay(
        tmp_path_factory.mktemp("bitstamp"),
        "archive",
        "--venue",
        "bitstamp",
        "--symbols",
        "BTC-USD",
        "--tape",
        str(FIXTURES / "bitstamp_live_orders.jsonl"),
        "--book-level",
        "3",
    )
