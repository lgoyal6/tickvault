"""Opening an archive and asking it what it holds."""

from __future__ import annotations

import pytest

import tickvault


def test_venues_are_the_six_recorded():
    assert tickvault.venues() == [
        "coinbase",
        "kraken",
        "okx",
        "bybit",
        "binance-us",
        "bitstamp",
    ]


def test_capabilities_state_what_each_venue_can_prove():
    caps = {c["venue"]: c for c in tickvault.capabilities()}
    assert caps.keys() == set(tickvault.venues())
    # The headline honesty claim of the whole project: one of the six cannot
    # detect a dropped message at all, and says so rather than reporting clean.
    assert caps["bitstamp"]["detects_loss"] is False
    assert caps["kraken"]["detects_loss"] is True
    for cap in caps.values():
        assert cap["blind_spots"], f"{cap['venue']} claims no blind spots"


def test_open_lists_partitions(kraken_archive):
    archive = tickvault.open(kraken_archive)
    parts = archive.partitions()
    assert len(parts) == 1
    part = parts[0]
    assert (part.venue, part.symbol, part.book_level) == ("kraken", "BTC-USD", 2)
    assert part.rows > 0
    assert part.first_ns <= part.last_ns


def test_symbols_and_span(kraken_archive):
    archive = tickvault.open(kraken_archive)
    assert archive.symbols() == [("kraken", "BTC-USD")]
    lo, hi = archive.span_ns("kraken", "BTC-USD")
    assert lo <= hi


def test_verify_reads_every_file_back(kraken_archive):
    report = tickvault.open(kraken_archive).verify()
    assert report["clean"] is True
    assert report["unreadable"] == []
    # The two kinds of file that open and still lie: rows of another
    # instrument, or a price scale this build does not read.
    assert report["mislabelled"] == []
    assert report["incompatible"] == []
    assert report["truncations"] == 0
    assert report["files"] == 1 and report["rows"] > 0


def test_unknown_partition_is_an_error_not_an_empty_frame(kraken_archive):
    archive = tickvault.open(kraken_archive)
    with pytest.raises(Exception):
        archive.span_ns("kraken", "DOGE-USD")


def test_bitstamp_archive_is_recorded_order_by_order(bitstamp_archive):
    part = tickvault.open(bitstamp_archive).partitions()[0]
    assert part.book_level == 3
    assert part.venue == "bitstamp"
