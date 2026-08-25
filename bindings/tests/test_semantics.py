"""The two conventions the README promises: None over zero, and labelled gaps."""

from __future__ import annotations

import math

import pytest

import tickvault


def test_aggregated_venues_report_no_volume_rather_than_zero(kraken_archive):
    archive = tickvault.open(kraken_archive)
    bars = archive.bars("kraken", "BTC-USD", bar_seconds=1, frame="arrow")
    volume = bars.column("volume").to_pylist()
    assert volume, "no bars produced"
    # Kraken publishes price levels, not trades. A level shrinking might be a
    # fill or a cancellation and the feed never says which, so the traded
    # volume of the bar is not something this data contains. Zero would be a
    # claim that nothing traded, which is a different and false statement.
    assert all(v is None for v in volume)


def test_order_by_order_venues_report_real_volume(bitstamp_archive):
    archive = tickvault.open(bitstamp_archive)
    bars = archive.bars("bitstamp", "BTC-USD", bar_seconds=1, frame="arrow")
    volume = [v for v in bars.column("volume").to_pylist() if v is not None]
    assert volume, "an L3 feed should know what traded"
    assert all(v >= 0 for v in volume)
    # A row records the quantity still resting, which on a full fill is zero.
    # If the traded size were read off that, every L3 bar would report exactly
    # zero volume, which is the "nothing traded" claim this whole convention
    # exists to avoid making.
    assert any(v > 0 for v in volume)


def test_a_one_sided_book_has_no_mid_and_no_spread(bitstamp_archive):
    """Bitstamp starts empty and fills in, so the earliest books are lopsided."""
    archive = tickvault.open(bitstamp_archive)
    lo, _ = archive.span_ns("bitstamp", "BTC-USD")
    book = archive.book_at("bitstamp", "BTC-USD", lo)
    if book.bids and book.asks:
        pytest.skip("this tape's first book already has both sides")
    assert book.mid is None
    assert book.spread is None


def test_imbalance_is_none_on_an_empty_book_and_bounded_otherwise(kraken_archive):
    archive = tickvault.open(kraken_archive)
    _, hi = archive.span_ns("kraken", "BTC-USD")
    book = archive.book_at("kraken", "BTC-USD", hi)
    value = book.imbalance(depth=10)
    assert value is not None
    assert -1.0 <= value <= 1.0
    assert not math.isnan(value)


def test_every_book_says_whether_it_is_trustworthy(kraken_archive):
    archive = tickvault.open(kraken_archive)
    _, hi = archive.span_ns("kraken", "BTC-USD")
    book = archive.book_at("kraken", "BTC-USD", hi)
    assert isinstance(book.suspect, bool)
    assert isinstance(book.suspect_rows, int)
    assert book.origin
    # This tape is complete, so nothing in it should be marked suspect.
    assert book.suspect is False
    assert book.suspect_rows == 0


def test_spread_bps_agrees_with_spread_over_mid(kraken_archive):
    archive = tickvault.open(kraken_archive)
    _, hi = archive.span_ns("kraken", "BTC-USD")
    book = archive.book_at("kraken", "BTC-USD", hi)
    assert book.spread_bps == pytest.approx(book.spread / book.mid * 10_000)


def test_isoformat_keeps_all_nine_digits():
    # datetime stops at microseconds, and feeds routinely put several messages
    # inside one, so the last three digits are what tells them apart.
    assert tickvault.isoformat(1787626160284980917) == "2026-08-25T02:49:20.284980917Z"
    assert tickvault.isoformat(0) == "1970-01-01T00:00:00.000000000Z"
    assert tickvault.isoformat(1) == "1970-01-01T00:00:00.000000001Z"


def test_traded_volume_matches_the_tape(bitstamp_archive):
    """The three executions this tape contains, at the sizes the venue reported.

    A fourth event reports a fill on an order created before the recording
    started. What that order had been resting for is not known, so it is left
    out rather than counted at a guessed size.
    """
    archive = tickvault.open(bitstamp_archive)
    bars = archive.bars("bitstamp", "BTC-USD", bar_seconds=3600, frame="arrow")
    total = sum(v for v in bars.column("volume").to_pylist() if v is not None)
    assert total == pytest.approx(0.001 + 0.009 + 0.009)


def test_origin_says_when_in_words_not_in_nanoseconds(kraken_archive):
    """Whoever asks where a book came from has to be able to place it in time."""
    archive = tickvault.open(kraken_archive)
    _, hi = archive.span_ns("kraken", "BTC-USD")
    origin = archive.book_at("kraken", "BTC-USD", hi).origin
    assert "snapshot" in origin or "row" in origin or "checkpoint" in origin
    assert "T" in origin and origin.endswith("Z")
    # The raw count must not be what a reader is handed.
    assert not any(part.isdigit() and len(part) > 15 for part in origin.split())
