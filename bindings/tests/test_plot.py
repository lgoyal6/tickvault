"""The depth chart, which is what the README promises in ten lines."""

from __future__ import annotations

import pytest

import tickvault

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")


def _book_and_axes(archive_path):
    archive = tickvault.open(archive_path)
    _, hi = archive.span_ns("kraken", "BTC-USD")
    ax = archive.plot_book("kraken", "BTC-USD", hi, depth=5)
    return archive.book_at("kraken", "BTC-USD", hi, depth=5), ax


def test_it_draws_both_sides_and_labels_them(kraken_archive):
    _, ax = _book_and_axes(kraken_archive)
    labels = {line.get_label() for line in ax.lines}
    assert {"bids", "asks"} <= labels
    assert ax.get_xlabel() == "price"
    assert ax.get_ylabel() == "cumulative quantity"


def test_the_title_is_a_readable_instant_not_a_pile_of_digits(kraken_archive):
    book, ax = _book_and_axes(kraken_archive)
    title = ax.get_title()
    assert str(book.at_ns) not in title
    assert tickvault.isoformat(book.at_ns) in title
    assert "kraken" in title and "BTC-USD" in title


def test_depth_accumulates_outward_from_the_mid(kraken_archive):
    """The cumulative curve must start small at the touch and grow outward.

    Both sides arrive best-first, so bids descend in price while asks ascend.
    Plotting them the same way puts the bid wall on the wrong side of its own
    prices: the value at the best bid becomes the whole side's total instead of
    that level's own quantity.
    """
    book, ax = _book_and_axes(kraken_archive)
    bids = next(line for line in ax.lines if line.get_label() == "bids")
    asks = next(line for line in ax.lines if line.get_label() == "asks")

    bid_x, bid_y = bids.get_data()
    ask_x, ask_y = asks.get_data()
    bid_total = sum(q for _, q in book.bids)
    ask_total = sum(q for _, q in book.asks)

    # Nearest the spread, only the best level has accumulated.
    assert bid_y[-1] == pytest.approx(book.bids[0][1])
    assert ask_y[0] == pytest.approx(book.asks[0][1])
    # Furthest away, the whole side has.
    assert bid_y[0] == pytest.approx(bid_total)
    assert ask_y[-1] == pytest.approx(ask_total)
    # And both are drawn left to right, whichever order the levels arrived in.
    assert list(bid_x) == sorted(bid_x)
    assert list(ask_x) == sorted(ask_x)


def test_a_suspect_book_says_so_on_the_chart(kraken_archive, monkeypatch):
    """A gap must reach the picture, not just the report nobody reads."""
    archive = tickvault.open(kraken_archive)
    _, hi = archive.span_ns("kraken", "BTC-USD")
    real = archive.book_at("kraken", "BTC-USD", hi)

    class Suspect:
        bids, asks = real.bids, real.asks
        mid, spread, at_ns = real.mid, real.spread, real.at_ns
        suspect = True

    monkeypatch.setattr(archive, "book_at", lambda *a, **k: Suspect())
    assert "[SUSPECT]" in archive.plot_book("kraken", "BTC-USD", hi).get_title()


def test_it_returns_the_axes_so_it_composes(kraken_archive):
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots()
    archive = tickvault.open(kraken_archive)
    _, hi = archive.span_ns("kraken", "BTC-USD")
    returned = archive.plot_book("kraken", "BTC-USD", hi, ax=ax)
    assert returned is ax
