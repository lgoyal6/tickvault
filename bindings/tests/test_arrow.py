"""Arrow handoff, and the exactness that survives it."""

from __future__ import annotations

import pyarrow as pa
import pytest

import tickvault

polars = pytest.importorskip("polars")
pandas = pytest.importorskip("pandas")


def test_book_to_arrow_keeps_prices_exact(kraken_archive):
    archive = tickvault.open(kraken_archive)
    _, hi = archive.span_ns("kraken", "BTC-USD")
    book = archive.book_at("kraken", "BTC-USD", hi)
    batch = book.to_arrow()

    assert isinstance(batch, pa.RecordBatch)
    # Exact integers counting 1e-9 units. A float64 cannot hold every price a
    # venue can quote, and Kraken validates its own book by CRC32 over the
    # price *strings*, so rounding here would make the archive unverifiable.
    assert batch.schema.field("price").type == pa.int64()
    assert batch.schema.field("qty").type == pa.int64()

    best_bid_price, _ = book.best_bid
    side = batch.column("side").to_pylist()
    price = batch.column("price").to_pylist()
    bids = [p for p, s in zip(price, side) if s == "bid"]
    assert max(bids) / 10**9 == pytest.approx(best_bid_price)


def test_arrow_batch_matches_the_book_level_for_level(kraken_archive):
    archive = tickvault.open(kraken_archive)
    _, hi = archive.span_ns("kraken", "BTC-USD")
    book = archive.book_at("kraken", "BTC-USD", hi)
    batch = book.to_arrow()
    assert batch.num_rows == len(book.bids) + len(book.asks)


def test_bars_reach_polars_and_pandas_with_the_same_numbers(kraken_archive):
    archive = tickvault.open(kraken_archive)
    pl_frame = archive.bars("kraken", "BTC-USD", bar_seconds=1, frame="polars")
    pd_frame = archive.bars("kraken", "BTC-USD", bar_seconds=1, frame="pandas")
    arrow = archive.bars("kraken", "BTC-USD", bar_seconds=1, frame="arrow")

    assert isinstance(pl_frame, polars.DataFrame)
    assert isinstance(pd_frame, pandas.DataFrame)
    assert isinstance(arrow, pa.RecordBatch)
    assert len(pl_frame) == len(pd_frame) == arrow.num_rows > 0
    assert pl_frame["close"].to_list() == pytest.approx(list(pd_frame["close"]))


def test_polars_conversion_is_zero_copy(kraken_archive):
    """Polars is backed by Arrow, so the buffers should be shared, not copied."""
    archive = tickvault.open(kraken_archive)
    arrow = archive.bars("kraken", "BTC-USD", bar_seconds=1, frame="arrow")
    frame = polars.from_arrow(arrow)
    assert frame.estimated_size() > 0
    # The columns survive the handoff with their Arrow types rather than being
    # rebuilt as Python objects.
    assert frame.schema["close"] == polars.Float64
    assert frame.schema["start"].time_unit == "ns"


def test_an_unknown_frame_name_is_rejected(kraken_archive):
    archive = tickvault.open(kraken_archive)
    with pytest.raises(ValueError, match="frame"):
        archive.bars("kraken", "BTC-USD", bar_seconds=1, frame="dask")
