"""Full-depth crypto order book archives, honest about their own gaps.

The point of this package is that almost nobody doing a backtest wants to learn
Rust to read a Parquet file. Everything below delegates to the same code the
recorder runs; this layer only makes it feel like Python.

Getting to a plot::

    import tickvault
    archive = tickvault.open("./archive")
    book = archive.book_at("kraken", "BTC-USD", "2026-08-25T12:00:00Z")
    archive.plot_book("kraken", "BTC-USD", "2026-08-25T12:00:00Z")

Two conventions worth knowing:

**`None` means the data cannot answer, never zero.** A one-sided book has no
mid. An empty book has no imbalance. An aggregated feed does not know what
traded, so ``volume`` is null on five of the six venues; only an order-by-order
feed reports executions.

**Exact where it matters.** The archive holds prices as exact integers at 1e-9
and :meth:`Book.to_arrow` hands them back that way. The float properties and the
bar tables are conveniences, because a bar's mid is already a truncated midpoint
and asking someone to divide by a billion before plotting is how a library goes
unused.
"""

from __future__ import annotations

import datetime as _dt

from typing import TYPE_CHECKING, Any, Iterator, Literal, Sequence

from . import _native
from ._native import Book, Partition, Replay, Tick, capabilities, venues

if TYPE_CHECKING:  # pragma: no cover
    import pyarrow

__all__ = [
    "Archive",
    "Book",
    "Partition",
    "Replay",
    "Tick",
    "capabilities",
    "isoformat",
    "open",
    "to_pandas",
    "to_polars",
    "venues",
    "__version__",
]

__version__: str = _native.__version__

Frame = Literal["arrow", "polars", "pandas"]


def to_polars(batch: "pyarrow.RecordBatch"):
    """Hand an Arrow batch to polars without copying it."""
    import polars as pl

    return pl.from_arrow(batch)


def to_pandas(batch: "pyarrow.RecordBatch"):
    """Hand an Arrow batch to pandas.

    Not zero copy: pandas has no Arrow-native column type for everything here,
    so this materialises. Use :func:`to_polars` when that matters.
    """
    return batch.to_pandas()


def _convert(batch, frame: Frame):
    if frame == "arrow":
        return batch
    if frame == "polars":
        return to_polars(batch)
    if frame == "pandas":
        return to_pandas(batch)
    raise ValueError(f"frame must be 'arrow', 'polars' or 'pandas', not {frame!r}")


class Archive:
    """An archive on disk.

    Args:
        path: directory written by ``tickvault record --archive``.
    """

    def __init__(self, path) -> None:
        self._inner = _native.Archive(str(path))

    # -- what is in it -----------------------------------------------------

    @property
    def path(self) -> str:
        """Where this archive lives on disk."""
        return self._inner.path

    def partitions(self) -> list[Partition]:
        """Every venue, symbol and day the archive holds."""
        return self._inner.partitions()

    def symbols(self) -> list[tuple[str, str]]:
        """``(venue, symbol)`` pairs present, deduplicated."""
        seen = {(p.venue, p.symbol) for p in self.partitions()}
        return sorted(seen)

    def span_ns(self, venue: str, symbol: str) -> tuple[int, int]:
        """First and last receipt timestamp, in nanoseconds since the epoch."""
        return self._inner.span_ns(venue, symbol)

    def verify(self) -> dict[str, Any]:
        """Open every file the archive vouches for and confirm it reads back.

        Reads the pages rather than trusting the footer's row count.
        """
        return self._inner.verify()

    # -- reading it --------------------------------------------------------

    def book_at(self, venue: str, symbol: str, at, depth: int | None = None) -> Book:
        """The book as it stood at an instant.

        Args:
            at: an RFC 3339 string or nanoseconds since the epoch.
            depth: keep only this many levels a side. Applied to the result,
                never during the replay, so a level pushed out of the window
                and updated later still comes back.
        """
        return self._inner.book_at(venue, symbol, at, depth)

    def bars(
        self,
        venue: str,
        symbol: str,
        bar_seconds: float = 60.0,
        start=None,
        end=None,
        depth: int = 10,
        frame: Frame = "arrow",
    ):
        """Bars over a range.

        Open, high, low and close are the mid price sampled from the book after
        every message, not resampled from other bars.

        ``volume`` is null on an aggregated feed: such a feed shows a level
        shrinking and never says whether it traded or was cancelled, so zero
        would be a claim the data does not support.

        Args:
            frame: ``"arrow"``, ``"polars"`` or ``"pandas"``.
        """
        batch = self._inner.bars(venue, symbol, bar_seconds, start, end, depth)
        return _convert(batch, frame)

    def replay(
        self,
        venue: str,
        symbol: str,
        speed: float = 0.0,
        start=None,
        end=None,
        depth: int | None = None,
    ) -> Replay:
        """Replay a range, message by message.

        ``speed`` of zero runs as fast as the archive reads, which is what a
        backtest wants. Any other value preserves the recorded gaps, scaled, so
        a burst that arrived in one millisecond is delivered as one millisecond
        of work. That is the difference between testing whether a strategy
        works and whether it can keep up.

        ``start`` positions the book rather than filtering it. The archive is
        rebuilt to exactly that instant first, so the message that landed on it
        is already reflected in ``replay.book`` and is not delivered again;
        ticks begin with the next one. Re-delivering it would apply the same
        order-by-order event twice.

        Returns an iterator of :class:`Tick`::

            for tick in archive.replay("kraken", "BTC-USD"):
                if tick.mid is not None:
                    ...
        """
        return self._inner.replay(venue, symbol, speed, start, end, depth)

    def ticks(self, venue: str, symbol: str, **kwargs) -> Iterator[Tick]:
        """:meth:`replay` as a plain generator, for when that reads better."""
        yield from self.replay(venue, symbol, **kwargs)

    # -- looking at it -----------------------------------------------------

    def plot_book(
        self,
        venue: str,
        symbol: str,
        at,
        depth: int = 20,
        ax=None,
        show: bool = False,
    ):
        """Plot the book at an instant as a depth chart.

        Needs ``matplotlib``; install with ``pip install tickvault[plot]``.
        Returns the axes so it composes with whatever else you are drawing.
        """
        import matplotlib.pyplot as plt

        book = self.book_at(venue, symbol, at, depth=depth)
        if ax is None:
            _, ax = plt.subplots(figsize=(9, 5))

        # Both sides arrive best-first, so bids descend in price and asks
        # ascend. A depth chart accumulates outward from the mid, which makes
        # the two sides mirror images: on bids the running total belongs to the
        # interval left of each price ("pre"), on asks to the interval right of
        # it ("post"). Plotting both the same way is what makes a depth chart
        # come out with the bid wall stranded away from the spread.
        for levels, colour, label, step in (
            (book.bids, "tab:green", "bids", "pre"),
            (book.asks, "tab:red", "asks", "post"),
        ):
            if not levels:
                continue
            cumulative: list[float] = []
            running = 0.0
            for _, qty in levels:
                running += qty
                cumulative.append(running)
            prices = [price for price, _ in levels]
            if step == "pre":
                # Ascending x, so the outward end of the wall comes first.
                prices, cumulative = prices[::-1], cumulative[::-1]
            # The outermost level has no neighbour to step towards, so without
            # an edge point the deepest level of each side goes undrawn.
            # A one-sided book has no spread, so fall back to the side's own
            # width; a single level has neither, so use a hair off the price.
            pad = book.spread or (prices[-1] - prices[0]) / max(len(prices), 1) or prices[0] * 1e-5
            edge = prices[0] - pad if step == "pre" else prices[-1] + pad
            if step == "pre":
                prices, cumulative = [edge, *prices], [cumulative[0], *cumulative]
            else:
                prices, cumulative = [*prices, edge], [*cumulative, cumulative[-1]]
            ax.step(prices, cumulative, where=step, color=colour, label=label)
            ax.fill_between(prices, cumulative, step=step, alpha=0.25, color=colour)

        if book.mid is not None:
            ax.axvline(book.mid, color="0.4", linestyle="--", linewidth=1)
        title = f"{venue} {symbol} at {isoformat(book.at_ns)}"
        if book.suspect:
            # Never quietly: a book built over a gap has to say so on the chart.
            title += "  [SUSPECT]"
        ax.set_title(title)
        ax.set_xlabel("price")
        ax.set_ylabel("cumulative quantity")
        ax.legend()
        if show:
            plt.show()
        return ax

    def __repr__(self) -> str:
        return repr(self._inner)


def isoformat(at_ns: int) -> str:
    """Render a nanosecond instant as RFC 3339, keeping all nine digits.

    ``datetime`` stops at microseconds, so converting to one would quietly
    discard the last three digits of every timestamp in the archive. Feeds
    routinely put several messages inside one microsecond, and the whole point
    of recording receipt time is to tell them apart, so this formats the
    integer directly rather than handing back a lossy object.
    """
    seconds, nanos = divmod(int(at_ns), 1_000_000_000)
    stamp = _dt.datetime.fromtimestamp(seconds, _dt.timezone.utc)
    return f"{stamp.strftime('%Y-%m-%dT%H:%M:%S')}.{nanos:09d}Z"


def open(path) -> Archive:  # noqa: A001 - deliberately shadows the builtin here
    """Open an archive. ``tickvault.open("./archive")``."""
    return Archive(path)
