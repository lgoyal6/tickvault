"""The iterator should feel like Python, and stream rather than materialise."""

from __future__ import annotations

import itertools

import pytest

import tickvault


def test_replay_is_an_iterator_not_a_list(kraken_archive):
    replay = tickvault.open(kraken_archive).replay("kraken", "BTC-USD")
    assert iter(replay) is replay
    first = next(replay)
    assert isinstance(first, tickvault.Tick)
    # It picks up where it left off rather than restarting, which is what
    # distinguishes an iterator from something that merely supports `for`.
    second = next(replay)
    assert second.at_ns >= first.at_ns


def test_it_stops_by_raising_stopiteration(kraken_archive):
    replay = tickvault.open(kraken_archive).replay("kraken", "BTC-USD")
    count = sum(1 for _ in replay)
    assert count > 0
    with pytest.raises(StopIteration):
        next(replay)


def test_it_composes_with_itertools(kraken_archive):
    archive = tickvault.open(kraken_archive)
    head = list(itertools.islice(archive.replay("kraken", "BTC-USD"), 3))
    assert len(head) == 3
    assert [t.at_ns for t in head] == sorted(t.at_ns for t in head)


def test_taking_the_first_few_does_not_read_every_file(kraken_archive):
    """Laziness is the point: a consumer must never hold a day in memory."""
    replay = tickvault.open(kraken_archive).replay("kraken", "BTC-USD")
    next(replay)
    assert replay.delivered == 1
    assert replay.files_opened <= 1


def test_ticks_is_the_same_stream_by_another_name(kraken_archive):
    archive = tickvault.open(kraken_archive)
    a = [t.at_ns for t in archive.ticks("kraken", "BTC-USD")]
    b = [t.at_ns for t in archive.replay("kraken", "BTC-USD")]
    assert a == b


def test_the_book_travels_with_the_replay(kraken_archive):
    replay = tickvault.open(kraken_archive).replay("kraken", "BTC-USD")
    for _ in replay:
        pass
    book = replay.book
    assert book.bids or book.asks
    _, hi = tickvault.open(kraken_archive).span_ns("kraken", "BTC-USD")
    # Running the stream to its end must land on the same book that asking for
    # the final instant directly produces.
    direct = tickvault.open(kraken_archive).book_at("kraken", "BTC-USD", hi)
    assert book.digest == direct.digest


def test_a_time_window_narrows_the_stream(kraken_archive):
    archive = tickvault.open(kraken_archive)
    lo, hi = archive.span_ns("kraken", "BTC-USD")
    everything = [t.at_ns for t in archive.replay("kraken", "BTC-USD")]
    window = [t.at_ns for t in archive.replay("kraken", "BTC-USD", end=hi)]
    assert window == everything

    later = [t.at_ns for t in archive.replay("kraken", "BTC-USD", start=everything[0])]
    # start positions the book rather than filtering the stream: the message at
    # that instant is already folded into the starting book, and delivering it
    # again would apply the same L3 event twice.
    assert later == everything[1:]


def test_start_leaves_the_book_where_the_stream_begins(kraken_archive):
    archive = tickvault.open(kraken_archive)
    stamps = [t.at_ns for t in archive.replay("kraken", "BTC-USD")]
    resumed = archive.replay("kraken", "BTC-USD", start=stamps[0])
    assert resumed.book.digest == archive.book_at("kraken", "BTC-USD", stamps[0]).digest


def test_wall_clock_replay_actually_waits(bitstamp_archive):
    """speed=1 must preserve the recorded spacing, or it is not a replay."""
    import time

    archive = tickvault.open(bitstamp_archive)
    lo, hi = archive.span_ns("bitstamp", "BTC-USD")
    span_seconds = (hi - lo) / 1e9
    assert span_seconds > 0.05, "this tape is too short to time"

    started = time.perf_counter()
    for _ in archive.replay("bitstamp", "BTC-USD", speed=1.0):
        pass
    paced = time.perf_counter() - started

    started = time.perf_counter()
    for _ in archive.replay("bitstamp", "BTC-USD", speed=0.0):
        pass
    unpaced = time.perf_counter() - started

    assert paced >= span_seconds * 0.5, f"{paced}s for a {span_seconds}s tape"
    assert unpaced < paced


def test_a_timestamp_may_be_a_string_or_nanoseconds(kraken_archive):
    archive = tickvault.open(kraken_archive)
    _, hi = archive.span_ns("kraken", "BTC-USD")
    by_int = archive.book_at("kraken", "BTC-USD", hi)
    by_str = archive.book_at("kraken", "BTC-USD", tickvault.isoformat(hi))
    assert by_int.digest == by_str.digest


def test_a_nonsense_timestamp_says_so(kraken_archive):
    archive = tickvault.open(kraken_archive)
    with pytest.raises(ValueError, match="RFC 3339"):
        archive.book_at("kraken", "BTC-USD", "last tuesday")
