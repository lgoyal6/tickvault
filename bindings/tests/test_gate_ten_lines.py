"""The phase gate: the README's snippet, executed exactly as it is written.

The bar the build prompt set was "if someone cannot reach a plot in ten lines,
they will not use it". An example in a README is a claim, and an unexecuted
claim rots. So this reads the snippet out of the shipped README, counts it, and
runs it. The only thing substituted is the archive path, because the reader's
archive is not this test's archive.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import tickvault

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")

README = Path(__file__).resolve().parents[1] / "README.md"
LIMIT = 10


def snippet() -> str:
    blocks = re.findall(r"```python\n(.*?)```", README.read_text(), re.S)
    assert blocks, "the README has no Python block"
    return blocks[0]


def test_the_snippet_is_within_ten_lines():
    code = [line for line in snippet().splitlines() if line.strip()]
    assert len(code) <= LIMIT, f"{len(code)} lines:\n" + "\n".join(code)


def test_the_snippet_runs_and_produces_a_plot(kraken_archive, monkeypatch):
    import matplotlib.pyplot as plt

    shown: list[object] = []
    monkeypatch.setattr(plt, "show", lambda *a, **k: shown.append(plt.gcf()))

    printed: list[str] = []
    code = snippet().replace('"./archive"', repr(str(kraken_archive)))
    exec(  # noqa: S102 - running the README is the whole point of this gate
        compile(code, str(README), "exec"),
        {"print": lambda *a, **k: printed.append(" ".join(str(x) for x in a))},
    )

    # It printed bars, then the top of the book.
    assert len(printed) == 2
    assert "close" in printed[0]
    assert printed[1].count(" ") == 2, printed[1]

    # And it drew something with real levels in it, not an empty frame.
    assert shown, "plot_book(show=True) never reached plt.show"
    axes = shown[-1].axes[0]
    assert {"bids", "asks"} <= {line.get_label() for line in axes.lines}
    assert all(len(line.get_xdata()) > 1 for line in axes.lines if line.get_label() in {"bids", "asks"})
    assert tickvault.isoformat(
        tickvault.open(kraken_archive).span_ns("kraken", "BTC-USD")[1]
    ) in axes.get_title()


def test_the_snippet_only_needs_what_the_install_provides():
    """No import beyond the package itself, or ten lines is a lie."""
    imported = re.findall(r"^\s*(?:import|from)\s+([\w.]+)", snippet(), re.M)
    assert set(imported) <= {"tickvault"}, imported
