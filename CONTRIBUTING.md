# Contributing to tickvault

Thanks for looking. tickvault records order books, and its whole claim is that
the resulting dataset is honest about its own holes. So the bar for anything
touching what the archive says about itself is deliberately high, and the bar
for everything else is normal.

## The contract you must not break

**Nothing may claim to be clean unless it was checked.** That is the sentence
the repo exists to defend, and most of the review of a change goes into whether
it quietly weakens one of these:

- **A detected gap stops the book.** Never stitch deltas across a hole. A
  stitched book looks continuous and is wrong, which is the exact failure that
  makes free order book data untrustworthy.
- **Unverifiable is not clean.** A message that could not be checked is counted
  apart from one that was checked and passed. Bitstamp's aggregated feed reports
  0% verified, and it is never rounded up to look like the others.
- **Null is not zero.** Where the data cannot answer, it returns nothing. A
  one-sided book has no mid. An aggregated feed does not know what traded.
  Writing a zero there is a claim, and a false one.
- **Our faults are labelled as ours.** Rows lost because the writer fell behind
  are counted separately from messages the venue never sent. Same for retention:
  data we chose to stop keeping is a different manifest entry from data we lost.
- **Never blame the venue for our own defect.** If our decimal rendering lost
  digits, or the book was shallower than the checksum covers, that message is
  unverifiable, not a gap.

If a change of yours makes a gate fail, the change is wrong, not the gate. Three
of the gates were written after a passing test turned out to be passing for the
wrong reason, which is why they assert properties rather than outputs.

## Getting oriented

| Path | What lives there |
|---|---|
| `src/venue/` | The `Venue` trait, six implementations, and the capability matrix that keeps the ingest loop free of special cases. |
| `src/session.rs` | The ingest loop. Exactly two branches on venue capabilities; everything else is data. |
| `src/gap.rs` | Suspect windows and the report they feed. The product. |
| `src/store/` | Parquet schema, writer, manifest, recovery, retention. |
| `src/reconstruct/`, `src/query/` | Rebuilding a book at an instant, and reading the archive. |
| `src/supervise.rs`, `src/status.rs` | Running as a service, and answering for it. |
| `docs/venues.md` | Per-venue findings, measured rather than read off a doc page. |
| `docs/schema.md` | Every column, and what null means in it. |
| `tests/gate_*.rs` | The gates. Each states a property. |
| `viewer/` | The reconstruction engine compiled to wasm, behind the demo page. |
| `bindings/` | The Python package. |

## Building and testing

```bash
cargo test                     # the suite
cargo test -- --ignored        # real SIGKILLs, plus one live venue reconciliation
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

CI runs all of that except the live reconciliation, which measures Bitstamp's two
feeds against each other and would make the build depend on a venue's afternoon.

The Python and wasm halves have their own commands:

```bash
cd bindings && maturin develop --release && pytest tests
scripts/build-viewer.sh && cd viewer && cargo test
```

## Adding a venue

This is meant to be mechanical rather than archaeological, and the conformance
suite in `tests/conformance.rs` is what makes it so. Implement `Venue`, declare
its capabilities honestly, add a captured fixture, and the suite will tell you
what is inconsistent.

Two things worth knowing before you start:

- **Measure the feed, do not read the docs.** Nearly every interesting line in
  `docs/venues.md` contradicts what the venue's documentation implies. Capture a
  tape with `tickvault record --out tape.jsonl` and look at it.
- **Declare what the venue cannot prove.** A venue with no sequencing gets a
  `MonotonicTimestamp` scheme and a blind spot entry, not a hopeful guess. The
  capability matrix is what keeps the honesty rules enforceable in one place.

## Style

Commits are one logical change each, buildable on their own, with a subject in
the imperative and a body explaining why rather than what. Several of the
existing messages record a measurement that overturned an assumption; that is
the useful kind.

No em dashes anywhere, in code, comments, docs or commit messages.

Comments explain why, not what. If a comment restates the line below it, delete
one of them.

## Publishing a number

Any figure in the README has to be reproducible by a command in the README.
`tickvault bench` backs the throughput claim, `tests/gate_*.rs` back the
correctness ones, and `docs/venues.md` says which machine and which capture each
measurement came from. A number without a way to check it does not belong here.
