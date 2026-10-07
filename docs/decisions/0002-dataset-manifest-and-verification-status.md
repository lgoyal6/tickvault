# 0002: Immutable dataset manifests and the five verification states

Status: accepted, 2026-10-06

## Context

The archive already has a durable index, `_manifest.jsonl`, which records which
Parquet files are complete. It is append only, it changes every time the
recorder rotates a file, compaction retires files, and retention removes them.
That is right for a live archive and wrong as the input to an experiment: a
result has to name the exact bytes it read, and those bytes must not be able to
change underneath it.

The coverage grid already distinguishes four states (`absent`, `suspect`,
`clean`, `unverifiable`). An experiment needs one more: a stretch where data
exists and nothing is flagged, but the book could not have been established,
because no snapshot had arrived yet. The rows there are deltas against a book
nobody saw.

## Decision

### Dataset manifest

A dataset is a frozen, named selection of an archive:
`<archive>/_datasets/<id>@v<version>.json`. It holds the fields the build
prompt lists: id and version, venue and symbol per partition, time coverage,
schema version, every file with its sha256 and size, the price and quantity
scale, the validator and adapter versions, clock metadata, a coverage and
verification summary, known blind spots, and the command that created it.

- **Immutable.** Publishing writes to a temporary file and renames it into
  place only if no manifest of that id and version exists. Republishing
  identical content is a no-op; republishing different content under the same
  version is refused. A new selection is a new version.
- **Self-hashed.** `manifest_sha256` is computed over the canonical JSON of the
  manifest with that one field blanked, so an edited manifest is detected
  without a side file.
- **Verifiability is frozen at creation.** Whether a venue can detect loss is
  read from the capability matrix in the capture half when the manifest is
  built, and written into the manifest per partition. The read half, which has
  no venue code, reads it back. A reader never decides on its own that a venue
  is verifiable.
- **Reads verify first.** `VerifiedDataset::open` recomputes the manifest hash,
  hashes every file it names, and checks the archive still lists each file and
  has not retired it. Only an opened `VerifiedDataset` can produce rows. There
  is no unchecked read path in the lab.

### The five states

| state | meaning | used for research |
|---|---|---|
| `verified` | data present, the venue publishes a loss check, the recorder applied it and flagged nothing, and the book was established from a snapshot | yes |
| `observed_unverifiable` | data present and unflagged, but the venue publishes nothing to check against (Bitstamp at L2) | no, unless an experiment opts in, and then it is reported |
| `invalid` | the recorder flagged the rows (`suspect`), from the flagged message until the next clean snapshot rebuilds the book | no |
| `missing` | nothing recorded: outside the files, inside a recorded truncation, or a silence longer than the recorder's idle timeout | no |
| `unknown` | rows exist but the book they modify was never established, because no snapshot preceded them | no |

Rules that follow from the honesty rules in the README:

- Unknown is never encoded as zero. A feature at an instant that is not
  `verified` is `null`, and the reason is counted.
- An `invalid` stretch ends only at a clean snapshot. The recorder rebuilds
  from a snapshot after a gap, so that is the first instant the book is known
  again; nothing earlier is upgraded.
- A silence longer than the idle timeout (60 s, the recorder's own) is
  `missing` for its whole length, not for the part past 60 s. The archive
  cannot say when the feed actually died.
- Nothing is ever proven safe after the fact. The prompt allows a suspect
  interval back in "if the validator proves it safe"; no validator here can,
  because a flagged message's correct content is not in the archive.

## Consequences

- `tickvault query --dataset ... --verified-only` refuses to return anything
  outside `verified` and prints the exclusion summary instead.
- Kraken's checksum covers ten levels a side. A Kraken window is `verified` in
  the sense above, and the manifest's blind spots say that loss beyond level
  ten is undetectable. Features deeper than ten levels on Kraken inherit that
  blind spot, and the report repeats it.
- A dataset is bound to an archive by relative paths and hashes, so moving the
  archive keeps it valid and editing a file breaks it.
