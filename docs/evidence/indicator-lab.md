# Indicator Lab evidence

The experiment layer accepts only versioned dataset references and explicit UTC instants. It normalizes equivalent offsets, rejects overlapping chronological splits, requires an embargo at least as long as the label horizon, and rejects sampling-grid misalignment.

Coverage is conservative. A clean archive snapshot is `observed_unverifiable` until capture capability metadata is independently attested. A suspect interval stays `invalid` until a later clean snapshot rebuilds the book. Rows before the first establishing snapshot are `unknown`; no row is converted to a zero feature.

`examples/imbalance-baseline.toml` is a starting specification. It is not a result and contains no performance claim. Use `tickvault dataset` to freeze an archive first, then `tickvault experiment-validate` with a JSON mapping from dataset reference to manifest digest.

The current archive format does not persist adapter version and validation-capability attestations, so verified status and exact truncation-to-grid attribution remain explicit limitations.
