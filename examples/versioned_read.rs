//! A consumer that checks what it is reading before it believes the numbers.
//!
//! Usage: `cargo run --example versioned_read -- <archive-root>`
//!
//! The archive is a published dataset, so a reader is not guaranteed to be the
//! same version as the writer. Three things can differ, and they need three
//! different reactions:
//!
//! - **Extra columns.** A file written by a newer recorder carries columns this
//!   build has never heard of. Read the ones you know and ignore the rest;
//!   there is nothing to do about it and nothing wrong with it.
//! - **Missing columns.** A file written by an older recorder lacks columns
//!   that exist now. They mean "this feed could not say", which is a real
//!   answer, not a defect. `qty` going missing is a different matter, and the
//!   decoder refuses that rather than defaulting it.
//! - **A different price scale.** The one that has to stop the read. `price` is
//!   an `Int64` at every scale, so a file at 1e-6 read as 1e-9 hands back a
//!   number a thousand times too small with no null, no error, and nothing to
//!   notice. Verification refuses it; this is what refusing looks like from a
//!   consumer's side.
//!
//! The pattern to copy is the order: verify, then check the scale you are about
//! to divide by, then read. Not the other way round.

use std::collections::BTreeSet;

use tickvault::fixed::SCALE;
use tickvault::store::reader::{self, ArchiveReader};
use tickvault::store::rows::decode;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::args()
        .nth(1)
        .ok_or("usage: versioned_read <archive-root>")?;
    let archive = ArchiveReader::open(&root)?;

    // 1. Verification first. It reads the pages rather than the footer, and it
    //    is where a mislabelled file or an unreadable scale is refused.
    let report = archive.verify();
    println!("{report}");
    if !report.is_clean() {
        return Err("archive did not verify; refusing to read it".into());
    }

    // 2. What scale are we about to divide by? Taken from the file, not
    //    assumed. A file that states nothing is from before the declaration
    //    existed and is read at this build's scale, which is what it was.
    let mut scales = BTreeSet::new();
    let mut unstated = 0usize;
    for record in archive.files() {
        match reader::inspect(archive.path_of(record))?.price_scale {
            Some(scale) => {
                scales.insert(scale);
            }
            None => unstated += 1,
        }
    }
    if let Some(&other) = scales.iter().find(|s| **s != SCALE) {
        return Err(
            format!("archive holds prices at 1e-{other}; this build reads 1e-{SCALE}").into(),
        );
    }
    println!(
        "price scale 1e-{SCALE} on {} file(s), unstated on {unstated}",
        scales.len().min(1) * (archive.files().len() - unstated)
    );

    // 3. Now read. The decoder tolerates a file with more columns than this
    //    build knows and one with fewer, so neither needs a branch here.
    let divisor = 10f64.powi(SCALE as i32);
    let mut rows = 0u64;
    let mut best: Option<(i64, f64)> = None;
    for record in archive.files() {
        for batch in reader::read_batches(archive.path_of(record))? {
            for row in decode(&batch)? {
                rows += 1;
                if row.suspect {
                    // The column exists so this is one line rather than a join
                    // against the gap report.
                    continue;
                }
                if row.side == tickvault::types::Side::Bid && !row.qty.is_zero() {
                    let price = row.price.mantissa() as f64 / divisor;
                    if best.is_none_or(|(_, b)| price > b) {
                        best = Some((row.recv_wall, price));
                    }
                }
            }
        }
    }

    match best {
        Some((at, price)) => println!("{rows} rows; highest trustworthy bid {price} at {at}"),
        None => println!("{rows} rows; no trustworthy two-sided bid in the archive"),
    }
    Ok(())
}
