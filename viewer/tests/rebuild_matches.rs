//! The browser and the command line must rebuild the same book.
//!
//! The viewer exists to demonstrate reconstruction, so a viewer that quietly
//! disagreed with the engine would be worse than no viewer at all. These build
//! a real archive from a captured tape, then hold the wasm-facing `Viewer` to
//! the same answer the library gives.

use std::sync::Arc;

use tempfile::TempDir;
use tickvault::book::replay::BookReplayer;
use tickvault::clock::{Clock, ManualClock};
use tickvault::recorder::RawTape;
use tickvault::session::BookSession;
use tickvault::store::rows::{Row, decode, split_messages};
use tickvault::store::writer::{ArchiveWriter, WriterConfig};
use tickvault::types::{BookLevel, Symbol, VenueId};
use tickvault_viewer::Viewer;

const FEED_DEPTH: Option<usize> = Some(10);

/// A real Kraken archive, replayed from the tape committed in the repo.
fn build(root: &std::path::Path) -> (Symbol, u32) {
    let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let tape = RawTape::read_payloads(
        repo.join("tests/fixtures/kraken_book.jsonl"),
        VenueId::Kraken,
        1_577_836_800_000_000_000,
    )
    .expect("read tape");

    let clock: Arc<dyn Clock> = Arc::new(ManualClock::default());
    let http = tickvault::transport::ReqwestFetch::shared().expect("http");
    let config = tickvault::venue::registry::VenueConfig {
        symbols: vec![Symbol::new("BTC", "USD")],
        kraken_precision: Some(tickvault::venue::kraken::Precision { price: 1, qty: 8 }),
        ..Default::default()
    };
    let venue = tickvault::venue::registry::build_offline(VenueId::Kraken, &config, http, clock)
        .expect("build venue");

    let symbol = Symbol::new("BTC", "USD");
    let mut session = BookSession::new(Arc::clone(&venue), vec![symbol.clone()]);
    session.enable_archive(16);
    let mut writer = ArchiveWriter::open(WriterConfig {
        max_rows_per_file: 32,
        ..WriterConfig::new(root)
    })
    .expect("open archive");

    for frame in &tape.frames {
        let _ = session.ingest(&frame.to_raw());
        for (key, batch, span) in session.take_archive_batches() {
            writer.write(&key, &batch, span).expect("write");
        }
    }
    for (key, batch, span) in session.flush_archive() {
        writer.write(&key, &batch, span).expect("write");
    }
    writer.close().expect("close");

    let digest = session
        .book(&symbol)
        .map(|b| b.digest())
        .unwrap_or_default();
    (symbol, digest)
}

/// Every Parquet file in the archive, in the order the manifest lists them.
fn files_in_order(root: &std::path::Path) -> Vec<Vec<u8>> {
    let reader = tickvault::store::reader::ArchiveReader::open(root).expect("open");
    reader
        .manifest()
        .files()
        .iter()
        .map(|f| std::fs::read(root.join(&f.path)).expect("read file"))
        .collect()
}

fn loaded(root: &std::path::Path, symbol: &Symbol) -> Viewer {
    let mut viewer = Viewer::new(&symbol.to_string(), 2, FEED_DEPTH).expect("viewer");
    for bytes in files_in_order(root) {
        viewer.add_file(&bytes).expect("add file");
    }
    viewer.seal();
    viewer
}

fn all_rows(root: &std::path::Path) -> Vec<Row> {
    let mut rows = Vec::new();
    for bytes in files_in_order(root) {
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            bytes::Bytes::from(bytes),
        )
        .expect("builder")
        .build()
        .expect("reader");
        for batch in reader {
            rows.extend(decode(&batch.expect("batch")).expect("decode"));
        }
    }
    rows
}

fn digest_of(json: &str) -> u32 {
    let v: serde_json::Value = serde_json::from_str(json).expect("json");
    v["digest"].as_u64().expect("digest") as u32
}

#[test]
fn the_viewer_ends_on_the_book_the_recorder_held() {
    let dir = TempDir::new().unwrap();
    let (symbol, recorded) = build(dir.path());
    let mut viewer = loaded(dir.path(), &symbol);
    let total = viewer.messages();
    let json = viewer.book_after(total, 1_000).expect("book");
    assert_eq!(
        digest_of(&json),
        recorded,
        "the browser rebuilt a different book than the recorder held"
    );
}

#[test]
fn every_prefix_matches_a_fresh_replay_of_that_prefix() {
    let dir = TempDir::new().unwrap();
    let (symbol, _) = build(dir.path());
    let rows = all_rows(dir.path());
    let messages: Vec<&[Row]> = split_messages(&rows).collect();
    let mut viewer = loaded(dir.path(), &symbol);

    for n in 0..=messages.len() {
        let mut fresh = BookReplayer::new(symbol.clone(), BookLevel::L2, FEED_DEPTH);
        for m in messages.iter().take(n) {
            fresh.apply_message(m);
        }
        let expected = fresh.book().digest();
        let got = digest_of(&viewer.book_after(n, 1_000).expect("book"));
        assert_eq!(got, expected, "prefix of {n} messages disagreed");
    }
}

#[test]
fn scrubbing_backwards_lands_where_scrubbing_forwards_did() {
    // A delta carries no inverse, so going back means rebuilding from a
    // snapshot. That path is the only logic the viewer adds, so it is the one
    // worth pinning.
    let dir = TempDir::new().unwrap();
    let (symbol, _) = build(dir.path());
    let mut viewer = loaded(dir.path(), &symbol);
    let total = viewer.messages();
    assert!(total > 2, "tape produced only {total} messages");

    let forward: Vec<u32> = (0..=total)
        .map(|n| digest_of(&viewer.book_after(n, 1_000).expect("book")))
        .collect();

    let mut backward = vec![0u32; total + 1];
    for n in (0..=total).rev() {
        backward[n] = digest_of(&viewer.book_after(n, 1_000).expect("book"));
    }
    assert_eq!(forward, backward, "direction of travel changed the book");
}

#[test]
fn asking_by_timestamp_agrees_with_asking_by_message_count() {
    let dir = TempDir::new().unwrap();
    let (symbol, _) = build(dir.path());
    let mut viewer = loaded(dir.path(), &symbol);
    let total = viewer.messages();

    for n in 1..=total {
        let at = viewer.message_at(n - 1).expect("stamp");
        let by_time = digest_of(&viewer.book_at(&at, 1_000).expect("book"));
        let by_count = digest_of(&viewer.book_after(n, 1_000).expect("book"));
        assert_eq!(by_time, by_count, "message {n} at {at}");
    }
}

#[test]
fn the_fixture_actually_produces_a_book_worth_comparing() {
    // Guards the tests above from passing vacuously: an empty book compared to
    // an empty book agrees about nothing.
    let dir = TempDir::new().unwrap();
    let (symbol, recorded) = build(dir.path());
    let mut viewer = loaded(dir.path(), &symbol);
    assert!(
        viewer.messages() >= 3,
        "only {} messages",
        viewer.messages()
    );
    assert!(viewer.rows() >= 20, "only {} rows", viewer.rows());
    assert_ne!(recorded, 0, "recorded digest is zero");

    let json = viewer.book_after(viewer.messages(), 1_000).expect("book");
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert!(v["bids"].as_array().unwrap().len() >= 5, "{json}");
    assert!(v["asks"].as_array().unwrap().len() >= 5, "{json}");
    assert!(v["mid"].as_f64().unwrap() > 0.0);
    // The feed is ten deep and the rebuild must not grow past it.
    assert!(v["bid_levels"].as_u64().unwrap() <= 10, "{json}");
    assert!(v["ask_levels"].as_u64().unwrap() <= 10, "{json}");
}
