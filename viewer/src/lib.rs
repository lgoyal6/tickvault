//! The reconstruction engine, compiled to wasm so the book rebuilds in the tab.
//!
//! This deliberately does not reimplement anything. It hands rows to the same
//! [`BookReplayer`] the recorder and the query layer use, splits them into
//! messages with the same [`split_messages`] rule, and truncates to the same
//! recorded feed depth. If the browser and the CLI ever disagreed about what
//! the book looked like at an instant, one of them would be lying, and the
//! whole point of the dataset is that neither does.
//!
//! # Why the browser holds rows rather than files
//!
//! [`tickvault::store::reader`] opens files, and a tab has no filesystem. So
//! the page fetches the Parquet bytes itself and passes them in, and this
//! decodes them with the same schema. What is skipped is file *selection* and
//! checkpointing, which is orchestration; what is kept is the part that decides
//! what the book actually was.

use serde::Serialize;
use tickvault::book::replay::BookReplayer;
use tickvault::store::rows::{Row, decode, split_messages};
use tickvault::store::schema::EventKind;
use tickvault::types::{BookLevel, Side, Symbol};
use wasm_bindgen::prelude::*;

/// Turn a panic into something readable in the console rather than
/// `unreachable executed`.
#[wasm_bindgen(start)]
pub fn start() {
    console_error_panic_hook::set_once();
}

fn js_err(e: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&e.to_string())
}

/// One price level, as the chart wants it.
#[derive(Serialize)]
struct Level {
    /// Exact 1e-9 mantissa, as a string: JSON numbers are doubles and these are
    /// not all representable as one. The page divides for display only.
    price: String,
    qty: String,
    /// Pre-divided for plotting, where a rounded double is harmless.
    p: f64,
    q: f64,
}

fn levels(from: &[(tickvault::Fixed, tickvault::Fixed)]) -> Vec<Level> {
    from.iter()
        .map(|(p, q)| Level {
            price: p.mantissa().to_string(),
            qty: q.mantissa().to_string(),
            p: p.to_f64_lossy(),
            q: q.to_f64_lossy(),
        })
        .collect()
}

/// The book at an instant, plus what the archive can and cannot vouch for.
#[derive(Serialize)]
struct BookView {
    at_ns: String,
    bids: Vec<Level>,
    asks: Vec<Level>,
    mid: Option<f64>,
    spread: Option<f64>,
    /// Levels held per side, before any display truncation.
    bid_levels: usize,
    ask_levels: usize,
    /// Messages applied to reach this instant, and rows within them.
    messages: u64,
    rows: u64,
    /// Rows the recorder could not vouch for, and the window they span.
    suspect_rows: u64,
    suspect: bool,
    suspect_from: Option<String>,
    suspect_to: Option<String>,
    /// The book's own fingerprint. Two rebuilds of one instant must match.
    digest: u32,
    /// How many messages were replayed to get here from the last reset.
    replayed_from: usize,
}

/// A scrubbable partition: one venue, one symbol, one day.
#[wasm_bindgen]
pub struct Viewer {
    symbol: Symbol,
    level: BookLevel,
    feed_depth: Option<usize>,
    rows: Vec<Row>,
    /// Row index where each message begins, plus a terminating length.
    starts: Vec<usize>,
    /// Message indices that begin a snapshot, so a rewind can start from one
    /// rather than from the beginning of the day. This is the same trick the
    /// checkpointing reconstructor uses, for the same reason.
    resets: Vec<usize>,
    replayer: BookReplayer,
    /// Messages applied so far.
    applied: usize,
    /// Where the current run of applications started, for the display.
    ran_from: usize,
}

#[wasm_bindgen]
impl Viewer {
    /// `feed_depth` is the window the venue's feed carried, taken from the
    /// manifest. A rebuild has to truncate exactly as the recorder did or it
    /// grows levels the feed had already dropped, and eventually crosses.
    #[wasm_bindgen(constructor)]
    pub fn new(symbol: &str, book_level: u8, feed_depth: Option<usize>) -> Result<Viewer, JsValue> {
        let symbol = Symbol::parse(symbol).map_err(js_err)?;
        let level = match book_level {
            3 => BookLevel::L3,
            _ => BookLevel::L2,
        };
        Ok(Viewer {
            replayer: BookReplayer::new(symbol.clone(), level, feed_depth),
            symbol,
            level,
            feed_depth,
            rows: Vec::new(),
            starts: Vec::new(),
            resets: Vec::new(),
            applied: 0,
            ran_from: 0,
        })
    }

    /// Add one Parquet file's bytes, in archive order. Returns rows decoded.
    ///
    /// Files must arrive in the order the manifest lists them. The archive is
    /// an append-only record of arrival, so out of order files would rebuild a
    /// book that never existed.
    pub fn add_file(&mut self, bytes: &[u8]) -> Result<usize, JsValue> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let before = self.rows.len();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes.to_vec()))
            .map_err(js_err)?
            .build()
            .map_err(js_err)?;
        for batch in reader {
            let batch = batch.map_err(js_err)?;
            self.rows.extend(decode(&batch).map_err(js_err)?);
        }
        Ok(self.rows.len() - before)
    }

    /// Index the rows into messages. Call once, after the last `add_file`.
    pub fn seal(&mut self) {
        self.starts.clear();
        self.resets.clear();
        let mut at = 0usize;
        for message in split_messages(&self.rows) {
            if message
                .first()
                .is_some_and(|r| r.event == EventKind::Snapshot)
            {
                self.resets.push(self.starts.len());
            }
            self.starts.push(at);
            at += message.len();
        }
        self.starts.push(at);
        self.reset();
    }

    /// Messages in the partition.
    pub fn messages(&self) -> usize {
        self.starts.len().saturating_sub(1)
    }

    pub fn rows(&self) -> usize {
        self.rows.len()
    }

    /// Receipt instant of message `i`, as a decimal string of nanoseconds.
    ///
    /// A string rather than a number because these are past 2^53, where a
    /// double starts skipping integers, and the page turns them back into
    /// BigInt.
    pub fn message_at(&self, i: usize) -> Option<String> {
        self.rows
            .get(*self.starts.get(i)?)
            .map(|r| r.recv_wall.to_string())
    }

    /// First and last receipt instants, as decimal nanosecond strings.
    pub fn span(&self) -> Vec<JsValue> {
        match (self.rows.first(), self.rows.last()) {
            (Some(a), Some(b)) => vec![
                JsValue::from_str(&a.recv_wall.to_string()),
                JsValue::from_str(&b.recv_wall.to_string()),
            ],
            _ => Vec::new(),
        }
    }

    /// The book after the last message at or before `at_ns`.
    ///
    /// `at_ns` is a decimal string for the same precision reason.
    pub fn book_at(&mut self, at_ns: &str, depth: usize) -> Result<String, JsValue> {
        let at: i64 = at_ns.parse().map_err(js_err)?;
        let count = self.messages_through(at);
        self.apply_through(count);
        self.render(at, depth)
    }

    /// The book after exactly `count` messages, which is what the scrubber
    /// steps through when someone drags it one message at a time.
    pub fn book_after(&mut self, count: usize, depth: usize) -> Result<String, JsValue> {
        let count = count.min(self.messages());
        self.apply_through(count);
        let at = if count == 0 {
            self.rows.first().map(|r| r.recv_wall).unwrap_or(0)
        } else {
            self.rows[self.starts[count - 1]].recv_wall
        };
        self.render(at, depth)
    }
}

impl Viewer {
    /// How many messages have a receipt instant at or before `at`.
    fn messages_through(&self, at: i64) -> usize {
        let total = self.messages();
        let mut lo = 0usize;
        let mut hi = total;
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.rows[self.starts[mid]].recv_wall <= at {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    fn reset(&mut self) {
        self.replayer = BookReplayer::new(self.symbol.clone(), self.level, self.feed_depth);
        self.applied = 0;
        self.ran_from = 0;
    }

    /// Bring the book to exactly `count` messages.
    ///
    /// Forwards is cheap: keep applying. Backwards cannot be undone, because a
    /// delta carries no inverse, so it restarts from the nearest snapshot at or
    /// before the target rather than from the beginning of the partition.
    fn apply_through(&mut self, count: usize) {
        if count < self.applied {
            let from = self
                .resets
                .iter()
                .rev()
                .copied()
                .find(|&r| r <= count)
                .unwrap_or(0);
            self.reset();
            self.applied = from;
            self.ran_from = from;
        }
        while self.applied < count {
            let start = self.starts[self.applied];
            let end = self.starts[self.applied + 1];
            let message: Vec<Row> = self.rows[start..end].to_vec();
            self.replayer.apply_message(&message);
            self.applied += 1;
        }
    }

    fn render(&mut self, at: i64, depth: usize) -> Result<String, JsValue> {
        let depth = depth.max(1);
        let suspect_span = self.replayer.suspect_span();
        let messages = self.replayer.messages();
        let rows = self.replayer.rows();
        let suspect_rows = self.replayer.suspect_rows();
        let book = self.replayer.book_ref();
        let bids = book.top(Side::Bid, depth);
        let asks = book.top(Side::Ask, depth);
        let bid_levels = book.level_count(Side::Bid);
        let ask_levels = book.level_count(Side::Ask);
        let best_bid = book.best_bid().map(|(p, _)| p.to_f64_lossy());
        let best_ask = book.best_ask().map(|(p, _)| p.to_f64_lossy());
        let digest = book.digest();

        let view = BookView {
            at_ns: at.to_string(),
            bids: levels(&bids),
            asks: levels(&asks),
            // Only where both sides exist. Half a book has no middle, and
            // inventing one would put a price on the chart that never was.
            mid: match (best_bid, best_ask) {
                (Some(b), Some(a)) => Some((b + a) / 2.0),
                _ => None,
            },
            spread: match (best_bid, best_ask) {
                (Some(b), Some(a)) => Some(a - b),
                _ => None,
            },
            bid_levels,
            ask_levels,
            messages,
            rows,
            suspect_rows,
            suspect: suspect_rows > 0,
            suspect_from: suspect_span.map(|(a, _)| a.to_string()),
            suspect_to: suspect_span.map(|(_, b)| b.to_string()),
            digest,
            replayed_from: self.ran_from,
        };
        serde_json::to_string(&view).map_err(js_err)
    }
}
