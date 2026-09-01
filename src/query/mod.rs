//! Querying the archive.
//!
//! Everything here streams. A range query opens one file and holds one Parquet
//! batch at a time, so asking about a day costs the same resident memory as
//! asking about a second, and a consumer never has to decide how much of the
//! archive fits in RAM.
//!
//! The starting book comes from [`crate::reconstruct`], so a query over the
//! last hour of a day costs the last hour rather than the day. The two share
//! one implementation of applying archived rows to a book: they differ in which
//! rows they feed it, which is where the bugs have actually been.

pub mod aggregate;
pub mod replay;

use crate::book::L2Book;
use crate::book::l3::L3Book;
use crate::book::replay::BookReplayer;
use crate::error::Result;
use crate::reconstruct::{Origin, Reconstructor, Request, Trust};
use crate::store::rows::{MessageStream, Row, RowStream};
use crate::store::schema::EventKind;
use crate::types::{BookLevel, Symbol, VenueId};

/// A range of the archive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    pub venue: VenueId,
    pub symbol: Symbol,
    /// Inclusive start. The book is established as it stood here.
    pub from_wall: i64,
    /// Inclusive end.
    pub to_wall: i64,
    /// Levels a side to expose. Applied when the book is read, never during
    /// the replay.
    pub depth: Option<usize>,
    pub use_checkpoints: bool,
}

impl Query {
    pub fn new(venue: VenueId, symbol: &Symbol, from_wall: i64, to_wall: i64) -> Self {
        Query {
            venue,
            symbol: symbol.clone(),
            from_wall,
            to_wall,
            depth: None,
            use_checkpoints: true,
        }
    }

    pub fn with_depth(mut self, depth: usize) -> Self {
        self.depth = Some(depth);
        self
    }

    pub fn without_checkpoints(mut self) -> Self {
        self.use_checkpoints = false;
        self
    }

    pub fn duration_nanos(&self) -> i64 {
        (self.to_wall - self.from_wall).max(0)
    }

    /// Open a streaming cursor over this range.
    pub fn cursor(&self, reconstructor: &Reconstructor) -> Result<BookCursor> {
        BookCursor::open(reconstructor, self)
    }
}

/// One message, as the cursor advances over it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tick {
    /// When the recorder received it.
    pub at_wall: i64,
    /// What the venue said the time was, where it said anything.
    pub venue_ts: Option<i64>,
    pub kind: EventKind,
    /// Levels or orders the message carried.
    pub changes: usize,
    /// True when the recorder could not vouch for this message.
    pub suspect: bool,
    /// Quantity that traded, where the feed reports executions.
    ///
    /// `None` on an aggregated feed, which does not say whether a level shrank
    /// because it traded or because it was cancelled. Not zero: zero would
    /// claim nothing traded.
    pub traded_qty: Option<crate::Fixed>,
}

/// A book advancing through a range of the archive.
///
/// Deliberately not an `Iterator`. Yielding the book would mean copying it once
/// per message, and a day of a busy venue is millions of messages; borrowing it
/// is what a lending iterator would do, and Rust has no such trait. So the
/// cursor advances and the caller reads the book in place.
pub struct BookCursor {
    replayer: BookReplayer,
    stream: MessageStream,
    depth: Option<usize>,
    at_wall: i64,
    origin: Origin,
    seeded_trust: Trust,
    book_level: BookLevel,
    messages: u64,
    exhausted: bool,
}

impl BookCursor {
    fn open(reconstructor: &Reconstructor, query: &Query) -> Result<Self> {
        // The book as it stood at the left edge, which is what makes a range
        // query cost the range rather than the day.
        let mut request = Request::new(query.venue, &query.symbol, query.from_wall);
        if !query.use_checkpoints {
            request = request.without_checkpoints();
        }
        let start = reconstructor.at(&request)?;

        let reader = reconstructor.reader();
        let date = crate::clock::format_utc_date(query.from_wall);
        let mut files =
            crate::store::rows::partition_files(reader, query.venue, &query.symbol, &date);
        // A range can cross midnight into the next partition.
        let end_date = crate::clock::format_utc_date(query.to_wall);
        if end_date != date {
            files.extend(crate::store::rows::partition_files(
                reader,
                query.venue,
                &query.symbol,
                &end_date,
            ));
        }
        let feed_depth = files.first().and_then(|f| f.feed_depth);
        // Read before the prune, on purpose: the feed's depth window is a
        // property of the partition, and a range that prunes away the first
        // file still has to truncate exactly as the recorder did.
        let (files, files_pruned) = crate::store::scan::files_in_range(
            files,
            &crate::store::scan::Predicate::range(Some(query.from_wall), query.to_wall),
        );

        let mut replayer = BookReplayer::new(query.symbol.clone(), start.book_level, feed_depth);
        replayer.seed_l2(
            start.book.top(crate::types::Side::Bid, usize::MAX),
            start.book.top(crate::types::Side::Ask, usize::MAX),
            query.from_wall,
        );
        if let Some(l3) = start.l3.as_ref() {
            let mut orders = Vec::new();
            for side in [crate::types::Side::Bid, crate::types::Side::Ask] {
                for (price, _) in start.book.top(side, usize::MAX) {
                    for order in l3.queue_at(side, price) {
                        orders.push((order.id.clone(), side, order.price, order.qty));
                    }
                }
            }
            replayer.seed_l3(orders, query.from_wall);
        }

        let stream = MessageStream::new(
            RowStream::new(reader.root(), files, query.to_wall)
                .manifest_pruned(files_pruned)
                .after(query.from_wall),
        );

        Ok(BookCursor {
            replayer,
            stream,
            depth: query.depth,
            at_wall: query.from_wall,
            origin: start.origin,
            seeded_trust: start.trust,
            book_level: start.book_level,
            messages: 0,
            exhausted: false,
        })
    }

    /// Advance one message. `None` once the range is exhausted.
    pub fn advance(&mut self) -> Result<Option<Tick>> {
        if self.exhausted {
            return Ok(None);
        }
        let Some(message) = self.stream.next() else {
            self.exhausted = true;
            return Ok(None);
        };
        let message = message?;
        let Some(first) = message.first() else {
            self.exhausted = true;
            return Ok(None);
        };

        let tick = Tick {
            at_wall: first.recv_wall,
            venue_ts: first.venue_ts,
            kind: first.event,
            changes: message.len(),
            suspect: message.iter().any(|r| r.suspect),
            traded_qty: traded_quantity(&message),
        };
        self.at_wall = tick.at_wall;
        self.replayer.apply_message(&message);
        self.messages += 1;
        Ok(Some(tick))
    }

    /// The book as it stands, honouring the query's depth.
    ///
    /// Borrowed, not copied: a consumer reads this once per message.
    pub fn book(&mut self) -> &L2Book {
        self.replayer.book_ref()
    }

    /// The best `depth` levels a side, as the query asked for.
    pub fn top(&mut self, side: crate::types::Side) -> Vec<(crate::Fixed, crate::Fixed)> {
        let depth = self.depth.unwrap_or(usize::MAX);
        self.replayer.book_ref().top(side, depth)
    }

    pub fn l3(&self) -> Option<&L3Book> {
        self.replayer.l3()
    }

    pub fn at_wall(&self) -> i64 {
        self.at_wall
    }

    pub fn messages(&self) -> u64 {
        self.messages
    }

    pub fn rows(&self) -> u64 {
        self.replayer.rows()
    }

    pub fn book_level(&self) -> BookLevel {
        self.book_level
    }

    /// Where the starting book came from.
    pub fn origin(&self) -> Origin {
        self.origin
    }

    /// What could not be vouched for: the seed's doubts plus anything the range
    /// itself carried.
    pub fn trust(&self) -> Trust {
        Trust {
            suspect_rows: self.seeded_trust.suspect_rows + self.replayer.suspect_rows(),
            suspect_span: match (self.seeded_trust.suspect_span, self.replayer.suspect_span()) {
                (Some((a1, b1)), Some((a2, b2))) => Some((a1.min(a2), b1.max(b2))),
                (only, None) | (None, only) => only,
            },
            truncations: self.seeded_trust.truncations.clone(),
        }
    }

    /// Files opened so far. The whole point is that this grows with the range
    /// rather than with the archive.
    pub fn files_opened(&self) -> usize {
        self.stream.files_opened()
    }

    /// What the scan plan skipped, so far.
    ///
    /// Counts what has been opened, not what will be: the stream is lazy, so
    /// this is only the whole plan once the cursor is exhausted.
    pub fn explain(&self) -> &crate::store::scan::Explain {
        self.stream.explain()
    }

    /// Rows decoded and not yet consumed, for the memory-bound test.
    pub fn buffered_rows(&self) -> usize {
        self.stream.buffered_rows()
    }

    /// Run to the end of the range, calling `f` after each message.
    ///
    /// The streaming form of "give me the whole range": the closure sees every
    /// message and the book that followed it, and nothing accumulates.
    pub fn for_each<F>(&mut self, mut f: F) -> Result<u64>
    where
        F: FnMut(&Tick, &mut BookCursor) -> Result<()>,
    {
        let mut seen = 0;
        while let Some(tick) = self.advance()? {
            f(&tick, self)?;
            seen += 1;
        }
        Ok(seen)
    }
}

/// Quantity traded in a message, where the feed says.
///
/// Only an order-by-order feed reports executions. An aggregated feed shows a
/// level shrinking and does not say whether it traded or was cancelled, so the
/// answer there is "unknown" rather than zero.
fn traded_quantity(message: &[Row]) -> Option<crate::Fixed> {
    if message.first()?.book_level != BookLevel::L3 {
        return None;
    }
    // Summing the rows' own quantities would give zero on every full fill: the
    // row carries what is left resting, which is nothing. The size that changed
    // hands is worked out by the book while the order's previous state is still
    // known, and recorded in its own column.
    let mut total = crate::Fixed::ZERO;
    for row in message {
        if let Some(traded) = row.traded_qty {
            total = total.checked_add(traded).unwrap_or(total);
        }
    }
    Some(total)
}
