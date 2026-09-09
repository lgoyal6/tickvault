//! Loading a venue's recording, enforcing its sequence scheme, and cutting it
//! into chronological windows.
//!
//! The rule this module exists to keep is the archive's own: **a detected gap
//! stops the book.** A window is replayed in receive order and truncated at the
//! first violation, and nothing is ever applied across the hole. A truncated
//! window is reported as truncated rather than quietly short.
//!
//! What counts as a violation is deliberately stricter here than on a live
//! feed. The library's [`SeqVerdict::Duplicate`] and [`SeqVerdict::Stale`] are
//! harmless on a socket, where a venue may legitimately resend. In an archive
//! written in receive order they mean the identifiers are not the ones the
//! recorder saw, so they stop the window too. That is what makes the
//! `shuffle-seq` control fail rather than pass quietly.
//!
//! The one case that does not stop the book is a forward skip on a per-message
//! counter, and the reason is measured rather than assumed. Coinbase Advanced
//! Trade numbers every message on the connection, including messages that carry
//! no book levels, so its archived per-message identifier skips by two once in
//! this recording, thirteen microseconds after its predecessor, on rows the
//! recorder's own live check vouched for. The archive cannot tell that skip
//! apart from a lost book message. Calling it a venue gap would blame the venue
//! for a limit of our row projection, which is exactly the fault
//! `CONTRIBUTING.md` forbids, so it is counted by name and the recorder's
//! `suspect` column is what stops the book.

use tickvault::book::L2Book;
use tickvault::book::checksum::kraken_checksum;
use tickvault::book::replay::BookReplayer;
use tickvault::sequence::{
    ChainState, ChecksumState, CounterState, RangeState, SeqVerdict, TimestampState, Unverifiable,
};
use tickvault::store::rows::{Row, split_messages};
use tickvault::store::schema::EventKind;
use tickvault::{BookLevel, Fixed, Symbol, VenueId};

use crate::manifest::{Input, Precision, Window};
use crate::{SimResult, bail};

/// The venue identifiers a message carried.
///
/// The library's [`Row`] deliberately does not hold these: it is the decoded
/// *book* row, and four identifier columns that only some venues populate would
/// be four mostly-null fields on every row of every feed. They are read from
/// the same batch alongside it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Ident {
    pub seq: Option<u64>,
    pub first_seq: Option<u64>,
    pub prev_seq: Option<u64>,
    pub checksum: Option<u32>,
    pub venue_ts: Option<i64>,
}

/// One archived message: the rows that arrived together, plus its identifiers.
#[derive(Debug, Clone)]
pub struct Message {
    pub recv_wall: i64,
    pub event: EventKind,
    pub rows: Vec<Row>,
    pub ident: Ident,
    /// True when any row of the message fell inside a suspect window.
    pub suspect: bool,
}

impl Message {
    /// Quantity this message traded, where the feed can say.
    ///
    /// `None` on every aggregated feed, and every recording in this experiment
    /// is aggregated. Not zero: zero would claim nothing traded.
    pub fn traded_qty(&self) -> Option<Fixed> {
        let mut total: Option<Fixed> = None;
        for row in &self.rows {
            if let Some(t) = row.traded_qty {
                total = Some(total.unwrap_or(Fixed::ZERO).checked_add(t).unwrap_or(t));
            }
        }
        total
    }
}

/// Which loss detector a venue's identifiers feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    Counter,
    Range,
    Chain,
    Checksum,
    Timestamp,
}

impl Scheme {
    pub fn parse(s: &str) -> SimResult<Scheme> {
        Ok(match s {
            "counter" => Scheme::Counter,
            "range" => Scheme::Range,
            "chain" => Scheme::Chain,
            "checksum" => Scheme::Checksum,
            "timestamp" => Scheme::Timestamp,
            other => bail!("unknown sequence scheme {other}"),
        })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Scheme::Counter => "counter",
            Scheme::Range => "range",
            Scheme::Chain => "chain",
            Scheme::Checksum => "checksum",
            Scheme::Timestamp => "timestamp",
        }
    }
}

/// Why a window stopped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Violation {
    /// The venue's own scheme says the stream is not continuous.
    Sequence { verdict: String },
    /// Receive order is the archive's only reliable order, so a message stamped
    /// before its predecessor means the file is not in receive order.
    ReceiveOrder { previous: i64, observed: i64 },
    /// The recorder could not vouch for this message. Its live check saw every
    /// message, including the ones that carry no levels, so this verdict is
    /// authoritative in a way the archive's identifiers are not.
    RecorderMarkedSuspect { at_recv_wall: i64 },
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Violation::Sequence { verdict } => write!(f, "sequence violation: {verdict}"),
            Violation::ReceiveOrder { previous, observed } => write!(
                f,
                "receive order violation: {observed} arrived after {previous}"
            ),
            Violation::RecorderMarkedSuspect { at_recv_wall } => {
                write!(f, "recorder marked the message at {at_recv_wall} suspect")
            }
        }
    }
}

/// Per-venue sequence state, in whichever scheme the venue publishes.
#[derive(Debug, Clone)]
enum State {
    Counter(CounterState),
    Range(RangeState),
    Chain(ChainState),
    Checksum(ChecksumState),
    Timestamp(TimestampState),
}

/// What the enforcer concluded over a run of messages.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SeqCounts {
    pub in_order: u64,
    /// Messages the scheme could not judge. Counted apart from clean, always.
    pub unverifiable: u64,
    /// Forward skips on a per-message counter, which the archive cannot
    /// attribute between a lost book message and a message that carried none.
    pub counter_forward_skips: u64,
    /// Checksum comparisons that were our own rendering's fault rather than the
    /// venue's.
    pub our_rendering_lossy: u64,
}

/// Judges every message against the venue's scheme, and stops on the first
/// thing that is positively wrong.
#[derive(Debug, Clone)]
pub struct Enforcer {
    scheme: Scheme,
    state: State,
    precision: Precision,
    last_recv_wall: Option<i64>,
    pub counts: SeqCounts,
}

impl Enforcer {
    pub fn new(scheme: Scheme, precision: Precision) -> Self {
        let state = match scheme {
            // Step one, restart floor zero: the counter feeds here number from
            // zero per connection.
            Scheme::Counter => State::Counter(CounterState::new(1, 0)),
            Scheme::Range => State::Range(RangeState::new()),
            Scheme::Chain => State::Chain(ChainState::new()),
            Scheme::Checksum => State::Checksum(ChecksumState::new()),
            Scheme::Timestamp => State::Timestamp(TimestampState::new()),
        };
        Enforcer {
            scheme,
            state,
            precision,
            last_recv_wall: None,
            counts: SeqCounts::default(),
        }
    }

    pub fn scheme(&self) -> Scheme {
        self.scheme
    }

    /// Judge one message. Call after it has been applied, because a checksum is
    /// a statement about the book the message produced.
    pub fn observe(
        &mut self,
        event: EventKind,
        ident: Ident,
        recv_wall: i64,
        suspect: bool,
        book_after: &L2Book,
    ) -> Result<(), Violation> {
        if let Some(previous) = self.last_recv_wall
            && recv_wall < previous
        {
            return Err(Violation::ReceiveOrder {
                previous,
                observed: recv_wall,
            });
        }
        self.last_recv_wall = Some(recv_wall);

        if suspect {
            return Err(Violation::RecorderMarkedSuspect {
                at_recv_wall: recv_wall,
            });
        }

        let verdict = self.judge(event, ident, book_after);
        self.classify(verdict)
    }

    fn judge(&mut self, event: EventKind, ident: Ident, book_after: &L2Book) -> SeqVerdict {
        match &mut self.state {
            State::Counter(state) => match ident.seq {
                Some(seq) => state.observe(seq),
                None => SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck),
            },
            State::Range(state) => {
                // A snapshot is the anchor, exactly as syncing to a REST book
                // is on the live feed: everything at or below its identifier is
                // already in the book it carries.
                if event == EventKind::Snapshot {
                    match ident.seq {
                        Some(seq) => {
                            state.anchor_snapshot(seq);
                            return SeqVerdict::InOrder;
                        }
                        None => {
                            return SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck);
                        }
                    }
                }
                match (ident.first_seq, ident.seq) {
                    (Some(first), Some(last)) => state.observe(first, last),
                    _ => SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck),
                }
            }
            State::Chain(state) => match ident.seq {
                Some(seq) => state.observe(seq as u128, ident.prev_seq.map(|p| p as u128)),
                None => SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck),
            },
            State::Checksum(state) => match ident.checksum {
                Some(expected) => {
                    let computed =
                        kraken_checksum(book_after, self.precision.price, self.precision.qty);
                    state.observe(expected, computed)
                }
                None => SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck),
            },
            State::Timestamp(state) => match ident.venue_ts {
                Some(stamp) => state.observe(stamp),
                None => SeqVerdict::Unverifiable(Unverifiable::MessageCarriedNoCheck),
            },
        }
    }

    fn classify(&mut self, verdict: SeqVerdict) -> Result<(), Violation> {
        match verdict {
            SeqVerdict::InOrder => {
                self.counts.in_order += 1;
                Ok(())
            }
            SeqVerdict::Unverifiable(Unverifiable::RenderingWasLossy { .. }) => {
                // Ours, not theirs. Counted apart so the gap statistics cannot
                // be inflated by our own decimal rendering.
                self.counts.our_rendering_lossy += 1;
                self.counts.unverifiable += 1;
                Ok(())
            }
            SeqVerdict::Unverifiable(_) => {
                self.counts.unverifiable += 1;
                Ok(())
            }
            SeqVerdict::Gap(evidence) if self.scheme == Scheme::Counter => {
                self.counts.counter_forward_skips += 1;
                let _ = evidence;
                Ok(())
            }
            other => Err(Violation::Sequence {
                verdict: format!("{other:?}"),
            }),
        }
    }
}

/// A venue's whole recording, in receive order, held in memory.
///
/// One recording here is at most 136,000 rows, so loading it once and replaying
/// windows out of memory is cheaper than reopening the file for each of the two
/// thousand parameter fits, and gives the determinism the gate checks for free.
#[derive(Debug, Clone)]
pub struct VenueFeed {
    pub venue: VenueId,
    pub symbol: Symbol,
    pub feed_depth: Option<usize>,
    pub scheme: Scheme,
    pub verifiable: bool,
    /// The smallest positive gap between two prices this recording actually
    /// shows, which is the venue's tick size as observed rather than as read
    /// off a documentation page. Quotes are rounded to a multiple of it, so the
    /// simulator never rests an order at a price the venue would reject.
    pub observed_tick: Fixed,
    pub messages: Vec<Message>,
}

/// The smallest positive distance between two distinct prices in a recording.
///
/// Measured, not declared. The archive does not carry the instrument's tick
/// size, and guessing one finer than the venue's would let the simulator quote
/// at prices no venue would accept, which flatters every fill rate.
fn observed_tick(messages: &[Message]) -> Fixed {
    let mut prices: Vec<i64> = messages
        .iter()
        .flat_map(|m| m.rows.iter().map(|r| r.price.mantissa()))
        .collect();
    prices.sort_unstable();
    prices.dedup();
    let mut tick = i64::MAX;
    for pair in prices.windows(2) {
        let gap = pair[1] - pair[0];
        if gap > 0 && gap < tick {
            tick = gap;
        }
    }
    if tick == i64::MAX {
        // One price in the whole recording. Nothing to measure, so refuse to
        // invent a tick and use the coarsest sane unit instead.
        return Fixed::from_mantissa(1_000_000_000);
    }
    Fixed::from_mantissa(tick)
}

/// Read the identifier columns the library's `Row` does not carry.
fn idents(batch: &arrow::array::RecordBatch) -> Vec<Ident> {
    use arrow::array::{Array, TimestampNanosecondArray, UInt32Array, UInt64Array};

    let u64_col = |name: &str| {
        batch
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
    };
    let seq = u64_col("seq");
    let first_seq = u64_col("first_seq");
    let prev_seq = u64_col("prev_seq");
    let checksum = batch
        .column_by_name("checksum")
        .and_then(|c| c.as_any().downcast_ref::<UInt32Array>());
    let venue_ts = batch
        .column_by_name("venue_ts")
        .and_then(|c| c.as_any().downcast_ref::<TimestampNanosecondArray>());

    (0..batch.num_rows())
        .map(|i| Ident {
            seq: seq.filter(|c| !c.is_null(i)).map(|c| c.value(i)),
            first_seq: first_seq.filter(|c| !c.is_null(i)).map(|c| c.value(i)),
            prev_seq: prev_seq.filter(|c| !c.is_null(i)).map(|c| c.value(i)),
            checksum: checksum.filter(|c| !c.is_null(i)).map(|c| c.value(i)),
            venue_ts: venue_ts.filter(|c| !c.is_null(i)).map(|c| c.value(i)),
        })
        .collect()
}

impl VenueFeed {
    /// Load one venue's recording from the archive the manifest names.
    pub fn load(repo_root: &std::path::Path, input: &Input) -> SimResult<Self> {
        let venue: VenueId = input
            .venue
            .parse()
            .map_err(|e: String| crate::SimError(e))?;
        let symbol = Symbol::parse(&input.symbol).map_err(crate::SimError)?;
        let scheme = Scheme::parse(&input.sequence_scheme)?;
        let path = repo_root.join(&input.parquet);
        let batches = tickvault::store::reader::read_batches(&path)?;

        // Every batch is concatenated before the rows are grouped, because a
        // Parquet batch boundary falls wherever the writer's batch size put it
        // and a message's levels straddle it. Grouping per batch splits such a
        // message in two, and the second half then arrives carrying an
        // identifier the scheme has already seen, which reads as a duplicate.
        // That is a fault of ours, not of the venue, and it showed up as one.
        let mut rows: Vec<Row> = Vec::new();
        let mut row_idents: Vec<Ident> = Vec::new();
        for batch in &batches {
            rows.extend(tickvault::store::rows::decode(batch)?);
            row_idents.extend(idents(batch));
        }
        if row_idents.len() != rows.len() {
            bail!(
                "{}: decoded {} rows but {} identifier rows",
                path.display(),
                rows.len(),
                row_idents.len()
            );
        }

        // `split_messages` is the library's own definition of where a message
        // ends. Rewriting that rule here is how a replay ends up truncating per
        // level instead of per message on a depth-limited feed, which was a
        // real bug in this repository.
        let mut messages: Vec<Message> = Vec::new();
        let mut offset = 0usize;
        for group in split_messages(&rows) {
            let first = &group[0];
            messages.push(Message {
                recv_wall: first.recv_wall,
                event: first.event,
                suspect: group.iter().any(|r| r.suspect),
                rows: group.to_vec(),
                ident: row_idents[offset],
            });
            offset += group.len();
        }

        if messages.is_empty() {
            bail!("{} holds no messages", path.display());
        }
        let declared_rows: usize = messages.iter().map(|m| m.rows.len()).sum();
        if declared_rows as u64 != input.rows {
            bail!(
                "{} holds {declared_rows} rows, the manifest declares {}",
                path.display(),
                input.rows
            );
        }

        let tick = observed_tick(&messages);
        Ok(VenueFeed {
            venue,
            symbol,
            feed_depth: input.feed_depth,
            scheme,
            verifiable: input.verifiable,
            observed_tick: tick,
            messages,
        })
    }

    /// The first message index at or after `recv_wall`.
    pub fn index_at(&self, recv_wall: i64) -> usize {
        self.messages.partition_point(|m| m.recv_wall < recv_wall)
    }

    /// Message index range for a window, half open on the right.
    pub fn range_of(&self, window: &Window) -> (usize, usize) {
        (
            self.index_at(window.from_recv_wall_ns),
            self.index_at(window.to_recv_wall_ns),
        )
    }

    fn fresh_state(&self, precision: Precision) -> ReplayState {
        ReplayState {
            replayer: BookReplayer::new(self.symbol.clone(), BookLevel::L2, self.feed_depth),
            enforcer: Enforcer::new(self.scheme, precision),
            truncated_at: None,
        }
    }

    /// The median spread of this recording, in basis points of the mid.
    ///
    /// Measured rather than assumed, and reported next to the results, because
    /// a half-spread parameter chosen without it can be an order of magnitude
    /// wide for the instrument and then almost never gets filled. That is a
    /// property of the input, not a knob: it is measured after the manifest was
    /// frozen and changes nothing the manifest decided.
    pub fn median_spread_bps(&self) -> Option<f64> {
        let mut replayer = BookReplayer::new(self.symbol.clone(), BookLevel::L2, self.feed_depth);
        let mut samples: Vec<f64> = Vec::with_capacity(self.messages.len());
        for message in &self.messages {
            replayer.apply_message(&message.rows);
            let book = replayer.book_ref();
            if let (Some((bid, _)), Some((ask, _))) = (book.best_bid(), book.best_ask()) {
                let bid = bid.to_f64_lossy();
                let ask = ask.to_f64_lossy();
                let mid = (bid + ask) / 2.0;
                if mid > 0.0 {
                    samples.push((ask - bid) / mid * 10_000.0);
                }
            }
        }
        crate::costs::quantile(&samples, 0.5)
    }

    /// Replay the warmup prefix once and photograph the state at each window
    /// boundary.
    ///
    /// One pass, five clones, and every later run of a window starts from the
    /// same bytes. A violation inside the prefix is fatal for the venue: there
    /// is no honest way to start a window from a book we could not build.
    pub fn window_states(
        &self,
        windows: &[Window],
        precision: Precision,
    ) -> SimResult<Vec<ReplayState>> {
        let mut state = self.fresh_state(precision);
        let mut out = Vec::with_capacity(windows.len());
        let mut next = 0usize;
        for (i, message) in self.messages.iter().enumerate() {
            while next < windows.len() && i == self.index_at(windows[next].from_recv_wall_ns) {
                out.push(state.clone());
                next += 1;
            }
            if next == windows.len() {
                break;
            }
            state.apply(message, message.ident).map_err(|v| {
                crate::SimError(format!(
                    "{}: the warmup prefix stopped at message {i}: {v}",
                    self.venue
                ))
            })?;
        }
        while next < windows.len() {
            out.push(state.clone());
            next += 1;
        }
        Ok(out)
    }
}

/// A book being rebuilt, and the enforcer watching it.
#[derive(Debug, Clone)]
pub struct ReplayState {
    pub replayer: BookReplayer,
    pub enforcer: Enforcer,
    /// Receive time of the message that stopped the book, if one did.
    pub truncated_at: Option<i64>,
}

impl ReplayState {
    /// Apply one message and judge it.
    ///
    /// The order is deliberate: apply, then judge, because a checksum is a
    /// statement about the book the message produced. On a violation the book
    /// is left holding the message that produced it and the caller stops: the
    /// alternative is to carry on past a hole, which is the failure that makes
    /// free order book data untrustworthy.
    pub fn apply(&mut self, message: &Message, ident: Ident) -> Result<(), Violation> {
        self.replayer.apply_message(&message.rows);
        let book = self.replayer.book_ref();
        let verdict = self.enforcer.observe(
            message.event,
            ident,
            message.recv_wall,
            message.suspect,
            book,
        );
        if verdict.is_err() {
            self.truncated_at = Some(message.recv_wall);
        }
        verdict
    }

    pub fn book(&mut self) -> &L2Book {
        self.replayer.book_ref()
    }
}

/// How the `shuffle-seq` control corrupts a window's identifiers.
///
/// Never on by default. The gate script selects it, and the run is expected to
/// fail: a control that can pass is not a control.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SeqTamper {
    /// Swap the identifiers of the messages at this local index and the next.
    pub swap_at: Option<usize>,
    /// Observe the message at this local index twice.
    pub duplicate_at: Option<usize>,
}

impl SeqTamper {
    /// The tamper the control applies to a window of `len` messages.
    pub fn for_window(len: usize) -> Self {
        SeqTamper {
            swap_at: (len >= 4).then_some(len / 2),
            duplicate_at: (len >= 4).then_some(1),
        }
    }

    /// The identifier a local message index should be judged with.
    pub fn ident_at(&self, local: usize, messages: &[Message]) -> Ident {
        if let Some(m) = self.swap_at {
            if local == m && m + 1 < messages.len() {
                return messages[m + 1].ident;
            }
            if local == m + 1 {
                return messages[m].ident;
            }
        }
        messages[local].ident
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tickvault::book::{BookDelta, LevelChange};
    use tickvault::clock::{Stamp, Timestamps};

    fn precision() -> Precision {
        Precision { price: 1, qty: 8 }
    }

    fn empty_book() -> L2Book {
        L2Book::new(Symbol::new("BTC", "USD"))
    }

    fn counter_ident(seq: u64) -> Ident {
        Ident {
            seq: Some(seq),
            ..Ident::default()
        }
    }

    #[test]
    fn a_continuous_counter_run_is_all_in_order() {
        let mut e = Enforcer::new(Scheme::Counter, precision());
        let book = empty_book();
        for seq in 10..20 {
            e.observe(
                EventKind::Delta,
                counter_ident(seq),
                seq as i64,
                false,
                &book,
            )
            .expect("continuous");
        }
        assert_eq!(e.counts.in_order, 10);
        assert_eq!(e.counts.counter_forward_skips, 0);
    }

    #[test]
    fn a_repeated_counter_stops_the_book() {
        let mut e = Enforcer::new(Scheme::Counter, precision());
        let book = empty_book();
        e.observe(EventKind::Delta, counter_ident(5), 1, false, &book)
            .unwrap();
        let err = e
            .observe(EventKind::Delta, counter_ident(5), 2, false, &book)
            .expect_err("a duplicate identifier is not something an archive can hold");
        assert!(matches!(err, Violation::Sequence { .. }), "{err}");
    }

    #[test]
    fn a_counter_that_goes_backwards_stops_the_book() {
        let mut e = Enforcer::new(Scheme::Counter, precision());
        let book = empty_book();
        e.observe(EventKind::Delta, counter_ident(9), 1, false, &book)
            .unwrap();
        let err = e
            .observe(EventKind::Delta, counter_ident(8), 2, false, &book)
            .expect_err("receive order and identifier order must agree");
        assert!(matches!(err, Violation::Sequence { .. }), "{err}");
    }

    #[test]
    fn a_forward_counter_skip_is_counted_and_not_blamed_on_the_venue() {
        // Measured on this archive: coinbase advances its per-connection
        // counter by two thirteen microseconds after the previous message, on
        // rows the recorder vouched for, because the counter numbers messages
        // that carry no book levels.
        let mut e = Enforcer::new(Scheme::Counter, precision());
        let book = empty_book();
        e.observe(EventKind::Delta, counter_ident(5), 1, false, &book)
            .unwrap();
        e.observe(EventKind::Delta, counter_ident(7), 2, false, &book)
            .expect("an unattributable skip is not a gap");
        assert_eq!(e.counts.counter_forward_skips, 1);
        assert_eq!(e.counts.in_order, 1);
    }

    #[test]
    fn a_suspect_message_stops_the_book_whatever_the_identifiers_say() {
        let mut e = Enforcer::new(Scheme::Counter, precision());
        let book = empty_book();
        let err = e
            .observe(EventKind::Delta, counter_ident(1), 7, true, &book)
            .expect_err("the recorder's own verdict is authoritative");
        assert_eq!(err, Violation::RecorderMarkedSuspect { at_recv_wall: 7 });
    }

    #[test]
    fn receive_order_going_backwards_stops_the_book() {
        let mut e = Enforcer::new(Scheme::Counter, precision());
        let book = empty_book();
        e.observe(EventKind::Delta, counter_ident(1), 100, false, &book)
            .unwrap();
        let err = e
            .observe(EventKind::Delta, counter_ident(2), 99, false, &book)
            .expect_err("the archive's only reliable order is receipt");
        assert_eq!(
            err,
            Violation::ReceiveOrder {
                previous: 100,
                observed: 99
            }
        );
    }

    #[test]
    fn a_broken_chain_stops_the_book() {
        let mut e = Enforcer::new(Scheme::Chain, precision());
        let book = empty_book();
        let ident = |seq, prev| Ident {
            seq: Some(seq),
            prev_seq: prev,
            ..Ident::default()
        };
        e.observe(EventKind::Delta, ident(100, None), 1, false, &book)
            .unwrap();
        e.observe(EventKind::Delta, ident(117, Some(100)), 2, false, &book)
            .unwrap();
        let err = e
            .observe(EventKind::Delta, ident(140, Some(130)), 3, false, &book)
            .expect_err("a chain that names the wrong predecessor is broken");
        assert!(matches!(err, Violation::Sequence { .. }), "{err}");
    }

    #[test]
    fn a_broken_update_id_range_stops_the_book() {
        let mut e = Enforcer::new(Scheme::Range, precision());
        let book = empty_book();
        let snap = Ident {
            seq: Some(1_000),
            ..Ident::default()
        };
        e.observe(EventKind::Snapshot, snap, 1, false, &book)
            .unwrap();
        let delta = |first, last| Ident {
            seq: Some(last),
            first_seq: Some(first),
            ..Ident::default()
        };
        e.observe(EventKind::Delta, delta(1_001, 1_010), 2, false, &book)
            .unwrap();
        let err = e
            .observe(EventKind::Delta, delta(1_020, 1_030), 3, false, &book)
            .expect_err("nine update ids went missing");
        assert!(matches!(err, Violation::Sequence { .. }), "{err}");
    }

    #[test]
    fn a_feed_with_no_identifier_is_unverifiable_and_never_clean() {
        // Bitstamp's diff feed carries a timestamp and nothing else. A
        // well-ordered message on it must not be reported in order: doing so
        // would let a feed with no loss detection report itself fully verified.
        let mut e = Enforcer::new(Scheme::Timestamp, precision());
        let book = empty_book();
        for stamp in 1..5 {
            let ident = Ident {
                venue_ts: Some(stamp * 1_000),
                ..Ident::default()
            };
            e.observe(EventKind::Delta, ident, stamp, false, &book)
                .unwrap();
        }
        assert_eq!(e.counts.in_order, 0);
        assert_eq!(e.counts.unverifiable, 4);
    }

    #[test]
    fn a_timestamp_feed_still_catches_reordering() {
        let mut e = Enforcer::new(Scheme::Timestamp, precision());
        let book = empty_book();
        let ident = |ts| Ident {
            venue_ts: Some(ts),
            ..Ident::default()
        };
        e.observe(EventKind::Delta, ident(2_000), 1, false, &book)
            .unwrap();
        let err = e
            .observe(EventKind::Delta, ident(1_000), 2, false, &book)
            .expect_err("out of order is the one fault a timestamp can prove");
        assert!(matches!(err, Violation::Sequence { .. }), "{err}");
    }

    #[test]
    fn a_diverging_checksum_stops_the_book() {
        let mut book = L2Book::new(Symbol::new("BTC", "USD"));
        let mut changes = Vec::new();
        for i in 0..12i64 {
            changes.push(LevelChange::new(
                tickvault::Side::Bid,
                Fixed::from_mantissa(100_000_000_000_000 - i * 100_000_000),
                Fixed::from_mantissa(500_000_000),
            ));
            changes.push(LevelChange::new(
                tickvault::Side::Ask,
                Fixed::from_mantissa(100_100_000_000_000 + i * 100_000_000),
                Fixed::from_mantissa(500_000_000),
            ));
        }
        book.apply_delta(&BookDelta {
            symbol: Symbol::new("BTC", "USD"),
            changes,
            seq: None,
            checksum: None,
            stamps: Timestamps::recv_only(Stamp {
                mono_nanos: 0,
                wall_nanos: 1,
            }),
            prev_seq: None,
            first_seq: None,
        });
        let mut e = Enforcer::new(Scheme::Checksum, precision());
        let ident = Ident {
            checksum: Some(0xdead_beef),
            ..Ident::default()
        };
        let err = e
            .observe(EventKind::Delta, ident, 1, false, &book)
            .expect_err("a book that does not match the venue's hash of it is wrong");
        assert!(matches!(err, Violation::Sequence { .. }), "{err}");
    }

    // The gates below read the six committed recordings. They assert
    // properties of the replay rather than numbers, in the style of
    // `tests/gate_*.rs`, because three of that suite's gates exist after a
    // passing test turned out to be passing for the wrong reason.

    fn loaded() -> crate::manifest::Loaded {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("sim/ has a parent")
            .to_path_buf();
        crate::manifest::Loaded::open(&root.join("sim/manifest.json"), &root)
            .expect("the frozen manifest loads")
    }

    fn feeds() -> Vec<(VenueFeed, Vec<Window>)> {
        let loaded = loaded();
        loaded
            .manifest
            .inputs
            .iter()
            .map(|input| {
                let feed = VenueFeed::load(&loaded.repo_root, input)
                    .unwrap_or_else(|e| panic!("{} loads: {e}", input.venue));
                let windows = loaded.windows_for(&input.venue).unwrap().to_vec();
                (feed, windows)
            })
            .collect()
    }

    #[test]
    fn a_message_is_never_split_at_a_parquet_batch_boundary() {
        // A batch boundary falls wherever the writer's batch size put it, and a
        // message's levels straddle it. Grouping per batch splits such a
        // message, and its second half then arrives carrying an identifier the
        // scheme has already judged, which reads as a duplicate. That happened,
        // and the venue would have been blamed for it.
        for (feed, _) in feeds() {
            for pair in feed.messages.windows(2) {
                let a = &pair[0].rows[0];
                let b = &pair[1].rows[0];
                assert!(
                    !(a.msg_index == b.msg_index
                        && a.event == b.event
                        && a.recv_wall == b.recv_wall),
                    "{}: two adjacent messages share one message key, so one message was split",
                    feed.venue
                );
            }
        }
    }

    #[test]
    fn every_message_is_in_receive_order() {
        for (feed, _) in feeds() {
            for pair in feed.messages.windows(2) {
                assert!(
                    pair[1].recv_wall >= pair[0].recv_wall,
                    "{}: the archive is not in receive order",
                    feed.venue
                );
            }
        }
    }

    #[test]
    fn every_window_of_every_venue_replays_without_a_violation() {
        let precision = loaded().manifest.kraken_checksum_precision;
        for (feed, windows) in feeds() {
            let states = feed
                .window_states(&windows, precision)
                .unwrap_or_else(|e| panic!("{}: {e}", feed.venue));
            for (window, start) in windows.iter().zip(states) {
                let (from, to) = feed.range_of(window);
                assert!(to > from, "{} window {} is empty", feed.venue, window.index);
                let mut state = start;
                for (i, message) in feed.messages[from..to].iter().enumerate() {
                    state.apply(message, message.ident).unwrap_or_else(|v| {
                        panic!("{} window {} message {i}: {v}", feed.venue, window.index)
                    });
                }
                assert!(state.truncated_at.is_none());
            }
        }
    }

    #[test]
    fn tampering_with_the_identifiers_stops_every_window() {
        // A control that can pass is not a control.
        let precision = loaded().manifest.kraken_checksum_precision;
        for (feed, windows) in feeds() {
            let states = feed.window_states(&windows, precision).unwrap();
            for (window, start) in windows.iter().zip(states) {
                let (from, to) = feed.range_of(window);
                let messages = &feed.messages[from..to];
                let tamper = SeqTamper::for_window(messages.len());
                let mut state = start;
                let mut stopped = false;
                for (local, message) in messages.iter().enumerate() {
                    let ident = tamper.ident_at(local, messages);
                    if tamper.duplicate_at == Some(local) && state.apply(message, ident).is_err() {
                        stopped = true;
                        break;
                    }
                    if state.apply(message, ident).is_err() {
                        stopped = true;
                        break;
                    }
                }
                assert!(
                    stopped,
                    "{} window {} accepted shuffled identifiers",
                    feed.venue, window.index
                );
            }
        }
    }

    #[test]
    fn the_aggregated_feeds_report_no_traded_quantity_rather_than_zero() {
        // Every recording here is aggregated, so a shrinking level cannot be
        // split between a trade and a cancellation. A zero would be a claim,
        // and a false one.
        for (feed, _) in feeds() {
            let with_trades = feed
                .messages
                .iter()
                .filter(|m| m.traded_qty().is_some())
                .count();
            assert_eq!(
                with_trades, 0,
                "{} reports a traded quantity; the fill model's trade path is no longer inert here",
                feed.venue
            );
        }
    }

    #[test]
    fn an_unverifiable_feed_is_never_reported_in_order() {
        let loaded = loaded();
        let precision = loaded.manifest.kraken_checksum_precision;
        let input = loaded.input("bitstamp").unwrap();
        let feed = VenueFeed::load(&loaded.repo_root, input).unwrap();
        let windows = loaded.windows_for("bitstamp").unwrap();
        let states = feed.window_states(windows, precision).unwrap();
        for (window, start) in windows.iter().zip(states) {
            let (from, to) = feed.range_of(window);
            let before = start.enforcer.counts;
            let mut state = start;
            for message in &feed.messages[from..to] {
                state.apply(message, message.ident).unwrap();
            }
            let after = state.enforcer.counts;
            assert_eq!(
                after.in_order, before.in_order,
                "a feed that publishes neither a sequence number nor a checksum reported a \
                 message in order, which would let it claim to be fully verified"
            );
            assert!(after.unverifiable > before.unverifiable);
        }
    }

    #[test]
    fn the_measured_spread_is_a_fraction_of_a_basis_point_on_every_venue() {
        // The frozen half-spread grid runs from one to five basis points, and
        // this is what says how far outside the touch that puts a quote on a
        // BTC book. The number belongs in the report next to the fill counts.
        for (feed, _) in feeds() {
            let spread = feed
                .median_spread_bps()
                .unwrap_or_else(|| panic!("{} has a two-sided book", feed.venue));
            assert!(spread > 0.0, "{} spread {spread}", feed.venue);
            assert!(
                spread < 1.0,
                "{} median spread is {spread} bp; the grid's own scale is no longer the story",
                feed.venue
            );
        }
    }

    #[test]
    fn the_measured_tick_is_the_venues_own_price_increment() {
        let expected: &[(&str, i64)] = &[
            // Cent-priced books.
            ("coinbase", 10_000_000),
            ("binance-us", 10_000_000),
            ("bitstamp", 10_000_000),
            // Dime-priced books.
            ("kraken", 100_000_000),
            ("okx", 100_000_000),
            ("bybit", 100_000_000),
        ];
        for (feed, _) in feeds() {
            let want = expected
                .iter()
                .find(|(v, _)| *v == feed.venue.as_str())
                .expect("every venue is listed")
                .1;
            assert_eq!(
                feed.observed_tick.mantissa(),
                want,
                "{} tick changed; quotes would land off the venue's price grid",
                feed.venue
            );
        }
    }

    #[test]
    fn the_tamper_swaps_the_pair_it_names_and_leaves_the_rest_alone() {
        let message = |seq: u64, at: i64| Message {
            recv_wall: at,
            event: EventKind::Delta,
            rows: Vec::new(),
            ident: counter_ident(seq),
            suspect: false,
        };
        let messages: Vec<Message> = (0..6).map(|i| message(i as u64 + 10, i as i64)).collect();
        let tamper = SeqTamper::for_window(messages.len());
        assert_eq!(tamper.swap_at, Some(3));
        assert_eq!(tamper.duplicate_at, Some(1));
        assert_eq!(tamper.ident_at(3, &messages).seq, Some(14));
        assert_eq!(tamper.ident_at(4, &messages).seq, Some(13));
        assert_eq!(tamper.ident_at(0, &messages).seq, Some(10));
        assert_eq!(tamper.ident_at(5, &messages).seq, Some(15));
    }
}
