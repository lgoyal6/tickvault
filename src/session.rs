//! The ingest loop.
//!
//! Everything the two venues disagree about is read from
//! [`crate::venue::VenueCapabilities`] here, once, rather than branching on
//! which venue is which. There are exactly two such branches in
//! [`BookSession::ingest`]: whether the validator's state belongs to the
//! connection or the symbol, and whether the check runs before or after the
//! update is applied. Adding a venue in phase 2 should touch neither.
//!
//! The rule the whole file exists to enforce: **on a detected gap, stop
//! applying and rebuild from a snapshot.** Continuing to apply deltas across a
//! hole produces a book that looks continuous and is wrong, which is precisely
//! the failure mode that makes free order book data untrustworthy.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use crate::book::{BookDelta, BookSnapshot, L2Book};
use crate::clock::{Clock, Stamp};
use crate::error::{Error, Result};
use crate::gap::{GapLog, GapReport, SuspectCause};
use crate::limits::VenueLimiter;
use crate::recorder::RawRecorder;
use crate::sequence::{SeqState, SeqVerdict, Unverifiable};
use crate::types::{Symbol, VenueId};
use crate::venue::{
    ControlKind, FeedEvent, Keepalive, RawFrame, SnapshotSource, ValidationTiming, Venue,
    VenueCapabilities,
};

/// Something the session needs its caller to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionAction {
    /// Rebuild these symbols from a fresh snapshot. Until that happens their
    /// deltas are not applied and their suspect window stays open.
    Resnapshot {
        symbols: Vec<Symbol>,
        cause: SuspectCause,
    },
    /// The venue refused a subscription. Retrying the same request will not fix
    /// it, so this is surfaced rather than swallowed.
    SubscriptionFailed(String),
    /// The venue asked us to reconnect, ahead of dropping the socket itself.
    /// Honouring it converts an outage into a controlled resync.
    ReconnectRequested(String),
}

/// Book state and loss accounting for one venue connection.
pub struct BookSession {
    venue: Arc<dyn Venue>,
    caps: VenueCapabilities,
    venue_id: VenueId,
    symbols: Vec<Symbol>,
    books: BTreeMap<Symbol, L2Book>,
    /// Symbols holding a book we believe is faithful.
    ready: BTreeSet<Symbol>,
    /// Validator for a connection-scoped venue.
    connection_seq: SeqState,
    /// Validators for a symbol-scoped venue.
    symbol_seq: BTreeMap<Symbol, SeqState>,
    /// A book the connection-scoped path can hand to `validate_sequence`, which
    /// for a counter venue does not look at it.
    scratch: L2Book,
    /// Recently applied deltas, kept only for venues whose snapshot arrives
    /// over REST. See [`BookSession::replay_pending`].
    pending: BTreeMap<Symbol, VecDeque<BookDelta>>,
    /// Order-by-order books, for a venue that publishes L3.
    l3_books: BTreeMap<Symbol, crate::book::l3::L3Book>,
    /// The venue timestamp of each L3 book's seeding snapshot.
    ///
    /// Events at or before it are already contained in the snapshot. Replaying
    /// them re-adds orders the snapshot had already dropped, which puts
    /// phantoms on the book and leaves it permanently crossed. Same failure as
    /// splicing a stale REST book into the aggregated feed, and the same fix.
    ///
    /// The chain still observes those events, or it would fall out of step and
    /// report a break that never happened. Observed and applied are different
    /// questions.
    l3_snapshot_floor: BTreeMap<Symbol, i64>,
    /// When each L3 book first went crossed, in venue time.
    ///
    /// An order-by-order book crosses *by construction* whenever an aggressive
    /// order arrives: it exists on the book for the instant between being
    /// created and being matched. Observed on Bitstamp with both events sharing
    /// a microtimestamp, the deletion reporting the execution price rather than
    /// the placement price. Treating that as corruption, which is the right
    /// call at L2, would flag a healthy feed as suspect several times a minute.
    /// A crossing that *persists* is still a real fault, so it is timed.
    l3_crossed_since: BTreeMap<Symbol, i64>,
    /// Rows destined for the archive, when one is attached.
    archive: Option<crate::pipeline::RowAccumulator>,
    /// Groups the rows that arrived in one message.
    msg_index: u64,
    gaps: GapLog,
}

/// How many recent deltas to keep for splicing a REST snapshot into the stream.
///
/// Sized for the worst observed staleness rather than tuned: Bitstamp's REST
/// book runs about a second behind its own feed, and a fast venue can emit
/// hundreds of messages in that time.
const PENDING_CAPACITY: usize = 1024;

impl BookSession {
    pub fn new(venue: Arc<dyn Venue>, symbols: Vec<Symbol>) -> Self {
        let caps = venue.capabilities().clone();
        let venue_id = caps.id;
        let mut books = BTreeMap::new();
        let mut symbol_seq = BTreeMap::new();
        let mut gaps = GapLog::new();
        for symbol in &symbols {
            books.insert(symbol.clone(), caps.new_book(symbol));
            symbol_seq.insert(symbol.clone(), caps.new_seq_state());
            // Register up front so a symbol that never delivers a message is
            // reported as an outage rather than omitted from the table.
            gaps.ensure_symbol(caps.id, symbol);
        }
        for limit in &caps.detection_limits {
            gaps.declare_limit(limit.clone());
        }
        // Built before the struct literal moves `caps` and `symbols`.
        let l3_books: BTreeMap<Symbol, crate::book::l3::L3Book> =
            if caps.book_level == crate::types::BookLevel::L3 {
                symbols
                    .iter()
                    .map(|s| (s.clone(), crate::book::l3::L3Book::new(s.clone())))
                    .collect()
            } else {
                BTreeMap::new()
            };
        let connection_seq = caps.new_seq_state();
        let scratch = caps.new_book(&Symbol::new("SCRATCH", "SCRATCH"));
        BookSession {
            venue,
            caps,
            venue_id,
            symbols,
            books,
            ready: BTreeSet::new(),
            connection_seq,
            symbol_seq,
            scratch,
            pending: BTreeMap::new(),
            l3_books,
            l3_snapshot_floor: BTreeMap::new(),
            l3_crossed_since: BTreeMap::new(),
            archive: None,
            msg_index: 0,
            gaps,
        }
    }

    /// Start collecting rows for the archive, batching at `batch_rows`.
    ///
    /// Off by default: a session used for replay or for the gate has no reason
    /// to build Arrow arrays it will throw away.
    pub fn enable_archive(&mut self, batch_rows: usize) {
        // The feed's own depth window travels with the rows, so a rebuild can
        // truncate exactly as this session does rather than guessing.
        let depth = match self.caps.feed_depth {
            crate::venue::FeedDepth::Limited(n) => Some(n),
            crate::venue::FeedDepth::Full => None,
        };
        self.archive =
            Some(crate::pipeline::RowAccumulator::new(batch_rows).with_feed_depth(depth));
    }

    /// Batches for partitions that have accumulated enough rows.
    pub fn take_archive_batches(
        &mut self,
    ) -> Vec<(
        crate::store::writer::PartitionKey,
        arrow::array::RecordBatch,
        (i64, i64),
    )> {
        self.archive
            .as_mut()
            .map(|a| a.take_ready())
            .unwrap_or_default()
    }

    /// Everything held, whether or not it reached the batch size. Call before
    /// shutting down, or the last partial batch never reaches disk.
    pub fn flush_archive(
        &mut self,
    ) -> Vec<(
        crate::store::writer::PartitionKey,
        arrow::array::RecordBatch,
        (i64, i64),
    )> {
        self.archive
            .as_mut()
            .map(|a| a.take_all())
            .unwrap_or_default()
    }

    /// Rows collected but not yet handed over.
    pub fn archive_pending_rows(&self) -> usize {
        self.archive.as_ref().map(|a| a.pending_rows()).unwrap_or(0)
    }

    /// Note rows the writer could not keep up with.
    pub fn record_backpressure_drop(&mut self, symbol: &Symbol, rows: u64, at: Stamp) {
        self.gaps
            .record_backpressure_drop(self.venue_id, symbol, rows, at);
    }

    /// True when this venue needs recent deltas kept for snapshot splicing.
    ///
    /// Only REST-snapshot venues do. When the snapshot arrives in band, a
    /// resync means resubscribing and the venue resends the world, so there is
    /// nothing to splice and nothing to remember.
    fn buffers_deltas(&self) -> bool {
        self.caps.snapshot_source == SnapshotSource::Rest
    }

    /// Keep a delta for a future snapshot splice.
    ///
    /// Called for replayed deltas as well as live ones. The buffer records what
    /// we have *seen*, not what arrived most recently off the socket: after a
    /// replay empties it, a second snapshot arriving moments later would
    /// otherwise have no history to splice against and would leave exactly the
    /// hole the first splice repaired. Measured as a storm of twenty-two
    /// re-snapshots in twenty seconds.
    fn remember(&mut self, delta: &BookDelta) {
        if !self.buffers_deltas() {
            return;
        }
        let queue = self.pending.entry(delta.symbol.clone()).or_default();
        if queue.len() >= PENDING_CAPACITY {
            queue.pop_front();
        }
        queue.push_back(delta.clone());
    }

    pub fn venue_id(&self) -> VenueId {
        self.venue_id
    }

    pub fn symbols(&self) -> &[Symbol] {
        &self.symbols
    }

    pub fn book(&self, symbol: &Symbol) -> Option<&L2Book> {
        self.books.get(symbol)
    }

    /// The order-by-order book, on a venue that publishes one.
    pub fn l3_book(&self, symbol: &Symbol) -> Option<&crate::book::l3::L3Book> {
        self.l3_books.get(symbol)
    }

    /// Seed the order book from a venue's order-level snapshot.
    ///
    /// Everything seeded is marked uncertain: the snapshot gives the orders,
    /// but their relative order is the venue's listing order rather than
    /// something observed.
    pub fn apply_order_snapshot(
        &mut self,
        snapshot: crate::venue::OrderSnapshot,
    ) -> Vec<SessionAction> {
        let symbol = snapshot.symbol.clone();
        let at = snapshot.stamps.recv;
        if let Some(nanos) = snapshot.stamps.venue_nanos {
            self.l3_snapshot_floor.insert(symbol.clone(), nanos);
        }
        if let Some(book) = self.l3_books.get_mut(&symbol) {
            book.seed(snapshot.orders, at);
            // The aggregated view keeps everything built on L2 working.
            let l2 = book.to_l2(snapshot.stamps);
            self.books.insert(symbol.clone(), l2);
            self.ready.insert(symbol.clone());
            self.gaps.record_resnapshot(self.venue_id, &symbol, at);
            if let Some(state) = self.symbol_seq.get_mut(&symbol) {
                state.reset();
            }
        }
        Vec::new()
    }

    /// Validate and apply one order-by-order event.
    fn apply_order(
        &mut self,
        venue: &Arc<dyn Venue>,
        order: crate::book::l3::OrderEvent,
        at: Stamp,
        actions: &mut Vec<SessionAction>,
        blocked: bool,
    ) {
        let symbol = order.symbol.clone();
        if !self.l3_books.contains_key(&symbol) {
            return;
        }
        self.gaps.record_message(self.venue_id, &symbol, at);

        // The chain is checked whether or not the book is seeded: a break is
        // worth knowing about even while the book is still filling in.
        let event = FeedEvent::Order(Box::new(order.clone()));
        let verdict = {
            let state = self
                .symbol_seq
                .get_mut(&symbol)
                .expect("state exists for every subscribed symbol");
            venue.validate_sequence(state, &event, &self.scratch)
        };
        self.gaps
            .record_verdict(self.venue_id, &symbol, &verdict, at);
        if verdict.requires_resnapshot() {
            self.mark_suspect(&symbol, cause_for(&verdict), at, actions);
            return;
        }
        if blocked || !verdict.should_apply() {
            return;
        }

        // Already inside the snapshot we seeded from. The chain has seen it, so
        // it stays in step; applying it would resurrect orders the snapshot had
        // already dropped.
        if let (Some(floor), Some(event_nanos)) = (
            self.l3_snapshot_floor.get(&symbol),
            order.stamps.venue_nanos,
        ) && event_nanos <= *floor
        {
            self.gaps.record_verdict(
                self.venue_id,
                &symbol,
                &SeqVerdict::Stale {
                    seq: event_nanos.max(0) as u128,
                    floor: (*floor).max(0) as u128,
                },
                at,
            );
            return;
        }

        let mut outcome = self
            .l3_books
            .get_mut(&symbol)
            .expect("book exists")
            .apply(&order);

        // If the crossing is between an order we watched arrive and one we only
        // assumed was there, the assumption gives way. Bitstamp's snapshot
        // contains orders its stream never retracts.
        if outcome
            .anomalies
            .iter()
            .any(|a| matches!(a, crate::book::l3::L3Anomaly::Crossed { .. }))
        {
            let pruned = self
                .l3_books
                .get_mut(&symbol)
                .expect("book exists")
                .prune_crossed_seeded();
            if pruned > 0 {
                self.gaps
                    .record_stale_seed_orders(self.venue_id, &symbol, pruned as u64);
                outcome
                    .anomalies
                    .retain(|a| !matches!(a, crate::book::l3::L3Anomaly::Crossed { .. }));
            }
        }

        // An event for an order we never saw is expected while the book is
        // still filling in from an unseeded start, and is real evidence once it
        // has been seeded.
        let seeded = self.ready.contains(&symbol);
        let now = order.stamps.venue_nanos.unwrap_or(at.wall_nanos);
        let crossed_persisted = self.track_l3_crossing(&symbol, now);
        for anomaly in &outcome.anomalies {
            let corrupting = match anomaly {
                crate::book::l3::L3Anomaly::UnknownOrder { .. } => seeded,
                crate::book::l3::L3Anomaly::PriceMoved { .. } => false,
                // Transient by construction; only a lasting one is a fault.
                crate::book::l3::L3Anomaly::Crossed { .. } => crossed_persisted,
                _ => true,
            };
            self.gaps
                .record_l3_anomaly(self.venue_id, &symbol, anomaly, at, corrupting);
            // A crossing that outlasts a match is a book that needs rebuilding,
            // and saying so without asking for a new snapshot would leave the
            // symbol suspect for the rest of the run.
            if corrupting {
                self.mark_suspect(&symbol, SuspectCause::AwaitingSnapshot, at, actions);
            }
        }

        // Keep the aggregated view in step, so everything built on L2 still
        // works and the two can be compared against each other.
        let l2 = self
            .l3_books
            .get(&symbol)
            .expect("book exists")
            .to_l2(order.stamps);
        self.books.insert(symbol.clone(), l2);
        self.gaps.record_applied(self.venue_id, &symbol);
        self.archive_order(&order, outcome.traded_qty);
    }

    /// Track how long an L3 book has been crossed, in venue time.
    ///
    /// Returns true once a crossing has lasted longer than an aggressive order
    /// plausibly could, at which point it stops being a matching artefact and
    /// starts being a broken book.
    fn track_l3_crossing(&mut self, symbol: &Symbol, now_venue_nanos: i64) -> bool {
        /// An aggressive order and its match share a timestamp, so anything
        /// still crossed a millisecond later is not one.
        const TRANSIENT_LIMIT_NANOS: i64 = 1_000_000;

        let crossed = self
            .l3_books
            .get(symbol)
            .map(|b| b.is_crossed())
            .unwrap_or(false);
        if !crossed {
            self.l3_crossed_since.remove(symbol);
            return false;
        }
        match self.l3_crossed_since.get(symbol) {
            Some(since) => now_venue_nanos - since > TRANSIENT_LIMIT_NANOS,
            None => {
                self.l3_crossed_since
                    .insert(symbol.clone(), now_venue_nanos);
                false
            }
        }
    }

    /// Record an order event in the archive, with its inferred queue position.
    fn archive_order(&mut self, order: &crate::book::l3::OrderEvent, traded: Option<crate::Fixed>) {
        let suspect = self.gaps.is_suspect(self.venue_id, &order.symbol);
        let position = self
            .l3_books
            .get(&order.symbol)
            .and_then(|b| b.queue_position(&order.order_id));
        let venue_id = self.venue_id;
        let index = self.msg_index;
        self.msg_index += 1;
        if let Some(archive) = self.archive.as_mut() {
            archive.push_order(venue_id, order, position, traded, index, suspect);
        }
    }

    /// True when this symbol's book is currently trusted.
    pub fn is_ready(&self, symbol: &Symbol) -> bool {
        self.ready.contains(symbol)
    }

    pub fn gaps(&self) -> &GapLog {
        &self.gaps
    }

    pub fn report(&self) -> GapReport {
        self.gaps.report()
    }

    /// Close every open suspect window, as at the end of a run. Without this
    /// the report understates suspect time by however long the last window had
    /// been open.
    pub fn seal(&mut self, at: Stamp) {
        self.gaps.seal(at);
    }

    /// Feed one raw frame through parse, validate, and apply.
    pub fn ingest(&mut self, frame: &RawFrame) -> Result<Vec<SessionAction>> {
        let venue = Arc::clone(&self.venue);
        let events = venue.parse_delta(frame)?;
        let mut actions = Vec::new();
        let at = frame.stamp;

        for event in &events {
            if let FeedEvent::Control(control) = event
                && control.kind == ControlKind::SubscriptionError
            {
                for symbol in self.symbols.clone() {
                    self.gaps.open_window(
                        self.venue_id,
                        &symbol,
                        SuspectCause::AwaitingSnapshot,
                        at,
                    );
                    self.ready.remove(&symbol);
                }
                actions.push(SessionAction::SubscriptionFailed(control.detail.clone()));
            }
        }

        // Branch one of two: whose state is this?
        //
        // A connection-scoped counter numbers the socket, so the frame is judged
        // once and the verdict lands on every symbol we are recording here, not
        // only the ones this frame mentions. Messages for the other symbols
        // could just as easily have been the ones lost.
        let mut frame_blocked = false;
        if self.caps.gap_is_connection_wide() {
            // Every frame, including acknowledgements and heartbeats. On a
            // venue that numbers the whole socket, an unobserved control frame
            // leaves the counter one behind and the next book message looks
            // like a gap that never happened.
            if let Some(event) = events.first() {
                let verdict =
                    venue.validate_sequence(&mut self.connection_seq, event, &self.scratch);
                frame_blocked = !verdict.should_apply();
                if verdict.requires_resnapshot() {
                    let cause = cause_for(&verdict);
                    for symbol in self.symbols.clone() {
                        self.gaps
                            .record_verdict(self.venue_id, &symbol, &verdict, at);
                        self.ready.remove(&symbol);
                    }
                    actions.push(SessionAction::Resnapshot {
                        symbols: self.symbols.clone(),
                        cause,
                    });
                    frame_blocked = true;
                } else {
                    for symbol in self.symbols.clone() {
                        self.gaps
                            .record_verdict(self.venue_id, &symbol, &verdict, at);
                    }
                }
            }
        }

        for event in events {
            match event {
                FeedEvent::Control(_) => {}
                FeedEvent::Snapshot(snapshot) => {
                    // A snapshot is always applied. It is the thing that ends a
                    // suspect window, so refusing it because the stream is
                    // blocked would deadlock the session.
                    self.apply_snapshot(&venue, snapshot, at, &mut actions);
                }
                FeedEvent::Delta(delta) => {
                    self.apply_one_delta(&venue, delta, at, &mut actions, frame_blocked);
                }
                FeedEvent::Order(order) => {
                    self.apply_order(&venue, *order, at, &mut actions, frame_blocked);
                }
            }
        }
        Ok(actions)
    }

    /// Re-apply buffered deltas that postdate a freshly installed snapshot.
    ///
    /// This is what makes a mid-stream REST re-snapshot correct rather than
    /// merely plausible. A REST book is not current: Bitstamp's runs about a
    /// second behind its own websocket. Resetting to it and resuming from the
    /// next live message silently drops every change in that second, and those
    /// dropped changes surface later as deletes for levels we never saw, which
    /// forces another re-snapshot, which drops another second. Measured before
    /// this existed: twelve re-snapshots in forty seconds on a healthy feed.
    ///
    /// The validator decides what to replay. Deltas the snapshot already
    /// contains come back [`SeqVerdict::Stale`] and are dropped; the rest are
    /// applied in their original order.
    fn replay_pending(
        &mut self,
        venue: &Arc<dyn Venue>,
        symbol: &Symbol,
        actions: &mut Vec<SessionAction>,
    ) {
        if !self.buffers_deltas() {
            return;
        }
        let Some(queue) = self.pending.get_mut(symbol) else {
            return;
        };
        let buffered: Vec<BookDelta> = queue.drain(..).collect();
        for delta in buffered {
            if !self.ready.contains(symbol) {
                // The splice itself went wrong; stop rather than pile more on.
                break;
            }
            let at = delta.stamps.recv;
            self.apply_one_delta_inner(venue, delta, at, actions, false, false);
        }
    }

    /// Validate and apply one delta.
    ///
    /// `blocked` is true when the frame carrying it was already judged bad by a
    /// connection-scoped counter. `from_live` distinguishes a message arriving
    /// off the socket from one being replayed out of the pending buffer, so a
    /// replayed delta is neither counted twice nor buffered again.
    fn apply_one_delta(
        &mut self,
        venue: &Arc<dyn Venue>,
        delta: BookDelta,
        at: Stamp,
        actions: &mut Vec<SessionAction>,
        blocked: bool,
    ) {
        self.apply_one_delta_inner(venue, delta, at, actions, blocked, true)
    }

    fn apply_one_delta_inner(
        &mut self,
        venue: &Arc<dyn Venue>,
        delta: BookDelta,
        at: Stamp,
        actions: &mut Vec<SessionAction>,
        blocked: bool,
        from_live: bool,
    ) {
        let symbol = delta.symbol.clone();
        if !self.books.contains_key(&symbol) {
            // A symbol we never asked for. Not an error, but not something to
            // silently accumulate either.
            return;
        }
        if from_live {
            self.gaps.record_message(self.venue_id, &symbol, at);
        }

        if blocked || !self.ready.contains(&symbol) {
            // Inside a suspect window: count it, do not apply it. Stitching
            // deltas onto a book known to be wrong is how a dataset ends up
            // quietly corrupt. It is still remembered, because the snapshot
            // that ends this window may predate it, in which case this is
            // exactly what has to go back on top.
            self.remember(&delta);
            if from_live {
                self.gaps.record_verdict(
                    self.venue_id,
                    &symbol,
                    &SeqVerdict::Unverifiable(Unverifiable::AwaitingSnapshot),
                    at,
                );

                // A venue whose snapshot arrives in band will resend one when
                // we resubscribe, so waiting is enough. A REST-snapshot venue
                // will never volunteer one, so unless we ask again here the
                // symbol stays suspect for the rest of the run. Observed as a
                // report reading 0.24% clean on a feed that had recovered
                // within a second. The venue's REST rate limiter bounds how
                // often this actually fires.
                if self.caps.snapshot_source == SnapshotSource::Rest {
                    actions.push(SessionAction::Resnapshot {
                        symbols: vec![symbol.clone()],
                        cause: SuspectCause::AwaitingSnapshot,
                    });
                }
            }
            return;
        }

        // Branch two of two: before or after applying?
        match self.caps.timing {
            ValidationTiming::BeforeApply => {
                let verdict = if self.caps.gap_is_connection_wide() {
                    // Already judged once for the whole frame.
                    SeqVerdict::InOrder
                } else {
                    let state = self
                        .symbol_seq
                        .get_mut(&symbol)
                        .expect("state exists for every subscribed symbol");
                    let v = venue.validate_sequence(
                        state,
                        &FeedEvent::Delta(delta.clone()),
                        &self.scratch,
                    );
                    if from_live {
                        self.gaps.record_verdict(self.venue_id, &symbol, &v, at);
                    }
                    v
                };
                if verdict.requires_resnapshot() {
                    self.remember(&delta);
                    self.mark_suspect(&symbol, cause_for(&verdict), at, actions);
                    return;
                }
                if !verdict.should_apply() {
                    return;
                }
                let outcome = self
                    .books
                    .get_mut(&symbol)
                    .expect("book exists")
                    .apply_delta(&delta);
                self.remember(&delta);
                self.absorb_outcome(&symbol, outcome, at, actions);
                self.archive_delta(&delta, from_live);
            }
            ValidationTiming::AfterApply => {
                let outcome = self
                    .books
                    .get_mut(&symbol)
                    .expect("book exists")
                    .apply_delta(&delta);
                let book = self.books.get(&symbol).expect("book exists");
                let state = self
                    .symbol_seq
                    .get_mut(&symbol)
                    .expect("state exists for every subscribed symbol");
                let verdict =
                    venue.validate_sequence(state, &FeedEvent::Delta(delta.clone()), book);
                if from_live {
                    self.gaps
                        .record_verdict(self.venue_id, &symbol, &verdict, at);
                    self.remember(&delta);
                }
                if verdict.requires_resnapshot() {
                    // The update is already in the book, so the book is
                    // contaminated and only a snapshot fixes it.
                    self.mark_suspect(&symbol, cause_for(&verdict), at, actions);
                } else if from_live {
                    self.gaps.record_applied(self.venue_id, &symbol);
                }
                self.absorb_outcome(&symbol, outcome, at, actions);
                self.archive_delta(&delta, from_live);
            }
        }
    }

    /// Install an out-of-band snapshot, for venues whose snapshots arrive over
    /// REST rather than on the socket.
    pub fn apply_rest_snapshot(&mut self, snapshot: BookSnapshot) -> Vec<SessionAction> {
        let venue = Arc::clone(&self.venue);
        let at = snapshot.stamps.recv;
        let mut actions = Vec::new();
        self.apply_snapshot(&venue, snapshot, at, &mut actions);
        actions
    }

    fn apply_snapshot(
        &mut self,
        venue: &Arc<dyn Venue>,
        snapshot: BookSnapshot,
        at: Stamp,
        actions: &mut Vec<SessionAction>,
    ) {
        let symbol = snapshot.symbol.clone();
        if !self.books.contains_key(&symbol) {
            return;
        }
        self.gaps.record_message(self.venue_id, &symbol, at);

        // A snapshot supersedes everything, so the validator starts clean.
        if let Some(state) = self.symbol_seq.get_mut(&symbol) {
            state.reset();
        }

        let outcome = self
            .books
            .get_mut(&symbol)
            .expect("book exists")
            .reset_from_snapshot(&snapshot);

        let verdict = match self.caps.timing {
            ValidationTiming::BeforeApply => {
                let state = if self.caps.gap_is_connection_wide() {
                    &mut self.connection_seq
                } else {
                    self.symbol_seq.get_mut(&symbol).expect("state exists")
                };
                venue.validate_sequence(
                    state,
                    &FeedEvent::Snapshot(snapshot.clone()),
                    &self.scratch,
                )
            }
            ValidationTiming::AfterApply => {
                let book = self.books.get(&symbol).expect("book exists");
                let state = self.symbol_seq.get_mut(&symbol).expect("state exists");
                venue.validate_sequence(state, &FeedEvent::Snapshot(snapshot.clone()), book)
            }
        };
        self.gaps
            .record_verdict(self.venue_id, &symbol, &verdict, at);

        if verdict.is_loss() {
            // A snapshot whose own checksum does not match is not a usable
            // baseline. Trusting it would silently start the next window from a
            // wrong book.
            self.ready.remove(&symbol);
            self.mark_suspect(&symbol, cause_for(&verdict), at, actions);
        } else {
            self.ready.insert(symbol.clone());
            self.gaps.record_resnapshot(self.venue_id, &symbol, at);
        }
        self.absorb_outcome(&symbol, outcome, at, actions);
        // Snapshots are archived too: without them the deltas that follow have
        // nothing to be applied to, and phase 5 cannot rebuild a book at all.
        let suspect = self.gaps.is_suspect(self.venue_id, &symbol);
        let venue_id = self.venue_id;
        let index = self.msg_index;
        self.msg_index += 1;
        if let Some(archive) = self.archive.as_mut() {
            archive.push_snapshot(venue_id, &snapshot, index, suspect);
        }
        // A REST book is behind the stream it is being spliced into, so
        // whatever we already saw and it does not contain has to go back on top.
        self.replay_pending(venue, &symbol, actions);
    }

    /// Record a delta in the archive, marked with whether the book was suspect
    /// at that moment.
    ///
    /// The flag is read *after* the message has been judged, so a message that
    /// itself revealed a gap is marked suspect rather than clean. Replayed
    /// deltas are skipped: they were archived the first time they arrived.
    fn archive_delta(&mut self, delta: &BookDelta, from_live: bool) {
        if !from_live {
            return;
        }
        let suspect = self.gaps.is_suspect(self.venue_id, &delta.symbol);
        let venue_id = self.venue_id;
        let index = self.msg_index;
        self.msg_index += 1;
        if let Some(archive) = self.archive.as_mut() {
            archive.push_delta(venue_id, delta, index, suspect);
        }
    }

    fn absorb_outcome(
        &mut self,
        symbol: &Symbol,
        outcome: crate::book::ApplyOutcome,
        at: Stamp,
        actions: &mut Vec<SessionAction>,
    ) {
        for anomaly in &outcome.anomalies {
            let corrupting = self.caps.corrupts_book(anomaly);
            self.gaps
                .record_anomaly(self.venue_id, symbol, *anomaly, at, corrupting);
        }
        // Whether an anomaly invalidates the book is a venue-level question, so
        // it is read from capabilities rather than from the anomaly alone. A
        // delete for an absent level is loss evidence on Kraken and ordinary
        // noise on Coinbase; acting on the Coinbase case made the recorder
        // rebuild a 4.8 MB book every two seconds for no reason.
        if let Some(anomaly) = outcome
            .anomalies
            .iter()
            .find(|a| self.caps.corrupts_book(a))
        {
            self.mark_suspect(symbol, SuspectCause::Anomaly(*anomaly), at, actions);
        }
    }

    fn mark_suspect(
        &mut self,
        symbol: &Symbol,
        cause: SuspectCause,
        at: Stamp,
        actions: &mut Vec<SessionAction>,
    ) {
        self.ready.remove(symbol);
        self.gaps.open_window(self.venue_id, symbol, cause, at);
        let already_queued = actions.iter().any(|a| match a {
            SessionAction::Resnapshot { symbols, .. } => symbols.contains(symbol),
            _ => false,
        });
        if !already_queued {
            actions.push(SessionAction::Resnapshot {
                symbols: vec![symbol.clone()],
                cause,
            });
        }
    }

    /// The socket dropped. Every book is stale from this instant until a fresh
    /// snapshot arrives.
    pub fn on_disconnect(&mut self, at: Stamp) {
        for symbol in self.symbols.clone() {
            self.ready.remove(&symbol);
            self.gaps.record_disconnect(self.venue_id, &symbol, at);
        }
        self.connection_seq.reset();
        for state in self.symbol_seq.values_mut() {
            state.reset();
        }
    }
}

fn cause_for(verdict: &SeqVerdict) -> SuspectCause {
    match verdict {
        SeqVerdict::Gap(evidence) => SuspectCause::SequenceGap {
            size: evidence.size,
        },
        SeqVerdict::Divergence { expected, computed } => SuspectCause::ChecksumDivergence {
            expected: *expected,
            computed: *computed,
        },
        SeqVerdict::Reset { .. } => SuspectCause::SequenceReset,
        SeqVerdict::OutOfOrder { previous, observed } => SuspectCause::OutOfOrder {
            previous: *previous,
            observed: *observed,
        },
        _ => SuspectCause::AwaitingSnapshot,
    }
}

/// Exponential backoff with jitter, for reconnects.
///
/// The jitter is derived from the clock rather than a random number generator
/// so a replayed run reconnects on the same schedule as the recorded one.
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    pub base: Duration,
    pub max: Duration,
    pub factor: u32,
    attempt: u32,
}

impl Default for Backoff {
    fn default() -> Self {
        Backoff {
            base: Duration::from_millis(250),
            max: Duration::from_secs(30),
            factor: 2,
            attempt: 0,
        }
    }
}

impl Backoff {
    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Next delay, growing geometrically and capped, with up to 25% jitter so a
    /// fleet of recorders does not reconnect in lockstep and get rate limited
    /// together.
    pub fn next(&mut self, clock: &dyn Clock) -> Duration {
        let scale = self.factor.saturating_pow(self.attempt.min(16));
        let raw = self.base.saturating_mul(scale.max(1)).min(self.max);
        self.attempt = self.attempt.saturating_add(1);
        let jitter_bucket = (clock.stamp().mono_nanos % 256) as u32;
        let jitter = raw / 4 * jitter_bucket / 256;
        (raw.saturating_sub(raw / 8) + jitter).min(self.max)
    }
}

/// Where a run sends what it captures.
///
/// The two sinks are deliberately independent. The raw tape is the durable,
/// line-at-a-time record that a parser bug cannot destroy; the archive is the
/// published artifact. A run can have either, both, or neither.
#[derive(Default, Clone)]
pub struct RunSinks {
    /// Raw frames, exactly as they arrived.
    pub tape: Option<Arc<tokio::sync::Mutex<RawRecorder>>>,
    /// The Parquet archive, behind its bounded channel.
    pub archive: Option<Arc<crate::pipeline::Pipeline>>,
    /// Rows to accumulate per partition before handing a batch over.
    pub archive_batch_rows: usize,
    /// Hand over whatever has accumulated at least this often.
    ///
    /// Without this the crash bound is not the file rotation interval, it is
    /// however long a quiet venue takes to produce a full batch, which is
    /// unbounded. Measured on a real run: a forty second recording of two
    /// Kraken symbols produced only four files despite a five second rotation,
    /// because the rows were still sitting in memory waiting for a batch to
    /// fill. The true bound is this interval plus the rotation interval.
    pub archive_flush_interval: Duration,
}

impl RunSinks {
    pub fn tape(recorder: RawRecorder) -> Self {
        RunSinks {
            tape: Some(Arc::new(tokio::sync::Mutex::new(recorder))),
            ..Self::none()
        }
    }

    pub fn none() -> Self {
        RunSinks {
            tape: None,
            archive: None,
            archive_batch_rows: 4_096,
            archive_flush_interval: Duration::from_secs(1),
        }
    }

    pub fn with_archive(mut self, pipeline: Arc<crate::pipeline::Pipeline>) -> Self {
        self.archive = Some(pipeline);
        self
    }
}

/// When a recording run should stop.
#[derive(Debug, Clone, Copy, Default)]
pub struct StopAfter {
    pub frames: Option<u64>,
    pub duration: Option<Duration>,
}

impl StopAfter {
    pub fn frames(n: u64) -> Self {
        StopAfter {
            frames: Some(n),
            ..Default::default()
        }
    }

    pub fn duration(d: Duration) -> Self {
        StopAfter {
            duration: Some(d),
            ..Default::default()
        }
    }
}

/// What a recording run produced.
#[derive(Debug)]
pub struct RunOutcome {
    pub report: GapReport,
    pub frames: u64,
    pub reconnects: u32,
}

impl RunOutcome {
    /// Combine the results of two connections to the same venue.
    pub fn merge(self, other: RunOutcome) -> RunOutcome {
        RunOutcome {
            report: self.report.merge(other.report),
            frames: self.frames + other.frames,
            reconnects: self.reconnects + other.reconnects,
        }
    }
}

/// Connect, subscribe, ingest, and reconnect until the stop condition is met.
///
/// Every reconnect re-snapshots. There is no path here that resumes a book
/// across a disconnection, because there is no way to know what was missed
/// while the socket was down.
pub async fn run_connection(
    venue: Arc<dyn Venue>,
    symbols: Vec<Symbol>,
    clock: Arc<dyn Clock>,
    stop: StopAfter,
    sinks: RunSinks,
) -> Result<RunOutcome> {
    let recorder = sinks.tape.clone();
    let mut session = BookSession::new(Arc::clone(&venue), symbols.clone());
    if sinks.archive.is_some() {
        session.enable_archive(sinks.archive_batch_rows);
    }
    let mut backoff = Backoff::default();
    let mut frames: u64 = 0;
    let mut reconnects: u32 = 0;
    let started = tokio::time::Instant::now();
    let caps = venue.capabilities().clone();
    let mut limiter = VenueLimiter::new(&caps.budget);
    let deadline = stop.duration.map(|d| started + d);

    /// What woke the ingest loop.
    enum Wake {
        Frame(Result<Option<RawFrame>>),
        Keepalive,
        ArchiveFlush,
        Idle,
        Deadline,
    }

    /// How long a feed may say nothing before we treat the socket as dead.
    ///
    /// A server that stops sending without closing the connection leaves the
    /// TCP session ESTABLISHED and the reader waiting forever. Measured on a
    /// three hour capture: Bitstamp went silent and the recorder sat on a live
    /// socket at zero CPU for sixty two minutes, writing nothing. That is the
    /// worst failure this project can have, because the archive ends up with no
    /// rows and the gap report has nothing to report: it reads as a quiet
    /// market rather than as a lost feed.
    ///
    /// Sixty seconds is far longer than any venue in the matrix stays quiet on
    /// a major pair, and a genuinely quiet feed is indistinguishable from a dead
    /// one from here. Reconnecting costs a re-snapshot and is recorded as
    /// downtime; sitting there costs the data and says nothing.
    const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

    'outer: loop {
        if deadline.is_some_and(|d| tokio::time::Instant::now() >= d) {
            break;
        }

        // Venues ban an IP that reconnects too eagerly, and a ban looks exactly
        // like an outage the venue caused.
        let wait = limiter.connects.acquire(clock.as_ref());
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }

        let mut transport = match venue.connect().await {
            Ok(t) => t,
            Err(e) if e.is_transient() => {
                let delay = backoff.next(clock.as_ref());
                tracing::warn!(venue = %venue.id(), error = %e, ?delay, "connect failed, backing off");
                tokio::time::sleep(delay).await;
                reconnects += 1;
                continue;
            }
            Err(e) => return Err(e),
        };

        for frame in venue.subscribe(&symbols)? {
            transport.send_text(&frame).await?;
        }

        // A fresh connection always means a fresh snapshot. For a venue with no
        // in-band snapshot this is the REST fetch, and it happens *after*
        // subscribing so the diffs that arrive while it is in flight are
        // already queued rather than lost.
        // A feed with no usable snapshot starts empty and converges; asking it
        // for one would only splice in data it has already contradicted.
        if caps.snapshot_source == SnapshotSource::Rest {
            for symbol in &symbols {
                let wait = limiter.rest.acquire(clock.as_ref());
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
                // An order-by-order venue needs the order-level snapshot; the
                // aggregated one would leave every order unidentifiable.
                if let Some(orders) = venue.order_snapshot(symbol).await? {
                    session.apply_order_snapshot(orders);
                    continue;
                }
                let snapshot = venue.snapshot(symbol).await?;
                for action in session.apply_rest_snapshot(snapshot) {
                    tracing::warn!(
                        venue = %venue.id(), %symbol, ?action,
                        "snapshot did not settle the book"
                    );
                }
            }
        }
        backoff.reset();

        // Per connection, so a reconnect starts the silence clock fresh rather
        // than inheriting however long the dead one had been quiet.
        let mut last_frame_at = tokio::time::Instant::now();

        let mut archive_flush = sinks.archive.as_ref().map(|_| {
            let mut interval = tokio::time::interval(sinks.archive_flush_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval
        });
        let mut keepalive = match caps.keepalive {
            Keepalive::Text { every, .. } => {
                let mut interval = tokio::time::interval(every);
                // The first tick fires immediately otherwise, sending a ping
                // before the subscription is even acknowledged.
                interval.reset();
                Some(interval)
            }
            Keepalive::ProtocolPings => None,
        };

        loop {
            if stop.frames.is_some_and(|limit| frames >= limit) {
                break 'outer;
            }

            let wake = tokio::select! {
                biased;
                result = transport.recv() => Wake::Frame(result),
                _ = tokio::time::sleep_until(last_frame_at + IDLE_TIMEOUT) => Wake::Idle,
                _ = async {
                    match keepalive.as_mut() {
                        Some(ticker) => { ticker.tick().await; }
                        None => std::future::pending::<()>().await,
                    }
                } => Wake::Keepalive,
                _ = async {
                    match archive_flush.as_mut() {
                        Some(ticker) => { ticker.tick().await; }
                        None => std::future::pending::<()>().await,
                    }
                } => Wake::ArchiveFlush,
                _ = async {
                    match deadline {
                        Some(at) => tokio::time::sleep_until(at).await,
                        None => std::future::pending::<()>().await,
                    }
                } => Wake::Deadline,
            };

            let next = match wake {
                Wake::Deadline => break 'outer,
                Wake::Idle => {
                    tracing::warn!(
                        venue = %venue.id(), ?IDLE_TIMEOUT,
                        "feed went silent on a live socket, reconnecting"
                    );
                    // Recorded as downtime we noticed, not as a clean window.
                    session.on_disconnect(clock.stamp());
                    reconnects += 1;
                    let delay = backoff.next(clock.as_ref());
                    tokio::time::sleep(delay).await;
                    continue 'outer;
                }
                Wake::Keepalive => {
                    if let Keepalive::Text { payload, .. } = caps.keepalive {
                        // A missed heartbeat gets the socket closed, which then
                        // shows up in the dataset as a gap the venue did not
                        // cause.
                        transport.send_text(payload).await?;
                    }
                    continue;
                }
                Wake::ArchiveFlush => {
                    // Whatever has accumulated goes to the writer now, so a
                    // quiet venue's rows are not held in memory indefinitely.
                    let pending = session.flush_archive();
                    drain_to_archive(&mut session, &sinks, pending, clock.stamp()).await;
                    continue;
                }
                Wake::Frame(result) => result,
            };

            match next {
                Ok(Some(frame)) => {
                    frames += 1;
                    last_frame_at = tokio::time::Instant::now();
                    if let Some(rec) = recorder.as_ref() {
                        rec.lock().await.record(&frame).await?;
                    }
                    let ingested = session.ingest(&frame);
                    if sinks.archive.is_some() {
                        let batches = session.take_archive_batches();
                        drain_to_archive(&mut session, &sinks, batches, frame.stamp).await;
                    }
                    match ingested {
                        Ok(actions) => {
                            for action in actions {
                                match action {
                                    SessionAction::Resnapshot { symbols, cause } => {
                                        tracing::warn!(
                                            venue = %venue.id(), ?symbols, %cause,
                                            "re-snapshotting"
                                        );
                                        match caps.snapshot_source {
                                            // An in-band snapshot only arrives
                                            // on subscribe, so resyncing means
                                            // reconnecting.
                                            SnapshotSource::InBand => {
                                                session.on_disconnect(frame.stamp);
                                                reconnects += 1;
                                                let delay = backoff.next(clock.as_ref());
                                                tokio::time::sleep(delay).await;
                                                continue 'outer;
                                            }
                                            // Nothing to fetch. The book is
                                            // already rebuilding itself from
                                            // the stream, and the suspect
                                            // window closes when it stops
                                            // reporting faults.
                                            SnapshotSource::None => {}
                                            SnapshotSource::Rest => {
                                                for symbol in symbols {
                                                    let wait = limiter.rest.acquire(clock.as_ref());
                                                    if !wait.is_zero() {
                                                        tokio::time::sleep(wait).await;
                                                    }
                                                    let snapshot = venue.snapshot(&symbol).await?;
                                                    session.apply_rest_snapshot(snapshot);
                                                }
                                            }
                                        }
                                    }
                                    SessionAction::ReconnectRequested(detail) => {
                                        tracing::info!(
                                            venue = %venue.id(), %detail,
                                            "venue asked us to reconnect"
                                        );
                                        session.on_disconnect(frame.stamp);
                                        reconnects += 1;
                                        continue 'outer;
                                    }
                                    SessionAction::SubscriptionFailed(detail) => {
                                        return Err(Error::Subscription {
                                            venue: venue.id(),
                                            reason: detail,
                                        });
                                    }
                                }
                            }
                        }
                        // A frame we cannot parse is logged and skipped. Killing
                        // the recorder over one malformed message would lose the
                        // rest of the day as well.
                        Err(e) => tracing::warn!(venue = %venue.id(), error = %e, "unparsed frame"),
                    }
                }
                Ok(None) => {
                    tracing::warn!(venue = %venue.id(), "feed closed");
                    session.on_disconnect(clock.stamp());
                    reconnects += 1;
                    let delay = backoff.next(clock.as_ref());
                    tokio::time::sleep(delay).await;
                    continue 'outer;
                }
                Err(e) if e.is_transient() => {
                    tracing::warn!(venue = %venue.id(), error = %e, "transport error");
                    session.on_disconnect(clock.stamp());
                    reconnects += 1;
                    let delay = backoff.next(clock.as_ref());
                    tokio::time::sleep(delay).await;
                    continue 'outer;
                }
                Err(e) => return Err(e),
            }
        }
    }

    // Whatever is still held has to go over before the channel closes, or the
    // tail of every partition is silently missing from the archive.
    if sinks.archive.is_some() {
        let remaining = session.flush_archive();
        drain_to_archive(&mut session, &sinks, remaining, clock.stamp()).await;
    }
    session.seal(clock.stamp());
    if let Some(rec) = recorder.as_ref() {
        rec.lock().await.flush().await?;
    }
    Ok(RunOutcome {
        report: session.report(),
        frames,
        reconnects,
    })
}

/// Hand accumulated batches to the writer, recording anything it could not take.
///
/// A drop here is a hole in the archive that we caused rather than the venue,
/// and it is written into the same gap report so it cannot be mistaken for
/// clean data.
async fn drain_to_archive(
    session: &mut BookSession,
    sinks: &RunSinks,
    batches: Vec<(
        crate::store::writer::PartitionKey,
        arrow::array::RecordBatch,
        (i64, i64),
    )>,
    at: Stamp,
) {
    let Some(pipeline) = sinks.archive.as_ref() else {
        return;
    };
    for (key, batch, span) in batches {
        match pipeline.submit(key.clone(), batch, span).await {
            crate::pipeline::Submitted::Dropped { rows } => {
                tracing::warn!(
                    venue = %key.venue, symbol = %key.symbol, rows,
                    "writer fell behind, dropping rows"
                );
                session.record_backpressure_drop(&key.symbol, rows as u64, at);
            }
            crate::pipeline::Submitted::Blocked(waited) => {
                tracing::warn!(
                    venue = %key.venue, symbol = %key.symbol, ?waited,
                    "blocked waiting for the writer"
                );
            }
            crate::pipeline::Submitted::WriterStopped => {
                tracing::error!(venue = %key.venue, "the archive writer stopped");
            }
            crate::pipeline::Submitted::Accepted => {}
        }
    }
}

/// Record a venue, laying its symbols out across connections under its budget.
///
/// This is where the capability matrix stops being documentation. A venue whose
/// sequence numbers belong to the connection gets its symbols spread across
/// sockets, because there one lost message invalidates every symbol sharing
/// one; a venue that numbers per symbol gets them packed, because there it
/// costs nothing. Each connection runs its own session and the reports are
/// merged into one table.
pub async fn run(
    venue: Arc<dyn Venue>,
    symbols: Vec<Symbol>,
    clock: Arc<dyn Clock>,
    stop: StopAfter,
    sinks: RunSinks,
) -> Result<(RunOutcome, RunSinks)> {
    let caps = venue.capabilities().clone();
    let plan = caps
        .plan_subscriptions(&symbols)
        .map_err(|e| Error::Other(e.to_string()))?;
    tracing::info!(
        venue = %venue.id(),
        connections = plan.connection_count(),
        blast_radius = plan.worst_case_blast_radius(caps.gap_is_connection_wide()),
        rationale = plan.rationale,
        "subscription plan"
    );

    let mut tasks = Vec::with_capacity(plan.connection_count());
    for group in plan.connections {
        tasks.push(tokio::spawn(run_connection(
            Arc::clone(&venue),
            group,
            Arc::clone(&clock),
            stop,
            sinks.clone(),
        )));
    }

    let mut merged: Option<RunOutcome> = None;
    let mut failure: Option<Error> = None;
    for task in tasks {
        match task.await {
            Ok(Ok(outcome)) => {
                merged = Some(match merged {
                    Some(acc) => acc.merge(outcome),
                    None => outcome,
                });
            }
            // One socket failing must not discard what the others captured.
            Ok(Err(e)) => failure = failure.or(Some(e)),
            Err(join) => failure = failure.or(Some(Error::Other(join.to_string()))),
        }
    }

    match (merged, failure) {
        (Some(outcome), _) => Ok((outcome, sinks)),
        (None, Some(e)) => Err(e),
        (None, None) => Err(Error::Other("the plan produced no connections".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ManualClock;

    #[test]
    fn backoff_grows_geometrically_and_is_capped() {
        let clock = ManualClock::new(0, 0);
        let mut b = Backoff {
            base: Duration::from_millis(100),
            max: Duration::from_secs(2),
            factor: 2,
            attempt: 0,
        };
        let delays: Vec<Duration> = (0..8).map(|_| b.next(&clock)).collect();
        assert!(delays[0] < delays[1] && delays[1] < delays[2]);
        assert!(delays.iter().all(|d| *d <= Duration::from_secs(2)));
        // Once capped, the delay sits inside the jitter band below the ceiling
        // rather than exactly on it. This clock never advances, so the bucket is
        // always zero and the value pins to the bottom of that band.
        assert_eq!(delays[6], delays[7], "growth must stop at the cap");
        assert_eq!(delays[7], Duration::from_millis(1_750));
        b.reset();
        assert_eq!(b.attempt(), 0);
    }

    #[test]
    fn merging_connection_outcomes_sums_their_work() {
        let outcome = |frames, reconnects| RunOutcome {
            report: GapReport {
                rows: Vec::new(),
                limits: Vec::new(),
            },
            frames,
            reconnects,
        };
        let merged = outcome(100, 1).merge(outcome(50, 2));
        assert_eq!(merged.frames, 150);
        assert_eq!(merged.reconnects, 3);
    }

    #[test]
    fn backoff_jitter_is_deterministic_for_a_given_clock() {
        let a = ManualClock::new(0, 7);
        let b = ManualClock::new(0, 7);
        let mut x = Backoff::default();
        let mut y = Backoff::default();
        let xs: Vec<Duration> = (0..5).map(|_| x.next(&a)).collect();
        let ys: Vec<Duration> = (0..5).map(|_| y.next(&b)).collect();
        assert_eq!(xs, ys);
    }
}
