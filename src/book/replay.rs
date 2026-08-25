//! Applying archived rows back onto a book.
//!
//! One implementation, used by both reconstruction and streaming. They differ
//! in *which* rows they feed it and where they start, which is where the bugs
//! have actually been; having two copies of the book logic that must agree
//! would add a class of bug rather than catch one.
//!
//! The unit is a **message**, never a row. On a depth-limited feed, applying a
//! message's levels one at a time truncates per level instead of per message,
//! and a message that adds a new best while removing the old worst gives a
//! different book each way.

use crate::book::l3::{L3Book, OrderAction, OrderEvent, OrderId};
use crate::book::{BookDelta, BookSnapshot, L2Book, LevelChange};
use crate::clock::{Stamp, Timestamps};
use crate::fixed::Fixed;
use crate::store::rows::Row;
use crate::store::schema::EventKind;
use crate::types::{BookLevel, Side, Symbol};

/// Replays archived messages onto a book.
#[derive(Debug, Clone)]
pub struct BookReplayer {
    symbol: Symbol,
    book: L2Book,
    l3: Option<L3Book>,
    /// The aggregated view of an L3 book is recomputed on demand rather than
    /// after every order: a 3,000 level book aggregated per message would cost
    /// more than the replay itself.
    aggregate_dirty: bool,
    messages: u64,
    rows: u64,
    suspect_rows: u64,
    suspect_span: Option<(i64, i64)>,
    last_wall: Option<i64>,
}

impl BookReplayer {
    /// `feed_depth` is the window the venue's feed maintained, which a replay
    /// has to maintain too or it accumulates levels that fell out long ago.
    pub fn new(symbol: Symbol, level: BookLevel, feed_depth: Option<usize>) -> Self {
        BookReplayer {
            book: match feed_depth {
                Some(n) => L2Book::with_depth_limit(symbol.clone(), n),
                None => L2Book::new(symbol.clone()),
            },
            l3: (level == BookLevel::L3).then(|| L3Book::new(symbol.clone())),
            symbol,
            aggregate_dirty: false,
            messages: 0,
            rows: 0,
            suspect_rows: 0,
            suspect_span: None,
            last_wall: None,
        }
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    /// The aggregated book. On an L3 replay this is the order book aggregated,
    /// so the two can never drift apart.
    pub fn book(&self) -> L2Book {
        match self.l3.as_ref() {
            Some(l3) => l3.to_l2(Timestamps::recv_only(Stamp {
                mono_nanos: 0,
                wall_nanos: self.last_wall.unwrap_or(0),
            })),
            None => self.book.clone(),
        }
    }

    /// The aggregated book without copying it.
    ///
    /// Needs `&mut` because an L3 replay aggregates on demand. A streaming
    /// consumer reads this once per message, so cloning here would dominate.
    pub fn book_ref(&mut self) -> &L2Book {
        if self.aggregate_dirty
            && let Some(l3) = self.l3.as_ref()
        {
            self.book = l3.to_l2(Timestamps::recv_only(Stamp {
                mono_nanos: 0,
                wall_nanos: self.last_wall.unwrap_or(0),
            }));
            self.aggregate_dirty = false;
        }
        &self.book
    }

    pub fn l3(&self) -> Option<&L3Book> {
        self.l3.as_ref()
    }

    pub fn messages(&self) -> u64 {
        self.messages
    }
    pub fn rows(&self) -> u64 {
        self.rows
    }
    pub fn suspect_rows(&self) -> u64 {
        self.suspect_rows
    }
    pub fn suspect_span(&self) -> Option<(i64, i64)> {
        self.suspect_span
    }
    pub fn last_wall(&self) -> Option<i64> {
        self.last_wall
    }

    /// Install an aggregated starting book.
    pub fn seed_l2(&mut self, bids: Vec<(Fixed, Fixed)>, asks: Vec<(Fixed, Fixed)>, at_wall: i64) {
        self.book.reset_from_snapshot(&BookSnapshot {
            symbol: self.symbol.clone(),
            bids,
            asks,
            seq: None,
            checksum: None,
            stamps: Timestamps::recv_only(Stamp {
                mono_nanos: 0,
                wall_nanos: at_wall,
            }),
        });
    }

    /// Install an order-by-order starting book, in queue order.
    pub fn seed_l3(&mut self, orders: Vec<(OrderId, Side, Fixed, Fixed)>, at_wall: i64) {
        self.aggregate_dirty = true;
        if let Some(l3) = self.l3.as_mut() {
            l3.seed(
                orders,
                Stamp {
                    mono_nanos: 0,
                    wall_nanos: at_wall,
                },
            );
        }
    }

    fn stamps(row: &Row) -> Timestamps {
        Timestamps::new(
            Stamp {
                // Never the archived `recv_mono`: it is anchored to whichever
                // recorder process wrote the row and does not survive a restart.
                mono_nanos: 0,
                wall_nanos: row.recv_wall,
            },
            row.venue_ts,
        )
    }

    /// Apply one whole message: all the rows that arrived together.
    pub fn apply_message(&mut self, rows: &[Row]) {
        let Some(first) = rows.first() else {
            return;
        };
        self.messages += 1;
        self.rows += rows.len() as u64;
        self.last_wall = Some(first.recv_wall);
        for row in rows {
            if row.suspect {
                self.suspect_rows += 1;
                self.suspect_span = Some(match self.suspect_span {
                    Some((a, b)) => (a.min(row.recv_wall), b.max(row.recv_wall)),
                    None => (row.recv_wall, row.recv_wall),
                });
            }
        }

        if first.book_level == BookLevel::L3 && self.l3.is_some() {
            for row in rows {
                self.apply_order(row);
            }
            self.aggregate_dirty = true;
            return;
        }

        let stamps = Self::stamps(first);
        match first.event {
            EventKind::Snapshot => {
                let mut bids = Vec::new();
                let mut asks = Vec::new();
                for row in rows {
                    match row.side {
                        Side::Bid => bids.push((row.price, row.qty)),
                        Side::Ask => asks.push((row.price, row.qty)),
                    }
                }
                self.book.reset_from_snapshot(&BookSnapshot {
                    symbol: self.symbol.clone(),
                    bids,
                    asks,
                    seq: None,
                    checksum: None,
                    stamps,
                });
            }
            EventKind::Delta => {
                self.book.apply_delta(&BookDelta {
                    symbol: self.symbol.clone(),
                    changes: rows
                        .iter()
                        .map(|r| LevelChange::new(r.side, r.price, r.qty))
                        .collect(),
                    seq: None,
                    checksum: None,
                    stamps,
                    prev_seq: None,
                    first_seq: None,
                });
            }
        }
    }

    fn apply_order(&mut self, row: &Row) {
        let Some(id) = row.order_id.as_deref() else {
            return;
        };
        let action = match row.action.as_deref() {
            Some("add") => OrderAction::Add,
            Some("modify") => OrderAction::Modify,
            Some("cancel") => OrderAction::Cancel,
            Some("execute") => OrderAction::Execute,
            _ => return,
        };
        let event = OrderEvent {
            symbol: self.symbol.clone(),
            order_id: OrderId::new(id),
            action,
            side: row.side,
            price: row.price,
            qty: row.qty,
            original_qty: None,
            executed_qty: None,
            stamps: Self::stamps(row),
            event_token: None,
            prev_event_token: None,
            size_change_explained: true,
        };
        if let Some(l3) = self.l3.as_mut() {
            l3.apply(&event);
        }
    }
}
