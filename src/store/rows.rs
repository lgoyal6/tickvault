//! Decoded archive rows, and a stream of them.
//!
//! The stream exists so a consumer never has to hold a day in memory. It opens
//! one file at a time and pulls one Parquet batch at a time, so its footprint
//! is a batch rather than an archive, whatever the range asked for.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use arrow::array::{
    Array, BooleanArray, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
    UInt8Array, UInt32Array, UInt64Array,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;

use crate::error::{Error, Result};
use crate::fixed::Fixed;
use crate::store::manifest::FileRecord;
use crate::store::scan::{self, Explain, Predicate, ScanMode};
use crate::store::schema::EventKind;
use crate::types::{BookLevel, Side};

/// One archived row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub event: EventKind,
    /// Groups the levels that arrived in one message. Applying a message's
    /// levels one at a time is not the same as applying them together on a
    /// depth-limited feed, so this is load bearing rather than metadata.
    pub msg_index: u64,
    pub recv_wall: i64,
    pub venue_ts: Option<i64>,
    pub side: Side,
    pub price: Fixed,
    pub qty: Fixed,
    pub suspect: bool,
    pub book_level: BookLevel,
    pub order_id: Option<String>,
    pub action: Option<String>,
    pub queue_index: Option<u32>,
    pub queue_certainty: Option<String>,
    /// Quantity this event traded, where the feed reports executions.
    ///
    /// `None` on every aggregated feed: a level shrinking might be a fill or a
    /// cancellation and the venue never says which. Zero means the feed could
    /// answer and the answer was that nothing traded.
    pub traded_qty: Option<Fixed>,
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<T>())
        .ok_or_else(|| Error::Other(format!("archive row is missing column {name}")))
}

/// Decode a whole batch.
pub fn decode(batch: &RecordBatch) -> Result<Vec<Row>> {
    let event = column::<UInt8Array>(batch, "event")?;
    let msg_index = column::<UInt64Array>(batch, "msg_index")?;
    let recv_wall = column::<TimestampNanosecondArray>(batch, "recv_wall")?;
    let venue_ts = column::<TimestampNanosecondArray>(batch, "venue_ts")?;
    let side = column::<UInt8Array>(batch, "side")?;
    let price = column::<Int64Array>(batch, "price")?;
    let qty = column::<Int64Array>(batch, "qty")?;
    let suspect = column::<BooleanArray>(batch, "suspect")?;
    // Archives written before the L3 columns existed simply lack them.
    let level = batch
        .column_by_name("book_level")
        .and_then(|c| c.as_any().downcast_ref::<UInt8Array>());
    let order_id = batch
        .column_by_name("order_id")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let action = batch
        .column_by_name("action")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let queue_index = batch
        .column_by_name("queue_index")
        .and_then(|c| c.as_any().downcast_ref::<UInt32Array>());
    let queue_certainty = batch
        .column_by_name("queue_certainty")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());
    let traded_qty = batch
        .column_by_name("traded_qty")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>());

    let mut rows = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        rows.push(Row {
            event: EventKind::from_u8(event.value(i))
                .ok_or_else(|| Error::Other(format!("unknown event kind {}", event.value(i))))?,
            msg_index: msg_index.value(i),
            recv_wall: recv_wall.value(i),
            venue_ts: (!venue_ts.is_null(i)).then(|| venue_ts.value(i)),
            side: if side.value(i) == 0 {
                Side::Bid
            } else {
                Side::Ask
            },
            price: Fixed::from_mantissa(price.value(i)),
            qty: Fixed::from_mantissa(qty.value(i)),
            suspect: suspect.value(i),
            book_level: match level.map(|l| l.value(i)) {
                Some(3) => BookLevel::L3,
                _ => BookLevel::L2,
            },
            order_id: order_id
                .filter(|c| !c.is_null(i))
                .map(|c| c.value(i).to_string()),
            action: action
                .filter(|c| !c.is_null(i))
                .map(|c| c.value(i).to_string()),
            queue_index: queue_index.filter(|c| !c.is_null(i)).map(|c| c.value(i)),
            queue_certainty: queue_certainty
                .filter(|c| !c.is_null(i))
                .map(|c| c.value(i).to_string()),
            traded_qty: traded_qty
                .filter(|c| !c.is_null(i))
                .map(|c| Fixed::from_mantissa(c.value(i))),
        });
    }
    Ok(rows)
}

/// A lazy stream of rows across a set of files.
///
/// Bounded memory by construction: one open file, one decoded batch. A query
/// over a whole day costs the same resident memory as a query over a second.
///
/// The two wall-clock bounds are not only a filter on decoded rows. They are
/// the predicate [`crate::store::scan`] pushes into Parquet, so a row outside
/// them is usually never decoded at all; see that module for what is pruned
/// where. The row-level test below still runs, because pruning works in whole
/// pages and a page can straddle either edge.
pub struct RowStream {
    root: PathBuf,
    files: VecDeque<FileRecord>,
    current: Option<ParquetRecordBatchReader>,
    buffer: VecDeque<Row>,
    /// Stop once rows pass this instant. Rows are in arrival order, so the
    /// first one past it ends the stream rather than being skipped over.
    until_wall: i64,
    /// Skip rows at or before this instant without decoding further files.
    after_wall: Option<i64>,
    mode: ScanMode,
    explain: Explain,
    finished: bool,
    rows_yielded: u64,
}

impl RowStream {
    /// Stream `files` in the order given, up to and including `until_wall`.
    pub fn new(root: impl Into<PathBuf>, files: Vec<FileRecord>, until_wall: i64) -> Self {
        let offered = files.len();
        RowStream {
            root: root.into(),
            files: files.into(),
            current: None,
            buffer: VecDeque::new(),
            until_wall,
            after_wall: None,
            mode: ScanMode::Planned,
            explain: Explain {
                files_in_partition: offered,
                ..Default::default()
            },
            finished: false,
            rows_yielded: 0,
        }
    }

    /// Skip everything at or before `wall`.
    pub fn after(mut self, wall: i64) -> Self {
        self.after_wall = Some(wall);
        self
    }

    /// Record files the manifest already dropped, so the explain covers the
    /// whole plan rather than only the part that opened a file.
    pub fn manifest_pruned(mut self, pruned: usize) -> Self {
        self.explain.files_pruned = pruned;
        self.explain.files_in_partition += pruned;
        self
    }

    /// Read every row group and every column, as this did before there was a
    /// planner.
    ///
    /// Exists for the two callers that need the comparison: the equivalence
    /// gate, which asserts a planned scan yields exactly the rows an unplanned
    /// one does, and the benchmark that measures the difference.
    pub fn unpruned(mut self) -> Self {
        self.mode = ScanMode::Unpruned;
        self
    }

    pub fn files_opened(&self) -> usize {
        self.explain.files_opened
    }

    /// What the plan skipped.
    pub fn explain(&self) -> &Explain {
        &self.explain
    }

    pub fn rows_yielded(&self) -> u64 {
        self.rows_yielded
    }

    /// Rows currently decoded and waiting, for the memory-bound test.
    pub fn buffered_rows(&self) -> usize {
        self.buffer.len()
    }

    fn open_next(&mut self) -> Result<bool> {
        // Loops, because a file can survive the manifest and still hold no row
        // group the predicate wants: the manifest knows the file's span, the
        // footer knows each group's.
        while let Some(record) = self.files.pop_front() {
            let path = self.root.join(&record.path);
            let predicate = Predicate::range(self.after_wall, self.until_wall);
            let (reader, explain) = scan::open_planned(&path, &predicate, self.mode)?;
            let empty = explain.row_groups_read == 0;
            self.explain.absorb(&explain);
            if empty {
                continue;
            }
            self.current = Some(reader);
            return Ok(true);
        }
        Ok(false)
    }

    fn fill(&mut self) -> Result<bool> {
        loop {
            if let Some(reader) = self.current.as_mut() {
                match reader.next() {
                    Some(Ok(batch)) => {
                        let rows = decode(&batch)?;
                        if rows.is_empty() {
                            continue;
                        }
                        self.buffer.extend(rows);
                        return Ok(true);
                    }
                    Some(Err(e)) => return Err(Error::Other(e.to_string())),
                    None => self.current = None,
                }
            }
            if self.current.is_none() && !self.open_next()? {
                return Ok(false);
            }
        }
    }
}

impl Iterator for RowStream {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.finished {
                return None;
            }
            let Some(row) = self.buffer.pop_front() else {
                match self.fill() {
                    Ok(true) => continue,
                    Ok(false) => {
                        self.finished = true;
                        return None;
                    }
                    Err(e) => {
                        self.finished = true;
                        return Some(Err(e));
                    }
                }
            };
            if row.recv_wall > self.until_wall {
                // Arrival order, so nothing after this can be in range either.
                self.finished = true;
                return None;
            }
            if self.after_wall.is_some_and(|w| row.recv_wall <= w) {
                continue;
            }
            self.rows_yielded += 1;
            return Some(Ok(row));
        }
    }
}

/// Files for one partition, oldest first.
pub fn partition_files(
    reader: &crate::store::reader::ArchiveReader,
    venue: crate::types::VenueId,
    symbol: &crate::types::Symbol,
    date: &str,
) -> Vec<FileRecord> {
    let mut files: Vec<FileRecord> = reader
        .files()
        .iter()
        .filter(|f| f.venue == venue && f.symbol == *symbol && f.date == date)
        .cloned()
        .collect();
    files.sort_by_key(|f| (f.first_recv_wall, f.path.clone()));
    files
}

/// Path of a file relative to a root.
pub fn relative(root: &Path, record: &FileRecord) -> PathBuf {
    root.join(&record.path)
}

/// A stream of whole messages, each one the rows that arrived together.
///
/// Grouping is not a convenience. Applying a message's levels one at a time is
/// a different operation from applying them together on a depth-limited feed:
/// one truncates per level, the other per message, and a message that adds a
/// new best while removing the old worst gives a different book each way. This
/// makes the message the unit, so nothing downstream can get it wrong.
///
/// The group key is `(msg_index, event, recv_wall)` rather than `msg_index`
/// alone. That index is a per-recorder-run counter and restarts at zero when
/// the recorder does, so two genuinely different messages either side of a
/// restart can carry the same one.
pub struct MessageStream {
    rows: RowStream,
    held: Option<Row>,
    finished: bool,
}

impl MessageStream {
    pub fn new(rows: RowStream) -> Self {
        MessageStream {
            rows,
            held: None,
            finished: false,
        }
    }

    pub fn files_opened(&self) -> usize {
        self.rows.files_opened()
    }

    /// What the plan skipped.
    pub fn explain(&self) -> &Explain {
        self.rows.explain()
    }

    pub fn rows_yielded(&self) -> u64 {
        self.rows.rows_yielded()
    }

    pub fn buffered_rows(&self) -> usize {
        self.rows.buffered_rows() + usize::from(self.held.is_some())
    }
}

fn same_message(a: &Row, b: &Row) -> bool {
    a.msg_index == b.msg_index && a.event == b.event && a.recv_wall == b.recv_wall
}

/// Split rows already in hand into messages, the same way the stream does.
///
/// The unit of application is the message, never the row. Applying a message's
/// levels one at a time truncates once per *level* instead of once per message,
/// which on a depth-limited feed rebuilds a book that differs from the one the
/// recorder held; that was a real bug, and it survived a passing determinism
/// gate. So there is one definition of where a message ends, and this is how a
/// consumer holding rows from somewhere other than a file gets at it rather
/// than writing the rule out a second time.
pub fn split_messages(rows: &[Row]) -> Messages<'_> {
    Messages { rows }
}

/// Iterator returned by [`split_messages`].
pub struct Messages<'a> {
    rows: &'a [Row],
}

impl<'a> Iterator for Messages<'a> {
    type Item = &'a [Row];

    fn next(&mut self) -> Option<&'a [Row]> {
        let first = self.rows.first()?;
        let mut n = 1;
        while n < self.rows.len() && same_message(first, &self.rows[n]) {
            n += 1;
        }
        let (message, rest) = self.rows.split_at(n);
        self.rows = rest;
        Some(message)
    }
}

impl Iterator for MessageStream {
    type Item = Result<Vec<Row>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let mut message: Vec<Row> = Vec::new();
        if let Some(row) = self.held.take() {
            message.push(row);
        }
        loop {
            match self.rows.next() {
                Some(Ok(row)) => {
                    if message
                        .first()
                        .is_some_and(|first| !same_message(first, &row))
                    {
                        self.held = Some(row);
                        return Some(Ok(message));
                    }
                    message.push(row);
                }
                Some(Err(e)) => {
                    self.finished = true;
                    return Some(Err(e));
                }
                None => {
                    self.finished = true;
                    return (!message.is_empty()).then_some(Ok(message));
                }
            }
        }
    }
}

#[cfg(test)]
mod message_split_tests {
    use super::*;
    use crate::types::{BookLevel, Side};

    fn row(msg_index: u64, event: EventKind, recv_wall: i64, price: i64) -> Row {
        Row {
            event,
            msg_index,
            recv_wall,
            venue_ts: None,
            side: Side::Bid,
            price: Fixed::from_mantissa(price),
            qty: Fixed::from_mantissa(1),
            suspect: false,
            book_level: BookLevel::L2,
            order_id: None,
            action: None,
            queue_index: None,
            queue_certainty: None,
            traded_qty: None,
        }
    }

    #[test]
    fn levels_of_one_message_stay_together() {
        let rows = vec![
            row(0, EventKind::Delta, 100, 1),
            row(0, EventKind::Delta, 100, 2),
            row(1, EventKind::Delta, 200, 3),
        ];
        let split: Vec<usize> = split_messages(&rows).map(|m| m.len()).collect();
        assert_eq!(split, vec![2, 1]);
    }

    #[test]
    fn all_three_fields_end_a_message() {
        // A shared index is not enough on its own. A snapshot and a delta that
        // happen to carry the same index are different messages, and so are two
        // messages that arrived at different instants.
        let by_index = vec![
            row(0, EventKind::Delta, 100, 1),
            row(1, EventKind::Delta, 100, 2),
        ];
        let by_kind = vec![
            row(0, EventKind::Snapshot, 100, 1),
            row(0, EventKind::Delta, 100, 2),
        ];
        let by_time = vec![
            row(0, EventKind::Delta, 100, 1),
            row(0, EventKind::Delta, 101, 2),
        ];
        for rows in [by_index, by_kind, by_time] {
            assert_eq!(split_messages(&rows).count(), 2);
        }
    }

    #[test]
    fn every_row_lands_in_exactly_one_message() {
        let rows = vec![
            row(0, EventKind::Snapshot, 10, 1),
            row(0, EventKind::Snapshot, 10, 2),
            row(1, EventKind::Delta, 20, 3),
            row(2, EventKind::Delta, 30, 4),
            row(2, EventKind::Delta, 30, 5),
        ];
        let regrouped: Vec<Row> = split_messages(&rows).flatten().cloned().collect();
        assert_eq!(regrouped.len(), rows.len());
        for (a, b) in regrouped.iter().zip(rows.iter()) {
            assert_eq!(a.msg_index, b.msg_index);
            assert_eq!(a.price, b.price);
        }
    }

    #[test]
    fn an_empty_run_yields_no_messages() {
        assert_eq!(split_messages(&[]).count(), 0);
    }
}
