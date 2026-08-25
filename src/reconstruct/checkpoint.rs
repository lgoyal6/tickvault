//! Periodic book states, so a late query does not replay a whole day.
//!
//! A checkpoint is the book as it stood at an instant, written where a
//! reconstruction can find it. Replaying from the nearest one costs whatever
//! happened since, rather than everything since midnight.
//!
//! They matter most exactly where snapshots do not exist. A venue that
//! snapshots on every reconnect gives reconstruction a natural restart point
//! for free; Bitstamp's order-by-order feed has no snapshot at all, so without
//! checkpoints a question about the end of a day really would replay the whole
//! thing.
//!
//! Checkpoints live under `_checkpoints/`, outside the published tree. The
//! leading underscore keeps Hive partition discovery from serving them as data,
//! which matters: they are *derived*, and a consumer counting them as
//! observations would double every level in them. Deleting the directory costs
//! nothing but the time to rebuild it.

use std::path::{Path, PathBuf};

use crate::book::l3::{L3Book, OrderId};
use crate::book::{BookSnapshot, L2Book};
use crate::clock::{Stamp, Timestamps, format_utc_date};
use crate::error::{Error, Result};
use crate::fixed::Fixed;
use crate::store::reader::read_batches;
use crate::store::schema::RowBuilder;
use crate::store::writer::WriterConfig;
use crate::types::{BookLevel, Side, Symbol, VenueId};

/// Directory holding derived checkpoints, kept out of the published tree.
pub const CHECKPOINT_DIR: &str = "_checkpoints";

/// A book state at an instant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    pub venue: VenueId,
    pub symbol: Symbol,
    pub at_wall: i64,
    pub book_level: BookLevel,
    pub bids: Vec<(Fixed, Fixed)>,
    pub asks: Vec<(Fixed, Fixed)>,
    /// Individual orders, on an L3 book. Empty at L2.
    pub orders: Vec<(OrderId, Side, Fixed, Fixed)>,
}

impl Checkpoint {
    /// Capture an aggregated book.
    pub fn of_l2(venue: VenueId, book: &L2Book, at_wall: i64) -> Self {
        Checkpoint {
            venue,
            symbol: book.symbol().clone(),
            at_wall,
            book_level: BookLevel::L2,
            bids: book.top(Side::Bid, usize::MAX),
            asks: book.top(Side::Ask, usize::MAX),
            orders: Vec::new(),
        }
    }

    /// Capture an order-by-order book.
    ///
    /// The orders are written in queue order per level, so replaying the
    /// checkpoint restores the queue rather than just the depth. Their
    /// certainty becomes [`crate::book::l3::QueueCertainty::Seeded`] on the way
    /// back in, which is correct: a rebuild from a checkpoint did not watch
    /// them arrive.
    pub fn of_l3(venue: VenueId, book: &L3Book, at_wall: i64) -> Self {
        let mut orders = Vec::new();
        for side in [Side::Bid, Side::Ask] {
            let prices: Vec<Fixed> = match side {
                Side::Bid => book
                    .to_l2(Timestamps::recv_only(Stamp::ZERO))
                    .top(side, usize::MAX),
                Side::Ask => book
                    .to_l2(Timestamps::recv_only(Stamp::ZERO))
                    .top(side, usize::MAX),
            }
            .into_iter()
            .map(|(price, _)| price)
            .collect();
            for price in prices {
                for order in book.queue_at(side, price) {
                    orders.push((order.id.clone(), side, order.price, order.qty));
                }
            }
        }
        let l2 = book.to_l2(Timestamps::recv_only(Stamp::ZERO));
        Checkpoint {
            venue,
            symbol: book.symbol().clone(),
            at_wall,
            book_level: BookLevel::L3,
            bids: l2.top(Side::Bid, usize::MAX),
            asks: l2.top(Side::Ask, usize::MAX),
            orders,
        }
    }

    pub fn levels(&self) -> usize {
        self.bids.len() + self.asks.len()
    }

    fn dir(&self) -> PathBuf {
        checkpoint_dir(self.venue, &self.symbol, self.at_wall)
    }

    fn file_name(&self) -> String {
        // Zero padded so a lexical sort is a time sort.
        format!("ckpt-{:020}.parquet", self.at_wall.max(0))
    }
}

fn checkpoint_dir(venue: VenueId, symbol: &Symbol, at_wall: i64) -> PathBuf {
    PathBuf::from(CHECKPOINT_DIR)
        .join(format!("venue={}", venue.as_str()))
        .join(format!("symbol={}", symbol.as_str()))
        .join(format!("date={}", format_utc_date(at_wall)))
}

/// Write a checkpoint, returning its path relative to the archive root.
pub fn write(root: impl AsRef<Path>, checkpoint: &Checkpoint) -> Result<PathBuf> {
    let root = root.as_ref();
    let relative = checkpoint.dir().join(checkpoint.file_name());
    let path = root.join(&relative);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut builder = RowBuilder::new();
    let stamps = Timestamps::recv_only(Stamp {
        mono_nanos: 0,
        wall_nanos: checkpoint.at_wall,
    });
    if checkpoint.book_level == BookLevel::L3 {
        for (index, (id, side, price, qty)) in checkpoint.orders.iter().enumerate() {
            let event = crate::book::l3::OrderEvent {
                symbol: checkpoint.symbol.clone(),
                order_id: id.clone(),
                action: crate::book::l3::OrderAction::Add,
                side: *side,
                price: *price,
                qty: *qty,
                original_qty: None,
                executed_qty: None,
                stamps,
                event_token: None,
                prev_event_token: None,
                size_change_explained: true,
            };
            // A checkpoint lists what is resting, not what changed hands, so
            // it reports no traded quantity rather than a zero one.
            builder.push_order(checkpoint.venue, &event, None, None, index as u64, false);
        }
    } else {
        builder.push_snapshot(
            checkpoint.venue,
            &BookSnapshot {
                symbol: checkpoint.symbol.clone(),
                bids: checkpoint.bids.clone(),
                asks: checkpoint.asks.clone(),
                seq: None,
                checksum: None,
                stamps,
            },
            0,
            false,
        );
    }

    let Some(batch) = builder.finish() else {
        // An empty book is still a fact worth recording, but there is nothing
        // to write and a reader would find nothing to apply.
        return Ok(relative);
    };

    let config = WriterConfig::default();
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::try_new(config.zstd_level)
                .map_err(|e| Error::Other(format!("invalid zstd level: {e}")))?,
        ))
        .set_created_by(format!(
            "tickvault {} (checkpoint)",
            env!("CARGO_PKG_VERSION")
        ))
        .build();

    let partial = path.with_extension("parquet.partial");
    {
        let sink = std::fs::File::create(&partial)?;
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            sink,
            crate::store::schema::book_schema(),
            Some(props),
        )
        .map_err(|e| Error::Other(format!("checkpoint {}: {e}", partial.display())))?;
        writer
            .write(&batch)
            .map_err(|e| Error::Other(format!("checkpoint {}: {e}", partial.display())))?;
        let sink = writer
            .into_inner()
            .map_err(|e| Error::Other(format!("checkpoint {}: {e}", partial.display())))?;
        sink.sync_all()?;
    }
    // Renamed into place, so a reader never sees a half-written checkpoint.
    std::fs::rename(&partial, &path)?;
    Ok(relative)
}

/// Every checkpoint for a partition, oldest first, as `(wall, path)`.
pub fn list(
    root: impl AsRef<Path>,
    venue: VenueId,
    symbol: &Symbol,
    at_wall: i64,
) -> Vec<(i64, PathBuf)> {
    let dir = root.as_ref().join(checkpoint_dir(venue, symbol, at_wall));
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut out: Vec<(i64, PathBuf)> = entries
        .flatten()
        .filter_map(|e| {
            let path = e.path();
            let name = path.file_name()?.to_string_lossy().into_owned();
            let stamp = name
                .strip_prefix("ckpt-")?
                .strip_suffix(".parquet")?
                .parse::<i64>()
                .ok()?;
            Some((stamp, path))
        })
        .collect();
    out.sort();
    out
}

/// The newest checkpoint at or before an instant.
pub fn latest_before(
    root: impl AsRef<Path>,
    venue: VenueId,
    symbol: &Symbol,
    at_wall: i64,
) -> Result<Option<Checkpoint>> {
    let root = root.as_ref();
    let Some((stamp, path)) = list(root, venue, symbol, at_wall)
        .into_iter()
        .rfind(|(stamp, _)| *stamp <= at_wall)
    else {
        return Ok(None);
    };
    Ok(Some(read(&path, venue, symbol, stamp)?))
}

/// Read a checkpoint file back.
pub fn read(path: &Path, venue: VenueId, symbol: &Symbol, at_wall: i64) -> Result<Checkpoint> {
    use arrow::array::{Array, Int64Array, StringArray, UInt8Array};

    let mut bids = Vec::new();
    let mut asks = Vec::new();
    let mut orders = Vec::new();
    let mut book_level = BookLevel::L2;

    for batch in read_batches(path)? {
        let side = batch
            .column_by_name("side")
            .and_then(|c| c.as_any().downcast_ref::<UInt8Array>())
            .ok_or_else(|| Error::Other("checkpoint has no side column".into()))?;
        let price = batch
            .column_by_name("price")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| Error::Other("checkpoint has no price column".into()))?;
        let qty = batch
            .column_by_name("qty")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| Error::Other("checkpoint has no qty column".into()))?;
        let level = batch
            .column_by_name("book_level")
            .and_then(|c| c.as_any().downcast_ref::<UInt8Array>());
        let order_id = batch
            .column_by_name("order_id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        for i in 0..batch.num_rows() {
            let s = if side.value(i) == 0 {
                Side::Bid
            } else {
                Side::Ask
            };
            let p = Fixed::from_mantissa(price.value(i));
            let q = Fixed::from_mantissa(qty.value(i));
            if level.map(|l| l.value(i)) == Some(3) {
                book_level = BookLevel::L3;
                if let Some(ids) = order_id.filter(|c| !c.is_null(i)) {
                    orders.push((OrderId::new(ids.value(i)), s, p, q));
                }
            } else {
                match s {
                    Side::Bid => bids.push((p, q)),
                    Side::Ask => asks.push((p, q)),
                }
            }
        }
    }

    if book_level == BookLevel::L3 {
        // Aggregate the orders so the L2 view is available either way.
        let mut totals: std::collections::BTreeMap<(Side, Fixed), Fixed> =
            std::collections::BTreeMap::new();
        for (_, side, price, qty) in &orders {
            let slot = totals.entry((*side, *price)).or_insert(Fixed::ZERO);
            *slot = slot.checked_add(*qty).unwrap_or(*slot);
        }
        for ((side, price), qty) in totals {
            match side {
                Side::Bid => bids.push((price, qty)),
                Side::Ask => asks.push((price, qty)),
            }
        }
    }

    Ok(Checkpoint {
        venue,
        symbol: symbol.clone(),
        at_wall,
        book_level,
        bids,
        asks,
        orders,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(s: &str) -> Fixed {
        Fixed::from_decimal_str(s).unwrap()
    }

    fn sym() -> Symbol {
        Symbol::new("BTC", "USD")
    }

    fn book() -> L2Book {
        let mut b = L2Book::new(sym());
        b.reset_from_snapshot(&BookSnapshot {
            symbol: sym(),
            bids: vec![(f("100"), f("1")), (f("99"), f("2"))],
            asks: vec![(f("101"), f("3"))],
            seq: None,
            checksum: None,
            stamps: Timestamps::recv_only(Stamp::ZERO),
        });
        b
    }

    const AT: i64 = 1_787_000_000_000_000_000;

    #[test]
    fn a_checkpoint_round_trips_through_disk() {
        let dir = tempfile::tempdir().unwrap();
        let ckpt = Checkpoint::of_l2(VenueId::Kraken, &book(), AT);
        let relative = write(dir.path(), &ckpt).unwrap();
        assert!(dir.path().join(&relative).exists());

        let back = latest_before(dir.path(), VenueId::Kraken, &sym(), AT)
            .unwrap()
            .expect("a checkpoint at that instant");
        assert_eq!(back.at_wall, AT);
        assert_eq!(back.book_level, BookLevel::L2);
        let mut bids = back.bids.clone();
        bids.sort();
        assert_eq!(bids, vec![(f("99"), f("2")), (f("100"), f("1"))]);
        assert_eq!(back.asks, vec![(f("101"), f("3"))]);
    }

    #[test]
    fn the_newest_checkpoint_at_or_before_the_instant_wins() {
        let dir = tempfile::tempdir().unwrap();
        for offset in [0, 1_000_000_000, 2_000_000_000] {
            let mut b = L2Book::new(sym());
            b.reset_from_snapshot(&BookSnapshot {
                symbol: sym(),
                bids: vec![(f("100"), Fixed::from_mantissa(offset + 1))],
                asks: vec![],
                seq: None,
                checksum: None,
                stamps: Timestamps::recv_only(Stamp::ZERO),
            });
            write(
                dir.path(),
                &Checkpoint::of_l2(VenueId::Kraken, &b, AT + offset),
            )
            .unwrap();
        }
        let chosen = latest_before(dir.path(), VenueId::Kraken, &sym(), AT + 1_500_000_000)
            .unwrap()
            .unwrap();
        assert_eq!(chosen.at_wall, AT + 1_000_000_000);
        // And asking before the first one finds nothing rather than the closest.
        assert!(
            latest_before(dir.path(), VenueId::Kraken, &sym(), AT - 1)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn checkpoints_are_kept_out_of_the_published_tree() {
        // A consumer discovering them as data would double every level.
        let dir = tempfile::tempdir().unwrap();
        let relative = write(dir.path(), &Checkpoint::of_l2(VenueId::Kraken, &book(), AT)).unwrap();
        assert!(
            relative.starts_with(CHECKPOINT_DIR),
            "{relative:?} is inside the published partitions"
        );
        assert!(
            CHECKPOINT_DIR.starts_with('_'),
            "hive discovery skips these"
        );
    }

    #[test]
    fn an_l3_checkpoint_preserves_queue_order() {
        let dir = tempfile::tempdir().unwrap();
        let mut l3 = L3Book::new(sym());
        for (id, qty) in [("a", "1"), ("b", "2"), ("c", "3")] {
            l3.apply(&crate::book::l3::OrderEvent {
                symbol: sym(),
                order_id: OrderId::new(id),
                action: crate::book::l3::OrderAction::Add,
                side: Side::Bid,
                price: f("100"),
                qty: f(qty),
                original_qty: None,
                executed_qty: None,
                stamps: Timestamps::recv_only(Stamp::ZERO),
                event_token: None,
                prev_event_token: None,
                size_change_explained: true,
            });
        }
        let ckpt = Checkpoint::of_l3(VenueId::Bitstamp, &l3, AT);
        write(dir.path(), &ckpt).unwrap();
        let back = latest_before(dir.path(), VenueId::Bitstamp, &sym(), AT)
            .unwrap()
            .unwrap();
        assert_eq!(back.book_level, BookLevel::L3);
        let ids: Vec<&str> = back.orders.iter().map(|o| o.0.as_str()).collect();
        assert_eq!(ids, vec!["a", "b", "c"], "queue order must survive");
        // And the aggregate is recoverable from it.
        assert_eq!(back.bids, vec![(f("100"), f("6"))]);
    }

    #[test]
    fn no_partial_file_survives_a_write() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), &Checkpoint::of_l2(VenueId::Kraken, &book(), AT)).unwrap();
        let mut stack = vec![dir.path().to_path_buf()];
        while let Some(d) = stack.pop() {
            for entry in std::fs::read_dir(&d).unwrap().flatten() {
                let p = entry.path();
                if p.is_dir() {
                    stack.push(p);
                } else {
                    assert!(
                        !p.to_string_lossy().ends_with(".partial"),
                        "left a partial checkpoint behind"
                    );
                }
            }
        }
    }

    #[test]
    fn an_empty_book_checkpoints_without_error() {
        let dir = tempfile::tempdir().unwrap();
        let empty = L2Book::new(sym());
        write(dir.path(), &Checkpoint::of_l2(VenueId::Kraken, &empty, AT)).unwrap();
        // Nothing to read back, and asking is not an error.
        assert!(
            latest_before(dir.path(), VenueId::Kraken, &sym(), AT)
                .unwrap()
                .is_none()
        );
    }
}
