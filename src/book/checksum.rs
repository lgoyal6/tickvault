//! Book checksums.
//!
//! Two different things live here and they should not be confused.
//!
//! [`kraken_checksum`] reproduces a venue's own integrity check. Kraken v2
//! publishes a CRC32 with every book message and sends no sequence numbers at
//! all, so recomputing this value is the *only* way to know a message was lost.
//! Its exact byte layout is dictated by Kraken, not by us.
//!
//! [`book_digest`] is ours. It hashes the whole book in canonical form so two
//! reconstructions can be compared for equality. Phase 5 gates on it.

use crc32fast::Hasher;

use super::L2Book;
use crate::fixed::Fixed;
use crate::types::Side;

/// Kraken checksums the ten best levels per side, whatever depth you subscribed
/// at. Everything past this is invisible to the check.
pub const KRAKEN_CHECKSUM_DEPTH: usize = 10;

/// A recomputed Kraken checksum, plus how much to trust it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChecksumResult {
    /// The CRC32 value.
    pub value: u32,
    /// How many of the levels fed into it had to be rounded to the pair's
    /// declared precision.
    ///
    /// This should always be zero. If it is not, a mismatch against the venue's
    /// value is our rendering, not their message, and reporting it as a gap
    /// would be blaming the venue for our own bug.
    pub lossy_levels: usize,
    /// Levels actually hashed per side. Below [`KRAKEN_CHECKSUM_DEPTH`] means
    /// the book is not yet deep enough for the check to be meaningful.
    pub bid_levels: usize,
    pub ask_levels: usize,
}

impl ChecksumResult {
    /// True when the computation is a fair test of the venue's value.
    pub fn is_trustworthy(&self) -> bool {
        self.lossy_levels == 0
            && self.bid_levels == KRAKEN_CHECKSUM_DEPTH
            && self.ask_levels == KRAKEN_CHECKSUM_DEPTH
    }
}

/// Render one value the way Kraken's checksum expects: fixed precision, decimal
/// point deleted, leading zeros stripped.
///
/// `0.00234` at 8 decimals becomes `234000`, not `0.00234000`. The leading-zero
/// rule is the part that is easy to get wrong and produces a checksum that is
/// wrong for every level at once.
fn checksum_token(value: Fixed, precision: u32, lossy: &mut usize) -> String {
    if !value.fits(precision) {
        *lossy += 1;
    }
    let rendered = value.to_decimal_string(precision);
    let digits: String = rendered.chars().filter(|c| c.is_ascii_digit()).collect();
    let trimmed = digits.trim_start_matches('0');
    if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Recompute Kraken's book checksum from our copy of the book.
///
/// `price_precision` and `qty_precision` come from the venue's instrument
/// metadata and differ per pair. Guessing them produces a checksum that never
/// matches, which looks exactly like a permanently broken feed.
pub fn kraken_checksum(book: &L2Book, price_precision: u32, qty_precision: u32) -> ChecksumResult {
    let asks = book.top(Side::Ask, KRAKEN_CHECKSUM_DEPTH);
    let bids = book.top(Side::Bid, KRAKEN_CHECKSUM_DEPTH);

    let mut lossy_levels = 0;
    let mut buf = String::with_capacity(KRAKEN_CHECKSUM_DEPTH * 4 * 12);

    // Asks first, ascending price, then bids, descending price. `top` already
    // returns best-first, which is ascending for asks and descending for bids.
    for (price, qty) in &asks {
        buf.push_str(&checksum_token(*price, price_precision, &mut lossy_levels));
        buf.push_str(&checksum_token(*qty, qty_precision, &mut lossy_levels));
    }
    for (price, qty) in &bids {
        buf.push_str(&checksum_token(*price, price_precision, &mut lossy_levels));
        buf.push_str(&checksum_token(*qty, qty_precision, &mut lossy_levels));
    }

    let mut hasher = Hasher::new();
    hasher.update(buf.as_bytes());

    ChecksumResult {
        value: hasher.finalize(),
        lossy_levels,
        bid_levels: bids.len(),
        ask_levels: asks.len(),
    }
}

/// A stable digest of the entire book at full depth.
///
/// Unlike the Kraken checksum this covers every level and uses our canonical
/// decimal form, so it is a fair equality test between two books but means
/// nothing to any venue.
pub fn book_digest(book: &L2Book) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(book.symbol().as_str().as_bytes());
    for (side, tag) in [(Side::Bid, b'B'), (Side::Ask, b'A')] {
        hasher.update(&[tag]);
        // `top` with the full count walks each side in its canonical order.
        for (price, qty) in book.top(side, usize::MAX) {
            hasher.update(price.mantissa().to_le_bytes().as_slice());
            hasher.update(qty.mantissa().to_le_bytes().as_slice());
        }
    }
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::BookSnapshot;
    use crate::clock::{Stamp, Timestamps};
    use crate::types::Symbol;

    fn f(s: &str) -> Fixed {
        Fixed::from_decimal_str(s).unwrap()
    }

    fn stamps() -> Timestamps {
        Timestamps::recv_only(Stamp::ZERO)
    }

    /// Ten levels a side, so the checksum is trustworthy.
    fn ten_deep() -> L2Book {
        let symbol = Symbol::new("BTC", "USD");
        let mut book = L2Book::new(symbol.clone());
        let bids: Vec<(Fixed, Fixed)> = (0..10)
            .map(|i| (f(&format!("{}.1", 100 - i)), f("0.5")))
            .collect();
        let asks: Vec<(Fixed, Fixed)> = (0..10)
            .map(|i| (f(&format!("{}.1", 101 + i)), f("0.5")))
            .collect();
        book.reset_from_snapshot(&BookSnapshot {
            symbol,
            bids,
            asks,
            seq: None,
            checksum: None,
            stamps: stamps(),
        });
        book
    }

    #[test]
    fn token_strips_the_point_and_leading_zeros() {
        let mut lossy = 0;
        assert_eq!(checksum_token(f("0.00234"), 8, &mut lossy), "234000");
        assert_eq!(checksum_token(f("50000.1"), 1, &mut lossy), "500001");
        assert_eq!(checksum_token(f("1"), 0, &mut lossy), "1");
        assert_eq!(lossy, 0);
    }

    #[test]
    fn token_of_zero_is_a_single_zero_not_an_empty_string() {
        // An empty token would silently shorten the hashed string and make
        // every checksum wrong from that level down.
        let mut lossy = 0;
        assert_eq!(checksum_token(f("0"), 8, &mut lossy), "0");
        assert_eq!(lossy, 0);
    }

    #[test]
    fn rendering_that_loses_digits_is_counted_not_hidden() {
        let mut lossy = 0;
        // 0.005 cannot be written at 2 decimals.
        let token = checksum_token(f("0.005"), 2, &mut lossy);
        assert_eq!(lossy, 1);
        assert_eq!(token, "1"); // 0.01 -> "001" -> "1"
    }

    #[test]
    fn checksum_is_deterministic_and_precision_sensitive() {
        let book = ten_deep();
        let a = kraken_checksum(&book, 1, 8);
        let b = kraken_checksum(&book, 1, 8);
        assert_eq!(a, b);
        assert!(a.is_trustworthy());
        // Wrong precision produces a completely different value, which is why
        // instrument metadata is not optional.
        assert_ne!(a.value, kraken_checksum(&book, 2, 8).value);
    }

    #[test]
    fn checksum_changes_when_any_hashed_level_changes() {
        let mut book = ten_deep();
        let before = kraken_checksum(&book, 1, 8).value;
        let mut outcome = Default::default();
        book.apply_change(
            crate::book::LevelChange::new(Side::Bid, f("100.1"), f("0.6")),
            &mut outcome,
        );
        assert_ne!(before, kraken_checksum(&book, 1, 8).value);
    }

    #[test]
    fn a_shallow_book_is_not_trustworthy_for_checksum_validation() {
        let symbol = Symbol::new("BTC", "USD");
        let mut book = L2Book::new(symbol.clone());
        book.reset_from_snapshot(&BookSnapshot {
            symbol,
            bids: vec![(f("99"), f("1"))],
            asks: vec![(f("101"), f("1"))],
            seq: None,
            checksum: None,
            stamps: stamps(),
        });
        let result = kraken_checksum(&book, 1, 8);
        assert!(!result.is_trustworthy());
        assert_eq!(result.bid_levels, 1);
    }

    #[test]
    fn side_order_matters_asks_are_hashed_before_bids() {
        // A symmetric book hashed in the wrong side order would still produce a
        // stable value, so assert against a hand-computed expectation instead.
        let symbol = Symbol::new("X", "Y");
        let mut book = L2Book::new(symbol.clone());
        book.reset_from_snapshot(&BookSnapshot {
            symbol,
            bids: vec![(f("1"), f("2"))],
            asks: vec![(f("3"), f("4"))],
            seq: None,
            checksum: None,
            stamps: stamps(),
        });
        let mut expected = Hasher::new();
        expected.update(b"3412"); // ask price, ask qty, bid price, bid qty
        assert_eq!(kraken_checksum(&book, 0, 0).value, expected.finalize());
    }

    #[test]
    fn digest_covers_full_depth_not_just_the_top_ten() {
        let mut a = ten_deep();
        let mut b = ten_deep();
        let mut outcome = Default::default();
        // A level well outside any checksum window.
        a.apply_change(
            crate::book::LevelChange::new(Side::Bid, f("1"), f("1")),
            &mut outcome,
        );
        assert_ne!(a.digest(), b.digest());
        b.apply_change(
            crate::book::LevelChange::new(Side::Bid, f("1"), f("1")),
            &mut outcome,
        );
        assert_eq!(a.digest(), b.digest());
    }

    #[test]
    fn kraken_checksum_ignores_levels_past_the_tenth() {
        // The core limitation of checksum-based validation, asserted rather
        // than assumed: a change below the tenth level is invisible.
        let mut book = ten_deep();
        let before = kraken_checksum(&book, 1, 8).value;
        let mut outcome = Default::default();
        book.apply_change(
            crate::book::LevelChange::new(Side::Bid, f("50"), f("1")),
            &mut outcome,
        );
        assert_eq!(before, kraken_checksum(&book, 1, 8).value);
        assert_ne!(book.digest(), ten_deep().digest());
    }
}
