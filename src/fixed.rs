//! Exact fixed-point decimals for prices and quantities.
//!
//! Order book values must never round-trip through `f64`. Kraken validates a
//! book by taking a CRC32 over its price and quantity *strings* rendered at the
//! pair's own precision. A value that is off by one unit in the last place does
//! not produce a slightly wrong book, it produces a checksum mismatch that is
//! indistinguishable from a dropped message. Silent float drift would therefore
//! show up in the published gap report as venue unreliability that was actually
//! ours.
//!
//! Every price and quantity in this crate is a [`Fixed`]: a signed integer
//! count of 1e-9 units. That covers every venue we target (Kraken quotes at
//! most 8 decimals of quantity, Coinbase at most 8) with a digit to spare, and
//! it keeps ordering, equality, and hashing exact so a price can be a
//! `BTreeMap` key.

use std::fmt;
use std::ops::{Add, AddAssign, Neg, Sub, SubAssign};
use std::str::FromStr;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Number of decimal places retained by [`Fixed`].
pub const SCALE: u32 = 9;

const SCALE_FACTOR: i128 = 1_000_000_000;
const POW10: [i128; 20] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
    10_000_000_000_000,
    100_000_000_000_000,
    1_000_000_000_000_000,
    10_000_000_000_000_000,
    100_000_000_000_000_000,
    1_000_000_000_000_000_000,
    10_000_000_000_000_000_000,
];

/// A decimal number held as an exact count of 1e-9 units.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Fixed(i64);

/// Why a decimal string could not be represented exactly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseFixedError {
    /// The input was empty or contained no digits.
    Empty,
    /// A character that cannot appear in a decimal literal.
    BadChar(char),
    /// More than one decimal point, or a malformed exponent.
    Malformed(String),
    /// The value carries more than [`SCALE`] decimals, so storing it would
    /// silently discard information the venue sent us.
    TooPrecise { input: String, decimals: u32 },
    /// The magnitude does not fit in the fixed-point range.
    OutOfRange(String),
}

impl fmt::Display for ParseFixedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, "empty decimal literal"),
            Self::BadChar(c) => write!(f, "unexpected character {c:?} in decimal literal"),
            Self::Malformed(s) => write!(f, "malformed decimal literal {s:?}"),
            Self::TooPrecise { input, decimals } => write!(
                f,
                "decimal {input:?} has {decimals} decimals, more than the {SCALE} we can hold exactly"
            ),
            Self::OutOfRange(s) => write!(f, "decimal {s:?} is outside the representable range"),
        }
    }
}

impl std::error::Error for ParseFixedError {}

impl Fixed {
    /// Zero.
    pub const ZERO: Fixed = Fixed(0);

    /// The largest representable value.
    pub const MAX: Fixed = Fixed(i64::MAX);

    /// Build from a raw mantissa expressed in 1e-9 units.
    #[inline]
    pub const fn from_mantissa(mantissa: i64) -> Self {
        Fixed(mantissa)
    }

    /// The raw mantissa in 1e-9 units.
    #[inline]
    pub const fn mantissa(self) -> i64 {
        self.0
    }

    /// Build from a whole number of units.
    #[inline]
    pub fn from_units(units: i64) -> Result<Self, ParseFixedError> {
        units
            .checked_mul(SCALE_FACTOR as i64)
            .map(Fixed)
            .ok_or_else(|| ParseFixedError::OutOfRange(units.to_string()))
    }

    #[inline]
    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    #[inline]
    pub const fn is_positive(self) -> bool {
        self.0 > 0
    }

    #[inline]
    pub const fn is_negative(self) -> bool {
        self.0 < 0
    }

    /// Parse an exact decimal literal, optionally in scientific notation.
    ///
    /// Rejects rather than rounds when the literal is more precise than we can
    /// hold. A venue that starts quoting a tenth of a nano-unit is a schema
    /// change we want to hear about, not absorb.
    pub fn from_decimal_str(input: &str) -> Result<Self, ParseFixedError> {
        let s = input.trim();
        if s.is_empty() {
            return Err(ParseFixedError::Empty);
        }

        let (mantissa_str, exponent) = match s.find(['e', 'E']) {
            Some(idx) => {
                let (m, e) = s.split_at(idx);
                let e = &e[1..];
                let exp: i32 = e
                    .parse()
                    .map_err(|_| ParseFixedError::Malformed(input.to_string()))?;
                (m, exp)
            }
            None => (s, 0),
        };

        let (negative, digits_str) = match mantissa_str.as_bytes().first() {
            Some(b'-') => (true, &mantissa_str[1..]),
            Some(b'+') => (false, &mantissa_str[1..]),
            _ => (false, mantissa_str),
        };

        let mut int_digits = String::new();
        let mut frac_digits = String::new();
        let mut seen_point = false;
        for c in digits_str.chars() {
            match c {
                '0'..='9' if seen_point => frac_digits.push(c),
                '0'..='9' => int_digits.push(c),
                '.' if !seen_point => seen_point = true,
                '.' => return Err(ParseFixedError::Malformed(input.to_string())),
                other => return Err(ParseFixedError::BadChar(other)),
            }
        }
        if int_digits.is_empty() && frac_digits.is_empty() {
            return Err(ParseFixedError::Empty);
        }

        // Decimal places implied by the literal once the exponent is folded in.
        let implied = frac_digits.len() as i32 - exponent;

        let mut all = int_digits;
        all.push_str(&frac_digits);
        let trimmed = all.trim_start_matches('0');
        let digits: i128 = if trimmed.is_empty() {
            0
        } else {
            trimmed
                .parse()
                .map_err(|_| ParseFixedError::OutOfRange(input.to_string()))?
        };

        // mantissa = digits * 10^(SCALE - implied)
        let shift = SCALE as i32 - implied;
        let value: i128 = if shift >= 0 {
            let p = POW10
                .get(shift as usize)
                .copied()
                .ok_or_else(|| ParseFixedError::OutOfRange(input.to_string()))?;
            digits
                .checked_mul(p)
                .ok_or_else(|| ParseFixedError::OutOfRange(input.to_string()))?
        } else {
            let down = (-shift) as usize;
            let p = POW10
                .get(down)
                .copied()
                .ok_or_else(|| ParseFixedError::OutOfRange(input.to_string()))?;
            if digits % p != 0 {
                return Err(ParseFixedError::TooPrecise {
                    input: input.to_string(),
                    decimals: implied.max(0) as u32,
                });
            }
            digits / p
        };

        let signed = if negative { -value } else { value };
        i64::try_from(signed)
            .map(Fixed)
            .map_err(|_| ParseFixedError::OutOfRange(input.to_string()))
    }

    /// Parse a value that arrived as a JSON number rather than a string.
    ///
    /// Rust's `f64` `Display` emits the shortest literal that round-trips, so
    /// for the magnitudes and precisions crypto venues actually quote this
    /// recovers the digits the venue sent. It is still a lossier path than a
    /// quoted string, which is why [`crate::venue::VenueCapabilities`] records
    /// which form each venue uses.
    pub fn from_f64(value: f64) -> Result<Self, ParseFixedError> {
        if !value.is_finite() {
            return Err(ParseFixedError::OutOfRange(value.to_string()));
        }
        Fixed::from_decimal_str(&format!("{value}"))
    }

    /// True when the value can be written at `precision` decimals without loss.
    pub fn fits(self, precision: u32) -> bool {
        if precision >= SCALE {
            return true;
        }
        let divisor = POW10[(SCALE - precision) as usize];
        (self.0 as i128) % divisor == 0
    }

    /// Render at exactly `precision` decimals, rounding half away from zero.
    ///
    /// Pair this with [`Fixed::fits`] when the rendering feeds a checksum:
    /// rounding there would turn our own truncation into a phantom gap.
    pub fn to_decimal_string(self, precision: u32) -> String {
        let precision = precision.min(SCALE);
        let negative = self.0 < 0;
        let magnitude = (self.0 as i128).unsigned_abs() as i128;

        let divisor = POW10[(SCALE - precision) as usize];
        let mut scaled = magnitude / divisor;
        let remainder = magnitude % divisor;
        if divisor > 1 && remainder * 2 >= divisor {
            scaled += 1;
        }

        let unit = POW10[precision as usize];
        let whole = scaled / unit;
        let frac = scaled % unit;

        let mut out = String::new();
        if negative && (whole != 0 || frac != 0) {
            out.push('-');
        }
        out.push_str(&whole.to_string());
        if precision > 0 {
            out.push('.');
            let frac_str = frac.to_string();
            for _ in 0..(precision as usize - frac_str.len()) {
                out.push('0');
            }
            out.push_str(&frac_str);
        }
        out
    }

    #[inline]
    pub fn checked_add(self, rhs: Fixed) -> Option<Fixed> {
        self.0.checked_add(rhs.0).map(Fixed)
    }

    #[inline]
    pub fn checked_sub(self, rhs: Fixed) -> Option<Fixed> {
        self.0.checked_sub(rhs.0).map(Fixed)
    }

    /// The point halfway between two values.
    ///
    /// A midpoint is not always representable: the mid of two adjacent ticks
    /// falls between them. This truncates towards zero, so it can be up to one
    /// unit of 1e-9 below the true midpoint, and it says so rather than
    /// pretending the answer is exact. Everything the book itself holds stays
    /// exact; only this derived quantity rounds.
    pub fn midpoint(self, other: Fixed) -> Fixed {
        let sum = self.0 as i128 + other.0 as i128;
        Fixed((sum / 2) as i64)
    }

    /// Lossy conversion, for reporting and plots only. Never for book state.
    pub fn to_f64_lossy(self) -> f64 {
        self.0 as f64 / SCALE_FACTOR as f64
    }
}

impl FromStr for Fixed {
    type Err = ParseFixedError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Fixed::from_decimal_str(s)
    }
}

impl fmt::Display for Fixed {
    /// Canonical form: full precision with trailing zeros trimmed.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let full = self.to_decimal_string(SCALE);
        let trimmed = if full.contains('.') {
            full.trim_end_matches('0').trim_end_matches('.')
        } else {
            &full
        };
        f.write_str(if trimmed.is_empty() { "0" } else { trimmed })
    }
}

impl fmt::Debug for Fixed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Fixed({self})")
    }
}

impl Add for Fixed {
    type Output = Fixed;
    fn add(self, rhs: Fixed) -> Fixed {
        Fixed(self.0 + rhs.0)
    }
}

impl Sub for Fixed {
    type Output = Fixed;
    fn sub(self, rhs: Fixed) -> Fixed {
        Fixed(self.0 - rhs.0)
    }
}

impl Neg for Fixed {
    type Output = Fixed;
    fn neg(self) -> Fixed {
        Fixed(-self.0)
    }
}

impl AddAssign for Fixed {
    fn add_assign(&mut self, rhs: Fixed) {
        self.0 += rhs.0;
    }
}

impl SubAssign for Fixed {
    fn sub_assign(&mut self, rhs: Fixed) {
        self.0 -= rhs.0;
    }
}

impl Serialize for Fixed {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

struct FixedVisitor;

impl<'de> Visitor<'de> for FixedVisitor {
    type Value = Fixed;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a decimal number, as a string or a JSON number")
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<Fixed, E> {
        Fixed::from_decimal_str(v).map_err(de::Error::custom)
    }

    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Fixed, E> {
        Fixed::from_f64(v).map_err(de::Error::custom)
    }

    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Fixed, E> {
        Fixed::from_units(v).map_err(de::Error::custom)
    }

    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Fixed, E> {
        let v = i64::try_from(v).map_err(|_| de::Error::custom("quantity out of range"))?;
        Fixed::from_units(v).map_err(de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for Fixed {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Fixed, D::Error> {
        d.deserialize_any(FixedVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(s: &str) -> Fixed {
        Fixed::from_decimal_str(s).expect("parse")
    }

    #[test]
    fn parses_plain_decimals() {
        assert_eq!(f("0").mantissa(), 0);
        assert_eq!(f("1").mantissa(), 1_000_000_000);
        assert_eq!(f("0.5").mantissa(), 500_000_000);
        assert_eq!(f("50000.12").mantissa(), 50_000_120_000_000);
        assert_eq!(f("-2.25").mantissa(), -2_250_000_000);
        assert_eq!(f(".5").mantissa(), 500_000_000);
        assert_eq!(f("5.").mantissa(), 5_000_000_000);
    }

    #[test]
    fn parses_full_scale_without_loss() {
        assert_eq!(f("0.000000001").mantissa(), 1);
        assert_eq!(f("0.123456789").mantissa(), 123_456_789);
    }

    #[test]
    fn parses_scientific_notation() {
        assert_eq!(f("1e-5"), f("0.00001"));
        assert_eq!(f("1.5E3"), f("1500"));
        assert_eq!(f("-2e-2"), f("-0.02"));
    }

    #[test]
    fn rejects_rather_than_rounds_when_too_precise() {
        let err = Fixed::from_decimal_str("0.0000000001").unwrap_err();
        assert!(matches!(err, ParseFixedError::TooPrecise { .. }), "{err:?}");
    }

    #[test]
    fn rejects_garbage() {
        assert!(matches!(
            Fixed::from_decimal_str("1.2.3"),
            Err(ParseFixedError::Malformed(_))
        ));
        assert!(matches!(
            Fixed::from_decimal_str("12x"),
            Err(ParseFixedError::BadChar('x'))
        ));
        assert!(matches!(
            Fixed::from_decimal_str("   "),
            Err(ParseFixedError::Empty)
        ));
    }

    #[test]
    fn renders_at_venue_precision_exactly() {
        assert_eq!(f("50000.1").to_decimal_string(2), "50000.10");
        assert_eq!(f("0.001").to_decimal_string(8), "0.00100000");
        assert_eq!(f("7").to_decimal_string(0), "7");
        assert_eq!(f("-1.5").to_decimal_string(1), "-1.5");
    }

    #[test]
    fn rendering_rounds_half_away_from_zero_and_fits_reports_it() {
        let v = f("0.125");
        assert!(!v.fits(2), "0.125 does not fit two decimals");
        assert_eq!(v.to_decimal_string(2), "0.13");
        assert!(v.fits(3));
        assert_eq!(v.to_decimal_string(3), "0.125");
    }

    #[test]
    fn ordering_is_exact_across_scales() {
        // The classic float trap: 0.1 + 0.2 != 0.3 in binary floating point.
        assert_eq!(f("0.1") + f("0.2"), f("0.3"));
        assert!(f("0.00000001") < f("0.0000001"));
        let mut v = vec![f("2"), f("0.5"), f("-1"), f("1.0000001")];
        v.sort();
        assert_eq!(v, vec![f("-1"), f("0.5"), f("1.0000001"), f("2")]);
    }

    #[test]
    fn display_is_canonical() {
        assert_eq!(f("1.500").to_string(), "1.5");
        assert_eq!(f("2.000000000").to_string(), "2");
        assert_eq!(f("0").to_string(), "0");
        assert_eq!(f("-0.25").to_string(), "-0.25");
    }

    #[test]
    fn f64_path_recovers_venue_digits() {
        // Kraken sends book prices as JSON numbers, not strings.
        for s in ["50000.12", "0.00001", "1234.5678", "0.1", "99999.99999"] {
            let via_f64 = Fixed::from_f64(s.parse::<f64>().unwrap()).unwrap();
            assert_eq!(via_f64, f(s), "f64 path diverged for {s}");
        }
    }

    #[test]
    fn serde_round_trips_both_json_forms() {
        let from_string: Fixed = serde_json::from_str("\"50000.12\"").unwrap();
        let from_number: Fixed = serde_json::from_str("50000.12").unwrap();
        assert_eq!(from_string, from_number);
        let encoded = serde_json::to_string(&from_string).unwrap();
        assert_eq!(encoded, "\"50000.12\"");
        let round: Fixed = serde_json::from_str(&encoded).unwrap();
        assert_eq!(round, from_string);
    }

    #[test]
    fn a_midpoint_truncates_rather_than_pretending_to_be_exact() {
        assert_eq!(f("100").midpoint(f("102")), f("101"));
        assert_eq!(f("100").midpoint(f("101")), f("100.5"));
        // Adjacent ticks: the true mid is half a unit below the upper one and
        // is not representable, so it lands on the lower.
        let a = Fixed::from_mantissa(1);
        let b = Fixed::from_mantissa(2);
        assert_eq!(a.midpoint(b), a);
        // And it does not overflow at the extremes.
        assert_eq!(Fixed::MAX.midpoint(Fixed::MAX), Fixed::MAX);
    }

    #[test]
    fn negative_zero_renders_without_sign() {
        assert_eq!(Fixed::from_mantissa(0).to_decimal_string(2), "0.00");
    }
}
