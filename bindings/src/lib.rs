//! Python bindings.
//!
//! Almost nobody doing a backtest wants to learn Rust to read a Parquet file,
//! so this is the layer that decides whether any of the rest gets used. It is
//! deliberately thin: everything here delegates to the same code the recorder
//! runs, and the Python-facing sugar lives in `python/tickvault/__init__.py`
//! where it is easier to make idiomatic.
//!
//! Two conventions run through it.
//!
//! **Exact where it matters, convenient where it does not.** The archive holds
//! prices as exact integers at 1e-9 and [`Book::to_arrow`] hands them back that
//! way. Derived analysis tables use floats, because a bar's mid is already a
//! truncated midpoint and asking someone to divide by a billion before plotting
//! is how a library goes unused.
//!
//! **`None` rather than a plausible number.** A one-sided book has no mid, an
//! empty one has no imbalance, and an aggregated feed does not know what
//! traded. All three come back as `None`, never zero.

use std::path::PathBuf;

use arrow::array::{
    ArrayRef, Float64Builder, Int64Builder, RecordBatch, StringBuilder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::pyarrow::IntoPyArrow;
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyStopIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use tickvault::Fixed;
use tickvault::query::Query;
use tickvault::query::aggregate::{self, BarBuilder, Depth, TopOfBook};
use tickvault::query::replay::Speed;
use tickvault::reconstruct::{Reconstructor, Request};
use tickvault::types::{Side, Symbol, VenueId};

fn err<E: std::fmt::Display>(e: E) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

fn parse_venue(raw: &str) -> PyResult<VenueId> {
    raw.parse::<VenueId>().map_err(|e| {
        PyValueError::new_err(format!(
            "{e}. Known venues: {}",
            VenueId::ALL
                .iter()
                .map(|v| v.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))
    })
}

fn parse_symbol(raw: &str) -> PyResult<Symbol> {
    Symbol::parse(raw).map_err(PyValueError::new_err)
}

/// Accept an RFC 3339 string or raw nanoseconds, so a caller can paste either.
fn parse_instant(raw: &Bound<'_, PyAny>) -> PyResult<i64> {
    if let Ok(nanos) = raw.extract::<i64>() {
        return Ok(nanos);
    }
    let text: String = raw.extract().map_err(|_| {
        PyValueError::new_err(
            "a timestamp must be an RFC 3339 string or nanoseconds since the epoch",
        )
    })?;
    tickvault::clock::parse_rfc3339_nanos(&text).ok_or_else(|| {
        PyValueError::new_err(format!(
            "{text:?} is neither an RFC 3339 instant nor a nanosecond count"
        ))
    })
}

fn to_f64(v: Fixed) -> f64 {
    v.to_f64_lossy()
}

/// One partition of the archive.
#[pyclass(module = "tickvault._native", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct Partition {
    /// The venue this partition holds, e.g. `"kraken"`.
    #[pyo3(get)]
    venue: String,
    /// The canonical symbol, `BASE-QUOTE`.
    #[pyo3(get)]
    symbol: String,
    /// The UTC date it covers, `YYYY-MM-DD`.
    #[pyo3(get)]
    date: String,
    /// `2` for aggregated price levels, `3` for order by order.
    #[pyo3(get)]
    book_level: u8,
    /// How many archived rows it holds.
    #[pyo3(get)]
    rows: u64,
    /// How many Parquet files it is spread across.
    #[pyo3(get)]
    files: u64,
    /// Its size on disk.
    #[pyo3(get)]
    bytes: u64,
    /// Receipt time of its earliest row, nanoseconds since the Unix epoch.
    #[pyo3(get)]
    first_ns: i64,
    /// Receipt time of its latest row, nanoseconds since the Unix epoch.
    #[pyo3(get)]
    last_ns: i64,
}

#[pymethods]
impl Partition {
    fn __repr__(&self) -> String {
        format!(
            "Partition({} {} {} L{}, {} rows in {} files, {:.1}s)",
            self.venue,
            self.symbol,
            self.date,
            self.book_level,
            self.rows,
            self.files,
            (self.last_ns - self.first_ns) as f64 / 1e9
        )
    }

    /// Seconds of wall clock this partition covers.
    #[getter]
    fn seconds(&self) -> f64 {
        (self.last_ns - self.first_ns) as f64 / 1e9
    }
}

/// A book as it stood at an instant.
#[pyclass(module = "tickvault._native")]
pub struct Book {
    inner: tickvault::book::L2Book,
    venue: String,
    symbol: String,
    at_ns: i64,
    origin: String,
    rows_applied: u64,
    suspect_rows: u64,
    truncations: usize,
}

#[pymethods]
impl Book {
    /// Bids, best first, as `(price, quantity)` floats.
    ///
    /// Floats for convenience. `to_arrow()` returns the exact integers the
    /// archive holds if you need them.
    #[getter]
    fn bids(&self) -> Vec<(f64, f64)> {
        self.inner
            .top(Side::Bid, usize::MAX)
            .into_iter()
            .map(|(p, q)| (to_f64(p), to_f64(q)))
            .collect()
    }

    /// Asks, best first, as `(price, quantity)` floats.
    #[getter]
    fn asks(&self) -> Vec<(f64, f64)> {
        self.inner
            .top(Side::Ask, usize::MAX)
            .into_iter()
            .map(|(p, q)| (to_f64(p), to_f64(q)))
            .collect()
    }

    /// Best bid as `(price, quantity)`, or `None` if that side is empty.
    #[getter]
    fn best_bid(&self) -> Option<(f64, f64)> {
        self.inner.best_bid().map(|(p, q)| (to_f64(p), to_f64(q)))
    }

    /// The lowest ask as ``(price, quantity)``, or ``None`` on an empty side.
    #[getter]
    fn best_ask(&self) -> Option<(f64, f64)> {
        self.inner.best_ask().map(|(p, q)| (to_f64(p), to_f64(q)))
    }

    /// Halfway between the touch, or `None` when one side is empty.
    ///
    /// `None` rather than a made-up number: a mid with one side missing is not
    /// a mid.
    #[getter]
    fn mid(&self) -> Option<f64> {
        TopOfBook::of(&self.inner).mid().map(to_f64)
    }

    /// Best ask minus best bid, or ``None`` unless both sides are present.
    #[getter]
    fn spread(&self) -> Option<f64> {
        TopOfBook::of(&self.inner).spread().map(to_f64)
    }

    /// The spread as a fraction of the mid, in basis points.
    #[getter]
    fn spread_bps(&self) -> Option<f64> {
        TopOfBook::of(&self.inner).spread_bps()
    }

    /// Order book imbalance over the top `depth` levels, in `[-1, 1]`.
    ///
    /// `None` when both sides are empty: the ratio is undefined, not zero.
    #[pyo3(signature = (depth = 10))]
    fn imbalance(&self, depth: usize) -> Option<f64> {
        Depth::of(&self.inner, depth).imbalance()
    }

    /// Resting quantity within `offset` of the mid, as `(bid, ask)`.
    ///
    /// The question "how much can I trade before moving the price by X",
    /// answered from the book that was actually there. Levels count whole.
    fn depth_within(&self, offset: f64) -> Option<(f64, f64)> {
        let offset = Fixed::from_f64(offset).ok()?;
        let d = aggregate::depth_within(&self.inner, offset)?;
        Some((to_f64(d.bid_qty), to_f64(d.ask_qty)))
    }

    /// Levels a side.
    #[getter]
    fn depth(&self) -> (usize, usize) {
        (
            self.inner.level_count(Side::Bid),
            self.inner.level_count(Side::Ask),
        )
    }

    /// A stable hash of the whole book. Two rebuilds of one instant agree here.
    #[getter]
    fn digest(&self) -> u32 {
        self.inner.digest()
    }

    /// True when the recorder could not vouch for some of what built this book.
    #[getter]
    fn suspect(&self) -> bool {
        self.suspect_rows > 0 || self.truncations > 0
    }

    /// How many of the rows behind this book fell inside a suspect window.
    #[getter]
    fn suspect_rows(&self) -> u64 {
        self.suspect_rows
    }

    /// Where the rebuild *started*: a checkpoint, a venue snapshot, or the
    /// first archived row.
    ///
    /// This describes the seed, not the book you are holding. A feed with no
    /// snapshot reports `"nothing archived"` and fills in from the stream, so
    /// this reading alongside a book full of levels is the normal case for
    /// Bitstamp rather than a contradiction. `rows_applied` says how much of
    /// the book came from the stream.
    #[getter]
    fn origin(&self) -> String {
        self.origin.clone()
    }

    /// How many archived rows were applied to build this book.
    #[getter]
    fn rows_applied(&self) -> u64 {
        self.rows_applied
    }

    /// The instant this book stood at, nanoseconds since the Unix epoch.
    ///
    /// Nanoseconds rather than a ``datetime`` because ``datetime`` stops at
    /// microseconds, and feeds routinely put several messages inside one.
    #[getter]
    fn at_ns(&self) -> i64 {
        self.at_ns
    }

    /// The book as a `pyarrow.RecordBatch`, with **exact** prices.
    ///
    /// `price` and `qty` are integers counting 1e-9 units, the same
    /// representation the archive holds. Nothing here has been through a float.
    fn to_arrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("side", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
            Field::new("qty", DataType::Int64, false),
            Field::new("level", DataType::UInt64, false),
        ]));
        let mut side = StringBuilder::new();
        let mut price = Int64Builder::new();
        let mut qty = Int64Builder::new();
        let mut level = UInt64Builder::new();
        for (name, s) in [("bid", Side::Bid), ("ask", Side::Ask)] {
            for (index, (p, q)) in self.inner.top(s, usize::MAX).into_iter().enumerate() {
                side.append_value(name);
                price.append_value(p.mantissa());
                qty.append_value(q.mantissa());
                level.append_value(index as u64);
            }
        }
        let columns: Vec<ArrayRef> = vec![
            std::sync::Arc::new(side.finish()),
            std::sync::Arc::new(price.finish()),
            std::sync::Arc::new(qty.finish()),
            std::sync::Arc::new(level.finish()),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(err)?
            .into_pyarrow(py)
    }

    fn __repr__(&self) -> String {
        format!(
            "Book({} {} at {}, {} bid / {} ask levels, mid {}{})",
            self.venue,
            self.symbol,
            self.at_ns,
            self.inner.level_count(Side::Bid),
            self.inner.level_count(Side::Ask),
            self.mid()
                .map(|m| format!("{m:.2}"))
                .unwrap_or_else(|| "n/a".into()),
            if self.suspect() { ", SUSPECT" } else { "" }
        )
    }
}

/// One message during a replay.
#[pyclass(module = "tickvault._native", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct Tick {
    /// Receipt time, nanoseconds since the epoch. Nanoseconds because Python's
    /// `datetime` only holds microseconds and this is microstructure data.
    #[pyo3(get)]
    at_ns: i64,
    /// What the venue said the time was, where it said anything.
    #[pyo3(get)]
    venue_ts_ns: Option<i64>,
    /// Best bid after this message, or `None` when that side is empty.
    #[pyo3(get)]
    bid: Option<f64>,
    /// Best ask after this message, or `None` when that side is empty.
    #[pyo3(get)]
    ask: Option<f64>,
    /// Halfway between the touch, or `None` unless both sides are present.
    #[pyo3(get)]
    mid: Option<f64>,
    /// Best ask minus best bid, or `None` unless both sides are present.
    #[pyo3(get)]
    spread: Option<f64>,
    /// Levels or orders the message carried.
    #[pyo3(get)]
    changes: usize,
    /// True when the recorder could not vouch for this message.
    #[pyo3(get)]
    suspect: bool,
    /// Quantity traded, where the feed reports executions.
    ///
    /// `None` on an aggregated feed, which never says whether a level shrank
    /// because it traded or because it was cancelled. Not zero.
    #[pyo3(get)]
    traded_qty: Option<f64>,
}

#[pymethods]
impl Tick {
    fn __repr__(&self) -> String {
        format!(
            "Tick(at={} mid={}{})",
            self.at_ns,
            self.mid
                .map(|m| format!("{m:.2}"))
                .unwrap_or_else(|| "n/a".into()),
            if self.suspect { " SUSPECT" } else { "" }
        )
    }
}

/// A replay in progress. Iterate it.
///
/// `unsendable` because the cursor holds an open Parquet reader, which is not
/// `Sync`. That is the honest shape: a cursor is a position in a file and
/// sharing one across threads would mean sharing that position.
#[pyclass(module = "tickvault._native", unsendable)]
pub struct Replay {
    cursor: tickvault::query::BookCursor,
    speed: Speed,
    previous_ns: Option<i64>,
    started: std::time::Instant,
    first_ns: Option<i64>,
    delivered: u64,
}

#[pymethods]
impl Replay {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Tick> {
        let Some(tick) = self.cursor.advance().map_err(err)? else {
            return Err(PyStopIteration::new_err(()));
        };

        if let Some(previous) = self.previous_ns {
            let due = tick.at_ns_offset(self.first_ns.unwrap_or(tick.at_wall));
            let delay = self.speed.delay_for(
                tick.at_wall - previous,
                self.started
                    .elapsed()
                    .saturating_sub(std::time::Duration::from_nanos(due.max(0) as u64)),
            );
            if !delay.is_zero() {
                // The GIL is released while waiting, so a paced replay does not
                // freeze the interpreter for the whole of it.
                py.detach(|| std::thread::sleep(delay));
            }
        }
        self.previous_ns = Some(tick.at_wall);
        self.first_ns.get_or_insert(tick.at_wall);
        self.delivered += 1;

        let top = TopOfBook::of(self.cursor.book());
        Ok(Tick {
            at_ns: tick.at_wall,
            venue_ts_ns: tick.venue_ts,
            bid: top.bid.map(|b| to_f64(b.0)),
            ask: top.ask.map(|a| to_f64(a.0)),
            mid: top.mid().map(to_f64),
            spread: top.spread().map(to_f64),
            changes: tick.changes,
            suspect: tick.suspect,
            traded_qty: tick.traded_qty.map(to_f64),
        })
    }

    /// The book as it stands right now, mid-replay.
    #[getter]
    fn book(&mut self) -> Book {
        Book {
            inner: self.cursor.book().clone(),
            venue: String::new(),
            symbol: String::new(),
            at_ns: self.cursor.at_wall(),
            origin: self.cursor.origin().to_string(),
            rows_applied: self.cursor.rows(),
            suspect_rows: self.cursor.trust().suspect_rows,
            truncations: self.cursor.trust().truncations.len(),
        }
    }

    /// How many ticks this replay has handed over so far.
    #[getter]
    fn delivered(&self) -> u64 {
        self.delivered
    }

    /// How many Parquet files it has had to open. Laziness, measured.
    #[getter]
    fn files_opened(&self) -> usize {
        self.cursor.files_opened()
    }

    fn __repr__(&self) -> String {
        format!("Replay(delivered={})", self.delivered)
    }
}

/// Small helper so the pacing maths reads the same as the Rust replay.
trait AtOffset {
    fn at_ns_offset(&self, first: i64) -> i64;
}

impl AtOffset for tickvault::query::Tick {
    fn at_ns_offset(&self, first: i64) -> i64 {
        self.at_wall - first
    }
}

/// An archive on disk.
#[pyclass(module = "tickvault._native", unsendable)]
pub struct Archive {
    root: PathBuf,
    reconstructor: Reconstructor,
}

#[pymethods]
impl Archive {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let root = PathBuf::from(path);
        if !root.exists() {
            return Err(PyValueError::new_err(format!("no archive at {path}")));
        }
        Ok(Archive {
            reconstructor: Reconstructor::open(&root).map_err(err)?,
            root,
        })
    }

    /// Where this archive lives on disk.
    #[getter]
    fn path(&self) -> String {
        self.root.to_string_lossy().into_owned()
    }

    /// Everything the archive holds, one entry per venue, symbol, and day.
    fn partitions(&self) -> Vec<Partition> {
        use std::collections::BTreeMap;
        let mut grouped: BTreeMap<(String, String, String, u8), Partition> = BTreeMap::new();
        for file in self.reconstructor.reader().files() {
            let level = match file.book_level {
                tickvault::types::BookLevel::L2 => 2,
                tickvault::types::BookLevel::L3 => 3,
            };
            let key = (
                file.venue.as_str().to_string(),
                file.symbol.as_str().to_string(),
                file.date.clone(),
                level,
            );
            let entry = grouped.entry(key.clone()).or_insert_with(|| Partition {
                venue: key.0.clone(),
                symbol: key.1.clone(),
                date: key.2.clone(),
                book_level: level,
                rows: 0,
                files: 0,
                bytes: 0,
                first_ns: i64::MAX,
                last_ns: i64::MIN,
            });
            entry.rows += file.rows;
            entry.files += 1;
            entry.bytes += file.bytes;
            entry.first_ns = entry.first_ns.min(file.first_recv_wall);
            entry.last_ns = entry.last_ns.max(file.last_recv_wall);
        }
        grouped.into_values().collect()
    }

    /// Open every file the archive vouches for and confirm it reads back.
    fn verify<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let report = self.reconstructor.reader().verify();
        let out = PyDict::new(py);
        out.set_item("clean", report.is_clean())?;
        out.set_item("files", report.files_checked)?;
        out.set_item("rows", report.rows_read)?;
        out.set_item(
            "unreadable",
            report
                .failures
                .iter()
                .map(|(p, why)| (p.clone(), why.clone()))
                .collect::<Vec<_>>(),
        )?;
        // Both kinds of file that open perfectly well and would still hand
        // back a wrong number: one whose rows are a different instrument than
        // the manifest claims, and one written at a scale this build does not
        // read. They clear `clean`, so leaving them out of the dict would give
        // a caller a failed verification with nothing in it to look at.
        out.set_item(
            "mislabelled",
            report
                .mislabelled
                .iter()
                .map(|(p, claimed, found)| (p.clone(), claimed.clone(), found.clone()))
                .collect::<Vec<_>>(),
        )?;
        out.set_item(
            "incompatible",
            report
                .incompatible
                .iter()
                .map(|(p, why)| (p.clone(), why.clone()))
                .collect::<Vec<_>>(),
        )?;
        out.set_item(
            "truncations",
            self.reconstructor.reader().manifest().truncations().len(),
        )?;
        Ok(out)
    }

    /// The book as it stood at an instant.
    #[pyo3(signature = (venue, symbol, at, depth = None))]
    fn book_at(
        &self,
        venue: &str,
        symbol: &str,
        at: &Bound<'_, PyAny>,
        depth: Option<usize>,
    ) -> PyResult<Book> {
        let venue_id = parse_venue(venue)?;
        let sym = parse_symbol(symbol)?;
        let at_ns = parse_instant(at)?;
        let mut request = Request::new(venue_id, &sym, at_ns);
        if let Some(depth) = depth {
            request = request.with_depth(depth);
        }
        let built = self.reconstructor.at(&request).map_err(err)?;
        Ok(Book {
            inner: built.book,
            venue: venue.to_string(),
            symbol: symbol.to_string(),
            at_ns,
            origin: built.origin.to_string(),
            rows_applied: built.rows_applied,
            suspect_rows: built.trust.suspect_rows,
            truncations: built.trust.truncations.len(),
        })
    }

    /// Bars over a range, as a `pyarrow.RecordBatch`.
    ///
    /// Open, high, low and close are the **mid price sampled from the book
    /// after every message**, not resampled from other bars.
    ///
    /// `volume` is null on an aggregated feed. Such a feed shows a level
    /// shrinking and never says whether it traded or was cancelled, so zero
    /// would be a claim the data does not support.
    #[pyo3(signature = (venue, symbol, bar_seconds = 60.0, start = None, end = None, depth = 10))]
    fn bars<'py>(
        &self,
        py: Python<'py>,
        venue: &str,
        symbol: &str,
        bar_seconds: f64,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        depth: usize,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (venue_id, sym) = (parse_venue(venue)?, parse_symbol(symbol)?);
        let (first, last) = self.span(venue_id, &sym)?;
        let from = start.map(parse_instant).transpose()?.unwrap_or(first - 1);
        let to = end.map(parse_instant).transpose()?.unwrap_or(last);
        if bar_seconds <= 0.0 {
            return Err(PyValueError::new_err("bar_seconds must be positive"));
        }

        let query = Query::new(venue_id, &sym, from, to).with_depth(depth);
        let mut cursor = query.cursor(&self.reconstructor).map_err(err)?;
        let interval = (bar_seconds * 1e9) as i64;
        let mut builder = BarBuilder::new(interval.max(1));
        while let Some(tick) = cursor.advance().map_err(err)? {
            let book = cursor.book();
            builder.observe(&tick, book);
        }
        bars_to_arrow(py, venue, symbol, &builder.finish())
    }

    /// Replay a range. Iterate the result.
    ///
    /// `speed` of zero runs as fast as the archive reads, which is what a
    /// backtest wants. Any other value preserves the recorded gaps, scaled:
    /// `1.0` is real time, `10.0` is ten times faster.
    #[pyo3(signature = (venue, symbol, speed = 0.0, start = None, end = None, depth = None))]
    fn replay(
        &self,
        venue: &str,
        symbol: &str,
        speed: f64,
        start: Option<&Bound<'_, PyAny>>,
        end: Option<&Bound<'_, PyAny>>,
        depth: Option<usize>,
    ) -> PyResult<Replay> {
        let (venue_id, sym) = (parse_venue(venue)?, parse_symbol(symbol)?);
        let (first, last) = self.span(venue_id, &sym)?;
        let from = start.map(parse_instant).transpose()?.unwrap_or(first - 1);
        let to = end.map(parse_instant).transpose()?.unwrap_or(last);
        let mut query = Query::new(venue_id, &sym, from, to);
        if let Some(depth) = depth {
            query = query.with_depth(depth);
        }
        Ok(Replay {
            cursor: query.cursor(&self.reconstructor).map_err(err)?,
            speed: if speed > 0.0 {
                Speed::Scaled(speed)
            } else {
                Speed::Unpaced
            },
            previous_ns: None,
            started: std::time::Instant::now(),
            first_ns: None,
            delivered: 0,
        })
    }

    /// The wall-clock span the archive covers for one instrument.
    fn span_ns(&self, venue: &str, symbol: &str) -> PyResult<(i64, i64)> {
        self.span(parse_venue(venue)?, &parse_symbol(symbol)?)
    }

    fn __repr__(&self) -> String {
        format!(
            "Archive({:?}, {} partitions)",
            self.root.to_string_lossy(),
            self.partitions().len()
        )
    }
}

impl Archive {
    fn span(&self, venue: VenueId, symbol: &Symbol) -> PyResult<(i64, i64)> {
        let files: Vec<_> = self
            .reconstructor
            .reader()
            .files()
            .iter()
            .filter(|f| f.venue == venue && f.symbol == *symbol)
            .collect();
        match (
            files.iter().map(|f| f.first_recv_wall).min(),
            files.iter().map(|f| f.last_recv_wall).max(),
        ) {
            (Some(a), Some(b)) => Ok((a, b)),
            _ => Err(PyKeyError::new_err(format!(
                "the archive holds nothing for {venue} {symbol}"
            ))),
        }
    }
}

/// Bars as an Arrow batch.
///
/// Prices are `float64` here rather than the archive's exact integers. A bar's
/// mid is already a truncated midpoint, so the exactness argument has run out
/// by this point, and asking someone to divide by a billion before plotting is
/// how a library goes unused. `Book.to_arrow()` is the exact path.
fn bars_to_arrow<'py>(
    py: Python<'py>,
    venue: &str,
    symbol: &str,
    bars: &[aggregate::Bar],
) -> PyResult<Bound<'py, PyAny>> {
    let ts = || DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("venue", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("start", ts(), false),
        Field::new("end", ts(), false),
        Field::new("open", DataType::Float64, false),
        Field::new("high", DataType::Float64, false),
        Field::new("low", DataType::Float64, false),
        Field::new("close", DataType::Float64, false),
        Field::new("updates", DataType::UInt64, false),
        Field::new("suspect_updates", DataType::UInt64, false),
        Field::new("volume", DataType::Float64, true),
        Field::new("mean_spread_bps", DataType::Float64, true),
    ]));

    let mut venues = StringBuilder::new();
    let mut symbols = StringBuilder::new();
    let mut start = Int64Builder::new();
    let mut end = Int64Builder::new();
    let (mut open, mut high, mut low, mut close) = (
        Float64Builder::new(),
        Float64Builder::new(),
        Float64Builder::new(),
        Float64Builder::new(),
    );
    let mut updates = UInt64Builder::new();
    let mut suspect = UInt64Builder::new();
    let mut volume = Float64Builder::new();
    let mut spread = Float64Builder::new();

    for bar in bars {
        venues.append_value(venue);
        symbols.append_value(symbol);
        start.append_value(bar.start_wall);
        end.append_value(bar.end_wall);
        open.append_value(to_f64(bar.open));
        high.append_value(to_f64(bar.high));
        low.append_value(to_f64(bar.low));
        close.append_value(to_f64(bar.close));
        updates.append_value(bar.updates);
        suspect.append_value(bar.suspect_updates);
        // Null, not zero: the feed does not know what traded.
        volume.append_option(bar.traded_qty.map(to_f64));
        spread.append_option(bar.mean_spread_bps);
    }

    let cast = |b: &mut Int64Builder| -> PyResult<ArrayRef> {
        arrow::compute::cast(
            &b.finish(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        )
        .map_err(err)
    };
    let columns: Vec<ArrayRef> = vec![
        std::sync::Arc::new(venues.finish()),
        std::sync::Arc::new(symbols.finish()),
        cast(&mut start)?,
        cast(&mut end)?,
        std::sync::Arc::new(open.finish()),
        std::sync::Arc::new(high.finish()),
        std::sync::Arc::new(low.finish()),
        std::sync::Arc::new(close.finish()),
        std::sync::Arc::new(updates.finish()),
        std::sync::Arc::new(suspect.finish()),
        std::sync::Arc::new(volume.finish()),
        std::sync::Arc::new(spread.finish()),
    ];
    RecordBatch::try_new(schema, columns)
        .map_err(err)?
        .into_pyarrow(py)
}

/// Venues this build can record from.
#[pyfunction]
fn venues() -> Vec<String> {
    VenueId::ALL
        .iter()
        .map(|v| v.as_str().to_string())
        .collect()
}

/// What each venue can and cannot prove about its own data.
#[pyfunction]
fn capabilities<'py>(py: Python<'py>) -> PyResult<Vec<Bound<'py, PyDict>>> {
    use tickvault::venue::registry::{self, VenueConfig};
    let clock = std::sync::Arc::new(tickvault::clock::ManualClock::default());
    let http = tickvault::transport::CannedFetch::new().shared();

    let mut out = Vec::new();
    for id in VenueId::ALL {
        let config = VenueConfig {
            symbols: vec![VenueConfig::default_symbol(*id)],
            kraken_precision: Some(tickvault::venue::kraken::Precision { price: 1, qty: 8 }),
            ..VenueConfig::default()
        };
        let venue =
            registry::build_offline(*id, &config, http.clone(), clock.clone()).map_err(err)?;
        let caps = venue.capabilities();
        let entry = PyDict::new(py);
        entry.set_item("venue", id.as_str())?;
        entry.set_item("book_level", caps.book_level.to_string())?;
        entry.set_item("detects_loss", caps.can_detect_loss())?;
        entry.set_item("validation", format!("{:?}", caps.validation))?;
        entry.set_item("requires_auth", caps.requires_auth)?;
        entry.set_item(
            "blind_spots",
            caps.detection_limits
                .iter()
                .map(|l| format!("{}: {}", l.scope, l.consequence))
                .collect::<Vec<_>>(),
        )?;
        out.push(entry);
    }
    Ok(out)
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Archive>()?;
    m.add_class::<Book>()?;
    m.add_class::<Replay>()?;
    m.add_class::<Tick>()?;
    m.add_class::<Partition>()?;
    m.add_function(wrap_pyfunction!(venues, m)?)?;
    m.add_function(wrap_pyfunction!(capabilities, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
