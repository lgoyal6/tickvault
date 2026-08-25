//! A feed that goes silent must be noticed.
//!
//! This is the worst failure mode the project has, and it happened for real. On
//! a three hour six-venue capture, Bitstamp stopped sending without closing the
//! connection. The TCP session stayed ESTABLISHED, the reader sat waiting, and
//! the recorder held a live socket at zero CPU for sixty two minutes writing
//! nothing at all.
//!
//! What makes it the worst case is not the lost hour. It is that the archive
//! ends up with no rows for that window and the gap report has nothing to say
//! about it, so the hole reads as a quiet market rather than as a lost feed.
//! Every other failure in this project is recorded; this one was invisible.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tickvault::book::BookSnapshot;
use tickvault::clock::{Clock, MonotonicClock};
use tickvault::error::Result;
use tickvault::session::{RunSinks, StopAfter};
use tickvault::types::{Symbol, VenueId, VenueSymbol};
use tickvault::venue::{FeedEvent, FeedTransport, RawFrame, Venue, VenueCapabilities};

/// A socket that is open, healthy, and says nothing. Ever.
struct SilentTransport;

#[async_trait]
impl FeedTransport for SilentTransport {
    async fn recv(&mut self) -> Result<Option<RawFrame>> {
        // Not `None`: that would mean the source ended, which is a clean close
        // and already handled. This is the ambiguous case, a peer that has
        // stopped talking without saying goodbye.
        std::future::pending().await
    }

    async fn send_text(&mut self, _text: &str) -> Result<()> {
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

struct SilentVenue {
    caps: VenueCapabilities,
    connects: Arc<AtomicUsize>,
}

#[async_trait]
impl Venue for SilentVenue {
    fn capabilities(&self) -> &VenueCapabilities {
        &self.caps
    }

    fn venue_symbol(&self, symbol: &Symbol) -> VenueSymbol {
        VenueSymbol::new(symbol.to_string())
    }

    fn canonical_symbol(&self, _raw: &str) -> Result<Symbol> {
        Ok(Symbol::new("BTC", "USD"))
    }

    async fn connect(&self) -> Result<Box<dyn FeedTransport>> {
        self.connects.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(SilentTransport))
    }

    fn subscribe(&self, _symbols: &[Symbol]) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn snapshot(&self, _symbol: &Symbol) -> Result<BookSnapshot> {
        unreachable!("this venue's capabilities say its snapshot arrives in band")
    }

    fn parse_delta(&self, _frame: &RawFrame) -> Result<Vec<FeedEvent>> {
        Ok(Vec::new())
    }

    fn validate_sequence(
        &self,
        _state: &mut tickvault::sequence::SeqState,
        _event: &FeedEvent,
        _book: &tickvault::book::L2Book,
    ) -> tickvault::sequence::SeqVerdict {
        unreachable!("nothing arrives, so nothing is validated")
    }
}

/// Borrow a real venue's capability matrix rather than inventing one, so the
/// loop under test branches exactly as it does in production.
fn silent_venue() -> (Arc<dyn Venue>, Arc<AtomicUsize>) {
    let real = common::fixture(VenueId::Kraken).venue;
    let connects = Arc::new(AtomicUsize::new(0));
    let venue = SilentVenue {
        caps: real.capabilities().clone(),
        connects: Arc::clone(&connects),
    };
    (Arc::new(venue), connects)
}

#[tokio::test(start_paused = true)]
async fn a_feed_that_goes_silent_is_reconnected_rather_than_waited_on() {
    let (venue, connects) = silent_venue();
    let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());

    // Long enough for several idle timeouts. Time is paused, so this costs no
    // wall clock: tokio advances it as soon as everything is idle, which is
    // precisely the condition being tested.
    let (outcome, _) = tickvault::session::run(
        venue,
        vec![Symbol::new("BTC", "USD")],
        clock,
        StopAfter::duration(Duration::from_secs(10 * 60)),
        RunSinks::none(),
    )
    .await
    .expect("run");

    assert_eq!(outcome.frames, 0, "the transport sent nothing");
    assert!(
        outcome.reconnects >= 3,
        "ten minutes of silence produced only {} reconnects; before the idle \
         timeout existed this hung forever",
        outcome.reconnects
    );
    assert!(
        connects.load(Ordering::SeqCst) >= 4,
        "only connected {} times",
        connects.load(Ordering::SeqCst)
    );
}

#[tokio::test(start_paused = true)]
async fn the_silence_is_recorded_as_downtime_rather_than_as_a_clean_window() {
    // The reconnect matters less than the record of it. A window nobody
    // reported is a window a consumer would treat as good data.
    let (venue, _) = silent_venue();
    let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
    let (outcome, _) = tickvault::session::run(
        venue,
        vec![Symbol::new("BTC", "USD")],
        clock,
        StopAfter::duration(Duration::from_secs(5 * 60)),
        RunSinks::none(),
    )
    .await
    .expect("run");

    assert!(
        !outcome.report.is_spotless(),
        "a feed that said nothing for five minutes reported itself clean:\n{}",
        outcome.report
    );
}
