//! Point-in-time features, and why two timestamps are not one timestamp.
//!
//! A feature has two times and they are not interchangeable.
//!
//! - **Event time** is when the thing happened. In this archive that is
//!   [`Row::venue_ts`], the venue's own stamp, and it is optional because not
//!   every venue sends one.
//! - **Availability time** is when this recorder could first have known it. That
//!   is [`Row::recv_wall`], the moment the bytes came off the socket. It is
//!   never null: we cannot have a row we did not receive.
//!
//! Every point-in-time read filters on **availability**. A model trained at
//! `as_of` could only have seen what had arrived by `as_of`, whatever the venue
//! later claimed the event time was. Filtering on event time instead is the
//! classic leak, and it is not hypothetical here: [`crate::store::schema`]
//! already records a negative `skew_ns`, which is exactly the case where the
//! venue's clock runs ahead of ours and `venue_ts > recv_wall`. A naive join on
//! event time selects that row before it existed. [`FeatureStore::as_of`]
//! refuses it, and `gate_point_in_time` proves the refusal.
//!
//! Event time is not discarded; it is what staleness is measured against. A
//! read whose value is older than its view's TTL comes back [`Freshness::Stale`]
//! rather than as a bare number, so a consumer cannot silently act on an
//! outdated quote. Where the venue sent no timestamp at all the answer is
//! [`Freshness::AgeUnknown`], not a fabricated age.
//!
//! # Ordering is by arrival, not by claim
//!
//! When two observations compete for "latest", the winner is the one that
//! arrived last, not the one claiming the later event time. That matches how
//! the rest of the archive works ([`crate::clock`] orders by the monotonic
//! receipt reading precisely because a wall clock can step backwards) and it is
//! what makes online and historical reads agree: both walk arrival order.

use std::collections::HashMap;

use crate::store::rows::Row;
use crate::types::{Side, Symbol, VenueId};

/// Which entity a feature value belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Entity {
    pub venue: VenueId,
    pub symbol: Symbol,
}

impl Entity {
    pub fn new(venue: VenueId, symbol: &Symbol) -> Self {
        Entity {
            venue,
            symbol: symbol.clone(),
        }
    }
}

/// A named scalar derived from a single archive row.
///
/// Deliberately row-local: a feature that needed the whole book would drag book
/// reconstruction into a module about timestamps, and the timestamp rules are
/// identical either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Feature {
    /// Price of the most recent non-empty bid level, in 1e-9 units.
    LastBidPrice,
    /// Price of the most recent non-empty ask level, in 1e-9 units.
    LastAskPrice,
}

impl Feature {
    pub const ALL: &'static [Feature] = &[Feature::LastBidPrice, Feature::LastAskPrice];

    pub const fn name(self) -> &'static str {
        match self {
            Feature::LastBidPrice => "last_bid_price",
            Feature::LastAskPrice => "last_ask_price",
        }
    }

    /// The value this row carries for this feature, if it carries one.
    ///
    /// A zero quantity means the level was removed, which says nothing about
    /// where the best quote now is, so it yields nothing rather than a zero.
    fn extract(self, row: &Row) -> Option<i64> {
        if row.qty.mantissa() == 0 {
            return None;
        }
        let want = match self {
            Feature::LastBidPrice => Side::Bid,
            Feature::LastAskPrice => Side::Ask,
        };
        (row.side == want).then(|| row.price.mantissa())
    }
}

/// A feature and the age past which its values stop being usable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeatureView {
    pub feature: Feature,
    /// Values older than this are reported [`Freshness::Stale`].
    pub ttl_nanos: i64,
}

impl FeatureView {
    pub const fn new(feature: Feature, ttl_nanos: i64) -> Self {
        FeatureView { feature, ttl_nanos }
    }
}

/// One observation, with both of its times kept apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Observation {
    pub value: i64,
    /// When the venue says it happened. `None` when the venue sent no stamp.
    pub event_time: Option<i64>,
    /// When this recorder could first have known it. Always known.
    pub available_at: i64,
    /// Whether the row fell inside a suspect window.
    pub suspect: bool,
}

/// How much a read is worth, measured from event time to the read's `as_of`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Freshness {
    /// Within the view's TTL.
    Fresh { age_nanos: i64 },
    /// Older than the view's TTL. The value is still returned; the caller is
    /// told it is old rather than being handed it silently.
    Stale { age_nanos: i64, ttl_nanos: i64 },
    /// A value arrived, but the venue sent no event timestamp, so its age
    /// cannot be computed. Not the same as fresh.
    AgeUnknown,
    /// Nothing had arrived for this key by `as_of`.
    Missing,
}

impl Freshness {
    pub fn is_fresh(self) -> bool {
        matches!(self, Freshness::Fresh { .. })
    }
}

/// The answer to one point-in-time question.
///
/// There is no accessor that hands back a bare value: reaching the number goes
/// through [`Read::fresh_value`], which withholds it when the value is not
/// fresh, or through [`Read::value_with_freshness`], which makes the caller
/// take the verdict along with the number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Read {
    pub as_of: i64,
    pub freshness: Freshness,
    observation: Option<Observation>,
}

impl Read {
    fn missing(as_of: i64) -> Self {
        Read {
            as_of,
            freshness: Freshness::Missing,
            observation: None,
        }
    }

    /// The value only if it is fresh. A stale or absent value yields `None`.
    pub fn fresh_value(&self) -> Option<i64> {
        match self.freshness {
            Freshness::Fresh { .. } => self.observation.map(|o| o.value),
            _ => None,
        }
    }

    /// The value and the verdict together, for a caller that wants an old
    /// number and is prepared to say so.
    pub fn value_with_freshness(&self) -> Option<(i64, Freshness)> {
        self.observation.map(|o| (o.value, self.freshness))
    }

    /// The observation behind the read, for callers inspecting the timestamps.
    pub fn observation(&self) -> Option<Observation> {
        self.observation
    }
}

/// Every observation the archive holds, indexed for point-in-time reads.
///
/// Built once from rows in arrival order. Both read paths below run off this
/// same input but by different means, which is what makes their agreement worth
/// asserting rather than tautological.
#[derive(Debug, Default, Clone)]
pub struct FeatureStore {
    /// Arrival-ordered observations per key.
    series: HashMap<(Entity, Feature), Vec<Observation>>,
}

impl FeatureStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Ingest rows in the order they were archived.
    ///
    /// Rows must arrive in archive order, which is receipt order. The store
    /// does not sort them: re-sorting by a venue's own timestamp is the leak
    /// this module exists to prevent.
    pub fn ingest<'a>(
        &mut self,
        venue: VenueId,
        symbol: &Symbol,
        rows: impl IntoIterator<Item = &'a Row>,
    ) {
        let entity = Entity::new(venue, symbol);
        for row in rows {
            for feature in Feature::ALL {
                let Some(value) = feature.extract(row) else {
                    continue;
                };
                self.series
                    .entry((entity.clone(), *feature))
                    .or_default()
                    .push(Observation {
                        value,
                        event_time: row.venue_ts,
                        available_at: row.recv_wall,
                        suspect: row.suspect,
                    });
            }
        }
    }

    pub fn len(&self) -> usize {
        self.series.values().map(Vec::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.series.values().all(Vec::is_empty)
    }

    pub fn keys(&self) -> impl Iterator<Item = (&Entity, Feature)> {
        self.series.keys().map(|(e, f)| (e, *f))
    }

    /// Historical retrieval: the value as it stood at `as_of`.
    ///
    /// Scans the whole series and keeps the last observation whose
    /// **availability** time is at or before `as_of`. An observation whose
    /// venue timestamp is at or before `as_of` but which had not yet arrived is
    /// not a candidate, because at `as_of` nobody had it.
    pub fn as_of(&self, view: FeatureView, entity: &Entity, as_of: i64) -> Read {
        let key = (entity.clone(), view.feature);
        let Some(series) = self.series.get(&key) else {
            return Read::missing(as_of);
        };
        let mut chosen: Option<Observation> = None;
        for observation in series {
            // Availability, not event time. This one comparison is the whole
            // point-in-time guarantee.
            if observation.available_at > as_of {
                continue;
            }
            // Arrival order decides; a later arrival replaces an earlier one
            // even if it claims an earlier event time.
            chosen = Some(*observation);
        }
        finish(view, as_of, chosen)
    }

    /// Materialize the online store as it stands at `watermark`.
    ///
    /// A single forward fold, the shape a real materialization job has: walk
    /// arrivals in order, keep the newest per key, stop at the watermark. It
    /// shares no code with [`FeatureStore::as_of`], which filters and takes an
    /// argmax over the full series. Their answers must still match; see
    /// `gate_point_in_time`.
    pub fn materialize(&self, watermark: i64) -> OnlineStore {
        let mut latest: HashMap<(Entity, Feature), Observation> = HashMap::new();
        for (key, series) in &self.series {
            for observation in series {
                if observation.available_at > watermark {
                    // Skip, do not stop. Arrival order is monotonic in the
                    // recorder's *monotonic* clock, but `available_at` is the
                    // wall reading, and NTP can step that backwards. Stopping
                    // at the first ineligible arrival would silently truncate
                    // the fold and disagree with `as_of`.
                    continue;
                }
                latest.insert(key.clone(), *observation);
            }
        }
        OnlineStore { watermark, latest }
    }
}

/// The materialized latest value per key, plus the instant it is current to.
///
/// The watermark is part of the store rather than implicit. An online store
/// that cannot say how current it is cannot report staleness, and a consumer
/// asking "what is the price now" would get a number with no way to tell how
/// old it is.
#[derive(Debug, Clone)]
pub struct OnlineStore {
    watermark: i64,
    latest: HashMap<(Entity, Feature), Observation>,
}

impl OnlineStore {
    /// The instant this store is current to. Reads are answered as of here.
    pub fn watermark(&self) -> i64 {
        self.watermark
    }

    pub fn len(&self) -> usize {
        self.latest.len()
    }

    pub fn is_empty(&self) -> bool {
        self.latest.is_empty()
    }

    /// Latest read: the current value, judged against the watermark.
    ///
    /// Answered as of the watermark, which is what makes it comparable with a
    /// historical read at the same instant.
    pub fn latest(&self, view: FeatureView, entity: &Entity) -> Read {
        let key = (entity.clone(), view.feature);
        finish(view, self.watermark, self.latest.get(&key).copied())
    }
}

/// Turn a chosen observation into a read, judging its age against the TTL.
fn finish(view: FeatureView, as_of: i64, chosen: Option<Observation>) -> Read {
    let Some(observation) = chosen else {
        return Read::missing(as_of);
    };
    let freshness = match observation.event_time {
        // No venue stamp means no age. Saying "fresh" here would be a guess
        // dressed as a measurement.
        None => Freshness::AgeUnknown,
        Some(event_time) => {
            let age_nanos = as_of - event_time;
            if age_nanos > view.ttl_nanos {
                Freshness::Stale {
                    age_nanos,
                    ttl_nanos: view.ttl_nanos,
                }
            } else {
                Freshness::Fresh { age_nanos }
            }
        }
    };
    Read {
        as_of,
        freshness,
        observation: Some(observation),
    }
}
