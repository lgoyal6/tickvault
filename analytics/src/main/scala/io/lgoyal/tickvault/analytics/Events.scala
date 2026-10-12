package io.lgoyal.tickvault.analytics

/** A normalized event boundary for the Flink adapter.
  *
  * TickVault remains the source of truth. This type is intentionally smaller
  * than the Rust archive schema so downstream jobs cannot mutate book state.
  */
final case class TickEvent(
    venue: String,
    symbol: String,
    eventTimeMillis: Long,
    priceE9: Long,
    quantityE9: Long,
    sequence: Long,
    kind: String
)

final case class WindowMetric(
    venue: String,
    symbol: String,
    windowStartMillis: Long,
    windowEndMillis: Long,
    eventCount: Long,
    lateEventCount: Long,
    minPriceE9: Long,
    maxPriceE9: Long
)
