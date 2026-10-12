package io.lgoyal.tickvault.analytics

/** Deterministic event-time accounting used by both tests and the Flink job. */
final class WatermarkTracker(val allowedLatenessMillis: Long) {
  require(allowedLatenessMillis >= 0, "allowed lateness must be non-negative")

  private var maximumEventTimeMillis = Long.MinValue

  def watermarkMillis: Long =
    if (maximumEventTimeMillis == Long.MinValue) Long.MinValue
    else maximumEventTimeMillis - allowedLatenessMillis

  /** Returns true when the event is behind the current watermark. */
  def observe(eventTimeMillis: Long): Boolean = {
    val isLate = eventTimeMillis < watermarkMillis
    if (eventTimeMillis > maximumEventTimeMillis) maximumEventTimeMillis = eventTimeMillis
    isLate
  }
}

object Windowing {
  def startOf(timestampMillis: Long, windowMillis: Long): Long = {
    require(windowMillis > 0, "window size must be positive")
    Math.floorDiv(timestampMillis, windowMillis) * windowMillis
  }
}
