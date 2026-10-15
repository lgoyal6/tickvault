package io.lgoyal.tickvault.analytics

import org.apache.hadoop.fs.Path
import org.junit.Assert._
import org.junit.Test

final class AnalyticsTest {
  @Test def watermarkTracksOutOfOrderEvents(): Unit = {
    val tracker = new WatermarkTracker(2000)
    assertFalse(tracker.observe(10000))
    assertEquals(8000, tracker.watermarkMillis)
    assertTrue(tracker.observe(7000))
    assertFalse(tracker.observe(12000))
    assertEquals(10000, tracker.watermarkMillis)
  }

  @Test def windowStartsAreStableForNegativeAndPositiveTimes(): Unit = {
    assertEquals(0, Windowing.startOf(999, 1000))
    assertEquals(1000, Windowing.startOf(1000, 1000))
    assertEquals(-1000, Windowing.startOf(-1, 1000))
  }

  @Test def hadoopPartitionIsHiveCompatible(): Unit = {
    val result = HadoopLayout.partition(new Path("s3a://tickvault/metrics"), "kraken", "BTC-USD", "2026-10-12")
    assertEquals("s3a://tickvault/metrics/venue=kraken/symbol=BTC-USD/date=2026-10-12", result.toString)
  }

  @Test def replayCheckpointResumesWithoutDoubleCounting(): Unit = {
    val events = (1 to 4).map(i => TickEvent("kraken", "BTC-USD", i * 1000L, i * 100000000L, 1, i, "trade"))
    val first = Replay.run(events.take(2))
    val resumed = Replay.run(events, first.checkpoint)
    val full = Replay.run(events)
    assertEquals(full.processed, first.processed + resumed.processed)
    assertEquals(full.checksum, resumed.checksum)
  }

  @Test def replayRejectsOffsetsBeforeNarrowingToAnIndex(): Unit = {
    val events = (1 to 4).map(i => TickEvent("kraken", "BTC-USD", i * 1000L, i * 100000000L, 1, i, "trade"))
    val invalid = Seq(-1L, Int.MaxValue.toLong + 1L, 1L << 32, (1L << 32) + 1L, Long.MaxValue, Long.MinValue)
    invalid.foreach { offset =>
      try {
        Replay.run(events, ReplayCheckpoint(offset, 0L))
        fail(s"accepted invalid checkpoint offset $offset")
      } catch {
        case error: IllegalArgumentException =>
          assertEquals("requirement failed: checkpoint offset outside replay", error.getMessage)
      }
    }
  }

  @Test def replayAtTheEndPreservesItsCheckpoint(): Unit = {
    val events = Seq(TickEvent("kraken", "BTC-USD", 1000L, 100000000L, 1, 1, "trade"))
    val checkpoint = Replay.run(events).checkpoint
    val result = Replay.run(events, checkpoint)
    assertEquals(0L, result.processed)
    assertEquals(checkpoint, result.checkpoint)
    val empty = ReplayCheckpoint(0L, Long.MinValue, 17L)
    assertEquals(empty, Replay.run(Seq.empty, empty).checkpoint)
  }

  @Test def backtestReportsCostsAndReturns(): Unit = {
    val events = Seq(
      TickEvent("kraken", "BTC-USD", 1, 100, 1, 1, "trade"),
      TickEvent("kraken", "BTC-USD", 2, 110, 1, 2, "trade"),
      TickEvent("kraken", "BTC-USD", 3, 99, 1, 3, "trade"))
    val result = Backtest.previousPrice(events, transactionCostBps = 10)
    assertEquals(1, result.trades)
    assertTrue(result.cumulativeReturn < 0.0)
    assertEquals(0.0, result.hitRate, 0.0001)
  }
}
