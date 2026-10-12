package io.lgoyal.tickvault.analytics

import org.apache.flink.streaming.api.scala._

/** Runs one bounded replay through a real local Flink execution environment. */
object LiveReplay {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    val events = Seq(
      TickEvent("kraken", "BTC-USD", 1000L, 100000000L, 1, 1, "trade"),
      TickEvent("kraken", "BTC-USD", 2000L, 101000000L, 1, 2, "trade"),
      TickEvent("kraken", "BTC-USD", 1500L, 99000000L, 1, 3, "trade")
    )
    val stream = environment.fromCollection(events)
    val timestamped = FlinkPipeline.withWatermarks(stream, allowedLatenessMillis = 500L)
    timestamped
      .map(_.sequence)
      .keyBy(_ => 0)
      .reduce(_ + _)
      .map(total => s"processed=${events.size}, sequence_sum=$total")
      .print()

    environment.execute("tickvault-live-bounded-replay")
  }
}
