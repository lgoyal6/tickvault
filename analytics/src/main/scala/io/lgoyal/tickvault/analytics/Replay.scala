package io.lgoyal.tickvault.analytics

/** A bounded replay contract for local Flink-style jobs. */
final case class ReplayCheckpoint(offset: Long, watermarkMillis: Long, checksum: Long = 0L)
final case class ReplayResult(processed: Long, lastEventTimeMillis: Long, checksum: Long, checkpoint: ReplayCheckpoint)

object Replay {
  /** Consume the remaining events and return one final local resume checkpoint. */
  def run(events: Seq[TickEvent], from: ReplayCheckpoint = ReplayCheckpoint(0L, Long.MinValue)): ReplayResult = {
    require(from.offset >= 0L && from.offset <= events.size.toLong, "checkpoint offset outside replay")
    val start = from.offset.toInt
    val slice = events.drop(start)
    val processed = slice.foldLeft((0L, from.checksum)) {
      case ((count, checksum), event) => (count + 1L, checksum * 31L + event.sequence + event.eventTimeMillis)
    }
    val last = slice.lastOption.map(_.eventTimeMillis).getOrElse(from.watermarkMillis)
    val offset = from.offset + processed._1
    ReplayResult(processed._1, last, processed._2, ReplayCheckpoint(offset, last, processed._2))
  }
}

final case class BacktestMetrics(trades: Int, cumulativeReturn: Double, meanReturn: Double, hitRate: Double)

/** Deterministic baseline backtest with explicit transaction cost. */
object Backtest {
  def previousPrice(events: Seq[TickEvent], transactionCostBps: Double): BacktestMetrics = {
    require(transactionCostBps >= 0, "transaction cost cannot be negative")
    val returns = events.sliding(3).collect {
      case Seq(signalStart, previous, current)
          if signalStart.priceE9 > 0 && previous.priceE9 > 0 && current.priceE9 > 0 =>
        val direction = math.signum(previous.priceE9 - signalStart.priceE9)
        direction * (current.priceE9 - previous.priceE9).toDouble / previous.priceE9 - transactionCostBps / 10000.0
    }.toSeq
    val cumulative = returns.foldLeft(1.0)((wealth, value) => wealth * (1.0 + value)) - 1.0
    BacktestMetrics(returns.size, cumulative, if (returns.isEmpty) 0.0 else returns.sum / returns.size,
      if (returns.isEmpty) 0.0 else returns.count(_ > 0) / returns.size.toDouble)
  }
}
