package io.lgoyal.tickvault.analytics

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.datastream.DataStream

/** Flink boundary for replaying TickVault events with event-time semantics. */
object FlinkPipeline {
  def withWatermarks(
      events: DataStream[TickEvent],
      allowedLatenessMillis: Long
  ): DataStream[TickEvent] = {
    require(allowedLatenessMillis >= 0, "allowed lateness must be non-negative")
    val strategy = WatermarkStrategy
      .forBoundedOutOfOrderness[TickEvent](Duration.ofMillis(allowedLatenessMillis))
      .withTimestampAssigner(new SerializableTimestampAssigner[TickEvent] {
        override def extractTimestamp(event: TickEvent, recordTimestamp: Long): Long = event.eventTimeMillis
      })
    events.assignTimestampsAndWatermarks(strategy)
  }
}
