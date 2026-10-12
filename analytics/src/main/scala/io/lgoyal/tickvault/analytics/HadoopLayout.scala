package io.lgoyal.tickvault.analytics

import org.apache.hadoop.fs.Path

/** Hadoop-compatible partition layout for window metrics. */
object HadoopLayout {
  def partition(root: Path, venue: String, symbol: String, date: String): Path = {
    require(venue.nonEmpty && symbol.nonEmpty && date.matches("\\d{4}-\\d{2}-\\d{2}"))
    new Path(root, s"venue=$venue/symbol=$symbol/date=$date")
  }
}
