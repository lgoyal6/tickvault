"""Two Feast feature views over one TickVault feature table.

The table has two timestamp columns and they mean different things:
`event_time` is the venue's own stamp, `available_at` is when the recorder
first had the row. Feast's point-in-time join runs off exactly one column,
`timestamp_field`. So the two views below differ only in which column they hand
Feast, and the comparison is what that single choice costs.

`created_timestamp_column` is not a second point-in-time axis. Feast uses it to
break ties between rows sharing an event timestamp, not to filter.
"""

from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource
from feast.types import Int64

entity = Entity(name="entity_id", join_keys=["entity_id"])

# The configuration a team reaches for first: the event timestamp is the
# timestamp, and arrival is recorded as the created column.
event_time_source = FileSource(
    name="tv_event_time",
    path="data/features.parquet",
    timestamp_field="event_time",
    created_timestamp_column="available_at",
)

# The configuration that survives the leak: tell Feast that availability *is*
# the timeline. Correct as-of behaviour, but the TTL now measures arrival age,
# not event age.
availability_source = FileSource(
    name="tv_availability",
    path="data/features.parquet",
    timestamp_field="available_at",
)

TTL = timedelta(seconds=10)

bid_by_event_time = FeatureView(
    name="bid_by_event_time",
    entities=[entity],
    ttl=TTL,
    schema=[Field(name="last_bid_price", dtype=Int64)],
    source=event_time_source,
)

bid_by_availability = FeatureView(
    name="bid_by_availability",
    entities=[entity],
    ttl=TTL,
    schema=[Field(name="last_bid_price", dtype=Int64)],
    source=availability_source,
)
