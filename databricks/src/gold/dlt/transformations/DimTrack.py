import dlt
from pyspark import pipelines as dp

# This is used to load the data from silver layer to staging layer (source data)
@dlt.table
def dimtrack_stg():
    df = spark.readStream.table("spotify_cata.silver.dimtrack")
    return df

# Tells DLT that this is a stream table
dlt.create_streaming_table("dimtrack")

dp.create_auto_cdc_flow(
  target = "dimtrack",
  source = "dimtrack_stg",
  keys = ["track_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
  track_history_except_column_list = None,
  name = None,
  once = False
)

