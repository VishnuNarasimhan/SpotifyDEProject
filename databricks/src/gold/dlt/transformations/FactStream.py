import dlt
from pyspark import pipelines as dp

# This is used to load the data from silver layer to staging layer (source data)
@dlt.table
def factstream_stg():
    df = spark.readStream.table("spotify_cata.silver.factstream")
    return df

# Tells DLT that this is a stream table
dlt.create_streaming_table("factstream")

dp.create_auto_cdc_flow(
  target = "factstream",
  source = "factstream_stg",
  keys = ["stream_id"],
  sequence_by = "stream_timestamp",
  stored_as_scd_type = 1, # SCD Type-1 => UPSERT
  track_history_except_column_list = None,
  name = None,
  once = False
)

