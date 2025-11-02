import dlt
from pyspark import pipelines as dp

# Expectations - Data Quality Check (https://learn.microsoft.com/en-us/azure/databricks/ldp/expectations)
expectations = {
    "rule_1": "user_id IS NOT NULL"
}

# This is used to load the data from silver layer to staging layer (source data)
@dlt.table
@dlt.expect_all_or_drop(expectations)
def dimuser_stg():
    df = spark.readStream.table("spotify_cata.silver.dimuser")
    return df

# Tells DLT that this is a stream table
# Data Quality Check can be done in line 18
dlt.create_streaming_table(
    name = "dimuser",
    expect_all_or_drop= expectations
)

dp.create_auto_cdc_flow(
  target = "dimuser",
  source = "dimuser_stg",
  keys = ["user_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
  track_history_except_column_list = None,
  name = None,
  once = False
)