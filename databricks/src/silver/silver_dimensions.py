# Databricks notebook source
# MAGIC %md
# MAGIC ## **DimUser**

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Batch Processing**

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://bronze@spotifystorageproject.dfs.core.windows.net/DimUser")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Adding spotify_dab to system variables
import os
import sys

project_path = (os.path.join(os.getcwd(),'..','..','..'))
sys.path.append(project_path)

from spotify_dab.utils.transformations import reusable
print(project_path)


# COMMAND ----------

# MAGIC %md
# MAGIC #### **AUTOLOADER (Spark Streaming)**

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver@spotifystorageproject.dfs.core.windows.net/DimUser/checkpoint")\
    .option("schemaEvolutionMode", "addNewColumns")\
    .load("abfss://bronze@spotifystorageproject.dfs.core.windows.net/DimUser")

# COMMAND ----------

display(df_user)

# COMMAND ----------

df_user = df_user.withColumn("user_name", upper(col("user_name")))
display(df_user)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Using Utils **Function**

# COMMAND ----------

df_user_obj = reusable()
df_user = df_user_obj.dropColumns(df_user, ["_rescued_data"])
df_user = df_user.dropDuplicates(["user_id"])
display(df_user)

# COMMAND ----------

df_user.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@spotifystorageproject.dfs.core.windows.net/DimUser/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@spotifystorageproject.dfs.core.windows.net/DimUser/data")\
    .toTable("spotify_cata.silver.DimUser")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimArtist**

# COMMAND ----------

df_artist = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver@spotifystorageproject.dfs.core.windows.net/DimArtist/checkpoint")\
    .option("schemaEvolutionMode", "addNewColumns")\
    .load("abfss://bronze@spotifystorageproject.dfs.core.windows.net/DimArtist")

# COMMAND ----------

display(df_artist)

# COMMAND ----------

df_artist_obj = reusable()
df_artist = df_artist_obj.dropColumns(df_artist, ["_rescued_data"])
df_artist = df_artist.dropDuplicates(["artist_id"])
display(df_artist)

# COMMAND ----------

df_artist.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@spotifystorageproject.dfs.core.windows.net/DimArtist/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@spotifystorageproject.dfs.core.windows.net/DimArtist/data")\
    .toTable("spotify_cata.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimTrack**

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver@spotifystorageproject.dfs.core.windows.net/DimTrack/checkpoint")\
    .option("schemaEvolutionMode", "addNewColumns")\
    .load("abfss://bronze@spotifystorageproject.dfs.core.windows.net/DimTrack")
display(df_track)

# COMMAND ----------

df_track = df_track.withColumn("duration_flag", when(col('duration_sec') < 150, "low")\
                                            .when(col('duration_sec') < 300, "medium")\
                                            .otherwise("high"))

df_track = df_track.withColumn("track_name", regexp_replace(col('track_name'), '-', ' '))

df_track_obj = reusable()
df_track = df_track_obj.dropColumns(df_track, ["_rescued_data"])

display(df_track)


# COMMAND ----------

df_track.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@spotifystorageproject.dfs.core.windows.net/DimTrack/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@spotifystorageproject.dfs.core.windows.net/DimTrack/data")\
    .toTable("spotify_cata.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **DimDate**

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver@spotifystorageproject.dfs.core.windows.net/DimDate/checkpoint")\
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")\
    .load("abfss://bronze@spotifystorageproject.dfs.core.windows.net/DimDate")
display(df_date)

# COMMAND ----------

from pyspark.sql.functions import col

def is_binary_name(name: str) -> bool:
    # consider non-printable ASCII outside 32..126 (space..tilde) as binary
    for ch in name:
        if ord(ch) < 32 or ord(ch) > 126:
            return True
    # also treat names that start with PAR (parquet magic) as suspect
    if name.startswith("PAR"):
        return True
    return False

cols = df_date.columns
print("All columns:", cols)
bad_cols = [c for c in cols if c == "_rescued_data" or is_binary_name(c)]
print("Dropping bad columns:", bad_cols)

if bad_cols:
    df_date = df_date.drop(*bad_cols)

display(df_date)


# COMMAND ----------

df_date_obj = reusable()
df_date = df_date_obj.dropColumns(df_date, ["_rescued_data"])


# COMMAND ----------

df_date.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@spotifystorageproject.dfs.core.windows.net/DimDate/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@spotifystorageproject.dfs.core.windows.net/DimDate/data")\
    .toTable("spotify_cata.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **FactStream**

# COMMAND ----------

df_fact = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver@spotifystorageproject.dfs.core.windows.net/FactStream/checkpoint")\
    .option("schemaEvolutionMode", "addNewColumns")\
    .load("abfss://bronze@spotifystorageproject.dfs.core.windows.net/FactStream")
display(df_fact)

# COMMAND ----------

df_fact_obj = reusable()
df_fact = df_fact_obj.dropColumns(df_fact, ["_rescued_data"])
display(df_fact)

# COMMAND ----------

df_fact.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@spotifystorageproject.dfs.core.windows.net/FactStream/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@spotifystorageproject.dfs.core.windows.net/FactStream/data")\
    .toTable("spotify_cata.silver.FactStream")

# COMMAND ----------

# MAGIC %md
# MAGIC