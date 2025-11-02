# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM spotify_cata.gold.dimuser;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM spotify_cata.gold.dimdate;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM spotify_cata.gold.dimtrack
# MAGIC WHERE track_id IN (46,5)