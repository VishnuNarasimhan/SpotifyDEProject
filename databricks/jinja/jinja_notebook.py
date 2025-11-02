# Databricks notebook source
# MAGIC %md
# MAGIC ### Dynamically Apply Joins for the given tables

# COMMAND ----------

# MAGIC %md
# MAGIC **Goal: Dynamically apply joins between FactStream, DimUser and DimTrack**

# COMMAND ----------

!pip install jinja2
dbutils.library.restartPython()
!pip show Jinja2

# COMMAND ----------

from jinja2 import Template

# COMMAND ----------

# MAGIC %md
# MAGIC Selection of table contents and condition

# COMMAND ----------

parameters = [
  {
    "table": "spotify_cata.silver.factstream",
    "alias": "factstream",
    "cols": "factstream.stream_id, factstream.listen_duration"
  },
  {
    "table": "spotify_cata.silver.dimuser",
    "alias": "dimuser",
    "cols": "dimuser.user_id, dimuser.user_name",
    "condition": "factstream.user_id = dimuser.user_id"
  },
  {
    "table": "spotify_cata.silver.dimtrack",
    "alias": "dimtrack",
    "cols": "dimtrack.track_id, dimtrack.track_name",
    "condition": "factstream.track_id = dimtrack.track_id"
  }
  
]

# COMMAND ----------

query_text = """
        SELECT
            {%  for param in parameters %}
                {{ param.cols }}
                    {% if not loop.last %}
                        ,
                    {% endif %}
            {% endfor %}
        FROM
            {{ parameters[0]['table'] }} AS {{ parameters[0]['alias']}}
            {% for param in parameters[1:] %}
                LEFT JOIN
                    {{ param['table'] }} AS {{ param['alias'] }}
                ON
                    {{ param['condition'] }}
            {% endfor %}               
"""

# COMMAND ----------

jinja_template = Template(query_text)
query = jinja_template.render(parameters=parameters)
print(query)

# COMMAND ----------

display(spark.sql(query))