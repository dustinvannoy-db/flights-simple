# Databricks notebook source
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("database", "dustinvannoy_dev")
artifact_path = f'/Workspace{dbutils.widgets.get("artifact_path")}/.internal'

# COMMAND ----------

# MAGIC %pip install {artifact_path}/flights-0.0.1-py3-none-any.whl

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read csv data (batch mode)

# COMMAND ----------

# DBTITLE 1,Setup vars and functions
from flights.transforms import flight_transforms, shared_transforms
catalog = dbutils.widgets.get("catalog")
database = dbutils.widgets.get("database")

path = "/databricks-datasets/airlines"
raw_table_name = f"{catalog}.{database}.flights_raw"


# COMMAND ----------

# DBTITLE 1,Read raw
print(f"Attempting to read table {raw_table_name}")
df = flight_transforms.read_batch(spark, path).limit(1000)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write raw Delta Lake table (batch mode)

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable(raw_table_name)
print(f"Succesfully wrote data to {raw_table_name}")

# COMMAND ----------

# MAGIC %environment
# MAGIC "client": "1"
# MAGIC "base_environment": ""
# TODO this works: MAGIC "dependencies": ["/Workspace/Users/lorenzo.rubio@databricks.com/.bundle/flights_simple/dev/artifacts/.internal/flights-0.0.1-py3-none-any.whl"]
# TODO should be something like MAGIC "dependencies": ["{artifact_path}/flights-0.0.1-py3-none-any.whl"], but the artifact_path needs to be passed to
