# Databricks notebook source
# MAGIC %sql
# MAGIC drop table cummins.ngca

# COMMAND ----------

from pyspark.sql.functions import *

df = spark.read.json("s3://fit-all-raw-data-230935021301/66303784/2022*")

(df
 .select(to_timestamp("frm_gen_ts").alias("frm_gen_ts_modified"), "*")
 .write
 .format("delta")
 .saveAsTable("cummins.ngca")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC desc detail cummins.ngca

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize cummins.ngca zorder by (frm_gen_ts_modified);

# COMMAND ----------

# MAGIC %sql
# MAGIC desc detail cummins.ngca

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM cummins.ngca retain 0 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history cummins.ngca
