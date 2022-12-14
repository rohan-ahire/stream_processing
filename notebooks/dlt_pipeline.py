# Databricks notebook source
# MAGIC %md
# MAGIC ### DLT Pipeline
# MAGIC Load Bronze tables and then apply transformations and load to Silver
# MAGIC 
# MAGIC #### Description
# MAGIC This notebook dynamically creates bronze tables based on the subfolders required to scan and loads the silver table based on the attributes selected from bronze

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1
# MAGIC 
# MAGIC This cell loads all bronze tables in parallel, including schema evolution and inference.

# COMMAND ----------

import dlt

def create_table_from_path(esn, input_path, esn_checkpoint_path, table_name):
  @dlt.table(
		name=table_name, # table name
		comment=f"table for {esn}" # comment that appears on the dlt pipeline ui
	)
  def build_table_from_json():
    df = (
      spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
#       .option("cloudFiles.schemaLocation", esn_checkpoint_path)
      .option("cloudFiles.inferColumnTypes", True)
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .load(input_path)
     )
    return df

esn_list = ["66303784"]
table_name_dict = {"66303784":"ngca"}
source_path = "s3://fit-all-raw-data-230935021301"
date_pattern = "2022-*"

for esn in esn_list:
  table_name = table_name_dict[esn]
  input_path = f"{source_path}/{esn}/{date_pattern}/"
  esn_checkpoint_path = f"{source_path}/checkpoint/{esn}"
  create_table_from_path(esn, input_path, esn_checkpoint_path, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Specific to data sources having spaces or columns in their column names
# MAGIC This cell reads sdk data sources, applies column name cleansing and dynamically creates bronze table

# COMMAND ----------

# import pyspark.sql.functions as F
# import re
# import dlt

# def get_clean_column_names_list(columns):
#   clean_columns = [F.col(v).alias(re.sub(r"[^0-9a-zA-Z_]",'_',v+'_'+str(k)+'_modified')) if re.search("[^0-9a-zA-Z_]", v) else v for k,v in enumerate(columns)]
#   return clean_columns

# esn = "sdk"
# source_path = "s3://oetrta/rohan/fit-all-raw-data"
# year = "2022"
# input_path = f"{source_path}/{esn}/{year}-*/"
# esn_checkpoint_path = f"{source_path}/checkpoint/{esn}"

# @dlt.table(
# 		name=esn,
# 		comment=f"table for {esn}"
# 	)
# def build_table_from_json():
#   df = (
#     spark.readStream.format("cloudFiles")
#     .option("cloudFiles.format", "json")
#     .option("cloudFiles.schemaLocation", esn_checkpoint_path)
#     .option("cloudFiles.inferColumnTypes", True)
#     .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
#     .load(input_path)
#   )
#   clean_columns = get_clean_column_names_list(df.columns)
#   df = df.select(clean_columns)
#   df.printSchema()
#   return df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2
# MAGIC 
# MAGIC This notebook selects data from bronze table, applies transformation and loads to silver table

# COMMAND ----------

source_table = "ngca"

@dlt.table(
		name=f"silver_{source_table}",
		comment=f"table for transformed layer for {source_table}"
	)
def build_silver_layer():
  source_delta_df = dlt.read_stream(source_table)
  df = (source_delta_df.selectExpr(
    "case when componentSerialNumber is not null then componentSerialNumber else 'Blank' end as vEsn",
    "case when telematicsDeviceId is not null then telematicsDeviceId else 'Blank' end as vAvl",
    "case when frm_gen_ts is not null then substring(regexp_replace(frm_gen_ts, r'(T)', ' '), 0, length(frm_gen_ts)-5) else '2001-01-01 01:01:01' end as vDevTimeStamp",
    "case when frm_rcvd_ts is not null then substring(regexp_replace(frm_rcvd_ts, r'(T)', ' '), 0, length(frm_rcvd_ts)-5) else '2001-01-01 01:01:01' end as vSerTimeStamp",
    "case when comm_ts is not null then substring(regexp_replace(comm_ts, r'(T)', ' '), 0, length(comm_ts)-5) else '2001-01-01 01:01:01' end as vLCommunicationTimeStamp",
    "case when telematicsPartnerName is not null then telematicsPartnerName else 'N/A' end as vtelematicsPName",
    "case when totalEngineHour is not null then totalEngineHour else '0.0' end as vTotalEngHrs",
    "case when (totalEngineHour is not null and totalEngineHour not like '%N%') then cast(totalEngineHour as double) else -7777 end as dTotalEngHrs",
    "case when totalFuelConsumption is not null then totalFuelConsumption else '0.0' end as vTotalFuelUsed",
    "case when (totalFuelConsumption is not null and totalFuelConsumption not like '%N%') then cast(totalFuelConsumption as double) else -7777 end as dTotalFuelUsed",
    "concat('NGCA - ',(case when telematicsPartnerName is not null then telematicsPartnerName else 'N/A' end)) as vDataSourProviderName",
    "case when in_serv_loc is not null then cast(in_serv_loc as long) else 0 end as vIn_Service_Location",
    "explode(samples) as samples" 
  ))
  
  transformed_df = df.selectExpr(
    "vEsn as fGEId",
    "vAvl as fAvl",
    "vDevTimeStamp as fDeviceTimestamp",
    "to_timestamp(vDevTimeStamp, 'yyyy-MM-dd HH:mm:ss') as fDeviceTimestamp_modified",
    "vSerTimeStamp as fServerTimestamp",
    "vLCommunicationTimeStamp as fLCommunication",
    "vDataSourProviderName as fDSPName",
    "dTotalEngHrs as fTEHours",
    "dTotalFuelUsed as fTFUsed",
    "vIn_Service_Location as fIn_Service_Location",
    "case when samples.convertedDeviceParameters is not null then samples.convertedDeviceParameters.latitude else 'Blank' end as fLatitude",
    "case when samples.convertedDeviceParameters is not null then samples.convertedDeviceParameters.longitude else 'Blank' end as fLongitude"
  )
  return transformed_df
