# Databricks notebook source
import dlt

def create_table_from_path(esn, input_path, esn_checkpoint_path):
  @dlt.table(
		name=esn,
		comment=f"table for {esn}"
	)
  def build_table_from_json():
    df = (
      spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", esn_checkpoint_path)
      .option("cloudFiles.inferColumnTypes", True)
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .load(input_path)
     )
    return df

esn_list = ["ngca","specto"]
source_path = "s3://oetrta/rohan/fit-all-raw-data"
year = "2022"

for esn in esn_list:  
  input_path = f"{source_path}/{esn}/{year}-*/"
  esn_checkpoint_path = f"{source_path}/checkpoint/{esn}"
  create_table_from_path(esn, input_path, esn_checkpoint_path)

# COMMAND ----------

import pyspark.sql.functions as F
import re
import dlt

def get_clean_column_names_list(columns):
  clean_columns = [F.col(v).alias(re.sub(r"[^0-9a-zA-Z_]",'_',v+'_'+str(k)+'_modified')) if re.search("[^0-9a-zA-Z_]", v) else v for k,v in enumerate(columns)]
  return clean_columns

esn = "sdk"
source_path = "s3://oetrta/rohan/fit-all-raw-data"
year = "2022"
input_path = f"{source_path}/{esn}/{year}-*/"
esn_checkpoint_path = f"{source_path}/checkpoint/{esn}"

@dlt.table(
		name=esn,
		comment=f"table for {esn}"
	)
def build_table_from_json():
  df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", esn_checkpoint_path)
    .option("cloudFiles.inferColumnTypes", True)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(input_path)
  )
  clean_columns = get_clean_column_names_list(df.columns)
  df = df.select(clean_columns)
  df.printSchema()
  return df

# COMMAND ----------

source_table = "ngca"

@dlt.table(
		name=f"silver_{source_table}",
		comment=f"table for transformed layer for {source_table}"
	)
def build_silver_layer():
  source_delta_df = dlt.read_stream(source_table)
  df = (source_delta_df.selectExpr(
    "'ESN' as fEsn",
    "'Global Equipment ID' as fGEId",    
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
    "fEsn",
    "fGEId",
    "vEsn",
    "vAvl",
    "vDevTimeStamp",
    "vSerTimeStamp",
    "vLCommunicationTimeStamp",
    "vtelematicsPName",
    "vTotalEngHrs",
    "dTotalEngHrs",
    "vTotalFuelUsed",
    "dTotalFuelUsed",
    "vDataSourProviderName",
    "vIn_Service_Location",
    "case when samples.convertedDeviceParameters is not null then samples.convertedDeviceParameters.latitude else 'Blank' end as vLatitude",
    "case when samples.convertedDeviceParameters is not null then samples.convertedDeviceParameters.longitude else 'Blank' end as vLongitude"
  )
  return transformed_df

# COMMAND ----------

# df = spark.createDataFrame([
#     (1, 4., 'GFG1'),
#     (2, 8., 'GFG2'),
#     (3, 5., 'GFG3')
# ], schema=['a_b','a(b);','c'])
# display(df)

# COMMAND ----------

# import pyspark.sql.functions as F
# import re

# def get_clean_column_names_list(columns):
#   clean_columns = [F.col(v).alias(re.sub(r"[^0-9a-zA-Z_]",'_',v+'_'+str(k)+'_modified')) if re.search("[^0-9a-zA-Z_]", v) else v for k,v in enumerate(columns)]
#   return clean_columns

# clean_columns = get_clean_column_names_list(df.columns)

# df = df.select(clean_columns)

# df.printSchema()
