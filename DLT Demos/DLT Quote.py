# Databricks notebook source
import dlt
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column

# COMMAND ----------

use_case = "dbx"
path_landing_quotes_dbx = f"/mnt/landing/quotes_{use_case}"
path_bronze_quotes = f"/mnt/{use_case}/bronze"
path_schema_autoloader = f"/mnt/{use_case}"

# COMMAND ----------

options_quotes_df =  {
    "format": "cloudFiles",
    "cloudFiles.format": "json",
    "cloudFiles.schemaLocation": path_schema_autoloader,
    "multiLine": "true" 
}

# COMMAND ----------

def add_metadata_cols(df: DataFrame) -> DataFrame:
    # Ingestion Datetime
    # File Name
    df = df.withColumn(
        "ingestion_datetime", F.col("_metadata.file_modification_time")
    ).withColumn("file_name", F.col("_metadata.file_name"))

    return df

# COMMAND ----------

@dlt.table(comment="Ingestion Quote data to Delta Table Bronze")
def bronze_table_quotes():
    df = spark.readStream.format("cloudFiles").options(**options_quotes_df).load(path_landing_quotes_dbx)
    df_quotes = df.transform(add_metadata_cols)
    return df_quotes
