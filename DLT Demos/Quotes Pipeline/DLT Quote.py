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

options_quotes_df = {
    "format": "cloudFiles",
    "cloudFiles.format": "json",
    # "cloudFiles.schemaLocation": path_schema_autoloader,
    "cloudFiles.schemaEvolutionMode": "addNewColumns",
    "InferSchema": "true",
    "cloudFiles.inferColumnTypes": "true",
    "multiLine": "true",
}

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def add_metadata_cols(df: DataFrame) -> DataFrame:
    """
    Adds metadata columns to a DataFrame.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: A new DataFrame with additional metadata columns.

    Metadata Columns Added:
        - 'ingestion_datetime': Contains the file modification time from the metadata.
        - 'file_name': Contains the file name from the metadata.
    """
    df = df.withColumn("ingestion_datetime", col("_metadata.file_modification_time")) \
           .withColumn("file_name", col("_metadata.file_name"))

    return df

# COMMAND ----------

@dlt.table(comment="Ingestion Quote data to Delta Table Bronze")
def bronze_table_quotes():
    df = spark.readStream.format("cloudFiles").options(**options_quotes_df).load(path_landing_quotes_dbx)
    df_quotes = df.transform(add_metadata_cols)
    return df_quotes
