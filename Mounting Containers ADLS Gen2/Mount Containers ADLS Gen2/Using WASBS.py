# Databricks notebook source
# MAGIC %run ./Set-Widgets
# MAGIC

# COMMAND ----------

# Import basic libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

libraries = """
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F """

print("----------Libraries imported----------")
print(libraries)

# COMMAND ----------

STORAGE_ACCOUNT = dbutils.widgets.get("ADLS_ACCOUNT_NAME")
ADLS_KEY = "YOURACCESSKEY OR SAS" 
BRONZE_LAYER="bronze"
SILVER_LAYER="silver"
GOLD_LAYER="gold"

# Connect using WASBS Protocol
# spark.conf.set( "fs.azure.account.key."+STORAGE_ACCOUNT+".blob.core.windows.net", ADLS_KEY)

# Base Paths
BRONZE_PATH = "wasbs://"+BRONZE_LAYER+"@"+STORAGE_ACCOUNT+".blob.core.windows.net/"
SILVER_PATH = "wasbs://"+SILVER_LAYER+"@"+STORAGE_ACCOUNT+".blob.core.windows.net/"
GOLD_PATH = "wasbs://"+GOLD_LAYER+"@"+STORAGE_ACCOUNT+".blob.core.windows.net/"

# COMMAND ----------

print("----------Bronze Path----------")
print(f"Bronze Path: {BRONZE_PATH}")
print("----------Silver Path----------")
print(f"Silver Path: {SILVER_PATH}")
print("----------Gold Path----------")
print(f"Gold Path: {GOLD_PATH}")

# COMMAND ----------


