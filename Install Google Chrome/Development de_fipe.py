# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *
import json
from datetime import datetime

# COMMAND ----------

json_data_in_array =[
    {
        "reference_month": "janeiro de 2023",
        "fipe_code": "023016-2",
        "brand": "Nissan",
        "model": "NX 2000",
        "manufacturing_year_fuel": "1994 Gasolina",
        "authentication": "cbrk2tx0427p",
        "query_date": "sexta-feira, 15 de setembro de 2023 19:00",
        "average_price": "R$ 8.309,00"
    },
    {
        "reference_month": "fevereiro de 2023",
        "fipe_code": "023016-2",
        "brand": "Nissan",
        "model": "NX 2000",
        "manufacturing_year_fuel": "1994 Gasolina",
        "authentication": "cbqrm6tmkydp",
        "query_date": "sexta-feira, 15 de setembro de 2023 19:00",
        "average_price": "R$ 8.302,00"
    }
]

# COMMAND ----------

json_data ={
        "reference_month": "dezembro de 2055",
        "fipe_code": "023016-2",
        "brand": "Nissan",
        "model": "NX 2000",
        "manufacturing_year_fuel": "1994 Gasolina",
        "authentication": "cbrk2tx0427p",
        "query_date": "sexta-feira, 15 de setembro de 2023 19:00",
        "average_price": "R$ 8.309,00"
    }


# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir /dbfs/mnt/json_demo

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /dbfs/mnt/json_demo

# COMMAND ----------

path_json  = "/mnt/json_on_demand/"

# COMMAND ----------

dbutils.fs.ls(path_json)

# COMMAND ----------

json_formatted = json.dumps(json_data)
json_datetime = f"{path_json}data_json_{datetime.now().timestamp()}"
dbutils.fs.put(json_datetime,json_formatted)

# COMMAND ----------


json_formatted_array = json.dumps(json_data_in_array)
json_datetime = f"{path_json}data_json_{datetime.now().timestamp()}"
dbutils.fs.put(json_datetime,json_formatted_array)

# COMMAND ----------

dbutils.fs.ls(path_json)

# COMMAND ----------

df = spark.read.format("json").load(path_json).show()

# COMMAND ----------


