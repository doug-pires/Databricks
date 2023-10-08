# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------


fields = [
    StructField("name",StringType(),True),
    StructField("age",IntegerType(),True),
    StructField("jobs",ArrayType(StringType()),True) ]
schema = StructType(fields)

info = [ 
  ("Douglas", 29,["Data Analyst","Data Engineer"])
  ,("Daniel", 36,["VideoMaker","Editor"] )
]
df = spark.createDataFrame(data=info,schema=schema)
display(df)

# COMMAND ----------

# Use Expressions inside method select

df_upper = df.select("name","age","jobs",F.expr("upper( name ) as name_upper"))
display(df_upper)

# COMMAND ----------

# Use SelectExpr instead method select
df_upper_2 = df.selectExpr("name")
display(df_upper_2)

# COMMAND ----------

# Casting column using select

df_casted = df.select("name",F.col("age").cast("float").alias("age_to_float"))
display(df_casted)

# COMMAND ----------

# creating another column using withColumn and casting it
df_withcolumn = df.withColumn("age_to_float",F.col("age").cast("float") )
display(df_withcolumn)

# COMMAND ----------

# Using Filter and Where with col
df_filtered = df.filter(F.col("name") == "Douglas")
display(df_filtered)

# COMMAND ----------

# Using Filter and Where withou  col
only_daniel = "name == 'Daniel'"
df_filtered_2 = df.filter("name == 'Daniel'")
display(df_filtered_2)

# COMMAND ----------

my_filter = "name == 'Daniel' OR age < 20"
df_filtered_3 = df.filter(my_filter)
display(df_filtered_3)

# COMMAND ----------

# Filter using && or || but AVOID this way
df_filtered_4 = df.filter( ( df.name == "Douglas" ) | ( df.age > 20 ) )
display(df_filtered_4)

# COMMAND ----------

df_with_array = df.withColumn("array_jobs",  F.col("jobs").cast(ArrayType(StringType(),True) ) )
display(df_with_array)

# COMMAND ----------

# Working with the Column Array
# df_size = df.selectExpr("name","age","size(jobs) as qty_jobs")
df_size = df.select("name","age",F.size(F.col("jobs")).alias("qty_jobs"))
display(df_size)

# COMMAND ----------

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

path_json  = "/mnt/json_demo/"

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

df = spark.read.format("json").load("/mnt/json_demo/").show()

# COMMAND ----------


