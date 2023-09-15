# Databricks notebook source
# MAGIC %pip install Faker

# COMMAND ----------

import json
from faker import Faker
import random as rd
import datetime
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %fs
# MAGIC ls 

# COMMAND ----------



# COMMAND ----------

# MAGIC %sh
# MAGIC rm /dbfs/mnt/ -rf

# COMMAND ----------

current_time = datetime.datetime.now()
  
time_stamp = current_time.timestamp()
print(time_stamp)

# COMMAND ----------

f = Faker(["de_DE","pt_BR"])

timestamp = datetime.datetime.timestamp
# Generate JSON Files
for _ in range(5):
    path_bronze = f"dbfs:/mnt/json_files/data.json_{_}"
    fake_name = f.name()
    fake_age = rd.randint(1,85)
    json_file = { "name":fake_name, "age":fake_age }
    print(json_file,"on",path)
    string_json = json.dumps(json_file,ensure_ascii=False)
    dbutils.fs.put(path_bronze,string_json,True)
    # print(json.dumps(json_file,ensure_ascii=False) )


# COMMAND ----------

f = Faker(["de_DE","pt_BR"])

current_time = datetime.datetime.now()
time_stamp = current_time.timestamp()

# Generate JSON Files
base_path = f"dbfs:/mnt/json_files/"
path_file = f"dbfs:/mnt/json_files/data_{time_stamp}.json"
fake_name = f.name()
fake_age = rd.randint(1,85)
json_file = { "name":fake_name, "age":fake_age }
print(json_file,"on",path_file)
string_json = json.dumps(json_file,ensure_ascii=False)
dbutils.fs.put(path_file,string_json,True)


# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/json_files -a

# COMMAND ----------

dbutils.fs.help()


# COMMAND ----------

dbutils.fs.ls("/mnt/json_files/")

# COMMAND ----------


dbutils.fs.rm("/mnt/json_files/",True)


# COMMAND ----------

schema = StructType(
    [
    StructField("name",StringType(),False),
    StructField("age",IntegerType(),True)
    ])

# COMMAND ----------

# Reding CONTINUASLY the folder 
# File Base
df_stream = (
    spark.readStream
    .format("json")
    .schema(schema)
    .load(base_path)
)

display(df_stream)

# COMMAND ----------

path_delta = f"dbfs:/mnt/delta/json_files"
check_point_delta = f"dbfs:/mnt/delta/checkpoint"

# COMMAND ----------


df_stream.writeStream.format("delta").outputMode("append").option("path", path_delta).option("checkpointLocation", check_point_delta).start().awaitTermination()

# COMMAND ----------

dbutils.fs.ls(path_delta)

# COMMAND ----------


dbutils.fs.rm(path_delta,True)



# COMMAND ----------


