# Databricks notebook source
# MAGIC %md
# MAGIC # Default Mode Directory Listing
# MAGIC ## 	File arriving RANDOMLY not too frequent, not once a day

# COMMAND ----------

# Add JSON Fake File

# COMMAND ----------

# Paths
mnt_bronze = "/mnt/bronze"
mnt_silver = "/mnt/silver"
mnt_gold = "/mnt/gold"
mnt_input_auto_loader = f"{mnt_bronze}/input_autoloader"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Inference
# MAGIC ### We need to provide the path to Keep the *Inferred Schema*

# COMMAND ----------

options_auto_loader = {
    "cloudFiles.format": "json",
    "cloudFiles.schemaLocation": mnt_input_auto_loader + "/inferred",
    "cloudFiles.inferColumnTypes":"true",
    "cloudFiles.schemaEvolutionMode":"addNewColumns", 
    # "rescue", "failOnNewColumns" , 
    # "rescuedDataColumn" : "_rescued_data"
    "cloudFiles.schemaHints":"address string, website array"}


# COMMAND ----------

df_inferred_schema = (
spark.readStream.format("cloudFiles")
.option("cloudFiles.format", "json")
.option("cloudFiles.schemaLocation",mnt_input_auto_loader + "/inferred" )
.option("cloudFiles.inferColumnTypes","true")
.load(mnt_input_auto_loader)
)


# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Providing Schema
# MAGIC ### We need to provide the path to Keep the *Inferred Schema*

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

fields = [
    StructField("address",StringType(),True),
    StructField("birthdate",DateType(),True),
    StructField("blood_group",StringType(),True),
    StructField("company",StringType(),True),
    StructField("current_location",ArrayType(StringType()),True),
    StructField("job",StringType(),True),
    StructField("mail",StringType(),True),
    StructField("name",StringType(),True),
    StructField("residence",StringType(),True),
    StructField("sex",StringType(),True),
    StructField("ssn",StringType(),False),
    StructField("username",StringType(),True),
    StructField("website",ArrayType(StringType()),True)
]

schema = StructType(fields = fields )
