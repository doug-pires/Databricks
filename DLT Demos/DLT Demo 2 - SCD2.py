# Databricks notebook source
import dlt
from pyspark.sql.functions import col, expr

# COMMAND ----------

catalog_name = "catalog_demo"
schema = "cdc_data"
table_source = "tb_demo"
path_table = f"{catalog_name}.{schema}.{table_source}"
target_table = "target_demo_scd2"

# COMMAND ----------

# # Read from a delta table
# # This delta table could be enabled CDC
# @dlt.view
# def users():
#   df_stream =  spark.readStream.format("delta").table(path_table)
#   return df_stream


# COMMAND ----------

# Create a STREAMING TABLE ( APPEND-ONLY ) to keep our SCD Dimension
# dlt.create_streaming_table("target")

# COMMAND ----------


# dlt.apply_changes(
#   target = "target",
#   source = "users",
#   keys = ["userId"],
#   # This is the control of the start at and end at
#   # Important to HAVE ( It could be the UPDATED DATE )
#   sequence_by = col("sequenceNum"),
#   apply_as_deletes = expr("operation = 'DELETE'"),
#    # These columns will not be included in the TARGET table
#   except_column_list = ["operation", "sequenceNum"],
#   # Choose the type of the Slowly Changing Dimensions
#   stored_as_scd_type = "2",
#   track_history_except_column_list = ["city"]
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ---

# COMMAND ----------

@dlt.view
def users():
    df_stream =  spark.readStream.format("delta").table(path_table)
    return df_stream

# COMMAND ----------

# Create a STREAMING TABLE ( APPEND-ONLY ) to keep our SCD Dimension

dlt.create_streaming_table(target_table)

# COMMAND ----------

dlt.apply_changes(
  target = target_table,
  # Name from the View
  source = "users",
  # The natural Key of the table
  keys = ["userId"],
  # This is the control of the start at and end at
  # Important to HAVE ( It could be the UPDATED DATE )
  sequence_by = col("id"),
  apply_as_deletes = expr("operation = 'DELETE'"),
   # These columns will not be included in the TARGET table
  except_column_list = ["operation", "id"],
  # Choose the type of the Slowly Changing Dimensions
  stored_as_scd_type = "2",
  # track_history_except_column_list = ["city"]
)
