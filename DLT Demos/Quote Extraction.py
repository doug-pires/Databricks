# Databricks notebook source
from quotes_dbx.request_quote import extract_quote, pick_random_category,save_to_storage

# COMMAND ----------

quote = extract_quote()
print(quote)

# COMMAND ----------

use_case = "dbx"
path_landing_quotes_dbx = f"/mnt/landing/quotes_{use_case}"
path_bronze_quotes = f"/mnt/{use_case}/bronze"
path_schema_autoloader = f"/mnt/{use_case}"
display(dbutils.fs.ls(path_schema_autoloader))

# COMMAND ----------

# dbutils.fs.rm("/mnt/landing/quotes_dbx",True)

# COMMAND ----------

path_landing_quotes_dbx = "/mnt/landing/quotes_dbx"

# COMMAND ----------

save_to_storage(path_dbfs=path_landing_quotes_dbx,data=quote)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW vw_quotes
# MAGIC USING json
# MAGIC OPTIONS (path="/mnt/landing/quotes_dbx/",multiline=true)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vw_quotes

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP CATALOG __databricks_internal CASCADE

# COMMAND ----------


