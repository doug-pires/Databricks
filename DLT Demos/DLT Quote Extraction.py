# Databricks notebook source
from quotes_dbx.request_quote import extract_quote, pick_random_category,save_to_storage

# COMMAND ----------

quote = extract_quote()
print(quote)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/landing"))

# COMMAND ----------

dbutils.fs.rm("/mnt//json_demo_1694815708.968057",True)

# COMMAND ----------

path_landing_quotes_dbx = "/mnt/landing/quotes_dbx"

# COMMAND ----------

save_to_storage(path_dbfs=path_landing_quotes_dbx,data=quote)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM JSON.`/mnt/landing/quotes_dbx/`

# COMMAND ----------


