# Databricks notebook source
# MAGIC %pip install dbdemos

# COMMAND ----------

import dbdemos

# COMMAND ----------

dbdemos.help()

# COMMAND ----------

dbdemos.list_demos()

# COMMAND ----------

dbdemos.install('dlt-loans',cloud="Azure")

# COMMAND ----------


