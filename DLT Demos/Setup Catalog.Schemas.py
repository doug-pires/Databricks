# Databricks notebook source
# MAGIC %md
# MAGIC ## Create CATALOG

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS catalog_profile;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create three SCHEMAS called `bronze`, `silver` and `gold`

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG catalog_profile;
# MAGIC CREATE SCHEMA  IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA  IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA  IF NOT EXISTS gold;
