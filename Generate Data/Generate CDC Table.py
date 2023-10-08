# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS catalog_demo;
# MAGIC CREATE SCHEMA IF NOT EXISTS  catalog_demo.cdc_data

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG catalog_demo;
# MAGIC CREATE SCHEMA IF NOT EXISTS cdc_data;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS
# MAGIC   catalog_demo.cdc_data.users
# MAGIC AS SELECT
# MAGIC   col1 AS userId,
# MAGIC   col2 AS name,
# MAGIC   col3 AS city,
# MAGIC   col4 AS operation,
# MAGIC   col5 AS sequenceNum
# MAGIC FROM (
# MAGIC   VALUES
# MAGIC   -- Initial load.
# MAGIC   (124, "Raul",     "Oaxaca",      "INSERT", 1),
# MAGIC   (123, "Isabel",   "Monterrey",   "INSERT", 1),
# MAGIC

# COMMAND ----------

  (124, "Raul",     "Oaxaca",      "INSERT", 1),
  (123, "Isabel",   "Monterrey",   "INSERT", 1),

# COMMAND ----------

# MAGIC   %sql
# MAGIC   INSERT INTO cdc_data.users 
# MAGIC   (userId,name,city,operation,sequenceNum)
# MAGIC   VALUES 
# MAGIC   -- New users.
# MAGIC   (125, "Mercedes", "Tijuana",     "INSERT", 2),
# MAGIC   (126, "Lily",     "Cancun",      "INSERT", 2);

# COMMAND ----------

# MAGIC   %sql
# MAGIC   
# MAGIC   INSERT INTO cdc_data.users 
# MAGIC   (userId,name,city,operation,sequenceNum)
# MAGIC   VALUES 
# MAGIC   -- Isabel is removed from the system and Mercedes moved to Guadalajara.
# MAGIC   (123, null,       null,          "DELETE", 6),
# MAGIC   (125, "Mercedes", "Guadalajara", "UPDATE", 6)

# COMMAND ----------

# MAGIC  %sql
# MAGIC  SELECT * FROM cdc_data.users 

# COMMAND ----------

# MAGIC   %sql
# MAGIC   
# MAGIC   INSERT INTO cdc_data.users 
# MAGIC   (userId,name,city,operation,sequenceNum)
# MAGIC   VALUES 
# MAGIC   -- This batch of updates arrived out of order. The above batch at sequenceNum 5 will be the final state.
# MAGIC   (125, "Mercedes", "Mexicali",    "UPDATE", 5),
# MAGIC   (123, "Isabel",   "Chihuahua",   "UPDATE", 5)
# MAGIC

# COMMAND ----------

# MAGIC  %sql
# MAGIC   -- Uncomment to test TRUNCATE.
# MAGIC   -- ,(null, null,      null,          "TRUNCATE", 3)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP SCHEMA catalog_demo.db_profile CASCADE

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ---

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG catalog_demo;
# MAGIC CREATE SCHEMA IF NOT EXISTS cdc_data;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS
# MAGIC   catalog_demo.cdc_data.tb_demo
# MAGIC  (
# MAGIC     id BIGINT GENERATED ALWAYS AS IDENTITY
# MAGIC     ,userId STRING
# MAGIC     ,name STRING
# MAGIC     ,city STRING
# MAGIC     ,operation STRING
# MAGIC
# MAGIC   )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO
# MAGIC   catalog_demo.cdc_data.tb_demo (userId, name, city, operation) (
# MAGIC     VALUES
# MAGIC       -- Initial load.
# MAGIC       (
# MAGIC         124,
# MAGIC         "Douglas",
# MAGIC         "Sao Paulo",
# MAGIC         "INSERT"
# MAGIC       ),
# MAGIC       (
# MAGIC         123,
# MAGIC         "Maria",
# MAGIC         "Parana",
# MAGIC         "INSERT"
# MAGIC       )
# MAGIC   );

# COMMAND ----------

# MAGIC  %sql
# MAGIC  SELECT * FROM catalog_demo.cdc_data.tb_demo

# COMMAND ----------

# MAGIC   %sql
# MAGIC   INSERT INTO catalog_demo.cdc_data.tb_demo 
# MAGIC   (userId,name,city,operation)
# MAGIC   VALUES 
# MAGIC   -- New users.
# MAGIC   (125, "Cesar", "Tijuana",     "INSERT"),
# MAGIC   (126, "Lily",     "Cancun",      "INSERT");

# COMMAND ----------

# MAGIC   %sql
# MAGIC   INSERT INTO catalog_demo.cdc_data.tb_demo 
# MAGIC   (userId,name,city,operation )
# MAGIC   VALUES 
# MAGIC   -- Update City in 124.
# MAGIC   (124, "Douglas", "Entroncamento",     "UPDATE")

# COMMAND ----------

# CDC Tracks all the LOG from the database or table
# Each transaction, CDC catches it

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM catalog_demo.cdc_data.tb_demo

# COMMAND ----------

# MAGIC   %sql
# MAGIC   INSERT INTO catalog_demo.cdc_data.tb_demo 
# MAGIC   (userId,name,city,operation )
# MAGIC   VALUES 
# MAGIC   -- Update City in 124.
# MAGIC   (123, "Maria", "Brasilia",  "UPDATE")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE catalog_demo.cdc_data.target_demo_scd2

# COMMAND ----------


