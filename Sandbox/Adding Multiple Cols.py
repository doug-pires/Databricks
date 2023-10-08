# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG catalog_demo;
# MAGIC CREATE SCHEMA IF NOT EXISTS catalog_demo.mb_rent;
# MAGIC USE SCHEMA mb_rent;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Table Vehicle
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS vehicle (
# MAGIC   Id BIGINT NOT NULL PRIMARY KEY,
# MAGIC   VehicleTypeId BIGINT,
# MAGIC   Vin STRING,
# MAGIC   CONSTRAINT fk_vehicletype FOREIGN KEY(VehicleTypeId) REFERENCES vehicletype(Id)
# MAGIC )
# MAGIC COMMENT 'this is demo table'
# MAGIC TBLPROPERTIES ('user_test'='Douglas Pires', 'tag'='dev');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Table Vehicle
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS vehicletype (
# MAGIC   Id BIGINT NOT NULL PRIMARY KEY 
# MAGIC   COMMENT "Comment in a Column, my PK"
# MAGIC   ,CultureCode STRING
# MAGIC   , Description STRING
# MAGIC   , Model_Body STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO vehicle (Id,VehicleTypeId,Vin) 
# MAGIC VALUES ( 10,20,'DD-XX' )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO vehicletype (Id,CultureCode,Description,Model_Body) 
# MAGIC VALUES 
# MAGIC ( 20,'en-GB','A description in English', 'Model and Body' ),
# MAGIC ( 20,'de-DE','Eine Beschreibung auf Englisch', 'Modell und Aufbau' ),
# MAGIC (20, 'fr-FR','Une description en anglais', 'Modèle et construction'),
# MAGIC
# MAGIC -- It does not exist in my vehicle table
# MAGIC (30, 'pt-BR','Uma descrição em portugues', 'Modelo e Marca'); 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vehicle

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vehicletype

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vehicle V LEFT JOIN vehicletype VT ON ( V.VehicleTypeId = VT.Id )

# COMMAND ----------

sql_cmd_distinct_culture_code = """SELECT DISTINCT ( CultureCode ) FROM vehicletype"""

get_list_of_culture_code = spark.sql(sql_cmd_distinct_culture_code)
print(type(get_list_of_culture_code))

# Extract culture codes into a list
culture_codes = [row.CultureCode for row in get_list_of_culture_code.collect()]

# Print the resulting list of culture codes
print(culture_codes)

# COMMAND ----------

for each_culture_code in  culture_codes:
    sql_dyn_cmd = f""" SELECT * FROM vehicle V LEFT JOIN vehicletype VT ON ( V.VehicleTypeId = VT.Id ) WHERE VT.CultureCode = '{each_culture_code}' """
    spark.sql(sql_dyn_cmd).show()

# COMMAND ----------

for each_culture_code in  culture_codes:
    sql_dyn_cmd = f""" SELECT * FROM vehicle V LEFT JOIN vehicletype VT ON ( V.VehicleTypeId = VT.Id ) WHERE VT.CultureCode = '{each_culture_code}' """
    print(sql_dyn_cmd)

# COMMAND ----------

df_vehicle = spark.table("vehicle")
df_vehicletype = spark.table("vehicletype")

# COMMAND ----------

for each_culture_code in  culture_codes:
    cmd = f"translation_{each_culture_code}"
    df = df_vehicle.join(df_vehicletype, df_vehicle.VehicleTypeId == df_vehicletype.Id , "left")

df.show()

# COMMAND ----------

df_test = df_vehicle.join(df_vehicletype, df_vehicle.VehicleTypeId == df_vehicletype.Id, "left").select("vehicle.Id","vehicle.Vin","vehicletype.CultureCode","vehicletype.Description","vehicletype.Model_Body")

# COMMAND ----------

df_test.show()

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# Pivot the DataFrame
pivot_df = df_test.groupBy("Id", "Vin" ).pivot("CultureCode").agg( F.col("Description"), F.col("Model_Body") ) 

# COMMAND ----------


