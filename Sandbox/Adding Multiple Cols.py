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
# MAGIC -- Create Table VehicleType
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

df_vehicle = spark.table("vehicle")
df_vehicletype = spark.table("vehicletype")

# COMMAND ----------

df_vehicle.schema

# COMMAND ----------

df_vehicle.show()

# COMMAND ----------

df_joined = df_vehicle.join(df_vehicletype, df_vehicle.VehicleTypeId == df_vehicletype.Id , "left").select("vehicle.*","vehicletype.CultureCode","vehicletype.Model_Body","vehicletype.Description")

# COMMAND ----------

df_joined.show()

# COMMAND ----------

from pyspark.sql import functions as F


def build_agg_method(cols_to_translate: list[str]):
    """
    Generate PySpark aggregation expressions for a list of columns.

    Args:
        cols_to_translate (list[str]): A list of column names for which you want to generate aggregation expressions.

    Returns:
        list: A list of PySpark Column expressions with alias names in the format "column_name_translated".
    """

    expr = []
    for col_name in cols_to_translate:
        expression = F.first(F.col(col_name)).alias(f"{col_name}_translated")
        expr.append(expression)
    return expr

def pivot_translations(
    df, pillar_cols: list, pivot_col: str, cols_translate: list[str]
):
    """
    Pivot and translate columns in a PySpark DataFrame.

    Args:
        df (DataFrame): The PySpark DataFrame to pivot and translate.
        pillar_cols (list): List of columns to use as pivot pillars.
        pivot_col (str): The column to pivot.
        cols_translate (list[str]): A list of column names to translate.

    Returns:
        DataFrame: A new DataFrame with columns pivoted and translated as specified.

    Example:
    >>> columns_to_translate = ["Description", "Model_Body"]
    >>> pivot_columns = ["Category"]
    >>> pivot_column = "Language"
    >>> translated_df = pivot_translations(df, pivot_columns, pivot_column, columns_to_translate)
    >>> # The result will be a DataFrame with the specified columns pivoted and translated.
    """
    
    build_expr = build_agg_method(cols_translate)
    pivot_df = df.groupBy(*pillar_cols).pivot(pivot_col).agg(*build_expr)
    return pivot_df


# COMMAND ----------

# Provide the Dataframe joined
cols = ["Id", "Vin"]
pivot_col = "CultureCode"
cols_translate = ["Description", "Model_Body"]
pivot_df = pivot_translations(
    df=df_joined,
    pillar_cols=cols,
    col_to_pivot=pivot_col,
    cols_to_translate=cols_translate,
)

# COMMAND ----------

pivot_df.show()

# COMMAND ----------


