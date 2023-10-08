# Databricks notebook source
# MAGIC %md
# MAGIC # Run to get path variables
# MAGIC

# COMMAND ----------

json_path = "/mnt/landing"
catalog_name = "catalog_demo"
bronze_path = f"{catalog_name}.bronze"
silver_path = f"{catalog_name}.silver"
gold_path = f"{catalog_name}.gold"

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column

# COMMAND ----------

print(json_path)

# COMMAND ----------

df = spark.read.format("json").load(json_path)

# COMMAND ----------

display(df)

# COMMAND ----------

def drop_columns(df:DataFrame, cols_to_drop:list[str]) -> DataFrame:
    """
    Drops specified columns from a DataFrame.
    
    Args:
        df (DataFrame): The input DataFrame.
        cols_to_drop (list[str]): List of column names to be dropped.
        
    Returns:
        DataFrame: A new DataFrame with specified columns removed.
    """
    df = df.drop(*cols_to_drop)
    return df

# COMMAND ----------



# COMMAND ----------

cols_drop = ["residence","website","current_location"]
df_profile_selected = drop_columns(df,cols_drop)
display(df_profile_selected)

# COMMAND ----------

def cast_cols(df: DataFrame, cols_to_cast: dict[str, str]) -> DataFrame:
    """
    Casts specified columns to the provided data types in a DataFrame.
    
    Args:
        df (DataFrame): The input DataFrame.
        cols_to_cast (dict[str, str]): A dictionary where keys are column names
            and values are the target data types for casting.
        
    Returns:
        DataFrame: A new DataFrame with specified columns cast to the target data types.
    """
    keys = list(cols_to_cast.keys())

    for key in keys:
        df = df.withColumn(key, F.col(key).cast(cols_to_cast.get(key)))
        return df

# COMMAND ----------

cols_cast= {"birthdate":"date"}
print(list(cols_cast.keys()))

# COMMAND ----------

df_casted = cast_cols(df=df_profile_selected, cols_to_cast=cols_cast)
display(df_casted )

# COMMAND ----------

def group_by(df: DataFrame, col: str):
    df_grouped = df.groupBy(col).count()
    return df_grouped

# COMMAND ----------

df_grouped_blood = group_by(df_casted,"blood_group")
display(df_grouped_blood)

# COMMAND ----------


