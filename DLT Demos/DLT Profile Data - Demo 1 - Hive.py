# Databricks notebook source
# MAGIC %md
# MAGIC # Create notebook to work with the Profile Data.

# COMMAND ----------

landing_files = spark.conf.get("path_landing_files")
# "/mnt/landing"
# /mnt/datalake
mnt_point = "/mnt/datalake"
path_profile_data = "profile_data"
bronze_path = f"{mnt_point}/{path_profile_data}/bronze"
silver_path = f"{mnt_point}/{path_profile_data}/silver"
gold_path = f"{mnt_point}/{path_profile_data}/gold"

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Functions to help us
# MAGIC     

# COMMAND ----------

import dlt
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column 

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

def group_by(df: DataFrame, col: str):
    """
    Group a DataFrame by a specified column and count the occurrences of each group.

    Parameters:
    df (DataFrame): The input DataFrame to be grouped.
    col (str): The name of the column by which to group the DataFrame.

    Returns:
    DataFrame: A new DataFrame with the groups and their corresponding counts.
    """
    df_grouped = df.groupBy(col).count()
    return df_grouped


# COMMAND ----------

# Read the datasource containing JSON files
profile_raw_metadata = {
    "name": "profile_raw",
    "comment": "The raw json profile file, ingested from /mnt/landing.",
    "path":bronze_path + "/profile_raw"
}
@dlt.table(
  **profile_raw_metadata
)
def profile_raw():
    df = spark.read.format("json").load(landing_files)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC # Transform the Data from table BRONZE and load into SILVER
# MAGIC ## Apply expectations on Data
# MAGIC 1. ssn must be filled --> Warn
# MAGIC 2. Blood Group must be valid A+ B+ AB+ O+ A- B- AB- O- ---> Drop
# MAGIC 3. Birthdate must not be empty ---> Allow
# MAGIC
# MAGIC > Be careful to no create conflict expectations

# COMMAND ----------


# Expectations

expectations_ssn = {"ssn_must_not_be_empty":"ssn IS NOT NULL"}
expectation_blood_group = {"blood_group_valid": "blood_group IN ('A+', 'B+', 'AB+', 'O+', 'A-', 'B-', 'AB-','O-' )"}

# Metadata
profile_prepared_metadata = {
    "name": "profile_prepared",
    "comment": "Profile data cleaned and prepared for analysis.",
    "path":silver_path + "/profile_prepared"
}
@dlt.table(
    **profile_prepared_metadata
)
@dlt.expect_all(expectations_ssn)
@dlt.expect_all_or_drop(expectation_blood_group)
def profile_prepared():
    df = dlt.read("profile_raw")
    cols_drop = ["residence", "website", "current_location"]
    cols_cast = {"birthdate": "date"}
    df_selected = drop_columns(df, cols_drop)
    df_casted = cast_cols(df=df_selected, cols_to_cast=cols_cast)
    return df_casted

# COMMAND ----------

# "name": "profile_grouped_by_blood_group",
path_grouped_by_blood_group = f"{gold_path}/tb_grouped_by_blood_group"
# Metadata
profile_grouped_by_blood_metadata = {
    
    "comment": "Table grouped by Blood Group to identify the blood types available",
    "path": path_grouped_by_blood_group
}


@dlt.table(**profile_grouped_by_blood_metadata)
def group_by_blood_group():

    df = dlt.read("profile_prepared")
    df_grouped = group_by(df, "blood_group")
    return df_grouped

# COMMAND ----------

# MAGIC %md
# MAGIC ## With that we can pass parameters as `json` or get from the pipeline configuration using `spark.conf.get("parameter.name")`
# MAGIC ####   name="<name>",
# MAGIC ####   comment="<comment>",
# MAGIC ####   spark_conf={"<key>" : "<value", "<key" : "<value>"},
# MAGIC ####   table_properties={"<key>" : "<value>", "<key>" : "<value>"},
# MAGIC ####   path="<storage-location-path>",
# MAGIC ####   partition_cols=["<partition-column>", "<partition-column>"],
# MAGIC ####   schema="schema-definition",
# MAGIC ####   temporary=False

# COMMAND ----------

#  "name": "table_grouped_by_sex",
path_grouped_by_sex = f"{gold_path}/tb_grouped_by_sex"
sex_grouped_metadata = {
   
    "comment": "Table grouped by Sex",
    "path": path_grouped_by_sex
}

# COMMAND ----------


# Create a table grouped by sex
@dlt.table(**sex_grouped_metadata)
def group_by_sex():

    df = dlt.read("profile_prepared")
    df_grouped = group_by(df, "sex")
    return df_grouped
