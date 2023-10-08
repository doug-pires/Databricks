# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

import json
import random as rd
import datetime
from faker import Faker

# COMMAND ----------

# MAGIC %sh
# MAGIC mkdir /mnt/landing
# MAGIC

# COMMAND ----------

# Paths
mnt_container = "/mnt/landing"

# COMMAND ----------

def create_json_profile():

    time_stamp = datetime.datetime.now().timestamp()
    bronze_data_profile = f"{mnt_container}/data_{time_stamp}.json"
    
    # Generate JSON File
    faker = Faker(["de_DE","pt_BR","fr_FR","pt_PT"])
    dict_file = faker.profile()
    dict_file["birthdate"] = dict_file["birthdate"].isoformat()
    dict_file["current_location"] = list(dict_file["current_location"])
    dict_file["current_location"][0] = str(dict_file["current_location"][0])
    dict_file["current_location"][1] = str(dict_file["current_location"][1])
    string_json = json.dumps(dict_file, ensure_ascii=False)
    
    return dbutils.fs.put(bronze_data_profile, string_json, True)

# COMMAND ----------

# Create disruptive JSON
def create_error_json_profile():

    time_stamp = datetime.datetime.now().timestamp()
    bronze_data_profile = f"{mnt_container}/data_{time_stamp}.json"
    
    # Generate JSON File
    faker = Faker(["de_DE","pt_BR","fr_FR","pt_BR"])
    dict_file = faker.profile()
    dict_file["birthdate"] = None
    dict_file["ssn"] = None
    dict_file["blood_group"] = rd.choice([None,"Z","I"])
    dict_file["current_location"] = list(dict_file["current_location"])
    dict_file["current_location"][0] = str(dict_file["current_location"][0])
    dict_file["current_location"][1] = str(dict_file["current_location"][1])
    string_json = json.dumps(dict_file, ensure_ascii=False)
    
    return dbutils.fs.put(bronze_data_profile, string_json, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create  JSON files

# COMMAND ----------

number_files = 1_000
for _ in range(number_files):
    create_json_profile()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create JSON files with ERRORS

# COMMAND ----------

number_files_w_error = 200
for _ in range(number_files_w_error):
    create_error_json_profile()

# COMMAND ----------

# Check files added there
display(dbutils.fs.ls(mnt_container))

# COMMAND ----------

# MAGIC %sh
# MAGIC # rm -rf /mnt/landing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create JSON files with same ssn to check how they track version
# MAGIC

# COMMAND ----------

profile_changed = {
    "address": "Street X",
    "birthdate": "1992-08-29",
    "blood_group": "A+",
    "company": "KI Performance",
    "current_location": "[]",
    "job": "Data Analyst",
    "mail": "d.martins@gmail.com",
    "name": "Douglas Pires",
    "residence": "Street X - 10",
    "sex": "M",
    "ssn": "",
    "username": "dpires",
    "website": "[]"
}

# COMMAND ----------

def insert_json_changed(dict_file):

    time_stamp = datetime.datetime.now().timestamp()
    bronze_data_profile = f"{mnt_container}/data_{time_stamp}.json"
    string_json = json.dumps(dict_file, ensure_ascii=False)
    
    return dbutils.fs.put(bronze_data_profile, string_json, True)
