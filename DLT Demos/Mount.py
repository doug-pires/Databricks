# Databricks notebook source
dbutils.widgets.text("scope", "kv-ADLS")
dbutils.widgets.text("ADLS_ACCOUNT_NAME","","ADLS Account Name")

# COMMAND ----------

ADLS_ACCOUNT_NAME = dbutils.widgets.get("ADLS_ACCOUNT_NAME")
scope_name = dbutils.widgets.get("scope")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC # Mounting 

# COMMAND ----------



if any(mount.mountPoint == "/mnt/datalake" for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount("/mnt/datalake")

conf = {
    "fs.azure.account.key.scalingstrategy.blob.core.windows.net": (
        dbutils.secrets.get(scope="dbento", key="AZURE_KEY")
    )
}


dbutils.fs.mount(
    source="wasbs://x-batch-challenge@scalingstrategy.blob.core.windows.net",
    mount_point="/mnt/datalake",
    extra_configs=conf,
)

# COMMAND ----------




# COMMAND ----------

# Define the Variables used for creating connection strings
# Define the containers

CONTAINERS = ["bronze","silver","gold"]
# Application ( client ID )
applicationID = dbutils.secrets.get(scope = scope_name,key="kv-clientID-ADLSGen2")
# Application Client Secret Key ( client ID )
authenticationKey = dbutils.secrets.get(scope = scope_name,key="kv-Secret-Application")
# Directory tenant ID
DirectoryTenantID = dbutils.secrets.get(scope = scope_name,key="kv-TenantID-ADLSGen2")
# endpoint
endpoint = f"https://login.microsoftonline.com/{DirectoryTenantID}/oauth2/token"

# COMMAND ----------

# Get configs as Dict

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": applicationID,
          "fs.azure.account.oauth2.client.secret": authenticationKey,
          "fs.azure.account.oauth2.client.endpoint":  endpoint  }

# COMMAND ----------

# The containers MUST be already created on the ADLS Gen2

for container in CONTAINERS:
    mountPoint =  f"/mnt/{container}"
    if not any ( mount.mountPoint == mountPoint for mount in dbutils.fs.mounts() ):
        # Mount my Containers
        dbutils.fs.mount(
        source = f"abfss://{container}@{ADLS_ACCOUNT_NAME}.dfs.core.windows.net/",
        mount_point = mountPoint,
        extra_configs = configs)
        print(f"Mounted successfully the container {container}")
    else:
        print(f"Containers already mounted successfully on {mountPoint}")

# COMMAND ----------

# Set the Location to save the Delta Table
mnt_bronze = "/mnt/bronze"
mnt_silver = "/mnt/silver"
mnt_gold = "/mnt/gold"
print("----------Bronze Layer----------")
print(f"Mounted Bronze layer on path: mnt_bronze = {mnt_bronze}")
print("----------Silver Layer----------")
print(f"Mounted Silver layer on path: mnt_silver = {mnt_silver}")
print("----------Gold Layer----------")
print(f"Mounted Gold layer on path: mount_gold = {mnt_gold}")
