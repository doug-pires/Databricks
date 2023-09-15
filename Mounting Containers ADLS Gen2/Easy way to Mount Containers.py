# Databricks notebook source
# Define the Variables used for creating connection strings
# Define the containers

ADLS_ACCOUNT_NAME = "stdbx92"
CONTAINERS = ["bronze","silver","gold"]
# Application ( client ID )
applicationID = dbutils.secrets.get(scope = "kv-ADLS",key="kv-clientID-ADLSGen2")
# Application Client Secret Key ( client ID )
authenticationKey = dbutils.secrets.get(scope = "kv-ADLS",key="kv-Secret-Application")
# Directory tenant ID
DirectoryTenantID = dbutils.secrets.get(scope = "kv-ADLS",key="kv-TenantID-ADLSGen2")
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

# MAGIC %sh
# MAGIC ls /dbfs/mnt/

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# Unmount my containers

for container in CONTAINERS:
    mountPoint =  f"/mnt/{container}"
    if any ( mount.mountPoint == mountPoint for mount in dbutils.fs.mounts() ):
        # Unmount my Containers
        dbutils.fs.unmount( mountPoint )
        print(f"Unmounted successfully the container {container}")

# COMMAND ----------


