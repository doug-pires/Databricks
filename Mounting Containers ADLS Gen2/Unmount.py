# Databricks notebook source
# MAGIC %run ./Set-Widgets

# COMMAND ----------

# Define the Variables used for creating connection strings
# Define the containers

ADLS_ACCOUNT_NAME = dbutils.widgets.get("ADLS_ACCOUNT_NAME")
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

# Unmount my containers

for container in CONTAINERS:
    mountPoint =  f"/mnt/{container}"
    if any ( mount.mountPoint == mountPoint for mount in dbutils.fs.mounts() ):
        # Unmount my Containers
        dbutils.fs.unmount( mountPoint )
        print(f"Unmounted successfully the container {container}")

# COMMAND ----------


