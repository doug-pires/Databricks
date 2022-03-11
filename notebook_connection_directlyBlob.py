# Databricks notebook source
# MAGIC %md
# MAGIC ## Vamos testar as conexões com Azure Data Lake Storage Gen2

# COMMAND ----------

# Acesse ADLS Gen2 diretamente - Acessar o armazenamento de BLOBs do Azure usando a API dataframe
    # Para isso você precisa configurar credenciais antes de poder acessar dados no armazenamento de BLOBs do Azure, seja como credenciais de sessão ou CREDENCIAIS DO CLUSTER.
    # Documentação https://docs.microsoft.com/pt-br/azure/databricks/data/data-sources/azure/azure-storage

# Vamos informar aqui o caminho para o Storage Account ADLS Gen2
STORAGE_ACCOUNT = "lakehousevetraining"
ADLS_KEY = "wgi/bhayUdq4oFxK1YmROrOI/0L493zCTB4yn4K5qwfgFBxqrEYbFST6hQQMYXOeQGl8L2ocBDCuXaeCcOEaZQ==" # aqui vamos colocar depois dentro de uma Key Vault que pode vir do ESCOPO ou do ADF como um Azure KeyVault.
BRONZE_LAYER="bronze"
SILVER_LAYER="silver"

# Configuração para obter credencial da SESSAO - MAS PODEMOS OBTER uma CREDENCIAL pela CONFIGURAÇÃO DO CLUSTER.
spark.conf.set(
  "fs.azure.account.key."+STORAGE_ACCOUNT+".blob.core.windows.net",
  ADLS_KEY)


# COMMAND ----------

# Importar pacotes necessários
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,array,ArrayType,DateType,TimestampType
from pyspark.sql import functions as F

# COMMAND ----------

# Path para Bronze
BRONZE_PATH = "wasbs://"+BRONZE_LAYER+"@"+STORAGE_ACCOUNT+".blob.core.windows.net/"
SF_FOLDER = "sf-department"


# COMMAND ----------

# Definir o Schema para o CSV
fields_sf = [StructField('CallNumber', IntegerType(), True),
                     StructField('UnitID', StringType(), True),
                     StructField('IncidentNumber', IntegerType(), True),
                     StructField('CallType', StringType(), True),                  
                     StructField('CallDate', StringType(), True),      
                     StructField('WatchDate', StringType(), True),
                     StructField('CallFinalDisposition', StringType(), True),
                     StructField('AvailableDtTm', StringType(), True),
                     StructField('Address', StringType(), True),       
                     StructField('City', StringType(), True),       
                     StructField('Zipcode', IntegerType(), True),       
                     StructField('Battalion', StringType(), True),                 
                     StructField('StationArea', StringType(), True),       
                     StructField('Box', StringType(), True),       
                     StructField('OriginalPriority', StringType(), True),       
                     StructField('Priority', StringType(), True),       
                     StructField('FinalPriority', IntegerType(), True),       
                     StructField('ALSUnit', BooleanType(), True),       
                     StructField('CallTypeGroup', StringType(), True),
                     StructField('NumAlarms', IntegerType(), True),
                     StructField('UnitType', StringType(), True),
                     StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                     StructField('FirePreventionDistrict', StringType(), True),
                     StructField('SupervisorDistrict', StringType(), True),
                     StructField('Neighborhood', StringType(), True),
                     StructField('Location', StringType(), True),
                     StructField('RowID', StringType(), True),
                     StructField('Delay', FloatType(), True)]
schema_sf = StructType(fields = fields_sf )

# COMMAND ----------

# Ler o CSV e transformar em um DataFrame
spdf_firedepartment = spark.read.csv(BRONZE_PATH+SF_FOLDER,schema = schema_sf)


# COMMAND ----------

# Cache o Dataframe para Performance 
spdf_firedepartment.cache()

# COMMAND ----------

# Contar quantidade de Linhas
spdf_firedepartment.count()

# COMMAND ----------


