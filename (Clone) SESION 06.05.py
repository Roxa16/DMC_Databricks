# Databricks notebook source
# DBTITLE 1,1era Opción
# Lectura de archivo
df = spark.read.csv(file_location + "/ventas.csv")
df.display()

# COMMAND ----------

# DBTITLE 1,1era Opción
# Dar formato adecuado al dataframe cargado desde blob storage
opciones = {"header": True, "delimiter": ",", "inferSchema":"true"}
df = spark.read.format("csv").options(**opciones).load(file_location + "/ventas.csv")

# COMMAND ----------

# DBTITLE 1,1era Opción
df.display()

# COMMAND ----------

# DBTITLE 1,1era Opción
# Apagar la configurarción
spark.conf.unset("fs.azure.account.key."+storage_account_name+".blob.core.windows.net")
