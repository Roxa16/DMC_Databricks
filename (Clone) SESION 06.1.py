# Databricks notebook source
# DBTITLE 1,2da Opción: Blob Storage
# Segunda conexión con Blob Storage ("Firma de acceso compartido")
storage_account_name = "g5roxanazarate" 
storage_name = "archivosdmc"
storage_account_access_key = "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-04-04T05:38:22Z&st=2024-04-03T21:38:22Z&spr=https&sig=e6KrKifLlcVEqJcwjST25Td%2B22wgMV938ay90Leqt2E%3D"

spark.conf.set(f"fs.azure.sas.archivosdmc.g5roxanazarate.blob.core.windows.net",storage_account_access_key)

# COMMAND ----------

# DBTITLE 1,2da Opción
# Establecer conexión con contenedor 'archivosdmc' con llave A
dbutils.fs.ls("wasbs://archivosdmc@g5roxanazarate.blob.core.windows.net")

# COMMAND ----------

# DBTITLE 1,2da Opción
# Establecer conexión con contenedor 'contenedor2' con llave A
spark.conf.set(f"fs.azure.sas.contenedor2.g5roxanazarate.blob.core.windows.net",storage_account_access_key)
dbutils.fs.ls("wasbs://contenedor2@g5roxanazarate.blob.core.windows.net")

# COMMAND ----------

# DBTITLE 1,2da Opción
# Apagar la configurarción
spark.conf.unset("fs.azure.sas.archivosdmc.g5roxanazarate.blob.core.windows.net")
spark.conf.unset("fs.azure.sas.contenedor2.g5roxanazarate.blob.core.windows.net")
