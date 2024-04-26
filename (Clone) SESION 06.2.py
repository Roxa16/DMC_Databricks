# Databricks notebook source
# DBTITLE 1,3ra Opcion
# Tercera opcion de conexion con Blob Storage ("Generar SAS")
storage_account_access_key = "sp=racwdli&st=2024-04-03T21:54:20Z&se=2024-04-04T05:54:20Z&spr=https&sv=2022-11-02&sr=c&sig=zAEPUv88fh7VXmSk8Mp3cjTgJfMQ5%2FJZIBfUi6TJybE%3D"
spark.conf.set(f"fs.azure.sas.archivosdmc.g5roxanazarate.blob.core.windows.net",storage_account_access_key)

# COMMAND ----------

# Establecer conexión con contenedor 'archivosdmc' con llave SAS A
dbutils.fs.ls("wasbs://archivosdmc@g5roxanazarate.blob.core.windows.net")

# COMMAND ----------

# No seteado para "contenedor2". Se necesita llave SAS B
spark.conf.set(f"fs.azure.sas.contenedor2.g5roxanazarate.blob.core.windows.net",storage_account_access_key)
dbutils.fs.ls("wasbs://contenedor2@g5roxanazarate.blob.core.windows.net")

# COMMAND ----------

# Leemos el archivo
df_nuevo = spark.read.csv("wasbs://archivosdmc@g5roxanazarate.blob.core.windows.net/Ventas tienda por departamento.csv")
df_nuevo.display()

# COMMAND ----------

# Dar formato al dataframe
file_location ="wasbs://archivosdmc@g5roxanazarate.blob.core.windows.net"
df_nuevo = spark.read.option("header", True).csv(file_location + "/Ventas tienda por departamento.csv",sep=";")

# COMMAND ----------

df_nuevo.display()

# COMMAND ----------

# Escribir sobre el contenedor
df_nuevo.write.mode('overwrite').format('parquet').save('wasbs://archivosdmc@g5roxanazarate.blob.core.windows.net/OutputDMC')

# COMMAND ----------

df_2 = spark.read.parquet("wasbs://archivosdmc@g5roxanazarate.blob.core.windows.net/OutputDMC")
df_2.display()

# COMMAND ----------

type(df_2)
