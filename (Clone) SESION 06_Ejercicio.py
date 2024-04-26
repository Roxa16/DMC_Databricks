# Databricks notebook source
# MAGIC %md
# MAGIC Dado el archivo SERVICIOS_USADOS.csv
# MAGIC 1. Cree un container nuevo (En blob storage) y deje dentro de la misma el archivo proporcionado.
# MAGIC 2. Cargue el archivo leyendo directamente desde blob storage a un df y estructure correctamente.
# MAGIC 3. Mostrar los 5 servicios más usados por cada instalación. (usar query).
# MAGIC 4. Guardar el resultado en el contenedor creado en el punto 1

# COMMAND ----------

# DBTITLE 1,PARTE 1
# En la plataforma de Azure, dentro de "Cuentas de almacenamiento" e ingresar a "g5roxanazarate"
# Dentro de Almacenamiento de Datos ingresar a "Contenedores"
# Crear contenedor para el ejercicio (serviciosusados)
# Ingresar al contenedor "servicios usados" y cargar el archivo SERVICIOS_USADOS.csv

# * Para obtener el TOKEN SAS: dar clic en "...", seleccionar "Generar SAS", brindar todos los permisos y dar clic en "Generar URL y token de SAS". Copiar el token SAS.

# COMMAND ----------

# DBTITLE 1,PARTE 2
# Cargando el nuevo archivo en Blob Storage
storage_account_access_key = "sp=racwdli&st=2024-04-02T03:16:52Z&se=2024-04-02T11:16:52Z&spr=https&sv=2022-11-02&sr=c&sig=yn9kQyumjj%2F7sNMwwgjIvuEgu7G8k3YX06ujajKwq6Y%3D"
spark.conf.set(f"fs.azure.sas.serviciosusados.g5roxanazarate.blob.core.windows.net",storage_account_access_key)

# COMMAND ----------

# DBTITLE 1,PARTE 2
dbutils.fs.ls("wasbs://serviciosusados@g5roxanazarate.blob.core.windows.net")

# COMMAND ----------

# DBTITLE 1,PARTE 2
df_ejercicio = spark.read.csv("wasbs://serviciosusados@g5roxanazarate.blob.core.windows.net/SERVICIOS_USADOS.csv")
df_ejercicio.display()

# COMMAND ----------

# DBTITLE 1,PARTE 2
# Brindando formato al Dataframe
file_location ="wasbs://serviciosusados@g5roxanazarate.blob.core.windows.net"
opciones = {"header": True, "delimiter": ";", "inferSchema":"true"}

df_ejercicio = spark.read.format("csv").options(**opciones).load(file_location + "/SERVICIOS_USADOS.csv")
df_ejercicio.display()

# COMMAND ----------

# DBTITLE 1,PARTE 3
# Configurando SQL
from pyspark.sql import SQLContext
sc = spark.sparkContext
sqlContext = SQLContext(sc)

# COMMAND ----------

# DBTITLE 1,PARTE 3
# Los 5 servicios mas usados por cada instalación
sqlContext.registerDataFrameAsTable(df_ejercicio, "df_ejercicio")
resultado = sqlContext.sql("""
               select SERVICIO, sum(USOS) AS Cantidad
               from df_ejercicio
               group by SERVICIO
               order by Cantidad desc
               limit 5
               """)

# COMMAND ----------

# DBTITLE 1,PARTE 3
resultado.display()

# COMMAND ----------

# DBTITLE 1,PARTE 4
# Guardando el resultado en el contenedor "serviciosusados"
resultado.write.mode('overwrite').format('parquet').save('wasbs://serviciosusados@g5roxanazarate.blob.core.windows.net/OutputDMC2')

# COMMAND ----------

# DBTITLE 1,PARTE 4
df_ejercicio_resultado = spark.read.parquet("wasbs://serviciosusados@g5roxanazarate.blob.core.windows.net/OutputDMC2")
df_ejercicio_resultado.display()
