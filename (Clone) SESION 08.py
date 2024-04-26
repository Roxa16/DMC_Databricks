# Databricks notebook source
# MAGIC %md Vamos a crear un BBDD para luego integrarlo dentro del Delta Lake (DL)

# COMMAND ----------

# Establecer el nombre de nuestra BBDD
db = "deltaEsp5Roxana"

#Creacion de BBDD
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
spark.sql(f"USE {db}")

# Preparar nuestra base de datos y configurar para habilitar DL
spark.sql("SET spark.databricks.delta.formatCheck.enable = false")
spark.sql("SET spark.databricks.delta.properties.autoOptimize.optimizeWrite = true")

# COMMAND ----------

# Vamos a importar algunas librerias necesarias
import random
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Creamos funcion para devolver la ruta del checkpoint
def my_checkpoint():
    return "tmp/deltaEsp5Roxana/chkpt/%s" % random.randint(0,10000)

# Generamos función para retornar aleatoriamente un estado
@udf(returnType=StringType())
def random_provincias():
    return random.choice(["CA", "TX", "NY", "WA"])

# Creamos una función donde de forma aleatoria va a empezar a cargar data y asociarlo a tablas parquet
def genera_data_stream(table_format, table_name, schema_ok = False, type = "batch"):
    streaming_data = (
        spark.readStream.format("rate").option("rowsPerSecond", 500).load()
        .withColumn("loan_id", 1000 + col("value"))
        .withColumn("funded_amnt", (rand() * 3000 + 2000).cast("integer"))
        .withColumn("paid_amnt", col("funded_amnt")-rand()*200)
        .withColumn("addr_state", random_provincias())
        .withColumn("type", lit(type))
    )    
    if schema_ok:
        streaming_data = streaming_data.select("loan_id","funded_amnt","paid_amnt","addr_state", "type", "timestamp")

    query = (streaming_data.writeStream
             .format(table_format)
             .option("checkpointLocation",my_checkpoint())
             .trigger(processingTime="5 seconds")
             .table(table_name)
            )
    
    return query

# COMMAND ----------

# Creamos una función para que que detenga los streaming, y así no se queden ejecutando innecesariamente
def  stop_all_strams():
    print("Parando todos los streams DMC Databricks")

    for s in spark.streams.active:
        try:
            s.stop()
        except:
            pass
    print("Todos los streams fueron detenidos")

#Crear una función para eliminar los path creados para almacenar la data y las tablas delta
def limpiar_path_tablas():
    dbutils.fs.rm("tmp/deltaEsp5Roxana/chkpt", True)
    dbutils.fs.rm("file:/dbfs/tmp/deltaEsp5Roxana/loans_parquet/", True)

    for table in ["deltaEsp5Roxana.loans_parquet","deltaEsp5Roxana.loans_delta","deltaEsp5Roxana.loans_deltar"]:
        spark.sql(f"DROP TABLE IF EXISTS {table} ")

# COMMAND ----------

# MAGIC %sh mkdir -p /dbfs/tmp/deltaEsp5Roxana/loans_parquet/; wget -O /dbfs/tmp/deltaEsp5Roxana/loans_parquet/loans.parquet https://pages.databricks.com/rs/094-YMS-629/images/SAISEU19-loan-risks.snappy.parquet

# COMMAND ----------

# Vamos a convertir la informacion al formato Delta Lake

# Vamos a cargar el archivo parquet
parquet_path = "file:/dbfs/tmp/deltaEsp5Roxana/loans_parquet"

df = (
    spark.read.format("parquet").load(parquet_path)
    .withColumn("type", lit("batch"))
    .withColumn("timestamp", current_timestamp())
)

#Vamos a escribirlo en formato delta
df.write.format("delta").mode("overwrite").saveAsTable("loans_delta")

# COMMAND ----------

# DBTITLE 1,tabla 2
# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE loans_deltaR
# MAGIC USING delta
# MAGIC as SELECT * FROM PARQUET.`/tmp/deltaEsp5Roxana/loans_parquet/`

# COMMAND ----------

# MAGIC %sql
# MAGIC convert to delta parquet.`/tmp/deltaEsp5Roxana/loans_parquet/`

# COMMAND ----------

spark.sql("select count(*) from loans_delta").show()
spark.sql("select * from loans_delta").show(5)

# COMMAND ----------

stream_query_A = genera_data_stream(table_format="delta",table_name="loans_delta", schema_ok=True, type="stream A")

stream_query_A = genera_data_stream(table_format="delta",table_name="loans_delta", schema_ok=True, type="stream B")

# COMMAND ----------

display(spark.readStream.format("delta").table("loans_delta").groupBy("type").count().orderBy("type"))

# COMMAND ----------

stop_all_strams()
limpiar_path_tablas()
