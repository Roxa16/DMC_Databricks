# Databricks notebook source
# DBTITLE 1,Crear un RDD
lista = ["AWS", "Azure", "GCP", "On Premise"]
type(lista)

# COMMAND ----------

# A partir de la Nota 2
RDD = spark.sparkContext.parallelize (lista)
type(RDD)

# COMMAND ----------

RDD.collect()

# COMMAND ----------

# Crear un RDD a partir de un archivo
df_spark = spark.read.table("ventas_roxana")
type(df_spark)

# COMMAND ----------

rdd2 = df_spark.rdd
type(rdd2)

# COMMAND ----------

rdd2.collect()

# COMMAND ----------

data = [("AWS", "20000"),("Azure", "55000"), ("GCP", "34000"), ("One Premise", "8000000")]
type(data)

# COMMAND ----------

rdd3 = spark.sparkContext.parallelize(data)

# COMMAND ----------

rdd3.collect()

# COMMAND ----------

# Convertir RDD a un Df
df_from_rdd = rdd3.toDF()
type(df_from_rdd)

# COMMAND ----------

df_from_rdd.printSchema()

# COMMAND ----------

# Agregar nombres de columna a mi Df
columnas = ["Nombre_Nube", "Num_Usuarios"]

df_from_rdd = rdd3.toDF(columnas)

# COMMAND ----------

df_from_rdd.printSchema()

# COMMAND ----------

columnas = ["Nombre_Nube", "Num_Usuarios"]
data = [("AWS", "20000"),("Azure", "55000"), ("GCP", "34000"), ("One Premise", "8000000")]

df_from_data = spark.createDataFrame(data).toDF(*columnas)

# COMMAND ----------

df_from_data.show()

# COMMAND ----------

# Vamos a importar librerías para el uso de StructType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# COMMAND ----------

# Creamos una lista
data2 = [("Abel", "Malpartida", 86448371, "M", 10000.0), ("Fernando", "Aguilar", 78449382, "M", 23000.0), ("Betsi", "Mendoza", 49220986, "F", 12000.0), ("Diego", "Adama", 76558743, "M", 145000.0)]

# COMMAND ----------

schema = StructType([
    StructField("Nombre", StringType(), True), \
    StructField("Apellido", StringType(), True), \
    StructField("Documento", StringType(), True), \
    StructField("Sexo", StringType(), True), \
    StructField("Salario", FloatType(), True), \
])

# COMMAND ----------

df = spark.createDataFrame(data = data2, schema= schema)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

# Importando datos desde una ruta

from pyspark.sql.types import *
ruta = "dbfs:/FileStore/data/ventas.csv"

schema = StructType([
	StructField("CODIGO", IntegerType(), True), \
	StructField("PRODUCTO", StringType(), True), \
	StructField("CLIENTE", StringType(), True), \
	StructField("CANTIDAD", FloatType(), True), \
	StructField("PRECIO", FloatType(), True), \
])

# COMMAND ----------

df_with_schema = spark.read.format("csv").option("header", "true").option("delimiter", ",").schema(schema).load(ruta)

# COMMAND ----------

df_with_schema.display()

# COMMAND ----------

# Función Select
df_with_schema.select("CODIGO", "CLIENTE").show()

# COMMAND ----------

df_with_schema.select(df_with_schema.CODIGO,df_with_schema.CLIENTE).show()

# COMMAND ----------

df_with_schema.select(df_with_schema["CODIGO"], df_with_schema["CLIENTE"]).show()

# COMMAND ----------

from pyspark.sql.functions import col

df_with_schema.select(col("CODIGO"), col("CLIENTE")).show()

# COMMAND ----------

# DBTITLE 1,Casteo de Variables
# Función WithColumn()
df_with_schema.withColumn("PRECIO",col("PRECIO").cast("Float")).printSchema()

# COMMAND ----------

# DBTITLE 1,Actualizar
df_with_schema.withColumn("PRECIO", col("PRECIO")*1.2).show()

# COMMAND ----------

# DBTITLE 1,Crear columna
df_with_schema.withColumn("PRECIO_IGV", col("PRECIO")*1.8).show()

# COMMAND ----------

# Función drop
df_with_schema.drop("PRECIO").show()

# COMMAND ----------

df_with_schema.display()
