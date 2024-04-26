# Databricks notebook source
from pyspark.sql.types import *
ruta_ejercicio = "dbfs:/FileStore/data/DATA_TEST.csv"

schema = StructType([
    StructField("ID", IntegerType(), True),\
    StructField("ID_1", IntegerType(), True),\
    StructField("NIVEL_EDUCATIVO", StringType(), True),\
    StructField("SEXO", StringType(), True),\
    StructField("CATEGORIA_EMPLEO", StringType(), True),\
    StructField("EXPERIENCIA_LABORAL", StringType(), True),\
    StructField("EDAD", StringType(), True),\
    StructField("UTILIZACION_TARJETA", StringType(), True),\
    StructField("NUMERO_ENTIDADES", FloatType(), True),\
    StructField("DEFAULT", StringType(), True),\
    StructField("NUM_ENT_W", FloatType(), True),\
    StructField("EDUC_W", FloatType(), True),\
    StructField("EXP_LAB_W", FloatType(), True),\
    StructField("EDAD_W", FloatType(), True),\
    StructField("UTIL_TC_W", FloatType(), True),\
    StructField("PD", FloatType(), True),\
    StructField("RPD", FloatType(), True),\
])

df_with_eschema = spark.read.option("header",True).schema(schema).csv(ruta_ejercicio, sep=";")
df_with_eschema.display()

# COMMAND ----------

df_with_eschema = df_with_eschema.drop("ID_1")
df_with_eschema.display()

# COMMAND ----------

# Crear un vista temporal
df_with_eschema.createOrReplaceGlobalTempView("View_cosecha_clientes_Roxana")
