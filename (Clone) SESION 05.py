# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

ruta_ejercicio = "dbfs:/FileStore/data/DATA_EJERCICIO_S4-3.csv"

schema = StructType([
    StructField("N",IntegerType(),True), \
    StructField("ID",StringType(),True), \
    StructField("NIVEL_EDUCATIVO",StringType(),True), \
    StructField("SEXO",StringType(),True), \
    StructField("CATEGORIA_EMPLEO",StringType(),True), \
    StructField("EXPERIENCIA_LABORAL",IntegerType(),True), \
    StructField("ESTADO_CIVIL",StringType(),True), \
    StructField("EDAD",IntegerType(),True), \
    StructField("UTILIZACION_TARJETAS",IntegerType(),True), \
    StructField("NUMERO_ENTIDADES",StringType(),True), \
    StructField("DEFAULT",IntegerType(),True), \
    StructField("NUM_ENT_W",FloatType(),True), \
    StructField("EDUC_W",FloatType(),True), \
    StructField("EXP_LAB_W",FloatType(),True), \
    StructField("EDAD_W",FloatType(),True), \
    StructField("UTIL_TC_W",FloatType(),True), \
    StructField("PD",FloatType(),True), \
    StructField("RPD",FloatType(),True), \
])

# COMMAND ----------

df_schema = spark.read.option("header", True).schema(schema).csv(ruta_ejercicio,sep=";")

# COMMAND ----------

# Invocar librerías para manipular con SQL
from pyspark.sql import SQLContext
sc = spark.sparkContext
# Usar SQL con Spark
sqlContext = SQLContext(sc)

# COMMAND ----------

# Registrar como tabla para manipular con comandos SQL
sqlContext.registerDataFrameAsTable(df_schema, "df_schema")
sqlContext.sql(" select * from df_schema ").display()

# COMMAND ----------

sqlContext.sql("""
               select NIVEL_EDUCATIVO
               from df_schema
               group by NIVEL_EDUCATIVO
               """).display()

# COMMAND ----------

sqlContext.sql("""
               select NIVEL_EDUCATIVO, AVG(EDAD) AS Promedio, COUNT(*) AS Total
               from df_schema
               group by NIVEL_EDUCATIVO
               order by Total
               """).display()


# COMMAND ----------

# MAGIC %md
# MAGIC EJERCICIOS:
# MAGIC
# MAGIC - Elaborar una consulta donde se muestren los clientes mayores a 35 años con menores probabilidades de default (los 5 primeros).
# MAGIC - Elaborar una consulta donde se muestren los solteros menores a 35 años que utilizan más de 3 veces su tc y tienen menor probabilidad de default (10 primeros)

# COMMAND ----------

sqlContext.sql("""
               select *
               from df_schema
               where edad > 35
               order by PD
               limit 5
               """).display()

# COMMAND ----------

sqlContext.sql("""
               select *
               from df_schema
               where EDAD<35 and ESTADO_CIVIL = "SOLTERO" and UTILIZACION_TARJETAS>3
               order by PD
               limit 10
               """).display()

# COMMAND ----------

# Joins
data_heroes = [
	("Deadpool", 3), 
	("Iron Man", 1),
	("Groot", 7),
]
data_races = [
	("Kryptoniano", 5),
	("Mutante", 3),
	("Humano", 1),
]

heroes = spark.createDataFrame(data_heroes, ["name", "id"])
races = spark.createDataFrame(data_races, ["races", "id"])

heroes.display()
races.display()

# COMMAND ----------

sqlContext.registerDataFrameAsTable(heroes, "heroes")
sqlContext.registerDataFrameAsTable(races, "races")

# COMMAND ----------

# Realizar Join entre ambas tablas
sqlContext.sql("""
               select *
               from heroes a
               inner join races b on a.id = b.id
               """).display()


# COMMAND ----------

sqlContext.sql("""
               select *
               from heroes a
               full outer join races b on a.id = b.id
               """).display()

# COMMAND ----------

sqlContext.sql("""
               select * from heroes
               union
               select * from races
               """).display()

# COMMAND ----------

sqlContext.sql("""
               select * from heroes
               intersect
               select * from races
               """).display()


# COMMAND ----------

# Se puede usar comandos Pyspark
df_schema.count()

# COMMAND ----------

df_schema.groupBy("SEXO").count().show()

# COMMAND ----------

import pyspark.sql.functions as f

df_schema.groupBy("SEXO").agg(f.mean("EDAD")).show()

# COMMAND ----------

heroes.join(races, on='id', how='inner').show()

# COMMAND ----------

heroes.join(races, on='id', how='outer').show()

# COMMAND ----------

heroes.join(races, on='id', how='left').show()

# COMMAND ----------

    df_schema.groupBy("SEXO").agg(f.mean("EDAD")).filter(f.col("SEXO").isin("M")).show()
