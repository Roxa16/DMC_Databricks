# Databricks notebook source
# MAGIC %md
# MAGIC EJERCICIO
# MAGIC
# MAGIC - Cargar el archivo proporcionado y procesarlo de manera directa respetando los tipos de dato.
# MAGIC - Eliminar el campo repetido, si existiera.
# MAGIC - Dado el supuesto que la edad de los estudiantes puede afectar su linea de credito, realice un shock a la variable edad, añadiendo un campo edad maxima de endeudamiento, la edad maxima será el 5% adicional a la variable edad, así como repita el mismo procedimiento para la probabilidad de default (CAMPO DEFAULT), donde debe de crear un nuevo campo con el crecimiento de 8% del default original.

# COMMAND ----------

# DBTITLE 1,Importando librerías
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Parte 1
# Cargar archivo y procesar
ruta2 = "dbfs:/FileStore/data/DATA_EJERCICIO_S4-3.csv"

schema2 = StructType([
    StructField("N",IntegerType(),True), \
    StructField("ID",StringType(),True), \
    StructField("NIVEL_EDUCATIVO",StringType(),True), \
    StructField("SEXO",StringType(),True), \
    StructField("CATEGORIA_EMPLEO",StringType(),True), \
    StructField("EXPERIENCIA_LABORAL",IntegerType(),True), \
    StructField("ESTADO_CIVIL",StringType(),True), \
    StructField("EDAD",IntegerType(),True), \
    StructField("UTILIZACIÓN_TARJETAS",IntegerType(),True), \
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

# DBTITLE 1,Parte 1
prueba = spark.read.format("csv").option("header",True).option("delimiter",";").schema(schema2).load(ruta2)

# COMMAND ----------

# DBTITLE 1,Parte 1
prueba.display()

# COMMAND ----------

# DBTITLE 1,Parte 2
# Eliminar campo duplicado
prueba.drop("N").display()

# COMMAND ----------

# DBTITLE 1,Parte 3
# Shock a la variable Edad y Default
from pyspark.sql.functions import col

prueba.withColumn("Edad_Max", col("EDAD")*1.05).display()

# COMMAND ----------

# DBTITLE 1,Parte 3
prueba.withColumn("Default_Nuevo", col("DEFAULT")*1.08).display()
