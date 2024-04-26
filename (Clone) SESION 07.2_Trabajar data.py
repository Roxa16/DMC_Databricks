# Databricks notebook source
# Invocar librerias SQL
from pyspark.sql import SQLContext
sqlContext = SQLContext(spark.sparkContext)

# COMMAND ----------

# Ejecutar query
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

df_view_imp = spark.sql("""
                        SELECT * FROM global_temp.View_
                        """)
df_view_imp.display()

# COMMAND ----------


