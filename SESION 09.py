# Databricks notebook source
# Trabajamos con conexiones a bases de datos transaccionales.
db = "S09_DBG5_Northwind_RZ"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
spark.sql(f"USE {db}")

# Preparamos nuestra base de datos para lectura delta
spark.sql("SET spark.databricks.delta.formatCheck.enable=false")
spark.sql("SET spark.databricks.delta.properties.default.autoOptimize.optimizeWrite=true")

# COMMAND ----------

# Vamos a conectar a nuestra base de datos
driver = "org.postgresql.Driver"
database_host = "espdatabricks5.postgres.database.azure.com"
database_port = "5432"
database_name = "Northwind"
table = "categories"

user = "grupo5_Antony"
password = "databricks_DMC"

url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"
