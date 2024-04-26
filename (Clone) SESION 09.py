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

# COMMAND ----------

# DBTITLE 1,Primera Parte
# Leemos una tabla de la base de datos postgresql
sql_categories = (spark.read.format("jdbc").option("driver", driver).option("url", url).option("dbtable","categories").option("user",user).option("password",password).load())

sql_categories.display()

# COMMAND ----------

# Realizamos lo mismo con las otras tablas provenientes de postgresql
tables = {"orders","customer_customer_demo", "customer_demographics", "customers", "employee_territories", "employees", "order_details", "products", "region", "shippers", "suppliers", "territories", "us_states"}

dataframes = {}

for table in tables:
    options = {"driver":driver,"url":url,"dbtable":table,"user":user,"password":password}
    dt_table = spark.read.format("jdbc").options(**options).load()

    dataframes[table] = dt_table

# COMMAND ----------

# Visualizamos las tablas postgresql
dataframes["shippers"].display()

# COMMAND ----------

# DBTITLE 1,Segunda Parte
# Escribiendo la tabla "categories" en eseuqm "s09_dbg5_northwind_rz"
sql_categories.write.format("delta").mode("overwrite").saveAsTable("S09_DBG5_Northwind_RZ.categories")

# COMMAND ----------

# Escribiendo para las otras tablas
for table in tables:
    dataframes[table].write.format("delta").mode("overwrite").saveAsTable("S09_DBG5_Northwind_RZ." + table)
