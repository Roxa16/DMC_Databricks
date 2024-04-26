# Databricks notebook source
# https://adb-2441242062893355.15.azuredatabricks.net#secrets/createScope
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="g5-key-vault-Roxana")

# COMMAND ----------

password = dbutils.secrets.get(scope="g5-key-vault-Roxana", key="contraseniaPostgre")

# COMMAND ----------

print(password)

# COMMAND ----------

driver = "org.postgresql.Driver"
database_host = "espdatabricks5.postgres.database.azure.com"
database_port = "5432"
database_name = "Northwind"
table = "categories"

user = "grupo5_Antony"
password = "databricks_DMC"

url = f"jdbc:postgresql://{database_host}:{database_port}/{database_name}"

# COMMAND ----------

sql_categories = (spark.read.format("jdbc").option("driver", driver).option("url", url).option("dbtable","categories").option("user",user).option("password",password).load())

# COMMAND ----------

sql_categories.show()
