# Databricks notebook source
dbutils.widgets.text("fechaProceso", "20240422", "ParametroFecha")
dbutils.widgets.text("nombreUsuario", "RZarate", "ParametroUsuario")

# COMMAND ----------

fechaProceso = dbutils.widgets.get("fechaProceso")
nombreUsuario = dbutils.widgets.get("nombreUsuario")

# COMMAND ----------

print(fechaProceso)
print(nombreUsuario)
