# Databricks notebook source
# Leer tabla delta
spark.read.table("ventas_roxana")

# COMMAND ----------

spark

# COMMAND ----------

df_ventas = spark.read.table("ventas_roxana")

# COMMAND ----------

# Imprimir schemas
df_ventas.printSchema()

# COMMAND ----------

# Mostrar los datos del df (Opción 1)
df_ventas.show()

# COMMAND ----------

# Mostrar los datos (Opción 2)
df_ventas.display()

# COMMAND ----------

# Seleccionar columnas específicas
df_ventas.select("Tienda", "Genero").show()

# COMMAND ----------

# Agregar columna calculada
df_ventas = df_ventas.withColumn("Venta_USD", df_ventas["Precio_soles"]/3.71)

# COMMAND ----------

df_ventas.display()

# COMMAND ----------

# MAGIC %md
# MAGIC EJERCICIO
# MAGIC
# MAGIC 1. Cargar el archivo
# MAGIC 2. Crear columna Precio_Total -> Cantidad * Precio
# MAGIC 3. Crear una columna Impuesto, que va a ser el 18% del precio total

# COMMAND ----------

# Visualizar tabla
df_ejercicio = spark.read.table("ventas_ejercicio_roxana")

# COMMAND ----------

df_ejercicio.display()

# COMMAND ----------

df_ejercicio2 = df_ejercicio.withColumn("Precio_Total", df_ejercicio["PRECIO"]*df_ejercicio["CANTIDAD"])
df_ejercicio2.display()

# COMMAND ----------

df_ejercicio2 = df_ejercicio2.withColumn("Impuesto", df_ejercicio2["Precio_Total"]*0.18)
df_ejercicio2.display()
