// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC ## Ejemplo 2-1 Line Count

// COMMAND ----------

// MAGIC %md
// MAGIC Se lee el archivo, se cuentan las líneas y se hace un filtrado de las líneas en las que aparece la palabra 'spark'

// COMMAND ----------

val strings = spark.read.text("/databricks-datasets/learning-spark-v2/SPARK_README.md")
strings.show(10, false)

// COMMAND ----------

strings.count

// COMMAND ----------

val filtered = strings.filter($"value".contains("Spark"))
filtered.count

// COMMAND ----------

