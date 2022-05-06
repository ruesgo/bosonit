// Databricks notebook source
// MAGIC %md
// MAGIC # Capitulo-2
// MAGIC Ejercicio propuesto del Quijote
// MAGIC 
// MAGIC En la pestaña File -> upload data y se arrastra el archivo

// COMMAND ----------

val lineas = spark.read.text("dbfs:/FileStore/shared_uploads/ruben.estebas@bosonit.com/el_quijote.txt")
lineas.count()

// COMMAND ----------

lineas.show(truncate=true)

// COMMAND ----------

lineas.take(5)

// COMMAND ----------

lineas.head(5)

// COMMAND ----------

lineas.first()

// COMMAND ----------

// MAGIC %md
// MAGIC Tanto 'head' como 'take' toman los n primeros elementos de la lista mientras que 'first' toma solo el primero

// COMMAND ----------

