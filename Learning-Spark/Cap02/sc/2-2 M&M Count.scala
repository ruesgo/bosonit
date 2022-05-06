// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC ## Ejemplo 2-1 M&M Count

// COMMAND ----------

import org.apache.spark.sql.functions._

val mnmFile = "/databricks-datasets/learning-spark-v2/mnm_dataset.csv"

// COMMAND ----------

// MAGIC %md
// MAGIC ### Leer el CSV y sacar el esquema

// COMMAND ----------

val mnmDF = spark
  .read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load(mnmFile)

display(mnmDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Añadir el count de todos los colores y agrupar por estado y color, ordenar por orden descendiente

// COMMAND ----------

val countMnMDF = mnmDF
  .select("State", "Color", "Count")
  .groupBy("State", "Color")
  .agg(count("Count")
  .alias("Total"))
  .orderBy(desc("Total"))

countMnMDF.show(60)
println(s"Total Rows = ${countMnMDF.count()}") //utilizando la s se aplica la función 

// COMMAND ----------

// MAGIC %md
// MAGIC ### Encontrar count para California filtrando por estado

// COMMAND ----------

val caCountMnMDF = mnmDF
  .select("State", "Color", "Count")
  .where(col("State") === "CA")
  .groupBy("State", "Color")
  .agg(count("Count").alias("Total"))
  .orderBy(desc("Total"))
   
// show the resulting aggregation for California
caCountMnMDF.show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Primer ejercicio propuesto: 
// MAGIC   i. Agrupar por estado y color y calcular el máximo de la cuenta

// COMMAND ----------

val maxMnMDF = mnmDF
  .select("State", "Color", "Count")
  .groupBy("State", "Color")
  .agg(max("Count")
  .alias("Max"))
  .orderBy("Max")

maxMnMDF.show(60)

// COMMAND ----------

// MAGIC %md
// MAGIC   ii. Acotar la búsqueda al estado de Texas con la claúsula where

// COMMAND ----------

val txMaxMnMDF = mnmDF
  .select("State", "Color", "Count")
  .where(col("State") === "TX")
  .groupBy("State", "Color")
  .agg(max("Count")
  .alias("Max"))
  .orderBy("Max")

txMaxMnMDF.show(60)

// COMMAND ----------

// MAGIC %md
// MAGIC   iii. Hacer un ejercicio donde se calculen en una misma operación el Max, Min, Avg, Count. Se odena por Avg

// COMMAND ----------

val totalMnMDF = mnmDF
  .select("State", "Color", "Count")
  .groupBy("State", "Color")
  .agg(
    max("Count").alias("Max"),
    min("Count").alias("Min"),
    avg("Count").alias("Avg"),
    count("Count").alias("Total"),
  )
  .orderBy(desc("Avg"))

totalMnMDF.show(60)

// COMMAND ----------

mnmDF.createOrReplaceTempView("df_temp")
spark.sql("select State, Color, avg(Count) as Avg from df_temp where State = 'CA' group by State, Color order by Avg desc").show()

// COMMAND ----------

