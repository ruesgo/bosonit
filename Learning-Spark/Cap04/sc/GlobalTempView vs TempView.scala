// Databricks notebook source
// MAGIC %md
// MAGIC Se cargan los datos con los que se va a trabajar

// COMMAND ----------

import org.apache.spark.sql.SparkSession 
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

val spark = SparkSession
 .builder
 .getOrCreate()
val csvFile="/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
val df = spark.read.format("csv")
 .schema(schema)
 .option("header", "true")
 .load(csvFile)
df.createOrReplaceTempView("us_delay_flights_tbl")
df.show(5,false)
df.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC Se crea un dataframe y se guarda como GlobalTempView, lo que nos permitirá utilizar esta tabla desde cualquier sparkSesion.

// COMMAND ----------

val df1 =  spark.sql("SELECT date, delay, origin, destination FROM us_delay_flights_tbl WHERE origin = 'SFO'")

// COMMAND ----------

df1.createOrReplaceGlobalTempView("us_origin_airport_SFO_tmp_view")

// COMMAND ----------

// MAGIC %md
// MAGIC Las vistas temporales globales se guardan en la carpeta global_temp y por tanto para utilizarlas se hace dela siguiente manera

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM global_temp.us_origin_airport_SFO_tmp_view

// COMMAND ----------

// MAGIC %md
// MAGIC Ahora se crea otro dataframe y se crea una vista temporal, que a diferencia de una vista temporal global, 
// MAGIC solo se puede trabajar con ella en la sparkSesion en la que se creó.

// COMMAND ----------

val df2 = spark.sql("SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE origin = 'JFK'")

// COMMAND ----------

df2.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM us_origin_airport_JFK_tmp_view

// COMMAND ----------

// MAGIC %md
// MAGIC Se crea otra sparkSesion y se comprueba que la vista global funciona 

// COMMAND ----------

val spark2 = SparkSession
 .builder
 .getOrCreate()
val df3 = spark2.sql("SELECT * from global_temp.us_origin_airport_SFO_tmp_view").show(5)

// COMMAND ----------

val df4 = spark2.sql("SELECT * from us_origin_airport_JFK_tmp_view").show(5)

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP VIEW IF EXISTS global_temp.us_origin_airport_JFK_tmp_view

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view

// COMMAND ----------

display(spark.catalog.listTables(dbName="global_temp"))

// COMMAND ----------

