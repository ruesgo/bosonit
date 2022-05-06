// Databricks notebook source
// MAGIC %md
// MAGIC Se leen los datos y se crea una vista temporal

// COMMAND ----------

import org.apache.spark.sql.SparkSession 
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

val spark = SparkSession
 .builder
 .getOrCreate()
// Path to data set 
val csvFile="/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
// Read and create a temporary view
// Infer schema (note that for larger files you may want to specify the schema)
val df0 = spark.read.format("csv")
 .option("inferSchema", "true")
 .option("header", "true")
 .load(csvFile)
// Create a temporary view
df0.createOrReplaceTempView("us_delay_flights_tbl")
df0.show(5,false)
df0.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC Como nos conviene leer la fecha como un string para cambiarle el formato, inferimos un esquema para la lectura

// COMMAND ----------

val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
val df = spark.read.format("csv")
 .schema(schema)
 .option("header", "true")
 .load(csvFile)
df.createOrReplaceTempView("us_delay_flights_tbl")
df.show(5,false)
df.printSchema

// COMMAND ----------

(df.select("distance", "origin", "destination")
 .where("distance > 1000")
 .orderBy(desc("distance")).show(10))

// COMMAND ----------

spark.sql("""SELECT distance, origin, destination 
FROM us_delay_flights_tbl WHERE distance > 1000 
ORDER BY distance DESC""").show(10)

// COMMAND ----------

spark.sql("""SELECT date, delay, origin, destination 
FROM us_delay_flights_tbl 
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD' 
ORDER by delay DESC""").show(10)

// COMMAND ----------

df.select("date", "delay", "origin", "destination")
  .where("delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'")
  .orderBy(desc("delay")).show(10)

// COMMAND ----------

// MAGIC %md
// MAGIC Cambiar el formato de la fecha y encontrar los días o los meses en los qe estos delays son más comunes. 
// MAGIC ¿Están relacionados los delays con meses de invierno o vacaciones?

// COMMAND ----------

// MAGIC %md
// MAGIC Se crea y registra una función que cambia el formato de la fecha al requerido

// COMMAND ----------

val newdf = df
  .withColumn("Date_fm", to_timestamp(col("date"),"MMddhhmm"))
  .withColumn("dia", dayofmonth($"Date_fm"))
  .withColumn("mes", month($"Date_fm"))
  .drop("Date_fm") 
newdf.show(5, false)

// COMMAND ----------

newdf.select("mes","dia")
  .where(col("mes").isNotNull)
  .where("delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'")
  .groupBy("mes","dia")
  .count()
  //.groupBy("mes")
  //.avg("count")
  .orderBy(desc("count")).show(20)

// COMMAND ----------

spark.sql("""SELECT delay, origin, destination,
 CASE
 WHEN delay > 360 THEN 'Very Long Delays'
 WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
 WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
 WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
 WHEN delay = 0 THEN 'No Delays'
 ELSE 'Early'
 END AS Flight_Delays
 FROM us_delay_flights_tbl
 ORDER BY origin, delay DESC""").show(10)


// COMMAND ----------

val df1 = df.withColumn("Flight_Delays", 
                    when($"delay" > 360,"Very Long Delays")
                    .when($"delay" > 120,"Long Delays")
                    .when($"delay" > 120,"Long Delays")
                    .when($"delay" > 60,"Short Delays")
                    .when($"delay" > 0,"Tolerable Delays")
                    .when($"delay" === 0,"No Delays")
                    .otherwise("Early"))
df1.select("delay","origin","destination","Flight_Delays")
  .orderBy(desc("delay")).show(10,false)
  

// COMMAND ----------

