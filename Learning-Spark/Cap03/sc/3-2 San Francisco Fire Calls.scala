// Databricks notebook source
// MAGIC 
// MAGIC %md
// MAGIC # San Francisco Fire Calls

// COMMAND ----------

// MAGIC %md
// MAGIC Se importan las librerías y guardamos la ruta de los datos

// COMMAND ----------

import org.apache.spark.sql.types._ 
import org.apache.spark.sql.functions._ 

val sfFireFile = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"

// COMMAND ----------

// MAGIC %md
// MAGIC Se define el esquema. Es mejor que no hacerlo para grandes cantidades de datos

// COMMAND ----------

val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
  StructField("UnitID", StringType, true),
  StructField("IncidentNumber", IntegerType, true),
  StructField("CallType", StringType, true),                  
  StructField("CallDate", StringType, true),      
  StructField("WatchDate", StringType, true),
  StructField("CallFinalDisposition", StringType, true),
  StructField("AvailableDtTm", StringType, true),
  StructField("Address", StringType, true),       
  StructField("City", StringType, true),       
  StructField("Zipcode", IntegerType, true),       
  StructField("Battalion", StringType, true),                 
  StructField("StationArea", StringType, true),       
  StructField("Box", StringType, true),       
  StructField("OriginalPriority", StringType, true),       
  StructField("Priority", StringType, true),       
  StructField("FinalPriority", IntegerType, true),       
  StructField("ALSUnit", BooleanType, true),       
  StructField("CallTypeGroup", StringType, true),
  StructField("NumAlarms", IntegerType, true),
  StructField("UnitType", StringType, true),
  StructField("UnitSequenceInCallDispatch", IntegerType, true),
  StructField("FirePreventionDistrict", StringType, true),
  StructField("SupervisorDistrict", StringType, true),
  StructField("Neighborhood", StringType, true),
  StructField("Location", StringType, true),
  StructField("RowID", StringType, true),
  StructField("Delay", FloatType, true)))

// COMMAND ----------

// MAGIC %md
// MAGIC Se leen los datos utilizando el esquema

// COMMAND ----------

val fireDF = spark
  .read
  .schema(fireSchema)
  .option("header", "true")
  .csv(sfFireFile)

// COMMAND ----------

fireDF.count()

// COMMAND ----------

fireDF.printSchema()

// COMMAND ----------

display(fireDF.limit(5))

// COMMAND ----------

// MAGIC %md
// MAGIC Filtro que quita las llamadas del tipo 'Medical Incident'

// COMMAND ----------

val fewFireDF = fireDF
  .select("IncidentNumber", "AvailableDtTm", "CallType") 
  .where($"CallType" =!= "Medical Incident")

fewFireDF.show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Contamos los tipos de llamada diferentes
// MAGIC 
// MAGIC Tenemos en cuenta solo los valores que no son nulos.

// COMMAND ----------

fireDF.select("CallType").where(col("CallType").isNotNull).distinct().count()

// COMMAND ----------

fireDF.select("CallType").where(col("CallType").isNotNull).distinct().show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC  Encontrar todas las respuestas con más de 5 minutos de delay
// MAGIC  
// MAGIC  renombrando, añadiendo y eliminando columnas

// COMMAND ----------

val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
newFireDF.select("ResponseDelayedinMins").where($"ResponseDelayedinMins" > 5).show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Cambiamos tipos de datos: date -> datetime

// COMMAND ----------

val fireTSDF = newFireDF
  .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy")).drop("CallDate") 
  .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy")).drop("WatchDate") 
  .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")).drop("AvailableDtTm")

// COMMAND ----------

// MAGIC %md
// MAGIC mostramos las nuevas columnas

// COMMAND ----------

fireTSDF.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Años en los que hay datos

// COMMAND ----------

fireTSDF.select(year($"IncidentDate")).distinct().orderBy(year($"IncidentDate")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Lista de las llamadas más comunes

// COMMAND ----------

fireTSDF
  .select("CallType")
  .where(col("CallType").isNotNull)
  .groupBy("CallType")
  .count()
  .orderBy(desc("count"))
  .show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC  query que devuelve la suma de las llamadas, y la media, minimo y maximo tiempo de respuesta para las llamadas

// COMMAND ----------

fireTSDF.select(sum("NumAlarms"), avg("ResponseDelayedinMins"), min("ResponseDelayedinMins"), max("ResponseDelayedinMins")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Codigos Postales

// COMMAND ----------

fireTSDF
  .select("CallType", "ZipCode")
  .where(col("CallType").isNotNull)
  .groupBy("CallType", "Zipcode")
  .count()
  .orderBy(desc("count"))
  .show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Barrios asociados a códigos postales

// COMMAND ----------

fireTSDF.select("Neighborhood", "Zipcode").where((col("Zipcode") === 94102) || (col("Zipcode") === 94103)).distinct().show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC Contamos las llamadas por semana del año y las ordenamos en orden descendente

// COMMAND ----------

fireTSDF.filter(year($"IncidentDate") === 2018).groupBy(weekofyear($"IncidentDate")).count().orderBy(desc("count")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ** Q-7) What neighborhoods in San Francisco had the worst response time in 2018?**
// MAGIC 
// MAGIC It appears that if you living in Presidio Heights, the Fire Dept arrived in less than 3 mins, while Mission Bay took more than 6 mins.

// COMMAND ----------

fireTSDF.select("Neighborhood", "ResponseDelayedinMins").filter(year($"IncidentDate") === 2018).show(10, false)

// COMMAND ----------

// MAGIC %md
// MAGIC ** Q-8a) How can we use Parquet files or SQL table to store data and read it back?**

// COMMAND ----------

fireTSDF.write.format("parquet").mode("overwrite").save("/tmp/fireServiceParquet/")

// COMMAND ----------

// MAGIC %md
// MAGIC ** Q-8b) How can we use Parquet SQL table to store data and read it back?**

// COMMAND ----------

fireTSDF.write.format("parquet").mode("overwrite").saveAsTable("FireServiceCalls")

// COMMAND ----------

// MAGIC %sql
// MAGIC CACHE TABLE FireServiceCalls

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM FireServiceCalls LIMIT 10

// COMMAND ----------

// MAGIC %md
// MAGIC ** Q-8c) How can read data from Parquet file?**
// MAGIC 
// MAGIC Note we don't have to specify the schema here since it's stored as part of the Parquet metadata

// COMMAND ----------

val fileParquetDF = spark.read.format("parquet").load("/tmp/fireServiceParquet/")

// COMMAND ----------

display(fileParquetDF.limit(10))
