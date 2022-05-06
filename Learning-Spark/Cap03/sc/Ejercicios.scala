// Databricks notebook source
// MAGIC %md
// MAGIC Leer el CSV del ejemplo del cap2 y obtener la estructura del schema dado por defecto.

// COMMAND ----------

import org.apache.spark.sql.functions._

val mnmFile = "/databricks-datasets/learning-spark-v2/mnm_dataset.csv"

val mnmDF = spark
  .read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load(mnmFile)

display(mnmDF)

// COMMAND ----------

mnmDF.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC Cuando se define un schema al definir un campo por ejemplo 
// MAGIC StructField('Delay', FloatType(), True) ¿qué significa el último 
// MAGIC parámetro Boolean?

// COMMAND ----------

// MAGIC %md
// MAGIC Significa que se admiten valores NULL

// COMMAND ----------

// MAGIC %md
// MAGIC Dataset vs DataFrame (Scala). ¿En qué se diferencian a nivel de código?

// COMMAND ----------

// MAGIC %md
// MAGIC Un Dataset está profundamente tipado mediante un esquema en el que cada elemento de la fila es del tipo especificado. Un Dataframe es un conjunto de filas a priori sin esquema

// COMMAND ----------

// MAGIC %md
// MAGIC Guardar DataFrames en diferentes formatos

// COMMAND ----------

import org.apache.spark.sql.types._ 
import org.apache.spark.sql.functions._ 

val sfFireFile = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"
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

val fireDF = spark
  .read
  .schema(fireSchema)
  .option("header", "true")
  .csv(sfFireFile)

// COMMAND ----------

fireDF.write.format("json").mode("overwrite").save("/tmp/fireServiceJSON/")
//fireTSDF.write.format("json").mode("overwrite").saveAsTable("FireServiceCalls")
val fileJsonDF = spark.read.format("json").load("/tmp/fireServiceJSON/")
display(fileJsonDF.limit(10))

// COMMAND ----------

fireDF.write.format("csv").mode("overwrite").save("/tmp/fireServiceCSV/")
val fileCsvDF = spark.read.format("csv").load("/tmp/fireServiceCSV/")
display(fileCsvDF.limit(10))

// COMMAND ----------

fireDF.write.format("avro").mode("overwrite").save("/tmp/fireServiceAVRO/")
val fileAvroDF = spark.read.format("avro").load("/tmp/fireServiceAVRO/")
display(fileAvroDF.limit(10))