# Web Server logs Analysis in Scala
### Limpieza de datos

Lo primero es crear la SparkSession y leer los logs del fichero de texto

>import org.apache.spark.sql.SparkSession\
>import org.apache.spark.sql.functions._ \
>import org.apache.spark.sql.types._
> 
> val spark = SparkSession \
    .builder\
    .master("local[*]")\
    .appName("Nasa")\
    .config("spark.some.config.option", "some-value")\
    .getOrCreate()
> 
>spark.sparkContext.setLogLevel("ERROR")
>
>import spark.sqlContext.implicits._\
import spark.implicits
> 
>val dataPath = "C:\\Bosonit\\GIT\\bosonit\\NasaLogs\\access.log\\"\
val rawLogs = spark.read.text(dataPath)

Utilizamos Expresiones Regulares para capturar los diferentes campos de los logs.

>val logs_parsed=rawLogs\
.select(\
regexp_extract(col("value"),"""^([^(\s|,)]+)""",1).as("host"),\
regexp_extract(col("value"),"""^.*\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})""",1).as("date"),\
regexp_extract(col("value"),"""^.*\s\"(\w+)\s""",1).as("method"),\
regexp_extract(col("value"),"""^.*\s+([^\s]+)\s+HTTP.""",1).as("resource"),\
regexp_extract(col("value"),"""^.*(HTTP/\d\.\d)""",1).as("protocol"),\
regexp_extract(col("value"),"""^.*\"\s(\d{3})\s""",1).cast("int").as("status"),\
regexp_extract(col("value"),"""^.*\s(\d+|-)""",1).cast("int").as("size")\
)

Ahora le cambiamos el formato a la fecha. Primero casteamos el mes como número, luego quitamos los dos puntos para cambiarlo a formato de fecha y hora. Esto nos permitirá trabajar con estos datos fácilmente.

>val logs = logs_parsed\
.withColumn("date", regexp_replace(col("date"), "Jan", "01"))\
.withColumn("date", regexp_replace(col("date"), "Feb", "02"))\
.withColumn("date", regexp_replace(col("date"), "Mar", "03"))\
.withColumn("date", regexp_replace(col("date"), "Apr", "04"))\
.withColumn("date", regexp_replace(col("date"), "May", "05"))\
.withColumn("date", regexp_replace(col("date"), "Jun", "06"))\
.withColumn("date", regexp_replace(col("date"), "Jul", "07"))\
.withColumn("date", regexp_replace(col("date"), "Aug", "08"))\
.withColumn("date", regexp_replace(col("date"), "Sep", "09"))\
.withColumn("date", regexp_replace(col("date"), "Oct", "10"))\
.withColumn("date", regexp_replace(col("date"), "Nov", "11"))\
.withColumn("date", regexp_replace(col("date"), "Dec", "12"))\
.withColumn("date", regexp_replace(col("date"), ":", " "))\
.withColumn("date", to_timestamp(col("date"),"dd/MM/yyyy HH mm ss"))

Una vez realizada la limpieza de los datos procedemos a guardarlos en formato parquet.

>logs.write.format("parquet")\
.option("header","true")\
.mode("overwrite")\
.save("C:/Bosonit/GIT/bosonit/NasaLogs/NasaSc/Data")

### Consultas

***1)*** ¿Cuáles son los distintos protocolos web utilizados? Agrúpalos.

>logs.select("protocol").distinct().show()

***2)*** ¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos
para ver cuál es el más común.

>logs.select("status")\
.groupBy("status")\
.count()\
.orderBy(desc("count"))\
.show(10)

***3)*** ¿Y los métodos de petición (verbos) más utilizados?

>logs.select("method")\
.groupBy("method")\
.count()\
.orderBy(desc("count"))\
.withColumnRenamed("count","n_logs")\
.show(10)

Nos da 4 resultados siendo el último una cadena vacía con 10. Analizando los datos he llegado a que estos logs coinciden con aquellos que tienen status 400, o sea, bad request.

>logs.filter(col("status")==="400").show()

***4)*** ¿Qué recurso tuvo la mayor transferencia de bytes de la página web?

>logs.select("resource","size")\
.groupBy("resource")\
.agg(\
sum(col("size")).alias("transferSize")\
)\
.orderBy(desc("transferSize"))\
.show(1,false)

***5)*** Además, queremos saber que recurso de nuestra web es el que más tráfico recibe. Es
decir, el recurso con más registros en nuestro log.

>logs.select("resource")\
.groupBy("resource")\
.count()\
.orderBy(desc("count"))\
.withColumnRenamed("count","n_logs")\
.show(1,false)

***6)*** ¿Qué días la web recibió más tráfico?

>logs.select("date")\
.groupBy(\
year(col("date")) as "year",\
month(col("date")) as "month",\
dayofmonth(col("date")) as "day"\
)\
.count()\
.orderBy(desc("count"))\
.withColumnRenamed("count","n_logs")\
.show(10,false)

***7)*** ¿Cuáles son los hosts son los más frecuentes?

>logs.select("host")\
.groupBy("host")\
.count()\
.orderBy(desc("count"))\
.withColumnRenamed("count","n_logs")\
.show(10,false)

***8)*** ¿A qué horas se produce el mayor número de tráfico en la web?

>logs.select("date")
.groupBy(hour(col("date")) as "hour")
.count()
.orderBy(desc("count"))
.withColumnRenamed("count","n_logs")
.show(10)

***9)*** ¿Cuál es el número de errores 404 que ha habido cada día?

>logs.select("date", "status")\
.filter(col("status") === 404)\
.groupBy(\
year(col("date")) as "year",\
month(col("date")) as "month",\
dayofmonth(col("date")) as "day"\
)\
.count()\
.orderBy(desc("count"))\
.withColumnRenamed("count","404-count")\
.show(10,false)