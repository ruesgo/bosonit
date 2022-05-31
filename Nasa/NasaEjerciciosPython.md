# Web Server logs Analysis in Python
### Limpieza de datos

Lo primero es crear la SparkSession y leer los logs del fichero de texto

>from pyspark.sql import SparkSession\
>from pyspark.sql import functions as f \
>spark = SparkSession \
.builder.master("local[*]") \
.appName("Padron").config("spark.some.config.option", "some-value") \
.getOrCreate()
> 
>spark.sparkContext.setLogLevel("ERROR")
>
>dataPath = "C:\\Bosonit\\GIT\\bosonit\\NasaLogs\\access.log\\"\
rawLogs = spark.read.text(dataPath)

Utilizamos Expresiones Regulares para capturar los diferentes campos de los logs.

>logs_parsed = (\
rawLogs\
.select(\
f.regexp_extract("value", """^([^(\s|,)]+)""",1).alias("host"),\
f.regexp_extract("value", """^.*\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})""",1).alias("date"),\
f.regexp_extract("value", """^.*\s\"(\w+)\s""",1).alias("method"),\
f.regexp_extract("value", """^.*\s+([^\s]+)\s+HTTP.""",1).alias("resource"),\
f.regexp_extract("value", """^.*(HTTP/\d\.\d)""",1).alias("protocol"),\
f.regexp_extract("value", """^.*\"\s(\d{3})\s""",1).cast("int").alias("status"),\
f.regexp_extract("value", """^.*\s(\d+|-)""",1).cast("int").alias("size")\
)
)

Ahora le cambiamos el formato a la fecha. Primero casteamos el mes como número, luego quitamos los dos puntos para cambiarlo a formato de fecha y hora. Esto nos permitirá trabajar con estos datos fácilmente.

>logs = (logs_parsed\
.withColumn("date",
f.regexp_replace("date", "Jan", "01"))\
.withColumn("date",
f.regexp_replace("date", "Feb", "02"))\
.withColumn("date",
f.regexp_replace("date", "Mar", "03"))\
.withColumn("date",
f.regexp_replace("date", "Apr", "04"))\
.withColumn("date",
f.regexp_replace("date", "May", "05"))\
.withColumn("date",
f.regexp_replace("date", "Jun", "06"))\
.withColumn("date",
f.regexp_replace("date", "Jul", "07"))\
.withColumn("date",
f.regexp_replace("date", "Aug", "08"))\
.withColumn("date",
f.regexp_replace("date", "Sep", "09"))\
.withColumn("date",
f.regexp_replace("date", "Oct", "10"))\
.withColumn("date",
f.regexp_replace("date", "Nov", "11"))\
.withColumn("date",
f.regexp_replace("date", "Dec", "12"))\
.withColumn("date",
f.regexp_replace("date", ":", " "))\
.withColumn("date",
f.to_timestamp("date","dd/MM/yyyy HH mm ss"))
)

Una vez realizada la limpieza de los datos procedemos a guardarlos en formato parquet.

>(logs.write.format("parquet")
.option("header","true")
.mode("overwrite")
.save("C:/Bosonit/GIT/bosonit/NasaLogs/NasaPy/Data")
)

### Consultas

***1)*** ¿Cuáles son los distintos protocolos web utilizados? Agrúpalos.

>logs.select("protocol").distinct().show()

***2)*** ¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos
para ver cuál es el más común.

>(logs.select("status")\
.groupBy("status")\
.count()\
.orderBy(f.desc("count"))\
.show(10)
)

***3)*** ¿Y los métodos de petición (verbos) más utilizados?

>(logs.select("method")\
.groupBy("method")\
.count()\
.orderBy(f.desc("count"))\
.withColumnRenamed("count","n_logs")\
.show(10)
)

Nos da 4 resultados siendo el último una cadena vacía con 10. Analizando los datos he llegado a que estos logs coinciden con aquellos que tienen status 400, o sea, bad request.

>logs.filter("status" == 400).show()

***4)*** ¿Qué recurso tuvo la mayor transferencia de bytes de la página web?

>(logs.select("resource","size")\
.groupBy("resource")\
.agg(\
f.sum("size").alias("transferSize")\
)\
.orderBy(f.desc("transferSize"))\
.show(1, False)
)

***5)*** Además, queremos saber que recurso de nuestra web es el que más tráfico recibe. Es
decir, el recurso con más registros en nuestro log.

>(logs.select("resource")\
.groupBy("resource")\
.count()\
.orderBy(f.desc("count"))\
.withColumnRenamed("count","n_logs")\
.show(1,False)
)

***6)*** ¿Qué días la web recibió más tráfico?

>(logs.select("date")\
.groupBy(\
f.year("date").alias("year"),\
f.month("date").alias("month"),\
f.dayofmonth("date").alias("day")\
)\
.count()\
.orderBy(f.desc("count"))\
.withColumnRenamed("count","n_logs")\
.show(10,False)
)

***7)*** ¿Cuáles son los hosts son los más frecuentes?

>(logs.select("host")\
.groupBy("host")\
.count()\
.orderBy(f.desc("count"))\
.withColumnRenamed("count","n_logs")\
.show(10,False)
)

***8)*** ¿A qué horas se produce el mayor número de tráfico en la web?

>(logs.select("date")\
.groupBy(f.hour("date").alias("hour"))\
.count()\
.orderBy(f.desc("count"))\
.withColumnRenamed("count","n_logs")\
.show(10)
)

***9)*** ¿Cuál es el número de errores 404 que ha habido cada día?

>(logs.select("date", "status")\
.filter("status" == 404)\
.groupBy(\
f.year("date").alias("year"),\
f.month("date").alias("month"),\
f.dayofmonth("date").alias("day")\
)\
.count()\
.orderBy(f.desc("count"))\
.withColumnRenamed("count","404-count")\
.show(10,False)
)