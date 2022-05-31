from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("Nasa")
         .config("spark.some.config.option", "some-value")
         .getOrCreate()
         )
spark.sparkContext.setLogLevel("ERROR")
dataPath = "C:/Bosonit/GIT/bosonit/NasaLogs/NasaSc/Data/"

logs = (spark.read.format("parquet")
        .option("header","true")
        .load(dataPath)
        )

logs.show(20, False)

logs.select("protocol").distinct().show()

(logs.select("status")
    .groupBy("status")
    .count()
    .orderBy(f.desc("count"))
    .show(10)
 )

(logs.select("method")
    .groupBy("method")
    .count()
    .orderBy(f.desc("count"))
    .withColumnRenamed("count","n_logs")
    .show(10)
 )

(logs.select("resource","size")
    .groupBy("resource")
    .agg(
        f.sum("size").alias("transferSize")
    )
    .orderBy(f.desc("transferSize"))
    .show(1, False)
 )

(logs.select("resource")
    .groupBy("resource")
    .count()
    .orderBy(f.desc("count"))
    .withColumnRenamed("count","n_logs")
    .show(1,False)
 )

(logs.select("date")
    .groupBy(
        f.year("date").alias("year"),
        f.month("date").alias("month"),
        f.dayofmonth("date").alias("day")
    )
    .count()
    .orderBy(f.desc("count"))
    .withColumnRenamed("count","n_logs")
    .show(10,False)
 )

(logs.select("host")
    .groupBy("host")
    .count()
    .orderBy(f.desc("count"))
    .withColumnRenamed("count","n_logs")
    .show(10,False)
 )

(logs.select("date")
    .groupBy(f.hour("date").alias("hour"))
    .count()
    .orderBy(f.desc("count"))
    .withColumnRenamed("count","n_logs")
    .show(10)
 )

(logs.select("date", "status")
    .filter("status" == 404)
    .groupBy(
        f.year("date").alias("year"),
        f.month("date").alias("month"),
        f.dayofmonth("date").alias("day")
    )
    .count()
    .orderBy(f.desc("count"))
    .withColumnRenamed("count","404-count")
    .show(10,False)
 )

logs.filter("status" == 400).show()