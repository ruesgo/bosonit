from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession \
    .builder.master("local[*]") \
    .appName("Padron").config("spark.some.config.option", "some-value") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

dataPath = "C:\\Bosonit\\GIT\\bosonit\\NasaLogs\\access.log\\"
rawLogs = spark.read.text(dataPath)
rawLogs.show(10,False)

logs_parsed = (
    rawLogs
    .select(
        f.regexp_extract("value", """^([^(\s|,)]+)""",1).alias("host"),
        f.regexp_extract("value", """^.*\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})""",1).alias("date"),
        f.regexp_extract("value", """^.*\s\"(\w+)\s""",1).alias("method"),
        f.regexp_extract("value", """^.*\s+([^\s]+)\s+HTTP.""",1).alias("resource"),
        f.regexp_extract("value", """^.*(HTTP/\d\.\d)""",1).alias("protocol"),
        f.regexp_extract("value", """^.*\"\s(\d{3})\s""",1).cast("int").alias("status"),
        f.regexp_extract("value", """^.*\s(\d+|-)""",1).cast("int").alias("size")
    )
)
logs_parsed.show(10,False)

logs = (logs_parsed
        .withColumn("date",
                    f.regexp_replace("date", "Jan", "01"))
        .withColumn("date",
                    f.regexp_replace("date", "Feb", "02"))
        .withColumn("date",
                    f.regexp_replace("date", "Mar", "03"))
        .withColumn("date",
                    f.regexp_replace("date", "Apr", "04"))
        .withColumn("date",
                    f.regexp_replace("date", "May", "05"))
        .withColumn("date",
                    f.regexp_replace("date", "Jun", "06"))
        .withColumn("date",
                    f.regexp_replace("date", "Jul", "07"))
        .withColumn("date",
                    f.regexp_replace("date", "Aug", "08"))
        .withColumn("date",
                    f.regexp_replace("date", "Sep", "09"))
        .withColumn("date",
                    f.regexp_replace("date", "Oct", "10"))
        .withColumn("date",
                    f.regexp_replace("date", "Nov", "11"))
        .withColumn("date",
                    f.regexp_replace("date", "Dec", "12"))
        .withColumn("date",
                    f.regexp_replace("date", ":", " "))
        .withColumn("date",
                    f.to_timestamp("date","dd/MM/yyyy HH mm ss"))
        )
logs.show(10)

(logs.write.format("parquet")
 .option("header","true")
 .mode("overwrite")
 .save("C:/Bosonit/GIT/bosonit/NasaLogs/NasaPy/Data")
 )