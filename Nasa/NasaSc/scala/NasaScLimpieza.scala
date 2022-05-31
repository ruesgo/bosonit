import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object NasaScLimpieza {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Nasa")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.sqlContext.implicits._
    import spark.implicits
    val dataPath = "C:\\Bosonit\\GIT\\bosonit\\NasaLogs\\access.log\\"
    val rawLogs = spark.read.text(dataPath)
    rawLogs.show(10,false)

    val logs_parsed=rawLogs
      .select(
        regexp_extract(col("value"),"""^([^(\s|,)]+)""",1).as("host"),
        regexp_extract(col("value"),"""^.*\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2})""",1).as("date"),
        regexp_extract(col("value"),"""^.*\s\"(\w+)\s""",1).as("method"),
        regexp_extract(col("value"),"""^.*\s+([^\s]+)\s+HTTP.""",1).as("resource"),
        regexp_extract(col("value"),"""^.*(HTTP/\d\.\d)""",1).as("protocol"),
        regexp_extract(col("value"),"""^.*\"\s(\d{3})\s""",1).cast("int").as("status"),
        regexp_extract(col("value"),"""^.*\s(\d+|-)""",1).cast("int").as("size")
      )
    val logs = logs_parsed
      .withColumn("date",
        regexp_replace(col("date"), "Jan", "01"))
      .withColumn("date",
        regexp_replace(col("date"), "Feb", "02"))
      .withColumn("date",
        regexp_replace(col("date"), "Mar", "03"))
      .withColumn("date",
        regexp_replace(col("date"), "Apr", "04"))
      .withColumn("date",
        regexp_replace(col("date"), "May", "05"))
      .withColumn("date",
        regexp_replace(col("date"), "Jun", "06"))
      .withColumn("date",
        regexp_replace(col("date"), "Jul", "07"))
      .withColumn("date",
      regexp_replace(col("date"), "Aug", "08"))
      .withColumn("date",
        regexp_replace(col("date"), "Sep", "09"))
      .withColumn("date",
        regexp_replace(col("date"), "Oct", "10"))
      .withColumn("date",
        regexp_replace(col("date"), "Nov", "11"))
      .withColumn("date",
        regexp_replace(col("date"), "Dec", "12"))
      .withColumn("date",
        regexp_replace(col("date"), ":", " "))
      .withColumn("date",
        to_timestamp(col("date"),"dd/MM/yyyy HH mm ss"))
//      .where(col("host").isNotNull)
//      .where(col("date").isNotNull)
//      .where(col("method").isNotNull)
//      .where(col("resource").isNotNull)
//      .where(col("protocol").isNotNull)
//      .where(col("status").isNotNull)
//      .where(col("size").isNotNull)
    logs.show(10)

    logs.write.format("parquet")
      .option("header","true")
      .mode("overwrite")
      .save("C:/Bosonit/GIT/bosonit/NasaLogs/NasaSc/Data")
  }
}
