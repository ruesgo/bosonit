import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NasaScQuerys {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Nasa")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val dataPath = "C:/Bosonit/GIT/bosonit/NasaLogs/NasaSc/Data/"
    val logs = spark.read.format("parquet")
      .option("header","true")
      .load(dataPath)
    logs.show(20,false)

    logs.select("protocol").distinct().show()

    logs.select("status")
      .groupBy("status")
      .count()
      .orderBy(desc("count"))
      .show(10)

    logs.select("method")
      .groupBy("method")
      .count()
      .orderBy(desc("count"))
      .withColumnRenamed("count","n_logs")
      .show(10)

    logs.select("resource","size")
      .groupBy("resource")
      .agg(
        sum(col("size")).alias("transferSize")
      )
      .orderBy(desc("transferSize"))
      .show(1,false)

    logs.select("resource")
      .groupBy("resource")
      .count()
      .orderBy(desc("count"))
      .withColumnRenamed("count","n_logs")
      .show(1,false)

    logs.select("date")
      .groupBy(
        year(col("date")) as "year",
        month(col("date")) as "month",
        dayofmonth(col("date")) as "day"
      )
      .count()
      .orderBy(desc("count"))
      .withColumnRenamed("count","n_logs")
      .show(10,false)

    logs.select("host")
      .groupBy("host")
      .count()
      .orderBy(desc("count"))
      .withColumnRenamed("count","n_logs")
      .show(10,false)

    logs.select("date")
      .groupBy(hour(col("date")) as "hour")
      .count()
      .orderBy(desc("count"))
      .withColumnRenamed("count","n_logs")
      .show(10)

    logs.select("date", "status")
      .filter(col("status") === 404)
      .groupBy(
        year(col("date")) as "year",
        month(col("date")) as "month",
        dayofmonth(col("date")) as "day"
      )
      .count()
      .orderBy(desc("count"))
      .withColumnRenamed("count","404-count")
      .show(10,false)

    logs.filter(col("status")==="400").show()
  }
}
