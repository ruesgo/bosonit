import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object padronSparkDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Padron")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val padronCsv = "C:/Bosonit/GIT/bosonit/Padron/datos_padron/datos_padron.csv"
    val padron = spark.read.format("csv")
      .option("header","true")
      .option("inferschema","true")
      .option("emptyValue", 0)
      .option("delimiter",";")
      .load(padronCsv)
      .withColumn("DESC_DISTRITO",trim(col("desc_distrito")))
      .withColumn("DESC_BARRIO",trim(col("desc_barrio")))

    padron.show(5,false)

    val padronSucio = spark.read.format("csv")
      .option("header","true")
      .option("inferschema","true")
      .option("delimiter",";")
      .load(padronCsv)
    val padronLimpio = padronSucio
      .na.fill(value=0)
      .withColumn("DESC_DISTRITO",trim(col("desc_distrito")))
      .withColumn("DESC_BARRIO",trim(col("desc_barrio")))

    val barrios = padron
      .select("DESC_BARRIO")
      .alias("Barrios")
      .where(col("DESC_BARRIO").isNotNull)
      .distinct()
    barrios.show(10, false)
    padron.select(countDistinct(col("DESC_BARRIO"))).alias("numeroDeBarrios").show()

    padron.createOrReplaceTempView("padron")
    spark.sql("""select count(distinct(desc_barrio)) as nBarrios from padron""").show()

    val padron2 = padron.withColumn("LONGITUD",length(col("DESC_DISTRITO")))
    padron2.show(5,false)

    val padron3 = padron2.withColumn("CINCOS", lit(5))
    padron3.show(5,false)

    val padron4 = padron3.drop("CINCOS")
    padron4.show(5,false)

    val padronParticionado = padron.repartition(col("DESC_DISTRITO"),col("DESC_BARRIO"))
    padronParticionado.cache()
    padronParticionado.explain()

    padronParticionado
      .groupBy(col("desc_barrio"),col("desc_distrito"))
      .agg(
        sum(col("espanolesHombres")).alias("espanolesHombres"),
        sum(col("espanolesMujeres")).alias("espanolesMujeres"),
        sum(col("extranjerosHombres")).alias("extranjerosHombres"),
        sum(col("extranjerosMujeres")).alias("extranjerosMujeres")
      )
      .orderBy(
        desc("extranjerosMujeres"),
        desc("extranjerosHombres")
      )
      .show(10,false)
    padronParticionado.unpersist()

    val df = padron
      .select(
        col("DESC_BARRIO"),
        col("DESC_DISTRITO"),
        col("ESPANOLESHOMBRES")
      )
      .groupBy(
        col("DESC_BARRIO"),
        col("DESC_DISTRITO")
      ) .agg(sum(col("espanolesHombres")).alias("espanolesHombres"))

    val padronJoin = df
      .join(
        padron,
        padron("DESC_BARRIO") === df("DESC_BARRIO") && padron("DESC_DISTRITO") === df("DESC_DISTRITO"),
        "inner"
      )
    padronJoin.show(5,false)

    import org.apache.spark.sql.expressions.Window

    val padronWindow = padron
      .withColumn(
        "TotalEspHom",
        sum(col("espanoleshombres"))
          .over(Window.partitionBy("DESC_DISTRITO", "DESC_BARRIO"))
      )
    padronWindow.show(20,false)

    val distritos = Seq("BARAJAS","CENTRO","RETIRO")
    val padronPivotado = padron
      .groupBy("COD_EDAD_INT")
      .pivot("DESC_DISTRITO", distritos)
      .sum("espanolesMujeres")
      .orderBy(col("COD_EDAD_INT"))
    padronPivotado.show(20,false)

    val padronPercent = padronPivotado
      .withColumn(
        "PORCENTAJE_BARAJAS",
        round(col("BARAJAS")/(col("BARAJAS")+col("CENTRO")+col("RETIRO"))*100,2)
      )
      .withColumn(
        "PORCENTAJE_CENTRO",
        round(col("CENTRO")/(col("BARAJAS")+col("CENTRO")+col("RETIRO"))*100,2)
      )
      .withColumn(
        "PORCENTAJE_RETIRO",
        round(col("RETIRO")/(col("BARAJAS")+col("CENTRO")+col("RETIRO"))*100,2)
      )

    padronPercent.show(20,false)

    padron.write
      .format("csv")
      .option("header","true")
      .mode("overwrite")
      .partitionBy("desc_distrito","desc_barrio")
      .save("C:/Bosonit/GIT/bosonit/Padron/datos_padron/padronParticionadoCsv/")

    padron.write
      .format("parquet")
      .option("header","true")
      .mode("overwrite")
      .partitionBy("desc_distrito","desc_barrio")
      .save("C:/Bosonit/GIT/bosonit/Padron/datos_padron/padronParticionadoParquet/")
  }
}