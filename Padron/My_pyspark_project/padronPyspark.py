from pyspark.sql import SparkSession
from pyspark.sql import functions as f
spark = SparkSession\
    .builder.master("local[*]")\
    .appName("Padron").config("spark.some.config.option", "some-value")\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

padron_csv = "C:/Bosonit/GIT/bosonit/Padron/datos_padron/datos_padron.csv"
padron = spark.read.format("csv")\
    .option("header","true").option("inferschema","true")\
    .option("emptyValue", 0).option("delimiter",";")\
    .load(padron_csv).withColumn("DESC_DISTRITO",f.trim(f.col("desc_distrito"))) \
    .withColumn("DESC_BARRIO",f.trim(f.col("desc_barrio")))

padron.show(5,False)

padron_sucio = spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema","true")\
    .option("delimiter",";")\
    .load(padron_csv)
padron_limpio = padron_sucio.na.fill(value=0)\
    .withColumn("DESC_DISTRITO",f.trim(f.col("desc_distrito")))\
    .withColumn("DESC_BARRIO",f.trim(f.col("desc_barrio")))
padron_limpio.show(5,False)

barrios = padron.select("DESC_BARRIO")\
    .alias("Barrios")\
    .where(f.col("DESC_BARRIO").isNotNull()).distinct()
barrios.show(10, False)
padron.select(f.countDistinct(f.col("DESC_BARRIO"))).alias("numeroDeBarrios").show()

padron.createOrReplaceTempView("padron")
spark.sql("""select count(distinct(desc_barrio)) as nBarrios from padron""").show()

padron2 = padron.withColumn("LONGITUD",f.length(f.col("DESC_DISTRITO")))
padron2.show(5,False)

padron3 = padron2.withColumn("CINCOS", f.lit(5))
padron3.show(5,False)

padron4 = padron3.drop("CINCOS")
padron4.show(5,False)

padronParticionado = padron.repartition(f.col("DESC_DISTRITO"),f.col("DESC_BARRIO"))
padronParticionado.cache()
padronParticionado.explain()

padronParticionado\
    .groupBy(f.col("desc_barrio"),f.col("desc_distrito"))\
    .agg(
        f.sum(f.col("espanolesHombres")).alias("espanolesHombres"),
        f.sum(f.col("espanolesMujeres")).alias("espanolesMujeres"),
        f.sum(f.col("extranjerosHombres")).alias("extranjerosHombres"),
        f.sum(f.col("extranjerosMujeres")).alias("extranjerosMujeres")
    )\
    .orderBy(
        f.desc("extranjerosMujeres"),
        f.desc("extranjerosHombres")
    )\
    .show(10,False)
padronParticionado.unpersist()

df = padron\
    .select(
        f.col("DESC_BARRIO"),
        f.col("DESC_DISTRITO"),
        f.col("ESPANOLESHOMBRES")
    )\
    .groupBy(
        f.col("DESC_BARRIO"),
        f.col("DESC_DISTRITO")
    ) \
    .agg(f.sum(f.col("espanolesHombres")).alias("espanolesHombres"))

padronJoin = df.join(
    padron,
    on = ((padron.DESC_BARRIO == df.DESC_BARRIO) & (padron.DESC_DISTRITO == df.DESC_DISTRITO)),
    how = "inner")
padronJoin.show(5,False)

from pyspark.sql import Window

padronWindow = padron\
    .withColumn(
        "TotalEspHom",
        f.sum(f.col("espanoleshombres"))
            .over(Window.partitionBy("DESC_DISTRITO", "DESC_BARRIO"))
    )
padronWindow.show(20,False)

distritos = ["BARAJAS","CENTRO","RETIRO"]
padronPivotado = padron\
    .groupBy("COD_EDAD_INT")\
    .pivot("DESC_DISTRITO", distritos)\
    .sum("espanolesMujeres")\
    .orderBy(padron.COD_EDAD_INT)
padronPivotado.show(20,False)

padronPercent = padronPivotado\
    .withColumn(
        "PORCENTAJE_BARAJAS",
        f.round(f.col("BARAJAS")/(f.col("BARAJAS")+f.col("CENTRO")+f.col("RETIRO"))*100,2)
    )\
    .withColumn(
        "PORCENTAJE_CENTRO",
        f.round(f.col("CENTRO")/(f.col("BARAJAS")+f.col("CENTRO")+f.col("RETIRO"))*100,2)
    )\
    .withColumn(
        "PORCENTAJE_RETIRO",
        f.round(f.col("RETIRO")/(f.col("BARAJAS")+f.col("CENTRO")+f.col("RETIRO"))*100,2)
    )

padronPercent.show(20,False)

padron.write\
    .format("csv")\
    .option("header","true")\
    .mode("overwrite")\
    .partitionBy("desc_distrito","desc_barrio")\
    .save("C:/Bosonit/GIT/bosonit/PadronPy/tablas_padron/padronParticionadoCsv/")

padron.write\
    .format("parquet")\
    .option("header","true")\
    .mode("overwrite")\
    .partitionBy("desc_distrito","desc_barrio")\
    .save("C:/Bosonit/GIT/bosonit/PadronPy/tablas_padron/padronParticionadoParquet/")