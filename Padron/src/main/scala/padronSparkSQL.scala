import org.apache.spark.sql.SparkSession

object padronSparkSQL {
    def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("Padron")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val padronCsv = "C:/Bosonit/GIT/bosonit/Padron/datos_padron/datos_padron.csv"


    spark.sql("create database datos_padron")
    spark.sql("use datos_padron")

    spark.sql("""CREATE TABLE padron_csv
                |    USING csv
                |    OPTIONS (
                |      path "C:/Bosonit/GIT/bosonit/Padron/datos_padron/datos_padron.csv",
                |      header "true",
                |      inferSchema "true",
                |      delimiter "\073",
                |      emptyValue 0,
                |      mode "FAILFAST"
                |    )""".stripMargin)

    spark.sql(
        """CREATE TABLE padron_txt USING csv AS
          |SELECT CAST(COD_DISTRITO  AS INT),
          |       DESC_DISTRITO,
          |       CAST(COD_DIST_BARRIO AS INT),
          |       DESC_BARRIO,
          |       CAST(COD_BARRIO AS INT),
          |       CAST(COD_DIST_SECCION AS INT),
          |       CAST(COD_SECCION AS INT),
          |       CAST(COD_EDAD_INT AS INT),
          |       CAST(EspanolesHombres AS INT),
          |       CAST(EspanolesMujeres AS INT),
          |       CAST(ExtranjerosHombres AS INT),
          |       CAST(ExtranjerosMujeres AS INT)
          |       FROM padron_csv""".stripMargin)
    spark.sql("""select * from padron_txt limit 10""").show()
    spark.sql(
        """CREATE TABLE padron_txt_2 USING csv AS
          |SELECT CAST(COD_DISTRITO  AS INT),
          |       TRIM(DESC_DISTRITO) AS DESC_DISTRITO,
          |       CAST(COD_DIST_BARRIO AS INT),
          |       TRIM(DESC_BARRIO) AS DESC_BARRIO,
          |       CAST(COD_BARRIO AS INT),
          |       CAST(COD_DIST_SECCION AS INT),
          |       CAST(COD_SECCION AS INT),
          |       CAST(COD_EDAD_INT AS INT),
          |       CAST(EspanolesHombres AS INT),
          |       CAST(EspanolesMujeres AS INT),
          |       CAST(ExtranjerosHombres AS INT),
          |       CAST(ExtranjerosMujeres AS INT)
          |       FROM padron_csv""".stripMargin)
        spark.sql("""select * from padron_txt_2 limit 10""").show()

        spark.sql(
            """CREATE TABLE padron_parquet USING parquet AS
              |SELECT * FROM padron_txt""".stripMargin)
        spark.sql(
            """CREATE TABLE padron_parquet_2 USING parquet AS
              |SELECT * FROM padron_txt_2""".stripMargin)

        spark.sql("""CREATE TABLE IF NOT EXISTS padron_particionado(
                    |    COD_DISTRITO INT,
                    |    COD_DIST_BARRIO INT,
                    |    COD_BARRIO INT,
                    |    COD_DIST_SECCION INT,
                    |    COD_SECCION INT,
                    |    COD_EDAD_INT INT,
                    |    EspanolesHombres INT,
                    |    EspanolesMujeres INT,
                    |    ExtranjerosHombres INT,
                    |    ExtranjerosMujeres INT,
                    |    DESC_DISTRITO STRING,
                    |    DESC_BARRIO STRING)
                    |USING parquet
                    |
                    |PARTITIONED BY(DESC_DISTRITO,DESC_BARRIO)
                    |""".stripMargin)

        spark.sql("""FROM padron_parquet_2
                    | INSERT OVERWRITE TABLE datos_padron.padron_particionado partition (desc_distrito, desc_barrio)
                    | SELECT CAST(cod_distrito AS INT), CAST(cod_dist_barrio AS INT), CAST(cod_barrio AS INT),
                    | CAST(cod_dist_seccion AS INT), CAST(cod_seccion AS INT), CAST(cod_edad_int AS INT),
                    | CAST(espanoleshombres AS INT), CAST(espanolesmujeres AS INT), CAST(extranjeroshombres AS INT),
                    | CAST(extranjerosmujeres AS INT), desc_distrito, desc_barrio""".stripMargin)
        spark.sql("""show partitions padron_particionado""").show()

        spark.sql("""SELECT  DESC_DISTRITO,DESC_BARRIO,SUM(EspanolesHombres) AS total_EspanolesHombres,
                    |        SUM(EspanolesMujeres) AS total_EspanolesMujeres,
                    |        SUM(ExtranjerosHombres) AS total_ExtranjerosHombres,
                    |        SUM(ExtranjerosMujeres) AS total_ExtranjerosMujeres
                    |FROM padron_particionado
                    |GROUP BY DESC_DISTRITO,DESC_BARRIO""".stripMargin).show()
        spark.sql("""SELECT  DESC_DISTRITO,DESC_BARRIO,SUM(EspanolesHombres) AS total_EspanolesHombres,
                    |        SUM(EspanolesMujeres) AS total_EspanolesMujeres,
                    |        SUM(ExtranjerosHombres) AS total_ExtranjerosHombres,
                    |        SUM(ExtranjerosMujeres) AS total_ExtranjerosMujeres FROM padron_particionado
                    |WHERE DESC_DISTRITO IN('CENTRO', 'LATINA', 'CHAMARTIN', 'TETUAN', 'VICALVARO', 'BARAJAS')
                    |GROUP BY DESC_DISTRITO,DESC_BARRIO""".stripMargin).show()
        spark.sql("""SELECT  DESC_DISTRITO,DESC_BARRIO,SUM(EspanolesHombres) AS total_EspanolesHombres,
                    |        SUM(EspanolesMujeres) AS total_EspanolesMujeres,
                    |        SUM(ExtranjerosHombres) AS total_ExtranjerosHombres,
                    |        SUM(ExtranjerosMujeres) AS total_ExtranjerosMujeres
                    |FROM padron_parquet
                    |WHERE DESC_DISTRITO IN('CENTRO', 'LATINA', 'CHAMARTIN', 'TETUAN', 'VICALVARO', 'BARAJAS')
                    |GROUP BY DESC_DISTRITO,DESC_BARRIO""".stripMargin).show()
        spark.sql("""SELECT  DESC_DISTRITO,DESC_BARRIO,AVG(EspanolesHombres) AS media_esp_hombres,
                    |        MAX(EspanolesMujeres) AS max_esp_mujeres,
                    |        MIN(ExtranjerosHombres) AS min_ex_hombres,
                    |        COUNT(ExtranjerosMujeres) AS n_ex_mujeres FROM padron_particionado
                    |WHERE DESC_DISTRITO IN('CENTRO', 'LATINA', 'CHAMARTIN', 'TETUAN', 'VICALVARO', 'BARAJAS')
                    |GROUP BY DESC_DISTRITO,DESC_BARRIO""".stripMargin).show()
        spark.sql("""SELECT  DESC_DISTRITO,DESC_BARRIO,AVG(EspanolesHombres) AS media_esp_hombres,
                    |        MAX(EspanolesMujeres) AS max_esp_mujeres,
                    |        MIN(ExtranjerosHombres) AS min_ex_hombres,
                    |        COUNT(ExtranjerosMujeres) AS n_ex_mujeres FROM padron_txt_2
                    |WHERE DESC_DISTRITO IN('CENTRO', 'LATINA', 'CHAMARTIN', 'TETUAN', 'VICALVARO', 'BARAJAS')
                    |GROUP BY DESC_DISTRITO,DESC_BARRIO""".stripMargin).show()
        spark.sql("""SELECT  DESC_DISTRITO,DESC_BARRIO,AVG(EspanolesHombres) AS media_esp_hombres,
                    |        MAX(EspanolesMujeres) AS max_esp_mujeres,
                    |        MIN(ExtranjerosHombres) AS min_ex_hombres,
                    |        COUNT(ExtranjerosMujeres) AS n_ex_mujeres FROM padron_parquet_2
                    |WHERE DESC_DISTRITO IN('CENTRO', 'LATINA', 'CHAMARTIN', 'TETUAN', 'VICALVARO', 'BARAJAS')
                    |GROUP BY DESC_DISTRITO,DESC_BARRIO""".stripMargin).show()
    }
}