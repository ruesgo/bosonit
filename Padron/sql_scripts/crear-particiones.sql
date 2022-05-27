
DROP TABLE padron_particionado;
CREATE TABLE IF NOT EXISTS padron_particionado(
    COD_DISTRITO INT,
    COD_DIST_BARRIO INT,
    COD_BARRIO INT,
    COD_DIST_SECCION INT,
    COD_SECCION INT,
    COD_EDAD_INT INT,
    EspanolesHombres INT,
    EspanolesMujeres INT,
    ExtranjerosHombres INT,
    ExtranjerosMujeres INT)
PARTITIONED BY(DESC_DISTRITO STRING,DESC_BARRIO STRING)
STORED AS PARQUET;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=non-strict;
SET hive.exec.max.dynamic.partitions = 10000;
SET hive.exec.max.dynamic.partitions.pernode = 1000;
SET mapreduce.map.memory.mb = 2048;
SET mapreduce.reduce.memory.mb = 2048;
SET mapreduce.map.java.opts=-Xmx1800m;


 FROM padron_parquet_def
 INSERT INTO TABLE padron_particionado partition (desc_distrito, desc_barrio)
 SELECT CAST(cod_distrito AS INT), CAST(cod_dist_barrio AS INT), CAST(cod_barrio AS INT),
 CAST(cod_dist_seccion AS INT), CAST(cod_seccion AS INT), CAST(cod_edad_int AS INT),
 CAST(espanoleshombres AS INT), CAST(espanolesmujeres AS INT), CAST(extranjeroshombres AS INT),
 CAST(extranjerosmujeres AS INT), desc_distrito, desc_barrio;

show partitions padron_particionado;


