
SELECT  DESC_DISTRITO,DESC_BARRIO,SUM(EspanolesHombres) AS total_EspanolesHombres,
        SUM(EspanolesMujeres) AS total_EspanolesMujeres,
        SUM(ExtranjerosHombres) AS total_ExtranjerosHombres,
        SUM(ExtranjerosMujeres) AS total_ExtranjerosMujeres
FROM padron_parquet_2
WHERE DESC_DISTRITO IN('CENTRO', 'LATINA', 'CHAMARTIN', 'TETUAN', 'VICALVARO', 'BARAJAS')
GROUP BY DESC_DISTRITO,DESC_BARRIO;
 -- hive 17.36
 -- impala 6.16

SELECT  DESC_DISTRITO,DESC_BARRIO,SUM(EspanolesHombres) AS total_EspanolesHombres,
        SUM(EspanolesMujeres) AS total_EspanolesMujeres,
        SUM(ExtranjerosHombres) AS total_ExtranjerosHombres,
        SUM(ExtranjerosMujeres) AS total_ExtranjerosMujeres FROM padron_particionado
WHERE DESC_DISTRITO IN('CENTRO', 'LATINA', 'CHAMARTIN', 'TETUAN', 'VICALVARO', 'BARAJAS')
GROUP BY DESC_DISTRITO,DESC_BARRIO;
-- hive 16.056
-- impala 6.55

-- source /cloudera-files/sql_scripts/test1.sql;