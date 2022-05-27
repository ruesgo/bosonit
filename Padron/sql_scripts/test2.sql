SELECT  DESC_DISTRITO,DESC_BARRIO,AVG(EspanolesHombres) AS media_esp_hombres,
        MAX(EspanolesMujeres) AS max_esp_mujeres,
        MIN(ExtranjerosHombres) AS min_ex_hombres,
        COUNT(ExtranjerosMujeres) AS n_ex_mujeres FROM padron_particionado
WHERE DESC_DISTRITO IN('CENTRO', 'LATINA', 'CHAMARTIN', 'TETUAN', 'VICALVARO', 'BARAJAS')
GROUP BY DESC_DISTRITO,DESC_BARRIO;
-- hive 16.046
-- impala 0.64

SELECT  DESC_DISTRITO,DESC_BARRIO,AVG(EspanolesHombres) AS media_esp_hombres,
        MAX(EspanolesMujeres) AS max_esp_mujeres,
        MIN(ExtranjerosHombres) AS min_ex_hombres,
        COUNT(ExtranjerosMujeres) AS n_ex_mujeres FROM padron_txt_2
WHERE DESC_DISTRITO IN('CENTRO', 'LATINA', 'CHAMARTIN', 'TETUAN', 'VICALVARO', 'BARAJAS')
GROUP BY DESC_DISTRITO,DESC_BARRIO;
-- hive 15.847
-- impala 5.35

SELECT  DESC_DISTRITO,DESC_BARRIO,AVG(EspanolesHombres) AS media_esp_hombres,
        MAX(EspanolesMujeres) AS max_esp_mujeres,
        MIN(ExtranjerosHombres) AS min_ex_hombres,
        COUNT(ExtranjerosMujeres) AS n_ex_mujeres FROM padron_parquet_2
WHERE DESC_DISTRITO IN('CENTRO', 'LATINA', 'CHAMARTIN', 'TETUAN', 'VICALVARO', 'BARAJAS')
GROUP BY DESC_DISTRITO,DESC_BARRIO;
-- hive 15.889
-- impala 0.33

-- source /cloudera-files/sql_scripts/test2.sql;