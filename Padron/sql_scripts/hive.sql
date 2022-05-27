drop database if exists datos_padron cascade;
create database datos_padron;
use datos_padron;
DROP TABLE padron_txt_raw;
CREATE TABLE IF NOT EXISTS padron_txt_raw(
cod_distrito INT,
desc_distrito STRING,
cod_dist_barrio INT,
desc_barrio STRING,
cod_barrio INT,
cod_dist_seccion INT,
cod_seccion INT,
cod_edad_int INT,
EspanolesHombres INT,
EspanolesMujeres INT,
ExtranjerosHombres INT,
ExtranjerosMujeres INT ) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ('separatorChar' = '\073',"serialization.encoding"='UTF-8')
STORED AS TEXTFILE TBLPROPERTIES ('store.charset'='UTF-8', 'retrieve.charset'='UTF-8', 'skip.header.line.count'='1','serialization.null.format'="");

LOAD DATA LOCAL INPATH '/cloudera-files/datos_padron/datos_padron.csv' INTO TABLE padron_txt_raw;
select desc_distrito,desc_barrio from padron_txt_raw group by desc_distrito,desc_barrio;

DROP TABLE padron_txt_sucio;
CREATE TABLE padron_txt_sucio TBLPROPERTIES ('store.charset'='UTF-8', 'retrieve.charset'='UTF-8')AS
SELECT COD_DISTRITO,
       regexp_replace(DESC_DISTRITO,'\\P{ASCII}','N') AS DESC_DISTRITO,
       COD_DIST_BARRIO,
       regexp_replace(DESC_BARRIO,'\\P{ASCII}','N') AS DESC_BARRIO,
       COD_BARRIO,
       COD_DIST_SECCION,
       COD_SECCION,
       COD_EDAD_INT,
       CASE WHEN LENGTH(EspanolesHombres)==0 THEN '0'
            ELSE EspanolesHombres
       END AS EspanolesHombres,
       CASE WHEN LENGTH(EspanolesMujeres)==0 THEN '0'
            ELSE EspanolesMujeres
       END AS EspanolesMujeres,
       CASE WHEN LENGTH(ExtranjerosHombres)==0 THEN '0'
            ELSE ExtranjerosHombres
       END AS ExtranjerosHombres,
       CASE WHEN LENGTH(ExtranjerosMujeres)==0 THEN '0'
            ELSE ExtranjerosMujeres
       END AS ExtranjerosMujeres
       FROM padron_txt_raw;
DROP TABLE padron_txt_raw;
select desc_distrito,desc_barrio from padron_txt_sucio group by desc_distrito,desc_barrio;

DROP TABLE padron_txt;
CREATE TABLE padron_txt STORED AS TEXTFILE AS
SELECT CAST(COD_DISTRITO  AS INT),
       DESC_DISTRITO,
       CAST(COD_DIST_BARRIO AS INT),
       DESC_BARRIO,
       CAST(COD_BARRIO AS INT),
       CAST(COD_DIST_SECCION AS INT),
       CAST(COD_SECCION AS INT),
       CAST(COD_EDAD_INT AS INT),
       CAST(EspanolesHombres AS INT),
       CAST(EspanolesMujeres AS INT),
       CAST(ExtranjerosHombres AS INT),
       CAST(ExtranjerosMujeres AS INT)
       FROM padron_txt_sucio;
select desc_distrito,desc_barrio from padron_txt group by desc_distrito,desc_barrio;

DROP TABLE padron_txt_2;
CREATE TABLE padron_txt_2 STORED AS TEXTFILE TBLPROPERTIES ('store.charset'='UTF-8', 'retrieve.charset'='UTF-8')AS
SELECT CAST(COD_DISTRITO  AS INT),
       TRIM(DESC_DISTRITO) AS DESC_DISTRITO,
       CAST(COD_DIST_BARRIO AS INT),
       TRIM(DESC_BARRIO) AS DESC_BARRIO,
       CAST(COD_BARRIO AS INT),
       CAST(COD_DIST_SECCION AS INT),
       CAST(COD_SECCION AS INT),
       CAST(COD_EDAD_INT AS INT),
       CAST(EspanolesHombres AS INT),
       CAST(EspanolesMujeres AS INT),
       CAST(ExtranjerosHombres AS INT),
       CAST(ExtranjerosMujeres AS INT)
       FROM padron_txt_sucio;
select desc_distrito,desc_barrio from padron_txt_2 group by desc_distrito,desc_barrio;

DROP TABLE padron_txt_limpio;
CREATE TABLE IF NOT EXISTS padron_txt_limpio
(
cod_distrito INT,
desc_distrito STRING,
cod_dist_barrio INT,
desc_barrio STRING,
cod_barrio INT,
cod_dist_seccion INT,
cod_seccion INT,
cod_edad_int INT,
EspanolesHombres INT,
EspanolesMujeres INT,
ExtranjerosHombres INT,
ExtranjerosMujeres INT )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES ('input.regex'='"(.*)"\073"([A-Za-z.\\P{ASCII}-]+(?: [A-Za-z.\\P{ASCII}-]+)*) *"\073"(.*)"\073"([A-Za-z.\\P{ASCII}-]+(?: [A-Za-z.\\P{ASCII}-]+)*) *"\073"(.*)"\073"(.*?)"\073"(.*?)"\073"(.*?)"\073"(.*?)"\073"(.*?)"\073"(.*?)"\073"(.*?)"',"serialization.encoding"='UTF-8')
STORED AS TEXTFILE TBLPROPERTIES ('store.charset'='UTF-8', 'retrieve.charset'='UTF-8', 'skip.header.line.count'='1','serialization.null.format'="");
LOAD DATA LOCAL INPATH '/cloudera-files/datos_padron/datos_padron.csv' INTO TABLE padron_txt_limpio;
DROP TABLE padron_txt_def;
CREATE TABLE padron_txt_def AS
SELECT COD_DISTRITO,
       regexp_replace(DESC_DISTRITO,'\\P{ASCII}','N') AS DESC_DISTRITO,
       COD_DIST_BARRIO,
       regexp_replace(DESC_BARRIO,'\\P{ASCII}','N') AS DESC_BARRIO,
       COD_BARRIO,
       COD_DIST_SECCION,
       COD_SECCION,
       COD_EDAD_INT,
       nvl(EspanolesHombres,0) AS EspanolesHombres,
       nvl(EspanolesMujeres,0) AS EspanolesMujeres,
       nvl(ExtranjerosHombres,0) AS ExtranjerosHombres,
       nvl(ExtranjerosMujeres,0) AS ExtranjerosMujeres
       FROM padron_txt_limpio;
select desc_distrito,desc_barrio from padron_txt_limpio group by desc_distrito,desc_barrio;
select desc_distrito,desc_barrio from padron_txt_def group by desc_distrito,desc_barrio;