
DROP TABLE padron_parquet;
CREATE TABLE padron_parquet STORED AS PARQUET AS
SELECT * FROM padron_txt;

DROP TABLE padron_parquet_2;
CREATE TABLE padron_parquet_2 STORED AS PARQUET AS
SELECT * FROM padron_txt_2;

DROP TABLE padron_parquet_def;
CREATE TABLE padron_parquet_def STORED AS PARQUET AS
SELECT * FROM padron_txt_def;

SELECT * FROM padron_parquet_2 limit 10;
SELECT * FROM padron_parquet_def limit 10;
select desc_distrito,desc_barrio from padron_parquet_def group by desc_distrito,desc_barrio;
