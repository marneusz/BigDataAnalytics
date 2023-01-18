DROP TABLE IF EXISTS crypto_avro;
CREATE TABLE crypto_avro STORED AS AVRO as SELECT * FROM crypto_table;



-- creating dynamically partitioned table
-- settings


SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode=non-strict;
SET hive.exec.max.dynamic.partitions=2000;
SET hive.exec.max.dynamic.partitions.pernode=500;


DROP TABLE IF EXISTS crypto_avro_part_table;
CREATE EXTERNAL TABLE crypto_avro_part_table(`date` string, price double, cryptocurrency string, day int, hour int)
PARTITIONED BY (year int, month int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/bda_reddit_pw/historical_crypto_processed/crypto_table';

INSERT INTO TABLE crypto_avro_part_table PARTITION(year, month)
SELECT `date`,price,cryptocurrency,day,hour,year,month FROM crypto_avro;
