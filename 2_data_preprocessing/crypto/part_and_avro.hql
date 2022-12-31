DROP TABLE IF EXISTS crypto_avro;
CREATE TABLE crypto_avro STORED AS AVRO as SELECT * FROM crypto_table;



-- creating dynamically partitioned table
-- settings


SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode=non-strict;
SET hive.exec.max.dynamic.partitions=2000;
SET hive.exec.max.dynamic.partitions.pernode=500;


DROP TABLE IF EXISTS crypto_part;
CREATE EXTERNAL TABLE crypto_part(`date` string, price double, cryptocurrency string, day int, hour int)
PARTITIONED BY (year int, month int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/bda_reddit_pw/historical_crypto_processed/crypto_part';

INSERT INTO TABLE crypto_part PARTITION(year, month) SELECT `date`, price, cryptocurrency, day, year, month, hour FROM crypto_avro;
