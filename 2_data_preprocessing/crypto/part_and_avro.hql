add jar hdfs:///user/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;
add jar hdfs:///user/hive/lib/json-udf-1.3.8-jar-with-dependencies.jar;


DROP TABLE IF EXISTS crypto.crypto_avro;
CREATE TABLE crypto.crypto_avro STORED AS AVRO as SELECT * FROM crypto.cryptotable;



-- creating dynamically partitioned table
-- settings
SET hive.execution.engine=tez;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode=non-strict;
SET hive.exec.max.dynamic.partitions=2000;
SET hive.exec.max.dynamic.partitions.pernode=500;

DROP TABLE IF EXISTS crypto.crypto_part;
CREATE EXTERNAL TABLE crypto.crypto_part(`date` string, price double, subreddit string, day string)
PARTITIONED BY (year string, month string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/bda_reddit_pw/historical_crypto_processed/crypto_part';

INSERT INTO TABLE crypto.crypto_part PARTITION(year, month)
SELECT `date`, price, subreddit, day, year, month
FROM crypto.cryptotable;

