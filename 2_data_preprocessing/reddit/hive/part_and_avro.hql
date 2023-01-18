--add jar hdfs:///user/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;
--add jar hdfs:///user/hive/lib/json-udf-1.3.8-jar-with-dependencies.jar;


-- avro

DROP TABLE IF EXISTS rs_avro;
CREATE TABLE rs_avro STORED AS AVRO as SELECT * FROM reddit_table;


-- creating dynamically partitioned table
-- settings
SET hive.execution.engine=tez;
SET hive.exec.dynamic.partition.mode=non-strict;

DROP TABLE IF EXISTS reddit_avro_part_table;
CREATE EXTERNAL TABLE reddit_avro_part_table (selftext string, created_utc float, cryptocurrency string, day int, hour int )
PARTITIONED BY (year int, month int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/bda_reddit_pw/historical_reddit_processed/reddit_table';

INSERT INTO TABLE reddit_avro_part_table PARTITION(year, month)
SELECT selftext, created_utc, cryptocurrency, day, hour, year, month 
FROM rs_avro;


