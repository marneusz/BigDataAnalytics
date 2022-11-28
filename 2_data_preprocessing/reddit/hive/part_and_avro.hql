add jar hdfs:///user/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;
add jar hdfs:///user/hive/lib/json-udf-1.3.8-jar-with-dependencies.jar;


DROP TABLE IF EXISTS rs_avro;
CREATE TABLE rs_avro STORED AS AVRO as SELECT * FROM test_rs;



-- creating dynamically partitioned table
-- settings
SET hive.execution.engine=tez;
SET hive.exec.dynamic.partition.mode=non-strict;

DROP TABLE IF EXISTS rs_part;
CREATE EXTERNAL TABLE rs_part (subreddit string, title string, selftext string, created_utc float, day string )
PARTITIONED BY (year string, month string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/bda_reddit_pw/historical_reddit_processed/rs_part';

INSERT INTO TABLE rs_part PARTITION(year, month)
SELECT subreddit, title, selftext, created_utc, day, year, month 
FROM test_rs;
