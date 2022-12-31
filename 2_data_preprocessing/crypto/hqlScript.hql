add jar hdfs:///user/hive/lib/json-serde-1.3.8-jar-with-dependencies.jar;
add jar hdfs:///user/hive/lib/json-udf-1.3.8-jar-with-dependencies.jar;
add jar hdfs:///user/hive/lib/hive-hcatalog-core-3.1.2.jar;

DROP TABLE IF EXISTS crypto_table;
CREATE EXTERNAL TABLE crypto_table(`date` string, price double, cryptocurrency string, `year` int, `month` int, `day` int, `hour` int)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/user/bda_reddit_pw/historical_crypto_processed/';


-- loading data

LOAD DATA INPATH '/user/bda_reddit_pw/historical_crypto_processed/historical_crypto_data_processed.json' INTO TABLE crypto_table;
