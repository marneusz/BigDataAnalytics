DROP TABLE IF EXISTS crypto_table;
CREATE EXTERNAL TABLE crypto_table(`date` string, price double, cryptocurrency string, `year` int, `month` int, `day` int, `hour` int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/bda_reddit_pw/historical_crypto_processed/table/';

-- loading data

LOAD DATA INPATH '/user/bda_reddit_pw/historical_crypto_processed/table/historical_crypto_data_processed.csv' INTO TABLE crypto_table;
