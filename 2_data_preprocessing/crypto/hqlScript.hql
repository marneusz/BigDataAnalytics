DROP TABLE IF EXISTS crypto_table;
CREATE EXTERNAL TABLE crypto_table(`date` string, price double, cryptocurrency string, `year` string, `month` string, `day` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION '/user/bda_reddit_pw/historical_crypto_processed/';


-- loading data

LOAD DATA INPATH '/user/bda_reddit_pw/historical_crypto_processed/crypto_historical_data.csv' INTO TABLE crypto.crypto_historical;
